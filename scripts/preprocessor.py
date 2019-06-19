#!/usr/bin/python3

import pandas as pd
import gc
import json
import os
import argparse
from hashlib import sha256
from utils import *

attributes_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../smpc-global/', 'attributes.json')
with open(attributes_file) as attribute_file:
    available_attribute_dicts = json.load(attribute_file)
available_attributes = [Attribute["name"] for Attribute in available_attribute_dicts]


def preprocess(
  computation_request,
  computation_request_id,
  attributes,
  data_file_name,
  mapping_file_name,
  mesh_codes_to_ids_file,
  mesh_ids_to_codes_file,
  mesh_terms_inverted_file,
  decimal_accuracy=5,
  filters=None):
  ''' Function to apply preprocessing to dataset.
  computation_request: 'str', one of '1d_categorical_histogram', '2d_categorical_histogram', '1d_numerical_histogram', '2d_numerical_histogram', '2d_mixed_histogram', 'secure_aggregation',
  computation_request_id: 'str', Unique id of computation request,
  attributes: 'list', contains attributes that will be used for the computation,
  data_file_name: 'str', path + file_name where data is located,
  filters: 'dict', maps attributes to filter functions that we want to apply,
  decimal_accuracy: 'int', how many decimal digits to consider for floats
  '''

  datasets_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../datasets/')
  if (not os.path.exists(datasets_directory)):
      os.mkdir(datasets_directory)

  output_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../datasets/', computation_request_id)
  if (not os.path.exists(output_directory)):
      os.mkdir(output_directory)

  if computation_request in ['1d_numerical_histogram', '2d_numerical_histogram']:
    data = load_dataset(data_file_name).head(5)
    uniquely_map_data_attribute_names_to_codes(data)
    print(data.head(5))
    mesh_codes_to_ids, mesh_ids_to_codes_file, attributeToValueMap = \
    load_all_json_files(mesh_codes_to_ids_file, mesh_ids_to_codes_file, mapping_file_name)

    attributes = [mesh_ids_to_codes_file[attribute]['code'] for attribute in attributes]
    assert set(attributes) <= set(data.dtypes.keys()), 'Some requested attribute is not available'

    attribute_type_map = create_attribute_type_map(data, attributes)

    attributes = sorted(attributes, key=sort_attributes(attribute_type_map))
    attribute_type_map.clear()
    attribute_ids = {}
    for attribute in attributes:
        attribute_ids[attribute] = mesh_codes_to_ids[attribute]["id"]

    attribute_type_map = create_attribute_type_map(data, attribute_ids)
    
    assert decimal_accuracy > 0, "Decimal accuracy must be positive"
    assert decimal_accuracy <= 10, "Maximal supported decimal accuracy is 10 digits"
    assert (filters is None) or (isinstance(filters, dict)), "Input 'filters' must be a dictionary or None"

    if isinstance(filters, dict):
        assert set(
            filters.keys()) <= set(
            data.columns), "Input 'filters' keys must be data attributes"
    
    if filters is None:
        dataset = data[attributes]
        dataset = dataset.dropna(axis = 0)
    else:
        valid_indices = []
        for i in range(data.shape[0]):
          boolean = True
          for attribute_, filter_ in filters.items():
              boolean = boolean and filter_(data[attribute_].iloc[i])
          if boolean:
              valid_indices.append(i)
        dataset = data.iloc[valid_indices]
        dataset = dataset[attributes]
        dataset = dataset.dropna(axis = 0)

    # this sorting mechanism ensures cat -> integer -> float
    attributes = sorted(attribute_ids.values(), key=sort_attributes(attribute_type_map))

    delete_cols = set(data.columns) - set(attributes)
    data = data.drop(list(delete_cols), axis=1)
    dataset_size = dataset.shape[0]
    sizeAlloc = 0

    attributeToInt, intToAttribute = {}, {}
    cellsX, cellsY = None, None

    for i, attribute in enumerate(attribute_ids.values()):
        attributeToInt[attribute] = i
        intToAttribute[i] = attribute
        if attribute_type_map[attribute] == 'Categorical':
            sizeAlloc += dataset_size
        else:
            sizeAlloc += 2 * dataset_size

    gc.collect()

    if computation_request == '2d_mixed_histogram':
        assert len(
        attributes) == 2, "Need 2 attributes for a '2d_mixed_histogram' computation request"
        assert (attribute_type_map[attributes[0]] == 'Categorical') or (attribute_type_map[attributes[1]]
                                                                        == 'Categorical'), "Need at least one categorical attribute for '2d-mixed computation request'"
        assert (attribute_type_map[attributes[0]] != 'Categorical') or (attribute_type_map[attributes[1]] !=
                                                                        'Categorical'), "Need at least one non-categorical attribute for '2d-mixed computation request'"
        output = mixed_preprocess(
            dataset,
            list(attribute_ids.keys()),
            attribute_type_map,
            decimal_accuracy,
            attributeToValueMap,
            attribute_ids,
            mesh_codes_to_ids)
        cellsX = len(attributeToValueMap[intToAttribute[0]])

    elif computation_request == '1d_categorical_histogram':
        assert len(
        attributes) == 1, "Need 1 attribute for a '1d_categorical_histogram' computation request"
        assert (attribute_type_map[attributes[0]] ==
        'Categorical'), "Need a categorical attribute for '1d_categorical_histogram'"
        output = categorical_1d(
        dataset,
        list(attribute_ids.keys()),
        attribute_type_map,
        attributeToValueMap,
        attribute_ids,
        mesh_codes_to_ids)
        cellsX = len(attributeToValueMap[intToAttribute[0]])

    elif computation_request == '1d_numerical_histogram':
        output = numerical_1d(
            dataset,
            list(attribute_ids.keys()),
            attribute_type_map,
            decimal_accuracy,
            attribute_ids)

    elif computation_request == 'secure_aggregation':
        raise NotImplementedError

    elif computation_request == '2d_numerical_histogram':
        output = numerical_2d(
            dataset,
            list(attribute_ids.keys()),
            attribute_type_map,
            decimal_accuracy,
            attribute_ids)

    elif computation_request == '2d_categorical_histogram':
        assert len(
        attributes) == 2, "Need 2 attributes for a '2d_categorical_histogram' computation request"
        assert (attribute_type_map[attributes[0]] == 'Categorical') and (attribute_type_map[attributes[1]] ==
                                                                    'Categorical'), "Need two categorical attributes for '2d-categorical_histogram' computation request"

        cellsX = len(attributeToValueMap[intToAttribute[0]])
        cellsY = len(attributeToValueMap[intToAttribute[1]])
        output = categorical_2d(
                    dataset,
                    list(attribute_ids.keys()),
                    attribute_type_map,
                    attributeToValueMap,
                    attribute_ids,
                    mesh_codes_to_ids)

    with open(output_directory + '/' + computation_request_id + '.txt', 'w') as f:
        for item in output:
            f.write("%s\n" % item)

    with open(output_directory + '/' + computation_request_id + '.txt', "rb") as f:
        SHA256 = sha256()
        for byte_block in iter(lambda: f.read(4096), b""):
            SHA256.update(byte_block)

    json_output = {'precision': '{0:.{1}f}'.format(10**(-decimal_accuracy), decimal_accuracy),
                    'sizeAlloc': sizeAlloc,
                    'cellsX': cellsX,
                    'cellsY': cellsY,
                    'dataSize': dataset_size,
                    'hash256': SHA256.hexdigest(),
                    'attributeToInt': attributeToInt,
                    'intToAttribute': intToAttribute}

    with open(output_directory + '/' + computation_request_id + '.json', 'w') as f:
        json.dump(json_output, f, indent=4)

  elif computation_request in ['1d_categorical_histogram', '2d_categorical_histogram']:  
    # TODO:here call api
    inverse = read_json(mesh_terms_inverted_file)
    read_patients = read_json("../smpc-global/_patient.json")
    attributeToValueMap = read_json(mapping_file_name)
    ValueMaps = [attributeToValueMap[attribute] for attribute in attributes]
    
    output = [categorical_handle(read_patients, inverse, vmap) for vmap in ValueMaps]

    sizeAlloc = 0
    with open(output_directory + '/' + computation_request_id + '.txt', 'w') as f:
      if len(output) == 1:
        for item in output[0]:
          sizeAlloc += 1
          f.write("%s\n" % item)
      else:
        while 1:
          try:
            f.write("%s\n" % next(output[0]))
            f.write("%s\n" % next(output[1]))
            sizeAlloc += 2
          except StopIteration:
            break

    with open(output_directory + '/' + computation_request_id + '.txt', "rb") as f:
      SHA256 = sha256()
      for byte_block in iter(lambda: f.read(4096), b""):
        SHA256.update(byte_block)
    cellsY = 0
    try:
      cellsY = len(ValueMaps[1])
    except Exception:
      pass
    json_output = {'precision': '{0:.{1}f}'.format(10**(-decimal_accuracy), decimal_accuracy),
                    'sizeAlloc': sizeAlloc,
                    'cellsX': len(ValueMaps[0]),
                    'cellsY': cellsY,
                    'dataSize': sizeAlloc,
                    'hash256': SHA256.hexdigest(),
                    'attributeToInt': [],
                    'intToAttribute': []}

    with open(output_directory + '/' + computation_request_id + '.json', 'w') as f:
      json.dump(json_output, f, indent=4)

def main():
  parser = argparse.ArgumentParser(
      description='SMPC local drive preprocessor')
  parser.add_argument(
      '-c',
      '--computation',
      required=True,
      type=str,
      help='The ID of the computation (required)')
  parser.add_argument(
      '-d',
      '--dataset',
      required=True,
      type=str,
      help='Dataset file (required)')
  parser.add_argument(
      '-m',
      '--mapping',
      required=True,
      type=str,
      help='Mapping file (required)')
  parser.add_argument(
      '-a',
      '--attributes',
      nargs='*',
      help='List of space seperated attributes which will be included in the output file (default: all)')
  parser.add_argument(
      '-g',
      '--algorithm',
      default='1d_categorical_histogram',
      choices=[
          '2d_mixed_histogram',
          '1d_categorical_histogram',
          '1d_numerical_histogram',
          'secure_aggregation',
          '2d_numerical_histogram',
          '2d_categorical_histogram'],
      type=str,
      help='Computation algorithm (default: %(default)s)')
  parser.add_argument(
      '-p',
      '--precision',
      default=5,
      type=int,
      help='The number of decimal digits to be consider for floats (default: %(default)s)')
  parser.add_argument('--version', action='version', version='%(prog)s 0.1')
  args = parser.parse_args()
  preprocess(args.algorithm, args.computation, args.attributes, args.dataset, args.mapping, args.precision)  # , filters = {"Medical Record Number":filter1, "RVEDV (ml)": filter2})


if __name__ == '__main__':
    main()
