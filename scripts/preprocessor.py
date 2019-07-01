#!/usr/bin/python3

import gc
import json
import os
import argparse
from hashlib import sha256
from utils import (
    read_json,
    get_codes_from_attributes,
    load_dataset,
    create_attribute_type_map,
    sort_attributes,
    categorical_handle,
    numerical_1d,
    numerical_2d,
    handle_categorical
)


def preprocess(
    computation_request,
    computation_request_id,
    attributes,
    data_file_name,
    decimal_accuracy=5,
    filters=None
):
    ''' Function to apply preprocessing to dataset.
    computation_request: 'str', one of '1d_categorical_histogram', '2d_categorical_histogram', '1d_numerical_histogram', '2d_numerical_histogram', '2d_mixed_histogram', 'secure_aggregation',
    computation_request_id: 'str', Unique id of computation request,
    attributes: 'list', contains attributes that will be used for the computation,
    data_file_name: 'str', path + file_name where data is located,
    filters: 'dict', maps attributes to filter functions that we want to apply,
    decimal_accuracy: 'int', how many decimal digits to consider for floats
    '''

    DATASET_DIRECTORY = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../datasets/', computation_request_id)
    SMPC_GLOBAL_DIRECTORY = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../smpc-global/')

    if (not os.path.exists(DATASET_DIRECTORY)):
        os.makedirs(DATASET_DIRECTORY, exist_ok=True)

    if computation_request in ['1d_numerical_histogram', '2d_numerical_histogram']:
        data = load_dataset(data_file_name)
        data.columns = get_codes_from_attributes(data.columns)

        mesh_codes = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'meshTermsByCode.json'))
        mesh_terms = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'meshTerms.json'))
        mapping = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'mapping.json'))

        attributes = [mesh_terms[attribute]['code'] for attribute in attributes]
        assert set(attributes) <= set(data.columns), 'Some requested attribute is not available'

        attribute_type_map = create_attribute_type_map(data, attributes)

        attributes = sorted(attributes, key=sort_attributes(attribute_type_map))
        attribute_type_map.clear()
        attribute_ids = {}
        for attribute in attributes:
            attribute_ids[attribute] = mesh_codes[attribute]["id"]

        attribute_type_map = create_attribute_type_map(data, attribute_ids)

        assert decimal_accuracy > 0, "Decimal accuracy must be positive"
        assert decimal_accuracy <= 10, "Maximal supported decimal accuracy is 10 digits"
        assert (filters is None) or (isinstance(filters, dict)), "Input 'filters' must be a dictionary or None"

        if isinstance(filters, dict):
            assert set(filters.keys()) <= set(data.columns), "Input 'filters' keys must be data attributes"

        if filters is None:
            dataset = data[attributes]
            dataset = dataset.dropna(axis=0)
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
            dataset = dataset.dropna(axis=0)

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

        if computation_request == '1d_numerical_histogram':
            output = numerical_1d(
                dataset,
                list(attribute_ids.keys()),
                attribute_type_map,
                decimal_accuracy,
                attribute_ids)

        elif computation_request == '2d_numerical_histogram':
            output = numerical_2d(
                dataset,
                list(attribute_ids.keys()),
                attribute_type_map,
                decimal_accuracy,
                attribute_ids)

        with open(DATASET_DIRECTORY + '/' + computation_request_id + '.txt', 'w') as f:
            for item in output:
                f.write("%s\n" % item)

        with open(DATASET_DIRECTORY + '/' + computation_request_id + '.txt', "rb") as f:
            SHA256 = sha256()
            for byte_block in iter(lambda: f.read(4096), b""):
                SHA256.update(byte_block)

        json_output = {
            'precision': '{0:.{1}f}'.format(10**(-decimal_accuracy), decimal_accuracy),
            'sizeAlloc': sizeAlloc,
            'cellsX': cellsX,
            'cellsY': cellsY,
            'dataSize': dataset_size,
            'hash256': SHA256.hexdigest(),
            'attributeToInt': attributeToInt,
            'intToAttribute': intToAttribute
        }

        with open(DATASET_DIRECTORY + '/' + computation_request_id + '.json', 'w') as f:
            json.dump(json_output, f, indent=4)

    elif computation_request in ['1d_categorical_histogram', '2d_categorical_histogram']:
        inverse = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'meshTermsInversed.json'))
        keywords = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'keywords.json'))
        mapping = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'mapping.json'))
        requested_keyword = []
        for i in keywords:
            if i['id'] in attributes:
                requested_keyword.append(i['name'])
        if computation_request == '1d_categorical_histogram':
            assert len(requested_keyword)==1, 'Bad keywords requested'
            mapping_values = [mapping[attribute] for attribute in attributes]
            generate_values = handle_categorical(requested_keyword[0])
            output = categorical_handle(generate_values, inverse, mapping_values[0]) 

            sizeAlloc = 0
            with open(DATASET_DIRECTORY + '/' + computation_request_id + '.txt', 'w') as f:
                for item in output:
                    if item != -1:
                        sizeAlloc += 1
                        f.write("%s\n" % item)

        if computation_request == '2d_categorical_histogram':
            assert len(requested_keyword)==2, 'Bad keywords requested'
            mapping_values = [mapping[attribute] for attribute in attributes]
            generate_values = [handle_categorical(rw) for rw in requested_keyword]
            output = [categorical_handle(generate_values[ind], inverse, mapping_value) for ind, mapping_value in enumerate(mapping_values)]

            sizeAlloc = 0
            with open(DATASET_DIRECTORY + '/' + computation_request_id + '.txt', 'w') as f:
                while 1:
                    try:
                        from_attribute_0 = next(output[0])
                        from_attribute_1 = next(output[1])
                        if (from_attribute_0!=-1) and (from_attribute_1 != -1):
                            f.write("%s\n" % from_attribute_0)
                            f.write("%s\n" % from_attribute_1)
                            sizeAlloc += 2
                    except StopIteration:
                        break
        # read_patients = read_json(os.path.join(os.path.dirname(os.path.realpath(__file__)), '_patient.json'))
        
        
           

        with open(DATASET_DIRECTORY + '/' + computation_request_id + '.txt', "rb") as f:
            SHA256 = sha256()
            for byte_block in iter(lambda: f.read(4096), b""):
                SHA256.update(byte_block)

        cellsY = 0
        try:
            cellsY = len(mapping_values[1])
        except Exception:
            pass

        json_output = {
            'precision': '{0:.{1}f}'.format(10**(-decimal_accuracy), decimal_accuracy),
            'sizeAlloc': sizeAlloc,
            'cellsX': len(mapping_values[0]),
            'cellsY': cellsY,
            'dataSize': sizeAlloc,
            'hash256': SHA256.hexdigest(),
            'attributeToInt': [],
            'intToAttribute': []
        }

        with open(DATASET_DIRECTORY + '/' + computation_request_id + '.json', 'w') as f:
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
    preprocess(args.algorithm, args.computation, args.attributes, args.dataset, args.precision)  # , filters = {"Medical Record Number":filter1, "RVEDV (ml)": filter2})


if __name__ == '__main__':
    main()
