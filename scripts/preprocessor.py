import numpy as np
import pandas as pd 
import decimal
import json
from hashlib import sha256
import os
from itertools import chain
import gc

def filter1(data) -> bool:
  if data % 1 == 0: return True
  else: return False

def filter2(data) -> bool:
  if data <= 250 and data >= 10: return True
  else: return False

def sort_attributes(attribute_type_mapping):
  def sorting(type_):
    return len(attribute_type_mapping[type_])
  return sorting

def categorical_preprocess(unprocessed_attribute) -> list:
  value_map = {}
  count = 0
  for data_instance in unprocessed_attribute.values:
    if data_instance in value_map.keys(): yield value_map[data_instance]
    else: 
      value_map[data_instance] = count
      yield value_map[data_instance]
      count += 1

def numerical_float_preprocess(unprocessed_attribute, decimal_accuracy) -> list:
  for data_instance in unprocessed_attribute.values:
    data_instance_as_decimal = decimal.Decimal(data_instance)
    abs_exponent = abs(data_instance_as_decimal.as_tuple().exponent)
    accuracy_difference = abs_exponent-decimal_accuracy
    assert len(data_instance_as_decimal.as_tuple().digits[:-abs_exponent]) <= 10, 'Integer values are too large'

    if accuracy_difference >= 0:
      yield int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[:-abs_exponent]))
      yield int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[-abs_exponent:-accuracy_difference]))
    else:
      yield int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[:-abs_exponent]))
      yield int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[-abs_exponent:] + ['0']*(-accuracy_difference)))

def numerical_int_preprocess(unprocessed_attribute) -> list:
  processed_attribute = []
  for data_instance in unprocessed_attribute.values:
    data_instance_as_string = str(data_instance)
    assert len(data_instance_as_string) <= 10, 'Integer values are too large'
    yield data_instance_as_string
    yield 0

def mixed_preprocess(dataset, attributes, attribute_type_map, decimal_accuracy) -> list:
  assert len(attributes) == 2, "Need 2 attributes for a '2d_mixed_histogram' computation request"
  assert (attribute_type_map[attributes[0]] == 'Categorical') or (attribute_type_map[attributes[1]] == 'Categorical'), "Need at least one categorical attribute for '2d-mixed computation request'"
  assert (attribute_type_map[attributes[0]] != 'Categorical') or (attribute_type_map[attributes[1]] != 'Categorical'), "Need at least one non-categorical attribute for '2d-mixed computation request'"
  processed_data = []
  for attribute in attributes:
    if attribute_type_map[attribute] == 'Categorical': 
      processed_data.append(categorical_preprocess(dataset[attribute]))
    if attribute_type_map[attribute] == 'Numerical_int': 
      processed_data.append(numerical_int_preprocess(dataset[attribute]))
    if attribute_type_map[attribute] == 'Numerical_float': 
      processed_data.append(numerical_float_preprocess(dataset[attribute], decimal_accuracy))
  for i in iter(range(dataset.shape[0])):
    yield next(processed_data[0])
    yield next(processed_data[1])
    yield next(processed_data[1])

def categorical_1d(dataset, attributes, attribute_type_map) -> list:
  assert len(attributes) == 1, "Need 1 attribute for a '1d_categorical_histogram' computation request"
  assert (attribute_type_map[attributes[0]] == 'Categorical'), "Need a categorical attribute for '1d_categorical_histogram'"
  for i in categorical_preprocess(dataset[attributes[0]]): yield i
  

def numerical_1d(dataset, attributes, attribute_type_map, decimal_accuracy) -> list:
  assert len(attributes) == 1, "Need 1 attribute for a '1d_numerical_histogram' computation request"
  assert (attribute_type_map[attributes[0]] != 'Categorical'), "Need a numerical attribute for '1d_numerical_histogram'"
  if attribute_type_map[attribute] == 'Numerical_int': 
    for i in numerical_int_preprocess(dataset[attributes[0]]): yield i
  elif attribute_type_map[attribute] == 'Numerical_float': 
    for i in numerical_float_preprocess(dataset[attributes[0]], decimal_accuracy): yield i

def numerical_2d(dataset, attributes, attribute_type_map, decimal_accuracy) -> list:
  assert len(attributes) == 2, "Need 2 attributes for a '2d_numerical_histogram' computation request"
  assert (attribute_type_map[attributes[0]] != 'Categorical') and (attribute_type_map[attributes[1]] != 'Categorical'), "Need two non-categorical attributes for '2d-numerical_histogram' computation request"
  processed_data = []
  for attribute in attributes: 
    if attribute_type_map[attribute] == 'Numerical_int': 
      processed_data.append(numerical_int_preprocess(dataset[attribute]))
    if attribute_type_map[attribute] == 'Numerical_float': 
      processed_data.append(numerical_float_preprocess(dataset[attribute], decimal_accuracy))
  for i in iter(range(dataset.shape[0])):
    yield next(processed_data[0])
    yield next(processed_data[0])
    yield next(processed_data[1])
    yield next(processed_data[1])

def categorical_2d(dataset, attributes, attribute_type_map) -> list:
  assert len(attributes) == 2, "Need 2 attributes for a '2d_categorical_histogram' computation request"
  assert (attribute_type_map[attributes[0]] == 'Categorical') and (attribute_type_map[attributes[1]] == 'Categorical'), "Need two categorical attributes for '2d-categorical_histogram' computation request"
  processed_data = []
  for attribute in attributes:    
    processed_data.append(categorical_preprocess(dataset[attribute]))
  for i in iter(range(dataset.shape[0])):
    yield next(processed_data[0])
    yield next(processed_data[1])


def preprocess(computation_request, computation_request_id, attributes, data_file_name, filters = None, decimal_accuracy = 5):
  ''' Function to apply preprocessing to dataset. 
      computation_request: 'str', one of '1d_categorical_histogram', '2d_categorical_histogram', '1d_numerical_histogram', '2d_numerical_histogram', '2d_mixed_histogram', 'secure_aggregation',
      computation_request_id: 'str', Unique id of computation request,
      attributes: 'list', contains attributes that will be used for the computation,
      data_file_name: 'str', path + file_name where data is located,
      filters: 'dict', maps attributes to filter functions that we want to apply,
      decimal_accuracy: 'int', how many decimal digits to consider for floats
  '''

  data = pd.read_csv(data_file_name)
  attribute_type_map = {}
  for index, value in data.dtypes.iteritems():
    if str(value) == 'object':
      attribute_type_map[index] = 'Categorical'
    elif 'float' in str(value):
      attribute_type_map[index] = 'Numerical_float'
    elif 'int' in str(value):
      attribute_type_map[index] = 'Numerical_int'
    else:
      raise NotImplementedError

  assert type(decimal_accuracy) == int, "decimal_accuracy must be of type 'int'"
  assert type(computation_request_id) == str, "computation_request_id must be of type 'str'"
  assert type(data_file_name) == str, "data_file_name must be of type 'str'"
  assert set(attributes) <= set(data.columns), 'Some requested attribute is not available'
  assert decimal_accuracy > 0, "Decimal accuracy must be positive"
  assert decimal_accuracy <= 10, "Maximal supported decimal accuracy is 10 digits"
  assert (filters is None) or (type(filters) == dict), "Input 'filters' must be a dictionary or None"
  if type(filters) == dict:
    assert set(filters.keys()) <= set(data.columns), "Input 'filters' keys must be data attributes"

  assert computation_request in ['1d_categorical_histogram', '2d_categorical_histogram', '1d_numerical_histogram', '2d_numerical_histogram', '2d_mixed_histogram', 'secure_aggregation'], "Unknown computation request; Give one of the following types '1d_categorical_histogram', '2d_categorical_histogram', '1d_numerical_histogram', '2d_numerical_histogram', '2d_mixed_histogram', 'secure_aggregation'"
       
  # this sorting mechanism ensures cat -> integer -> float
  attributes = sorted(attributes, key = sort_attributes(attribute_type_map))  

  if filters is None:
    dataset = data[attributes]
  else:
    valid_indices = []
    for i in range(data.shape[0]):
      boolean = True
      for attribute_, filter_ in filters.items():
        boolean = boolean and filter_(data[attribute_].iloc[i])
      if boolean: valid_indices.append(i)
    dataset = data.iloc[valid_indices]
    dataset = dataset[attributes]

  delete_cols = set(data.columns) - set(attributes) 
  data = data.drop(list(delete_cols), axis = 1)
  gc.collect()
  dataset_size = dataset.shape[0]
  
  output_directory = '/home/gpik/smpc-local-driver/datasets/' + computation_request_id

  try:
    os.mkdir(output_directory)
  except Exception:
    pass

  if computation_request == '2d_mixed_histogram':
    output = mixed_preprocess(dataset, attributes, attribute_type_map, decimal_accuracy)
    
  elif computation_request == '1d_categorical_histogram':
    output = categorical_1d(dataset, attributes, attribute_type_map)

  elif computation_request == '1d_numerical_histogram':
    output = numerical_1d(dataset, attributes, attribute_type_map, decimal_accuracy)

  elif computation_request == 'secure_aggregation':
    raise NotImplementedError

  elif computation_request == '2d_numerical_histogram':
    output = numerical_2d(dataset, attributes, attribute_type_map, decimal_accuracy)

  elif computation_request == '2d_categorical_histogram':
    output = categorical_2d(dataset, attributes, attribute_type_map)

  with open(output_directory + '/' + computation_request_id + '.txt', 'w') as f:
    for item in output:
     f.write("%s\n" % item)
    
  with open(output_directory + '/' + computation_request_id + '.txt',"rb") as f:
    SHA256 = sha256()    
    for byte_block in iter(lambda: f.read(4096),b""):
      SHA256.update(byte_block)    

  json_output = {'precision': 10**(-decimal_accuracy), 'dataSize': dataset_size, 'hash256': SHA256.hexdigest() }  #
  with open(output_directory + '/' + computation_request_id + '.json', 'w') as f:
    json.dump(json_output, f)  
  
  return 1


if __name__ == "__main__":
  computation_request_id = 'test_id_1'
  attribute_type_map = {"Gender": 'Categorical', "Address": 'Categorical', 'RVEDV (ml)': 'Numerical_float', 'Medical Record Number': "Numerical_int" }
  attributes = ['Gender','Address']#, 'RVEDV (ml)']#['Medical Record Number', 'RVEDV (ml)']#'Medical Record Number','RVEDV (ml)'] #'Gender']#, 
  computation_request = '2d_categorical_histogram' #'2d_mixed_histogram'
  data = preprocess(computation_request, computation_request_id, attributes, '/home/gpik/Documents/Data/cvi_identified_small.csv')#, filters = {"Medical Record Number":filter1, "RVEDV (ml)": filter2})
