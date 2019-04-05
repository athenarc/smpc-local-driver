import numpy as np
import pandas as pd 
import decimal
from itertools import chain

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
  processed_attribute = []
  value_map = {}
  count = 0
  for data_instance in unprocessed_attribute.values:
    if data_instance in value_map.keys(): processed_attribute.append(value_map[data_instance]) 
    else: 
      value_map[data_instance] = count
      processed_attribute.append(value_map[data_instance]) 
      count += 1
  return processed_attribute

def numerical_float_preprocess(unprocessed_attribute, decimal_accuracy) -> list:
  processed_attribute = []
  for data_instance in unprocessed_attribute.values:
    data_instance_as_decimal = decimal.Decimal(data_instance)
    abs_exponent = abs(data_instance_as_decimal.as_tuple().exponent)
    accuracy_difference = abs_exponent-decimal_accuracy
    assert len(data_instance_as_decimal.as_tuple().digits[:-abs_exponent]) <= 10, 'Integer values are too large'

    if accuracy_difference >= 0:
      processed_attribute.append([int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[:-abs_exponent])), \
int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[-abs_exponent:-accuracy_difference]))])
    else:
      processed_attribute.append([int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[:-abs_exponent])), \
int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[-abs_exponent:] + ['0']*(-accuracy_difference)))])
  return processed_attribute

def numerical_int_preprocess(unprocessed_attribute) -> list:
  processed_attribute = []
  for data_instance in unprocessed_attribute.values:
    data_instance_as_string = str(data_instance)
    assert len(data_instance_as_string) <= 10, 'Integer values are too large'
    processed_attribute.append([data_instance,0])
  return processed_attribute



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

  if computation_request == '2d_mixed_histogram':
    assert len(attributes) == 2, "Need 2 attributes for a '2d_mixed_histogram' computation request"
    assert (attribute_type_map[attributes[0]] == 'Categorical') or (attribute_type_map[attributes[1]] == 'Categorical'), "Need at least one categorical attribute for '2d-mixed computation request'"
    assert (attribute_type_map[attributes[0]] != 'Categorical') or (attribute_type_map[attributes[1]] != 'Categorical'), "Need at least one non-categorical attribute for '2d-mixed computation request'"
    processed_data = []
    for attribute in attributes:
      considered_data = dataset[attribute]
      if attribute_type_map[attribute] == 'Categorical': 
        processed_attribute = categorical_preprocess(considered_data)      
      if attribute_type_map[attribute] == 'Numerical_int': 
        processed_attribute = numerical_int_preprocess(considered_data)  
      if attribute_type_map[attribute] == 'Numerical_float': 
        processed_attribute = numerical_float_preprocess(considered_data, decimal_accuracy)
      processed_data.append(processed_attribute)
      output = []
      for i in chain.from_iterable(zip(*processed_data)):
        if type(i) == int:
          output += [i]
        else:
          output += [*i]
  elif computation_request == '1d_categorical_histogram':
    assert len(attributes) == 1, "Need 1 attribute for a '1d_categorical_histogram' computation request"
    assert (attribute_type_map[attributes[0]] == 'Categorical'), "Need a categorical attribute for '1d_categorical_histogram'"
    considered_data = dataset[attributes[0]]
    processed_attribute = categorical_preprocess(considered_data)  
    output = processed_attribute    
  elif computation_request == '1d_numerical_histogram':
    assert len(attributes) == 1, "Need 1 attribute for a '1d_numerical_histogram' computation request"
    assert (attribute_type_map[attributes[0]] != 'Categorical'), "Need a numerical attribute for '1d_numerical_histogram'"
    attribute = attributes[0]
    considered_data = dataset[attribute]
    processed_attribute = None
    if attribute_type_map[attribute] == 'Numerical_int': 
      processed_attribute = numerical_int_preprocess(considered_data)  
    elif attribute_type_map[attribute] == 'Numerical_float': 
      processed_attribute = numerical_float_preprocess(considered_data, decimal_accuracy)
    assert processed_attribute is not None, "Attribute needs to be numerical"
    processed_data = list(processed_attribute)
    output = []
    for i in processed_data:
      output+= [*i]
  elif computation_request == 'secure_aggregation':
    raise NotImplementedError
  elif computation_request == '2d_numerical_histogram':
    assert len(attributes) == 2, "Need 2 attributes for a '2d_numerical_histogram' computation request"
    assert (attribute_type_map[attributes[0]] != 'Categorical') and (attribute_type_map[attributes[1]] != 'Categorical'), "Need two non-categorical attributes for '2d-numerical_histogram' computation request"
    processed_data = []
    for attribute in attributes:
      considered_data = dataset[attribute]    
      if attribute_type_map[attribute] == 'Numerical_int': 
        processed_attribute = numerical_int_preprocess(considered_data)  
      if attribute_type_map[attribute] == 'Numerical_float': 
        processed_attribute = numerical_float_preprocess(considered_data, decimal_accuracy)
      processed_data.append(processed_attribute)
      output = []
      for i in chain.from_iterable(zip(*processed_data)):
        if type(i) == int:
          output += [i]
        else:
          output += [*i]  
  elif computation_request == '2d_categorical_histogram':
    assert len(attributes) == 2, "Need 2 attributes for a '2d_categorical_histogram' computation request"
    assert (attribute_type_map[attributes[0]] == 'Categorical') and (attribute_type_map[attributes[1]] == 'Categorical'), "Need two categorical attributes for '2d-categorical_histogram' computation request"
    output = []
    processed_data = []
    for attribute in attributes:
      considered_data = dataset[attribute]    
      processed_attribute = categorical_preprocess(considered_data)  
      processed_data.append(processed_attribute)
    output = []
    for i in chain.from_iterable(zip(*processed_data)):
      if type(i) == int:
        output += [i]
      else:
        output += [*i] 

  with open('/home/gpik/SCALE-MAMBA/Client_data0.txt', 'w') as f:
    for item in output:
        f.write("%s\n" % item)
  return output


if __name__ == "__main__":
  computation_request_id = 'test_id'
  attribute_type_map = {"Gender": 'Categorical', "Address": 'Categorical', 'RVEDV (ml)': 'Numerical_float', 'Medical Record Number': "Numerical_int" }
  attributes = ['RVEDV (ml)']#['Medical Record Number', 'RVEDV (ml)']#'Medical Record Number','RVEDV (ml)'] #'Gender']#, 
  computation_request = '1d_numerical_histogram' #'2d_mixed_histogram'
  data = preprocess(computation_request, computation_request_id, attributes, '/home/gpik/Documents/Data/cvi_identified_small.csv', filters = {"Medical Record Number":filter1, "RVEDV (ml)": filter2})
  print(data)
