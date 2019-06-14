import decimal
import json

def sort_attributes(attribute_type_mapping):
    def sorting(type_):
        return len(attribute_type_mapping[type_])
    return sorting

def lcs(X, Y): 
  m = len(X) 
  n = len(Y) 
  T = np.zeros((n+1,m+1))
  for j in range(1,m+1): 
    for i in range(1,n+1): 
      if Y[i-1] == X[j-1]: T[i,j] = T[i-1,j-1]+1
      else: T[i,j] = max(T[i-1,j], T[i,j-1])      
  return T[n,m] 

def load_mesh_mapping_file(mesh_mapping_file_path) -> dict:
  with open(mesh_mapping_file_path, 'r') as f:
    return json.load(f)

def map_mesh_to_values(mesh_code, loaded_mesh_mapping):
  for term in loaded_mesh_mapping:
    if term['code'] == mesh_code: return term['name']

def map_values_to_mesh(value, url = "https://goldorak.hesge.ch:8082/transmesh/translate/", banned_columns=None):
  assert type(value)==str, "Value passed to 'map_values_to_mesh' must be of type <str>"
  data = {
      'term': value,
      'terminology': 'text',
      'result_format': 'json'
      }
  response = requests.post(url, data=data)
  maximum_ed = None
  mesh_code = None
  if banned_columns is None:
    for mesh_candidate in response.json(): 
      if mesh_candidate['mesh_code'].startswith("D"): current_ed = 1000
      else: current_ed = lcs(value, mesh_candidate['mesh_label'])
      if maximum_ed is None or current_ed > maximum_ed:
        maximum_ed = current_ed 
        mesh_code = mesh_candidate['mesh_code']

    if mesh_code is not None:
      return mesh_code
    else:
      return value
  else:
    for mesh_candidate in response.json(): 
      if mesh_candidate['mesh_code'] not in banned_columns:
        if mesh_candidate['mesh_code'].startswith("D"): current_ed = 1000
        else: current_ed = lcs(value, mesh_candidate['mesh_label'])
        if maximum_ed is None or current_ed > maximum_ed:
          maximum_ed = current_ed 
          mesh_code = mesh_candidate['mesh_code']

    if mesh_code is not None:
      return mesh_code
    else:
      return value

def categorical_preprocess(unprocessed_attribute, valueToIntMap) -> list:
  count = 0
  for data_instance in unprocessed_attribute.values:
    if data_instance in valueToIntMap:
      yield valueToIntMap[data_instance]
    else:
      valueToIntMap[data_instance] = count
      yield valueToIntMap[data_instance]
      count += 1

def numerical_float_preprocess(unprocessed_attribute, decimal_accuracy) -> list:
    for data_instance in unprocessed_attribute.values:
        data_instance_as_decimal = decimal.Decimal(data_instance)
        abs_exponent = abs(data_instance_as_decimal.as_tuple().exponent)
        accuracy_difference = abs_exponent - decimal_accuracy
        assert len(data_instance_as_decimal.as_tuple(
        ).digits[:-abs_exponent]) <= 10, 'Integer values are too large'

        if abs_exponent == 0:
            yield int(data_instance_as_decimal)
            yield int("".join(['0'] * (-accuracy_difference)))
        else:
            if abs(data_instance) >= 1:
                if accuracy_difference >= 0:
                    yield int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[:-abs_exponent]))
                    yield int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[-abs_exponent:-accuracy_difference]))

                else:
                    yield int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[:-abs_exponent]))
                    yield int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[-abs_exponent:]))
            else:
                yield 0
                yield int("".join(str(i) for i in data_instance_as_decimal.as_tuple().digits[0:decimal_accuracy]))


def numerical_int_preprocess(unprocessed_attribute) -> list:
  for data_instance in unprocessed_attribute.values:
    data_instance_as_string = str(data_instance)
    assert len(data_instance_as_string) <= 10, 'Integer values are too large'
    yield data_instance_as_string
    yield 0

def mixed_preprocess(dataset, attributes, attribute_type_map, decimal_accuracy, attributeToValueMap, attribute_ids) -> list:
    processed_data = []
    for attribute in attributes:
      if attribute_type_map[attribute_ids[attribute]] == 'Categorical':
        processed_data.append(
            categorical_preprocess(
                dataset[attribute],
                attributeToValueMap[attribute_ids[attribute]]))
      if attribute_type_map[attribute_ids[attribute]] == 'Numerical_int':
        processed_data.append(numerical_int_preprocess(dataset[attribute]))
      if attribute_type_map[attribute_ids[attribute]] == 'Numerical_float':
        processed_data.append(
          numerical_float_preprocess(
            dataset[attribute],
            decimal_accuracy))
    for _ in iter(range(dataset.shape[0])):
      yield next(processed_data[0])
      yield next(processed_data[1])
      yield next(processed_data[1])

def categorical_1d(dataset, attributes, attribute_type_map, attributeToValueMap, attribute_ids) -> list:
  for i in categorical_preprocess(
      dataset[attributes[0]], attributeToValueMap[attribute_ids[attributes[0]]]):
    yield i

def numerical_1d(dataset, attributes, attribute_type_map, decimal_accuracy, attribute_ids) -> list:
    assert len(
      attributes) == 1, "Need 1 attribute for a '1d_numerical_histogram' computation request"
    assert (attribute_type_map[attributes[0]] !=
          'Categorical'), "Need a numerical attribute for '1d_numerical_histogram'"
    if attribute_type_map[attributes[0]] == 'Numerical_int':
        for i in numerical_int_preprocess(dataset[attributes[0]]):
          yield i
    elif attribute_type_map[attributes[0]] == 'Numerical_float':
        for i in numerical_float_preprocess(
            dataset[attributes[0]], decimal_accuracy):
          yield i

def numerical_2d(dataset, attributes, attribute_type_map, decimal_accuracy, attribute_ids) -> list:
  assert len(
    attributes) == 2, "Need 2 attributes for a '2d_numerical_histogram' computation request"
  assert (attribute_type_map[attributes[0]] != 'Categorical') and (attribute_type_map[attributes[1]] !=
                                                                  'Categorical'), "Need two non-categorical attributes for '2d-numerical_histogram' computation request"                                                                     
      
  processed_data = []
  for attribute in attributes:
    if attribute_type_map[attribute] == 'Numerical_int':
      processed_data.append(numerical_int_preprocess(dataset[attribute]))
    if attribute_type_map[attribute] == 'Numerical_float':
      processed_data.append(
          numerical_float_preprocess(
              dataset[attribute],
              decimal_accuracy))
  for _ in iter(range(dataset.shape[0])):
    yield next(processed_data[0])
    yield next(processed_data[0])
    yield next(processed_data[1])
    yield next(processed_data[1])


def categorical_2d(dataset, attributes, attribute_type_map, attributeToValueMap) -> list:
    processed_data = []
    for attribute in attributes:
        processed_data.append(
            categorical_preprocess(
                dataset[attribute],
                attributeToValueMap[attribute]))
    for i in iter(range(dataset.shape[0])):
        yield next(processed_data[0])
        yield next(processed_data[1])


def read_json(file):
    with open(file) as f:
        return json.load(f)


def write_json(file, out):
    with open(file, 'w') as f:
        json.dump(out, f, indent=4)
