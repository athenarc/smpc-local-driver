import decimal
import json
import numpy as np
import requests
import xml.etree.ElementTree as ET
import re
import pandas as pd
import os
from dotenv import load_dotenv


ENV_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../.env')
load_dotenv(dotenv_path=ENV_PATH)
CATALOGUE_API = os.getenv('CATALOGUE_API')

class Attribute:
    def __init__(self, id, name, code):
        self.id = id
        self.name = name
        self.code = code

def handle_categorical(keyword):
    data = {
            'keywords': keyword,
            'consents': 'academic research'
        }
    result = requests.post(url=CATALOGUE_API + "search/", data=data)
    dicto = result.json()['data']
    for entry in dicto:
        parse = entry['records']
        for value in parse:
            catalogue_id = value['catalogue_id']
            kw = requests.get(url = CATALOGUE_API + "getRecord/?catalogue_id=" + catalogue_id, headers={'accept': 'application/json'})
            json_obj = kw.json()['data']['keywords']
            for i in json_obj:
                to_yield = i['value']
                yield to_yield

def categorical_handle(value_generator, inverse, vmap):
    while 1:
        try:
            value = next(value_generator)
            if value in inverse:
                for k in vmap:
                    if (k in inverse[value]['id']):
                        yield vmap[k]
            else:
                yield -1
        except StopIteration:
            break


def get_children(parent_id, dictionary):
    result = []
    for k in dictionary:
        if dictionary[k]['parentCategoryId'] == parent_id:
            result.append(k)
    return result


def create_attribute_type_map(data, attributes):
    attribute_type_map = {}
    if type(attributes) == list:
        for index, value in data.dtypes.iteritems():
            if index in attributes:
                if str(value) == 'object':
                    attribute_type_map[index] = 'Categorical'
                elif 'float' in str(value):
                    attribute_type_map[index] = 'Numerical_float'
                elif 'int' in str(value):
                    attribute_type_map[index] = 'Numerical_int'
                else:
                    raise NotImplementedError
        return attribute_type_map
    elif type(attributes) == dict:
        for index, value in data.dtypes.iteritems():
            if index in attributes:
                if str(value) == 'object':
                    attribute_type_map[attributes[index]] = 'Categorical'
                elif 'float' in str(value):
                    attribute_type_map[attributes[index]] = 'Numerical_float'
                elif 'int' in str(value):
                    attribute_type_map[attributes[index]] = 'Numerical_int'
                else:
                    raise NotImplementedError
        return attribute_type_map
    else:
        raise NotImplementedError


def get_codes_from_attributes(attributes):
    codes = []
    for attribute in attributes:
        codes.append(map_values_to_mesh(attribute, banned_columns=codes))

    return codes


def search_loaded_json_by_field(field, value, loaded_json):
    for i in loaded_json:
        if i[field] == value:
            return i
    raise ValueError("The attribute you provided is not catalogued")


def convertToType(value):
    try:
        return int(value)
    except Exception:
        try:
            return float(value)
        except Exception:
            return value


def parse_xml(data_file_name):
    xml = ET.parse(data_file_name)
    root = xml.getroot()
    string = "\{(.*?)\}"
    prefix = re.search(string, root.tag)[0]
    data_dict = {}
    for tname in root.findall('.//' + prefix + 'ClinicalVariables'):
        attribute = tname.find(prefix + 'TypeName').text
        if attribute not in data_dict.keys():
            data_dict[attribute] = []
        data_dict[attribute].append(convertToType(tname.find(prefix + 'Value').text))
    data = pd.DataFrame.from_dict(data_dict)
    del data_dict
    return data


def parse_csv(data_file_name):
    data = pd.read_csv(data_file_name)
    return data


def load_dataset(data_file_name):
    data = None
    if data_file_name.endswith(".xml"):
        data = parse_xml(data_file_name)
    elif data_file_name.endswith(".csv"):
        data = parse_csv(data_file_name)
    else:
        raise NotImplementedError
    return data


def sort_attributes(attribute_type_mapping):
    def sorting(_type):
        return len(attribute_type_mapping[_type])
    return sorting


def lcs(X, Y):
    m = len(X)
    n = len(Y)
    T = np.zeros((n + 1, m + 1))
    for j in range(1, m + 1):
        for i in range(1, n + 1):
            if Y[i - 1] == X[j - 1]:
                T[i, j] = T[i - 1, j - 1] + 1
            else:
                T[i, j] = max(T[i - 1, j], T[i, j - 1])
    return T[n, m]


def map_mesh_to_values(mesh_code, loaded_mesh_mapping):
    for term in loaded_mesh_mapping:
        if term['code'] == mesh_code:
            return term['name']


def map_values_to_mesh(value, banned_columns=None):
    assert type(value) == str, "Value passed to 'map_values_to_mesh' must be of type <str>"

    data = {
        'term': value,
        'terminology': 'text',
        'result_format': 'json'
    }

    response = requests.post(CATALOGUE_API, data=data)
    maximum_ed = None
    mesh_code = None

    for mesh_candidate in response.json():
        if mesh_candidate['mesh_code'] not in banned_columns or banned_columns is None:
            if mesh_candidate['mesh_code'].startswith("D"):
                current_ed = 1000
            else:
                current_ed = lcs(value, mesh_candidate['mesh_label'])
            if maximum_ed is None or current_ed > maximum_ed:
                maximum_ed = current_ed
                mesh_code = mesh_candidate['mesh_code']

    if mesh_code is not None:
        return mesh_code
    else:
        return value


def categorical_preprocess(unprocessed_attribute, valueToIntMap, mesh_codes_to_ids) -> list:
    for data_instance in unprocessed_attribute.values:
        mesh = map_values_to_mesh(data_instance)
        if mesh in mesh_codes_to_ids.keys():
            mesh_term = mesh_codes_to_ids[mesh]['id']
            if mesh_term in valueToIntMap:
                yield valueToIntMap[mesh_term]
            else:
                yield -1
        else:
            yield -1


def numerical_float_preprocess(unprocessed_attribute, decimal_accuracy) -> list:
    for data_instance in unprocessed_attribute.values:
        data_instance_as_decimal = decimal.Decimal(data_instance)
        abs_exponent = abs(data_instance_as_decimal.as_tuple().exponent)
        accuracy_difference = abs_exponent - decimal_accuracy
        assert len(data_instance_as_decimal.as_tuple().digits[:-abs_exponent]) <= 10, 'Integer values are too large'

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


def mixed_preprocess(dataset, attributes, attribute_type_map, decimal_accuracy, attributeToValueMap, attribute_ids, mesh_codes_to_ids) -> list:
    processed_data = []
    for attribute in attributes:
        if attribute_type_map[attribute_ids[attribute]] == 'Categorical':
            processed_data.append(
                categorical_preprocess(
                    dataset[attribute],
                    attributeToValueMap[attribute_ids[attribute]],
                    mesh_codes_to_ids
                )
            )
        if attribute_type_map[attribute_ids[attribute]] == 'Numerical_int':
            processed_data.append(numerical_int_preprocess(dataset[attribute]))
        if attribute_type_map[attribute_ids[attribute]] == 'Numerical_float':
            processed_data.append(
                numerical_float_preprocess(
                    dataset[attribute],
                    decimal_accuracy
                )
            )

    for _ in iter(range(dataset.shape[0])):
        yield next(processed_data[0])
        yield next(processed_data[1])
        yield next(processed_data[1])


def categorical_1d(dataset, attributes, attribute_type_map, attributeToValueMap, attribute_ids, mesh_codes_to_ids) -> list:
    for i in categorical_preprocess(dataset[attributes[0]], attributeToValueMap[attribute_ids[attributes[0]]], mesh_codes_to_ids):
        yield i


def numerical_1d(dataset, attributes, attribute_type_map, decimal_accuracy, attribute_ids) -> list:
    assert len(attributes) == 1, "Need 1 attribute for a '1d_numerical_histogram' computation request"
    assert (attribute_type_map[attribute_ids[attributes[0]]] != 'Categorical'), "Need a numerical attribute for '1d_numerical_histogram'"
    if attribute_type_map[attribute_ids[attributes[0]]] == 'Numerical_int':
        for i in numerical_int_preprocess(dataset[attributes[0]]):
            yield i
    elif attribute_type_map[attribute_ids[attributes[0]]] == 'Numerical_float':
        for i in numerical_float_preprocess(dataset[attributes[0]], decimal_accuracy):
            yield i


def numerical_2d(dataset, attributes, attribute_type_map, decimal_accuracy, attribute_ids) -> list:
    assert len(attributes) == 2, "Need 2 attributes for a '2d_numerical_histogram' computation request"
    assert (attribute_type_map[attribute_ids[attributes[0]]] != 'Categorical') and (attribute_type_map[attribute_ids[attributes[1]]] != 'Categorical'), "Need two non-categorical attributes for '2d-numerical_histogram' computation request"

    processed_data = []
    for attribute in attributes:
        if attribute_type_map[attribute_ids[attribute]] == 'Numerical_int':
            processed_data.append(numerical_int_preprocess(dataset[attribute]))
        if attribute_type_map[attribute_ids[attribute]] == 'Numerical_float':
            processed_data.append(
                numerical_float_preprocess(
                    dataset[attribute],
                    decimal_accuracy
                )
            )

    for _ in iter(range(dataset.shape[0])):
        yield next(processed_data[0])
        yield next(processed_data[0])
        yield next(processed_data[1])
        yield next(processed_data[1])


def categorical_2d(dataset, attributes, attribute_type_map, attributeToValueMap, attribute_ids, mesh_codes_to_ids) -> list:
    processed_data = []
    for attribute in attributes:
        processed_data.append(
            categorical_preprocess(
                dataset[attribute],
                attributeToValueMap[attribute],
                mesh_codes_to_ids))
    for i in iter(range(dataset.shape[0])):
        yield next(processed_data[0])
        yield next(processed_data[1])


def read_json(file):
    with open(file) as f:
        return json.load(f)


def write_json(file, out):
    with open(file, 'w') as f:
        json.dump(out, f, indent=4)
