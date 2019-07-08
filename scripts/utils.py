import json
import numpy as np
import xml.etree.ElementTree as ET
import re
import pandas as pd
from hashlib import sha256


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
        data_dict[attribute].append(convert_to_type(tname.find(prefix + 'Value').text))
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


def get_children(parent_id, dictionary):
    result = []
    for k in dictionary:
        if dictionary[k]['parentCategoryId'] == parent_id:
            result.append(k)
    return result


def search_loaded_json_by_field(field, value, loaded_json):
    for i in loaded_json:
        if i[field] == value:
            return i
    raise ValueError("The attribute you provided is not catalogued")


def convert_to_type(value):
    try:
        return int(value)
    except Exception:
        try:
            return float(value)
        except Exception:
            return value


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


def read_json(file):
    with open(file) as f:
        return json.load(f)


def write_json(file, out):
    with open(file, 'w') as f:
        json.dump(out, f, indent=4)


def hash_file(file):
    h = sha256()

    with open(file, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            h.update(byte_block)

    return h.hexdigest()
