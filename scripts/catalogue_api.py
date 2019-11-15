import requests
from tqdm import tqdm
from settings import CATALOGUE_SEARCH_URL, CATALOGUE_RECORD_URL, CATALOGUE_TRANSLATE_URL
from utils import lcs


def get_catalogue_records(data):
    results = []

    res = requests.post(url=CATALOGUE_SEARCH_URL, data=data, headers={'accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'})

    if res.status_code != 200:
        return results

    res = res.json()

    if 'data_item_ids' not in res:
        return results

    for entry in tqdm(res['data_item_ids']):
        catalogue_id = entry
        RECORD_URL = '{0}/{1}'.format(CATALOGUE_RECORD_URL, catalogue_id)
        detailed_record = requests.get(url=RECORD_URL, headers={'accept': 'application/json'})
        if detailed_record.status_code == 200:
            results.append(detailed_record.json())

    return results


def map_attributes_to_mesh(value, banned_columns=None):
    assert type(value) == str, "Value passed to 'map_attributes_to_mesh' must be of type <str>"

    data = {
        'term': value,
        'terminology': 'text',
        'result_format': 'json'
    }

    response = requests.post(CATALOGUE_TRANSLATE_URL, data=data)

    if response.status_code != 200:
        return None

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


def normalize_attributes(attributes):
    codes = []
    for attribute in tqdm(attributes):
        codes.append(map_attributes_to_mesh(attribute, banned_columns=codes))

    return codes
