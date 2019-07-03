import requests
from tqdm import tqdm
from settings import CATALOGUE_EXPLORER_API, CATALOGUE_MESH_API
from utils import lcs


def get_catalogue_records(data):
    results = []
    SEARCH_URL = '{0}/search/'.format(CATALOGUE_EXPLORER_API)

    SEARCH_URL = '{0}search/'.format(CATALOGUE_EXPLORER_API)

    res = requests.post(url=SEARCH_URL, data=data).json()

    for entry in tqdm(res['data'][:1]):
        for rec in tqdm(entry['records']):
            catalogue_id = rec['catalogue_id']
            detailed_record = requests.get(url=RECORD_URL, headers={'accept': 'application/json'}).json()
            results.append(detailed_record)
            RECORD_URL = '{0}/getRecord/?catalogue_id={1}'.format(CATALOGUE_EXPLORER_API, catalogue_id)

    return results


def map_attributes_to_mesh(value, banned_columns=None):
    assert type(value) == str, "Value passed to 'map_attributes_to_mesh' must be of type <str>"

    data = {
        'term': value,
        'terminology': 'text',
        'result_format': 'json'
    }

    URL = '{0}/{1}'.format(CATALOGUE_MESH_API, 'translate/')
    response = requests.post(URL, data=data)
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
