import requests
from tqdm import tqdm
from settings import CATALOGUE_EXPLORER_API


def get_catalogue_records(keywords):
    results = []
    data = {
        'keywords': keywords
    }

    SEARCH_URL = '{0}search/'.format(CATALOGUE_EXPLORER_API)

    res = requests.post(url=SEARCH_URL, data=data).json()

    for entry in tqdm(res['data'][:1]):
        for rec in tqdm(entry['records']):
            catalogue_id = rec['catalogue_id']
            RECORD_URL = '{0}getRecord/?catalogue_id={1}'.format(CATALOGUE_EXPLORER_API, catalogue_id)
            detailed_record = requests.get(url=RECORD_URL, headers={'accept': 'application/json'}).json()
            results.append(detailed_record)

    return results
