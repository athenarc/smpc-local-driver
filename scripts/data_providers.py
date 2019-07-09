import abc
import requests
import xml.etree.ElementTree as ET
import re
import os
import pandas as pd
from tqdm import tqdm
from collections import defaultdict
from utils import convert_to_type, read_json
from settings import CATALOGUE_EXPLORER_API, DATASET_DIRECTORY, USE_CATALOGUE


class DataProvider:
    @abc.abstractmethod
    def get_dataset(self, attributes):
        pass


class CatalogueDataProvider(DataProvider):
    def get_catalogue_records(self, data):
        results = []
        SEARCH_URL = '{0}/search/'.format(CATALOGUE_EXPLORER_API)

        res = requests.post(url=SEARCH_URL, data=data, headers={'accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'})

        if res.status_code != 200:
            return results

        res = res.json()

        for entry in tqdm(res['data'][:1]):
            for rec in tqdm(entry['records']):
                catalogue_id = rec['catalogue_id']
                RECORD_URL = '{0}/getRecord/?catalogue_id={1}'.format(CATALOGUE_EXPLORER_API, catalogue_id)
                detailed_record = requests.get(url=RECORD_URL, headers={'accept': 'application/json'})
                if detailed_record.status_code == 200:
                    results.append(detailed_record.json())

        return results

    def get_dataset(self, attributes, request=None):
        data = defaultdict(set)

        if request:
            for rec in request:
                for key in rec:
                    if isinstance(rec[key], list):
                        data[key].update(rec[key])
                    else:
                        data[key].add(rec[key])

        data['keywords'] = ';'.join(attributes)
        data['consent'] = ';'.join(data['consent'])
        data['datatype'] = ';'.join(data['datatype'])
        data = {k: v for k, v in data.items()}
        records = []

        if USE_CATALOGUE == '1':
            records = self.get_catalogue_records(data)
        else:
            records = read_json(os.path.join(DATASET_DIRECTORY, 'dataset.json'))

        keywords = list(map(lambda rec: rec['data']['keywords'], records))
        return keywords


class FileDataProvider(DataProvider):
    def parse_xml(self, data_file_name):
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

    def parse_csv(self, data_file_name):
        data = pd.read_csv(data_file_name)
        return data

    def get_dataset(self, file):
        data = None
        if file.endswith(".xml"):
            data = self.parse_xml(file)
        elif file.endswith(".csv"):
            data = self.parse_csv(file)
        else:
            raise NotImplementedError
        return data
