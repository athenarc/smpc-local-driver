import abc
import os
from settings import DATASET_DIRECTORY, SMPC_GLOBAL_DIRECTORY, MESH_TERMS, MESH_INVERSED, MAPPING
from catalogue_api import get_catalogue_records
from utils import write_json, read_json, hash_file


class Processor:
    def __init__(self, strategy):
        self._strategy = strategy

    def process(self):
        return self._strategy.process()


class Attribute:
    def __init__(self, id, name, code):
        self.id = id
        self.name = name
        self.code = code


class Strategy(metaclass=abc.ABCMeta):
    def __init__(self, id, attributes, dataset, precision=5):
        self._id = id
        self._rattributes = attributes
        self._dataset = dataset
        self._precision = precision
        self._attributes = None
        self._directory = '{0}/{1}'.format(DATASET_DIRECTORY, id)
        self._file_path = '{0}/{1}.txt'.format(self._directory, id)
        self._desc_path = '{0}/{1}.json'.format(self._directory, id)

    def process_attributes(self):
        self._attributes = [Attribute(MESH_TERMS[a]['id'], MESH_TERMS[a]['name'], MESH_TERMS[a]['code']) for a in self._rattributes]

    def get_attributes_by_key(self, key):
        if self._attributes is None:
            self.process_attributes()

        return [getattr(a, key) for a in self._attributes]

    def get_mapping(self):
        if self._attributes is None:
            self.process_attributes()

        return [MAPPING[a.id] for a in self._attributes if a.id in MAPPING]

    def make_directory(self):
        if (not os.path.exists(self._directory)):
            os.makedirs(self._directory, exist_ok=True)

    def write_output(self, out):
        with open(self._file_path, mode='wt', encoding='utf-8') as f:
            f.write('\n'.join(str(line) for line in out))

    def get_base_description(self):
        return {
            'precision': '{0:.{1}f}'.format(10**(-self._precision), self._precision),
            'hash256': hash_file(self._file_path)
        }

    @abc.abstractmethod
    def validate(self):
        pass

    @abc.abstractmethod
    def process(self):
        pass

    @abc.abstractmethod
    def out(self, file, out):
        pass


class OneDimensionCategoricalHistogram(Strategy):
    def validate(self):
        assert self._rattributes is not None, 'Empty attributes'
        assert len(self._rattributes) == 1, 'Wrong number of attributes'

    def process(self):
        self.validate()
        self.process_attributes()
        attributes_by_name = self.get_attributes_by_key('name')
        records = get_catalogue_records(';'.join(attributes_by_name))
        # records = read_json('patient.json')
        results = []
        mapping = self.get_mapping()[0]
        keywords = list(map(lambda rec: rec['data']['keywords'], records))
        keywords = [k['value'] for sublist in keywords for k in sublist]

        for k in keywords:
            if k in MESH_INVERSED:
                for m in mapping:
                    if (m in MESH_INVERSED[k]['id']):
                        results.append(mapping[m])
            else:
                results.append(-1)

        results = list(filter(lambda v: v > -1, results))
        self.out(results, mapping)

    def out(self, out, mapping):
        self.make_directory()
        self.write_output(out)

        description = self.get_base_description()
        description['sizeAlloc'] = len(out) - 1
        description['dataSize'] = len(out) - 1
        description['cellsX'] = len(mapping)
        description['cellsY'] = 0
        description['attributeToInt'] = []
        description['intToAttribute'] = []

        write_json(self._desc_path, description)


class TwoDimensionCategoricalHistogram(Strategy):
    def process(self):
        pass


class OneDimensionNumericalHistogram(Strategy):
    def process(self):
        pass


class TwoDimensionNumericalHistogram(Strategy):
    def process(self):
        pass
