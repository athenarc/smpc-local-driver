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

    def add_data_types(self, data):
        for attribute in self._attributes:
            for index, value in data.dtypes.iteritems():
                if attribute.code == index:
                    if str(value) == 'object':
                        attribute.type = 'Categorical'
                    elif 'float' in str(value):
                        attribute.type = 'Numerical_float'
                    elif 'int' in str(value):
                        attribute.type = 'Numerical_int'
                    else:
                        raise NotImplementedError

    @abc.abstractmethod
    def process(self):
        pass

    @abc.abstractmethod
    def out(self, out, mapping):
        pass


class CategoricalHistogram(Strategy):
    def validate(self, num):
        assert self._rattributes is not None, 'Empty attributes'
        assert len(self._rattributes) == num, 'Wrong number of attributes'

    def get_dataset(self):
        attributes_by_name = self.get_attributes_by_key('name')
        records = get_catalogue_records(';'.join(attributes_by_name))
        keywords = list(map(lambda rec: rec['data']['keywords'], records))
        return keywords

    def out(self, out, mapping):
        self.make_directory()
        self.write_output(out)

        description = self.get_base_description()
        description['sizeAlloc'] = len(out)
        description['dataSize'] = len(out)
        description['cellsX'] = len(mapping[0])
        description['attributeToInt'] = []
        description['intToAttribute'] = []

        if len(mapping) == 2:
            description['cellsY'] = len(mapping[1])

        write_json(self._desc_path, description)


class NumericalHistogram(Strategy):
    def validate(self, num):
        assert self._rattributes is not None, 'Empty attributes'
        assert len(self._rattributes) == num, 'Wrong number of attributes'
        assert self._precision > 0, "Decimal accuracy must be positive"
        assert self._precision <= MAX_PRECISION, "Maximal supported decimal accuracy is {0} digits".format(MAX_PRECISION)

    def validate_normalized_attributes(self, dataset):
        assert set(self.get_attributes_by_key('code')) <= set(dataset.columns), 'Some requested attribute is not available'

        for attr in self._attributes:
            assert attr.type != 'Categorical', 'Categorical attribute provided'

    def get_dataset(self):
        pass


class OneDimensionCategoricalHistogram(CategoricalHistogram):
    def process(self):
        self.validate(1)
        self.process_attributes()
        mapping = self.get_mapping()
        results = []
        keywords = self.get_dataset()

        # Flatten keywords and filter those that are not mesh terms
        keywords = [k['value'] for sublist in keywords for k in sublist if k['value'] in MESH_INVERSED]

        for k in keywords:
            for m in mapping[0]:
                if (m in MESH_INVERSED[k]['id']):
                    results.append(mapping[0][m])

        self.out(results, mapping)


class TwoDimensionCategoricalHistogram(CategoricalHistogram):
    def process(self):
        self.validate(2)
        self.process_attributes()
        mapping = self.get_mapping()
        results = []
        records = self.get_dataset()

        for rec in records:
            first = []
            second = []
            for k in rec:
                if k['value'] in MESH_INVERSED:
                    for m in mapping[0]:
                        if (m in MESH_INVERSED[k['value']]['id']):
                            first.append(mapping[0][m])
                    for m in mapping[1]:
                        if (m in MESH_INVERSED[k['value']]['id']):
                            second.append(mapping[1][m])

            # Get only the first mapping. Ignore multiple values per attribute.
            if len(first) > 0 and len(second) > 0:
                results.append(first[0])
                results.append(second[0])

        self.out(results, mapping)


class OneDimensionNumericalHistogram(Strategy):
    def process(self):
        pass


class TwoDimensionNumericalHistogram(Strategy):
    def process(self):
        pass
