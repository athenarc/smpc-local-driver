import abc
import os
import decimal
from functools import wraps
from settings import REQUEST_DIRECTORY, MESH_TERMS, MESH_INVERSED, MAPPING
from catalogue_api import normalize_attributes
from utils import write_json, hash_file
from data_providers import CatalogueDataProvider, FileDataProvider
import numpy as np

MAX_PRECISION = 10


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
    def __init__(self, id, attributes, dataset, precision=5, request=None):
        self._id = id
        self._rattributes = attributes
        self._file = dataset
        self._precision = precision
        self._request = request
        self._attributes = None
        self._directory = '{0}/{1}'.format(REQUEST_DIRECTORY, id)
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
    def out(self, out):
        pass


class Histogram(Strategy):
    def histogram():
        pass


class CategoricalHistogram(Histogram):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._provider = CatalogueDataProvider()

    def _preprocess(validate_num):
        def decorator(func):
            @wraps(func)
            def preprocess(self, *args, **kwargs):
                self.validate(validate_num)
                self.process_attributes()
                self._mapping = self.get_mapping()
                request = self._request['raw_request'] if self._request and 'raw_request' in self._request else None
                self._dataset = self._provider.get_dataset(self.get_attributes_by_key('name'), request)

                return func(self, *args, **kwargs)

            return preprocess
        return decorator

    def validate(self, num):
        assert self._rattributes is not None, 'Empty attributes'
        assert len(self._rattributes) == num, 'Wrong number of attributes'

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


class NumericalHistogram(Histogram):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._provider = FileDataProvider()

    def validate(self, num):
        assert self._rattributes is not None, 'Empty attributes'
        assert len(self._rattributes) == num, 'Wrong number of attributes'
        assert self._precision > 0, "Decimal accuracy must be positive"
        assert self._precision <= MAX_PRECISION, "Maximal supported decimal accuracy is {0} digits".format(MAX_PRECISION)

    def validate_normalized_attributes(self, dataset):
        assert set(self.get_attributes_by_key('code')) <= set(dataset.columns), 'Some requested attribute is not available'

        for attr in self._attributes:
            assert attr.type != 'Categorical', 'Categorical attribute provided'

    def process_column(self, attribute, dataset):
        if attribute.type == 'Numerical_int':
            return self.procress_integer(dataset[attribute.code])
        elif attribute.type == 'Numerical_float':
            return self.procress_float(dataset[attribute.code])

    def procress_integer(self, dataset):
        return [str(v) for v in dataset if len(str(v)) <= MAX_PRECISION]

    def procress_float(self, dataset):
        results = []
        for value in dataset:
            decimal_value = decimal.Decimal(value)
            exponent = abs(decimal_value .as_tuple().exponent)

            assert len(decimal_value.as_tuple().digits[:-exponent]) <= MAX_PRECISION, 'Integer values are too large'

            accuracy_difference = exponent - self._precision

            if exponent == 0:
                results.append(int(decimal_value))
                results.append(int("".join(['0'] * (-accuracy_difference))))
            else:
                if abs(value) >= 1:
                    if accuracy_difference >= 0:
                        results.append(int("".join(str(i) for i in decimal_value.as_tuple().digits[:-exponent])))
                        results.append(int("".join(str(i) for i in decimal_value.as_tuple().digits[-exponent:-accuracy_difference])))
                    else:
                        results.append(int("".join(str(i) for i in decimal_value.as_tuple().digits[:-exponent])))
                        results.append(int("".join(str(i) for i in decimal_value.as_tuple().digits[-exponent:])))
                else:
                    results.append(0)
                    results.append(int("".join(str(i) for i in decimal_value.as_tuple().digits[0:self._precision])))

        return results

    def _preprocess(validate_num):
        def decorator(func):
            @wraps(func)
            def preprocess(self, *args, **kwargs):
                self.validate(validate_num)
                self.process_attributes()
                self._dataset = self._provider.get_dataset(self._file)
                self._dataset.columns = normalize_attributes(self._dataset.columns)
                self.add_data_types(self._dataset)
                self.validate_normalized_attributes(self._dataset)

                delete_cols = set(self._dataset.columns) - set(self.get_attributes_by_key('code'))
                self._dataset = self._dataset.drop(list(delete_cols), axis=1)
                return func(self, *args, **kwargs)

            return preprocess
        return decorator

    def out(self, out):
        self.make_directory()
        self.write_output(out)

        description = self.get_base_description()
        description['sizeAlloc'] = len(out)
        description['dataSize'] = int(len(out) / 2)
        description['cellsX'] = None
        description['cellsY'] = None
        description['attributeToInt'] = []
        description['intToAttribute'] = []

        write_json(self._desc_path, description)


class OneDimensionCategoricalHistogram(CategoricalHistogram):
    @CategoricalHistogram._preprocess(1)
    def process(self):
        # Flatten keywords and filter those that are not mesh terms
        keywords = [k['value'] for sublist in self._dataset for k in sublist if k['value'] in MESH_INVERSED]
        results = np.zeros((len(self._mapping[0])))

        for k in keywords:
            for m in self._mapping[0]:
                if (m in MESH_INVERSED[k]['id']):
                    results[self._mapping[0][m]]+= 1
        self.out(results, self._mapping)


class TwoDimensionCategoricalHistogram(CategoricalHistogram):
    @CategoricalHistogram._preprocess(2)
    def process(self):
        results = np.zeros((len(self._mapping[0]),len(self._mapping[1])))
        for rec in self._dataset:
            first = []
            second = []
            for k in rec:
                if k['value'] in MESH_INVERSED:
                    for m in self._mapping[0]:
                        if (m in MESH_INVERSED[k['value']]['id']):
                            first.append(self._mapping[0][m])
                    for m in self._mapping[1]:
                        if (m in MESH_INVERSED[k['value']]['id']):
                            second.append(self._mapping[1][m])
            # Get only the first mapping. Ignore multiple values per attribute.
            if len(first) > 0 and len(second) > 0:
                results[first[0],second[0]] += 1
        results = results.flatten()
        # results order is for each value of first list all second (11,12,13...,21,22,23,....)
        self.out(results, self._mapping)


class OneDimensionNumericalHistogram(NumericalHistogram):
    @NumericalHistogram._preprocess(1)
    def process(self):
        results = self.process_column(self._attributes[0], self._dataset)

        del self._dataset
        self.out(results)


class TwoDimensionNumericalHistogram(NumericalHistogram):
    @NumericalHistogram._preprocess(2)
    def process(self):
        results = []
        dataset_size = self._dataset.shape[0]

        for attribute in self._attributes:
            results.append(self.process_column(attribute, self._dataset))

        del self._dataset

        out = []
        for index in range(dataset_size):
            start = index * 2
            end = start + 2

            out.extend(results[0][start:end])
            out.extend(results[1][start:end])

        del results
        self.out(out)
