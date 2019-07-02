import abc
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
        self.__precision = precision
        self._attributes = None

    @abc.abstractmethod
    def validate(self):
        pass

    def process_attributes(self):
        self._attributes = [Attribute(MESH_TERMS[a]['id'], MESH_TERMS[a]['name'], MESH_TERMS[a]['code']) for a in self._rattributes]

    def get_attributes_by_key(self, key):
        if self._attributes is None:
            return []

        return [getattr(a, key) for a in self._attributes]

    @abc.abstractmethod
    def process(self):
        pass

    @abc.abstractmethod
    def out(self):
        pass


class OneDimensionCategoricalHistogram(Strategy):
    def validate(self):
        assert self._rattributes is not None, 'Empty attributes'
        assert len(self._rattributes) == 1, 'Wrong number of attributes'

    def out(self):
        pass


class TwoDimensionCategoricalHistogram(Strategy):
    def process(self):
        pass


class OneDimensionNumericalHistogram(Strategy):
    def process(self):
        pass


class TwoDimensionNumericalHistogram(Strategy):
    def process(self):
        pass
