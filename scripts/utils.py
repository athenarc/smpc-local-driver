import decimal
import json


def sort_attributes(attribute_type_mapping):
    def sorting(type_):
        return len(attribute_type_mapping[type_])
    return sorting


def categorical_preprocess(unprocessed_attribute, valueToIntMap) -> list:
    count = 0
    for data_instance in unprocessed_attribute.values:
        if data_instance in valueToIntMap.keys():
            yield valueToIntMap[data_instance]
        else:
            valueToIntMap[data_instance] = count
            yield valueToIntMap[data_instance]
            count += 1


def numerical_float_preprocess(unprocessed_attribute, decimal_accuracy) -> list:
    for data_instance in unprocessed_attribute.values:
        data_instance_as_decimal = decimal.Decimal(data_instance)
        abs_exponent = abs(data_instance_as_decimal.as_tuple().exponent)
        accuracy_difference = abs_exponent - decimal_accuracy
        assert len(data_instance_as_decimal.as_tuple(
        ).digits[:-abs_exponent]) <= 10, 'Integer values are too large'

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


def mixed_preprocess(dataset, attributes, attribute_type_map, decimal_accuracy, attributeToValueMap) -> list:
    processed_data = []
    for attribute in attributes:
        if attribute_type_map[attribute] == 'Categorical':
            processed_data.append(
                categorical_preprocess(
                    dataset[attribute],
                    attributeToValueMap[attribute]))
        if attribute_type_map[attribute] == 'Numerical_int':
            processed_data.append(numerical_int_preprocess(dataset[attribute]))
        if attribute_type_map[attribute] == 'Numerical_float':
            processed_data.append(
                numerical_float_preprocess(
                    dataset[attribute],
                    decimal_accuracy))
    for _ in iter(range(dataset.shape[0])):
        yield next(processed_data[0])
        yield next(processed_data[1])
        yield next(processed_data[1])


def categorical_1d(dataset, attributes, attribute_type_map, attributeToValueMap) -> list:
    for i in categorical_preprocess(
            dataset[attributes[0]], attributeToValueMap[attributes[0]]):
        yield i


def numerical_1d(dataset, attributes, attribute_type_map, decimal_accuracy) -> list:
    assert len(
        attributes) == 1, "Need 1 attribute for a '1d_numerical_histogram' computation request"
    assert (attribute_type_map[attributes[0]] !=
            'Categorical'), "Need a numerical attribute for '1d_numerical_histogram'"
    if attribute_type_map[attributes[0]] == 'Numerical_int':
        for i in numerical_int_preprocess(dataset[attributes[0]]):
            yield i
    elif attribute_type_map[attributes[0]] == 'Numerical_float':
        for i in numerical_float_preprocess(
                dataset[attributes[0]], decimal_accuracy):
            yield i


def numerical_2d(dataset, attributes, attribute_type_map, decimal_accuracy) -> list:
    assert len(
        attributes) == 2, "Need 2 attributes for a '2d_numerical_histogram' computation request"
    assert (attribute_type_map[attributes[0]] != 'Categorical') and (attribute_type_map[attributes[1]] !=
                                                                    'Categorical'), "Need two non-categorical attributes for '2d-numerical_histogram' computation request"                                                                     
        
    processed_data = []
    for attribute in attributes:
        if attribute_type_map[attribute] == 'Numerical_int':
            processed_data.append(numerical_int_preprocess(dataset[attribute]))
        if attribute_type_map[attribute] == 'Numerical_float':
            processed_data.append(
                numerical_float_preprocess(
                    dataset[attribute],
                    decimal_accuracy))
    for i in iter(range(dataset.shape[0])):
        yield next(processed_data[0])
        yield next(processed_data[0])
        yield next(processed_data[1])
        yield next(processed_data[1])


def categorical_2d(dataset, attributes, attribute_type_map, attributeToValueMap) -> list:
    processed_data = []
    for attribute in attributes:
        processed_data.append(
            categorical_preprocess(
                dataset[attribute],
                attributeToValueMap[attribute]))
    for i in iter(range(dataset.shape[0])):
        yield next(processed_data[0])
        yield next(processed_data[1])


def read_json(file):
    with open(file) as f:
        return json.load(f)


def write_json(file, out):
    with open(file, 'w') as f:
        json.dump(out, f, indent=4)
