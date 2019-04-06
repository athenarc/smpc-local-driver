from utils import *


def preprocess(
        computation_request,
        computation_request_id,
        attributes,
        data_file_name,
        mapping_file_name,
        filters=None,
        decimal_accuracy=5):
    ''' Function to apply preprocessing to dataset.
        computation_request: 'str', one of '1d_categorical_histogram', '2d_categorical_histogram', '1d_numerical_histogram', '2d_numerical_histogram', '2d_mixed_histogram', 'secure_aggregation',
        computation_request_id: 'str', Unique id of computation request,
        attributes: 'list', contains attributes that will be used for the computation,
        data_file_name: 'str', path + file_name where data is located,
        filters: 'dict', maps attributes to filter functions that we want to apply,
        decimal_accuracy: 'int', how many decimal digits to consider for floats
    '''

    data = pd.read_csv(data_file_name)
    attribute_type_map = {}
    for index, value in data.dtypes.iteritems():
        if str(value) == 'object':
            attribute_type_map[index] = 'Categorical'
        elif 'float' in str(value):
            attribute_type_map[index] = 'Numerical_float'
        elif 'int' in str(value):
            attribute_type_map[index] = 'Numerical_int'
        else:
            raise NotImplementedError
    assert isinstance(
        mapping_file_name, str), "'mapping_file_name' must be of type 'str'"
    assert isinstance(
        decimal_accuracy, int), "'decimal_accuracy' must be of type 'int'"
    assert isinstance(computation_request_id,
                      str), "'computation_request_id' must be of type 'str'"
    assert isinstance(
        data_file_name, str), "'data_file_name' must be of type 'str'"
    assert set(attributes) <= set(['Gender', 'Address', 'Patient Age', 'Heart rate', 'Height (cm)', 'Weight (kg)', 'LVEDV (ml)', 'LVESV (ml)', 'LVSV (ml)', 'LVEF (%)', 'LV Mass (g)', 'RVEDV (ml)', 'RVESV (ml)', 'RVSV (ml)', 'RVEF (%)', 'RV Mass (g)',
                                   'BMI (kg/msq)', 'BMI (kg/m²)', 'BSA', 'BSA (msq)', 'CO (L/min)', 'Central PP (mmHg)', 'DBP (mmHg)', 'LVEF (ratio)', 'MAP', 'PAP (mmHg)', 'PP (mmHg)', 'RVEF (ratio)', 'SBP (mmHg)', 'SVR (mmHg/L/min)']), 'Some requested attribute is not available'
    assert decimal_accuracy > 0, "Decimal accuracy must be positive"
    assert decimal_accuracy <= 10, "Maximal supported decimal accuracy is 10 digits"
    assert (filters is None) or (isinstance(filters, dict)
                                 ), "Input 'filters' must be a dictionary or None"
    if isinstance(filters, dict):
        assert set(
            filters.keys()) <= set(
            data.columns), "Input 'filters' keys must be data attributes"

    assert computation_request in ['1d_categorical_histogram', '2d_categorical_histogram', '1d_numerical_histogram', '2d_numerical_histogram', '2d_mixed_histogram',
                                   'secure_aggregation'], "Unknown computation request; Give one of the following types '1d_categorical_histogram', '2d_categorical_histogram', '1d_numerical_histogram', '2d_numerical_histogram', '2d_mixed_histogram', 'secure_aggregation'"

    # this sorting mechanism ensures cat -> integer -> float
    attributes = sorted(attributes, key=sort_attributes(attribute_type_map))

    if filters is None:
        dataset = data[attributes]
    else:
        valid_indices = []
        for i in range(data.shape[0]):
            boolean = True
            for attribute_, filter_ in filters.items():
                boolean = boolean and filter_(data[attribute_].iloc[i])
            if boolean:
                valid_indices.append(i)
        dataset = data.iloc[valid_indices]
        dataset = dataset[attributes]

    delete_cols = set(data.columns) - set(attributes)
    data = data.drop(list(delete_cols), axis=1)
    dataset_size = dataset.shape[0]
    sizeAlloc = 0

    attributeToInt, intToAttribute = {}, {}
    cellsX, cellsY = None, None
    for i, attribute in enumerate(attributes):
        attributeToInt[attribute] = i
        intToAttribute[i] = attribute
        if attribute_type_map[attribute] == 'Categorical':
            sizeAlloc += dataset_size
        else:
            sizeAlloc += 2 * dataset_size
    with open(mapping_file_name, 'r') as f:
        attributeToValueMap = json.load(f)

    gc.collect()

    output_directory = '../datasets/' + computation_request_id

    try:
        os.mkdir(output_directory)
    except Exception:
        pass

    if computation_request == '2d_mixed_histogram':
        output = mixed_preprocess(
            dataset,
            attributes,
            attribute_type_map,
            decimal_accuracy,
            attributeToValueMap)
        cellsX = len(attributeToValueMap[intToAttribute[0]])

    elif computation_request == '1d_categorical_histogram':
        output = categorical_1d(
            dataset,
            attributes,
            attribute_type_map,
            attributeToValueMap)
        cellsX = len(attributeToValueMap[intToAttribute[0]])

    elif computation_request == '1d_numerical_histogram':
        output = numerical_1d(
            dataset,
            attributes,
            attribute_type_map,
            decimal_accuracy)

    elif computation_request == 'secure_aggregation':
        raise NotImplementedError

    elif computation_request == '2d_numerical_histogram':
        output = numerical_2d(
            dataset,
            attributes,
            attribute_type_map,
            decimal_accuracy)

    elif computation_request == '2d_categorical_histogram':
        cellsX = len(attributeToValueMap[intToAttribute[0]])
        cellsY = len(attributeToValueMap[intToAttribute[1]])
        output = categorical_2d(
            dataset,
            attributes,
            attribute_type_map,
            attributeToValueMap)

    with open(output_directory + '/' + computation_request_id + '.txt', 'w') as f:
        for item in output:
            f.write("%s\n" % item)

    with open(output_directory + '/' + computation_request_id + '.txt', "rb") as f:
        SHA256 = sha256()
        for byte_block in iter(lambda: f.read(4096), b""):
            SHA256.update(byte_block)

    json_output = {'precision': 10**(-decimal_accuracy),
                   'sizeAlloc': sizeAlloc,
                   'cellsX': cellsX,
                   'cellsY': cellsY,
                   'dataSize': dataset_size,
                   'hash256': SHA256.hexdigest(),
                   'attributeToInt': attributeToInt,
                   'intToAttribute': intToAttribute}
    with open(output_directory + '/' + computation_request_id + '.json', 'w') as f:
        json.dump(json_output, f)

    return 1
