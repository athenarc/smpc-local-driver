from preprocessor import *


def filter1(data) -> bool:
    if data % 1 == 0:
        return True
    else:
        return False


def filter2(data) -> bool:
    if data <= 250 and data >= 10:
        return True
    else:
        return False


computation_request_id = 'test_id_3'
attributes = ['Address', 'Ethnicity']
mapping_file = '../smpc-global/attributes.json'
computation_request = '2d_categorical_histogram'  # '2d_mixed_histogram'
data = preprocess(
    computation_request,
    computation_request_id,
    attributes,
    '../dataset_sample.xml',
    mapping_file)  # , filters = {"Medical Record Number":filter1, "RVEDV (ml)": filter2})
