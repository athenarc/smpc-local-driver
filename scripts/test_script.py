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
attributes = ['D001835','D005006']
mapping_file = '../smpc-global-master/mapping.json'
mesh_mapping_file_path = '../smpc-global-master/catalogue.json'
mesh_codes_and_ids_file = '../smpc-global-master/mesh.json'

computation_request = '2d_mixed_histogram'  # '2d_mixed_histogram'
preprocess(
    computation_request,
    computation_request_id,
    attributes,
    '../dataset_sample.xml',
    mapping_file,
    mesh_mapping_file_path,
    mesh_codes_and_ids_file)  # , filters = {"Medical Record Number":filter1, "RVEDV (ml)": filter2})
