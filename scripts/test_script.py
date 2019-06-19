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


computation_request_id = 'test_id_5'
attributes = ['Z01.586.950.450']
mapping_file = '../smpc-global/mapping.json'
mesh_codes_to_ids_file = '../smpc-global/meshTermsByCode.json'
mesh_ids_and_codes_file = '../smpc-global/meshTerms.json'
parents_file = '../smpc-global/meshByParent.json'
mesh_terms_inverted_file = '../smpc-global/meshTermsInversed.json'


computation_request = '1d_numerical_histogram'  # '2d_mixed_histogram'
preprocess(
    computation_request,
    computation_request_id,
    attributes,
    '../dataset_sample.xml',
    mapping_file,
    mesh_codes_to_ids_file,
    mesh_ids_and_codes_file,
    mesh_terms_inverted_file)  # , filters = {"Medical Record Number":filter1, "RVEDV (ml)": filter2})
