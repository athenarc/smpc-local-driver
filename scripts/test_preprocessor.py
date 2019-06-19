from preprocessor import preprocess

mapping_file = '../smpc-global/mapping.json'
mesh_codes_to_ids_file = '../smpc-global/meshTermsByCode.json'
mesh_ids_and_codes_file = '../smpc-global/meshTerms.json'
parents_file = '../smpc-global/meshByParent.json'
mesh_terms_inverted_file = '../smpc-global/meshTermsInversed.json'

computations = {
    '1d_categorical_histogram': {'id': 'test_id_1', 'attributes': ['C14.280']},
    '2d_categorical_histogram': {'id': 'test_id_2', 'attributes': ['C14.280', 'C13.703']},
    '1d_numerical_histogram': {'id': 'test_id_3', 'attributes': ['M01.060']},
    '2d_numerical_histogram': {'id': 'test_id_4', 'attributes': ['M01.060', 'E01.370.600.115.100.160.120']}
}

for c in computations:
    print('[*] Testing: ', c)
    preprocess(
        c,
        computations[c]['id'],
        computations[c]['attributes'],
        '/Users/laxmana/Downloads/dataset_sample.xml',
        mapping_file,
        mesh_codes_to_ids_file,
        mesh_ids_and_codes_file,
        mesh_terms_inverted_file
    )
    print('[*] Done!')
