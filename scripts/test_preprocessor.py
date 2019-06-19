from preprocessor import preprocess

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
        '/Users/laxmana/Downloads/dataset_sample.xml'
    )
    print('[*] Done!')
