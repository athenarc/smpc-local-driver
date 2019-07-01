import sys
from preprocessor import preprocess

computations = {
    '1d_categorical_histogram': {'id': 'test_id_1', 'attributes': ['C14.280']}
    # '2d_categorical_histogram': {'id': 'test_id_2', 'attributes': ['C14.280', 'C13.703']},
    # '1d_numerical_histogram': {'id': 'test_id_3', 'attributes': ['C23.888.144']},
    # '2d_numerical_histogram': {'id': 'test_id_4', 'attributes': ['C23.888.144', 'Z01.252.100.450']}
}

for c in computations:
    print('[*] Testing: ', c)
    preprocess(
        c,
        computations[c]['id'],
        computations[c]['attributes'],
        sys.argv[1]
    )
    print('[*] Done!')
