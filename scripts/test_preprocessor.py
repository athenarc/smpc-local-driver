#!/usr/bin/python3

import sys
from preprocessor import preprocess
from collections import namedtuple

Arguments = namedtuple('args', ('algorithm', 'computation', 'attributes', 'dataset', 'precision', 'request'))

computations = {
    '1d_categorical_histogram': {
        'id': 'test_id_1',
        'attributes': ['C14.280'],
        'request':
        # A string of request description. The string will be json decoded.
        '''
            {
                "attributes":[{"name":"C14.280"}],
                "algorithm":"1d_categorical_histogram",
                "dataProviders":[0],
                "link":"http://example.com/download",
                "id":"e6e0f024-9879-4400-823e-b9db0db3af68",
                "timestamps":{"accepted":1562145951528,"process":1562145951535},
                "status":"1",
                "totalClients":"1",
                "raw_request": [{
                    "consent": "marketing",
                    "datatype": "crs_identified,Magnetic Resonance Imaging (MRI) of Heart,Cardiac CT",
                    "keywords": ["Heart Diseases"]
                }, {
                    "consent": "academic research",
                    "datatype": "crs_identified,Echocardiography",
                    "keywords": ["Heart Diseases"]
                }, {
                    "consent": "academic research",
                    "datatype": "crs_identified,Cardiac CT,Echocardiography",
                    "keywords": ["Heart Diseases"]
                }]
            }
        '''
    },
    '2d_categorical_histogram': {'id': 'test_id_2', 'attributes': ['C14.280', 'C13.703'], 'request': None},
    '1d_numerical_histogram': {'id': 'test_id_3', 'attributes': ['C23.888.144'], 'request': None},
    '2d_numerical_histogram': {'id': 'test_id_4', 'attributes': ['C23.888.144', 'Z01.252.100.450'], 'request': None}
}

for c in computations:
    print('[*] Testing: ', c)
    args = Arguments(c, computations[c]['id'], computations[c]['attributes'], sys.argv[1], 5, computations[c]['request'])
    preprocess(args)
    print('[*] Done!')
