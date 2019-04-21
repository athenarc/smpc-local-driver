import unittest
import sys

from preprocessor import preprocess

categorical = [
    'Ethnicity',
    'Gender',
]

numerical = [
    'Patient Age',
    'Heart rate',
    'Height (cm)',
    'Weight (kg)',
    'LVEDV (ml)',
    'LVESV (ml)',
    'LVSV (ml)',
    'LVEF (%)',
    'LV Mass (g)',
    'RVEDV (ml)',
    'RVESV (ml)',
    'RVSV (ml)',
    'RVEF (%)',
    'RV Mass (g)',
    'BMI (kg/m²)',
    'BMI (kg/m²)',
    'BSA',
    'BSA (m²)',
    'CO (L/min)',
    'Central PP (mmHg)',
    'DBP (mmHg)',
    'LVEF (ratio)',
    'MAP',
    'PAP (mmHg)',
    'PP (mmHg)',
    'RVEF (ratio)',
    'SBP (mmHg)',
    'SVR (mmHg/L/min)'
]


class TestSum(unittest.TestCase):
    def test_categorical(self):
        for attr in categorical:
            print('---------------------Testing attribute: ' + attr + '---------------------')
            preprocess('1d_categorical_histogram', 'test', [attr], sys.argv[1], '../smpc-global/mapping.json', 5)
            print('---------------------Done!---------------------')

    def test_numerical(self):
        for attr in numerical:
            print('---------------------Testing attribute: ' + attr + '---------------------')
            preprocess('1d_numerical_histogram', 'test', [attr], sys.argv[1], '../smpc-global/mapping.json', 5)
            print('---------------------Done!---------------------')


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
