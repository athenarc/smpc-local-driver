import pandas as pd
import json
import argparse

attributes = [
    'Ethnicity',
    'Gender',
    'Address',
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
    'BMI (kg/msq)',
    'BMI (kg/m²)',
    'BSA',
    'BSA (msq)',
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


def map(dataset, output):
    data = pd.read_csv(dataset)
    globalMap = {}
    catAttributes = []
    for index, value in data.dtypes.items():  # in python3 items is like iteritems of python2
        if (str(index) in attributes) and (str(value) == 'object'):
            catAttributes.append(index)
            globalMap[index] = {}

    data = data[catAttributes]
    for i in catAttributes:
        count = 0
        for j in iter(range(data.shape[0])):
            if not data[i].values[j] in globalMap[i].keys():
                globalMap[i][data[i].values[j]] = count
                count += 1

    with open(output, 'w') as f:
        json.dump(globalMap, f)


def main():
    parser = argparse.ArgumentParser(
        description='SMPC global mapping generator')
    parser.add_argument(
        '-d',
        '--dataset',
        required=True,
        type=str,
        help='Dataset file (required)')
    parser.add_argument(
        '-o',
        '--output',
        required=True,
        type=str,
        help='Output mapping file (required)')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()
    map(args.dataset, args.output)


if __name__ == '__main__':
    main()
