import pandas as pd
import json

data = pd.read_csv('../cvi_identified_small.csv')
attributes = [
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
    'SVR (mmHg/L/min)']

globalMap = {}
catAttributes = []
for index, value in data.dtypes.iteritems():

    if (str(index) in attributes) and (str(value) == 'object'):
        catAttributes += [index]
        globalMap[index] = {}

data = data[catAttributes]
for i in catAttributes:
    count = 0
    for j in iter(range(data.shape[0])):
        if data[i].values[j] in globalMap[i].keys():
            pass
        else:
            globalMap[i][data[i].values[j]] = count
            count += 1

with open('../mapping.json', 'w') as f:
    json.dump(globalMap, f)
