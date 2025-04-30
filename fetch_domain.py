import sys
sys.path.append('company')


import pandas as pd
from consumer import process # type: ignore
from all_imports import wrreplace # type: ignore

import json

with open('AllDomain.json', 'r') as file:
    all_domains = json.load(file)

boxMap = {}
df = pd.read_csv('sample.csv')
for index, row in df.iterrows():
    if not pd.isna(row['Domain']):
        continue
    if row['Company'] in all_domains:
        boxMap[row['Company']] += 1
        wrreplace('sample.csv', row['Company'], f"{row['Company']},{all_domains[row['Company']]},{boxMap[row['Company']]}")
        continue
    domain = process([{'name': row['Company']}])
    wrreplace('sample.csv', row['Company'], f"{row['Company']},{domain}")
    boxMap[row['Company']] = 1