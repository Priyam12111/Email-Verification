import sys
sys.path.append('D:\Email Verification\LocalVerification\APIs\company')


import pandas as pd
from consumer import process # type: ignore
from all_imports import wrreplace # type: ignore
df = pd.read_csv('sample.csv')
for index, row in df.iterrows():
    if not pd.isna(row['Domain']):
        continue
    domain = process([{'name': row['Company']}])
    wrreplace('sample.csv', row['Company'], f"{row['Company']},{domain}")