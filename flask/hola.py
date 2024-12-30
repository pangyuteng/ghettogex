import os
import sys
import pathlib
import json
import pandas as pd

ticker = '^SPX'
ticker_folder = f"/mnt/hd1/data/fi/{ticker}"
csv_file_list = sorted([str(x) for x in pathlib.Path(ticker_folder).rglob("*.csv")])
json_file_list = sorted([str(x) for x in pathlib.Path(ticker_folder).rglob("*.json")])

for json_file in json_file_list:
    with open(json_file,'r') as f:
        content = json.loads(f.read())
    print(json_file,content['last_price'])

for csv_file in csv_file_list:
    df = pd.read_csv(csv_file)
    print(csv_file,df.open_interest.sum())

