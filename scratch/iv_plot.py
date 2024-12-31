
import os
import sys
import json
import pathlib
import pandas as pd


CACHE_TASTY_FOLDER = "tmp"

folder_list = os.listdir(CACHE_TASTY_FOLDER)
print(folder_list)
sys.exit(1)
summary_folder = os.path.join(folder,'summary')
timeandsale_folder = os.path.join(folder,'timeandsale')


# sample event_symbol ".TSLA240927C105"
PATTERN = r"\.([A-Z]+)(\d{6})([CP])(\d+)"

def parse_symbol(event_symbol):
    matched = re.match(PATTERN,event_symbol)
    ticker = matched.group(1)
    expiration = datetime.datetime.strptime(matched.group(2),'%y%m%d').date()
    contract_type = matched.group(3)
    strike = float(matched.group(4))
    return ticker,expiration,contract_type,strike


open_interest = None
json_file_list = sorted([str(x) for x in sorted(pathlib.Path(summary_folder).rglob("*.json"))])
for json_file in json_file_list:
    with open(json_file,'r') as f:
        content = json.loads(f.read())
        #print(json_file,content['open_interest'])
        open_interest = content['open_interest']

mylist = []
json_file_list = sorted([str(x) for x in sorted(pathlib.Path(timeandsale_folder).rglob("*.json"))])
for json_file in json_file_list:
    with open(json_file,'r') as f:
        content = json.loads(f.read())
        content['json_file']=json_file
        content['open_interest']=open_interest
        key_list = ['type','aggressor_side','size','open_interest','json_file']
        item = {x:content[x] for x in key_list}
        mylist.append(item)
        print(json_file)
df = pd.DataFrame(mylist)
df.to_csv("ok.csv",index=False)
