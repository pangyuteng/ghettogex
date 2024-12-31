
import os
import sys
import json
import re
import pathlib
import pandas as pd
from tqdm import tqdm

# sample event_symbol ".TSLA240927C105"
PATTERN = r"\.([A-Z]+)(\d{6})([CP])(\d+)"

def parse_symbol(event_symbol):
    matched = re.match(PATTERN,event_symbol)
    ticker = matched.group(1)
    expiration = datetime.datetime.strptime(matched.group(2),'%y%m%d').date()
    contract_type = matched.group(3)
    strike = float(matched.group(4))
    return ticker,expiration,contract_type,strike

def process(json_file):
    event_dict = {}
    file_split = json_file.split("/")
    tstamp, uid = file_split[-1].replace(".json","").split("-uid-")
    event_type = file_split[-2]
    streamer_symbol = file_split[-3]
    with open(json_file, 'r') as f:
        content = json.loads(f.read())

    event_dict.update(content)
    if streamer_symbol.startswith("."):
        ticker,expiration,contract_type,strike = parse_symbol(streamer_symbol)
        event_dict['ticker']=ticker
        event_dict['expiration']=expiration
        event_dict['contract_type']=contract_type
        event_dict['strike']=strike
    else:
        event_dict['ticker']=streamer_symbol

    event_dict["streamer_symbol"]=streamer_symbol
    event_dict["event_type"]=event_type
    event_dict["uid"]=uid
    event_dict["tstamp"]=tstamp
    return event_dict

def main():

    CACHE_TASTY_FOLDER = "tmp"
    ticker = 'SPX'
    root_folder = os.path.join(CACHE_TASTY_FOLDER,ticker)

    json_file_list = [str(x) for x in pathlib.Path(root_folder).rglob("*.json")]
    json_file_list = [x for x in json_file_list if 'greeks' in x]

    print(len(json_file_list))

    for json_file in tqdm(json_file_list):
        myitem = process(json_file)
        mylist.append(myitem)

    df = pd.DataFrame(mylist)
    df.to_csv("ok.csv",index=False)

if __name__ == "__main__":
    main()