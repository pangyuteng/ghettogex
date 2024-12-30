import os
import sys
import traceback
import pathlib
import datetime
import json
import pandas as pd
import re
import asyncio
import aiofiles
import time
from tqdm import tqdm

def hola_cboe():
    ticker = '^SPX'
    ticker_folder = f"/mnt/hd1/data/fi/{ticker}/2024/2024-12-30"
    csv_file_list = sorted([str(x) for x in pathlib.Path(ticker_folder).rglob("*.csv")])
    json_file_list = sorted([str(x) for x in pathlib.Path(ticker_folder).rglob("*.json")])

    for json_file in json_file_list:
        with open(json_file,'r') as f:
            content = json.loads(f.read())
        print(json_file,content['last_price'])
        break
    for csv_file in csv_file_list:
        df = pd.read_csv(csv_file)
        print(csv_file,df.spot_price[0])
        print(csv_file,df.open_interest.sum())
        break

# sample eventSymbol ".TSLA240927C105"
PATTERN = r"\.([A-Z]+)(\d{6})([CP])(\d+)"

def parse_symbol(eventSymbol):
    matched = re.match(PATTERN,eventSymbol)
    ticker = matched.group(1)
    expiration = datetime.datetime.strptime(matched.group(2),'%y%m%d').date()
    contract_type = matched.group(3)
    strike = float(matched.group(4))
    return ticker,expiration,contract_type,strike


async def get_json(json_file):
    event_dict = {"json_file":json_file}
    file_split = json_file.split("/")
    event_type = file_split[-2]
    streamer_symbol = file_split[-3]
    tstamp, uid = file_split[-1].replace(".json","").split("-uid-")
    # TODO: parse later?
    tstamp = datetime.datetime.strptime(tstamp,'%Y-%m-%d-%H-%M-%S.%f')
    async with aiofiles.open(json_file, mode='r') as f:
        content = json.loads(await f.read())
    event_dict.update(content)
    if streamer_symbol.startswith("."):
        ticker,expiration,contractType,strike = parse_symbol(streamer_symbol)
        event_dict['ticker']=ticker
        event_dict['expiration']=expiration
        event_dict['contractType']=contractType
        event_dict['strike']=strike
    else:
        event_dict['ticker']=streamer_symbol
    event_dict["event_type"]=event_type
    event_dict["uid"]=uid
    event_dict["tstamp"]=tstamp
    return event_dict

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

async def main(ticker,tstamp,pq_file):
    date_stamp_str = tstamp.strftime("%Y-%m-%d")
    ticker_folder = f"/mnt/hd1/data/tastyfi/{ticker}/{date_stamp_str}"
    print(f"started at {time.strftime('%X')}")
    json_file_list = [str(x) for x in pathlib.Path(ticker_folder).rglob(f"*.json")]
    print('file count',len(json_file_list))
    #json_file_list = json_file_list[:1000]
    print(f"find done. {time.strftime('%X')}")

    data_list = []
    """
    for json_file in tqdm(json_file_list):
        try:
            mydict = await get_json(json_file)
            data_list.append(mydict)
        except:
            print(json_file)
            traceback.print_exc()
    """
    chunk_n = 1000
    list_of_list = list(chunks(json_file_list, chunk_n))
    for mylist in tqdm(list_of_list):
        func_list = [get_json(x) for x in mylist]
        ret_list = await asyncio.gather(*func_list)
        data_list.extend(ret_list)
    
    print(f"parse done {time.strftime('%X')}")
    print("saving...")
    df = pd.DataFrame(data_list)
    df.to_parquet(pq_file,compression='gzip')
    print(f"finished at {time.strftime('%X')}")


def hola_tasty():

    ticker = 'SPX'
    tstamp = datetime.datetime(2024,12,30)
    date_stamp_str = tstamp.strftime("%Y-%m-%d")
    print(ticker,tstamp)
    pq_file = f'{ticker}-{date_stamp_str}.parquet.gzip'
    if not os.path.exists(pq_file):
        asyncio.run(main(ticker,tstamp,pq_file))
    else:
        df = pd.read_parquet(pq_file)
        print(df.shape)

    # candle quote
    # candle  greeks  profile  quote  summary  trade

if __name__ == "__main__":
    hola_tasty()
    