import os
import sys
import traceback
import pathlib
import datetime
import json
import numpy as np
import pandas as pd
import re
import asyncio
import aiofiles
import time
from tqdm import tqdm
import matplotlib.pyplot as plt
import PIL
from moviepy import editor


def hola_cboe():
    ticker = '^SPX'
    ticker_folder = f"/mnt/hd1/data/fi/{ticker}/2024"
    csv_file_list = sorted([str(x) for x in pathlib.Path(ticker_folder).rglob("*.csv")])
    json_file_list = sorted([str(x) for x in pathlib.Path(ticker_folder).rglob("*.json")])

    for json_file in json_file_list:
        with open(json_file,'r') as f:
            content = json.loads(f.read())
        print(json_file,content['last_price'])
        break
    for csv_file in csv_file_list:
        df = pd.read_csv(csv_file)
        print(csv_file,df.spot_price[0],df.open_interest.sum())


# sample streamer_symbol ".TSLA240927C105"
PATTERN = r"\.([A-Z]+)(\d{6})([CP])(\d+)"

def parse_symbol(streamer_symbol):
    matched = re.match(PATTERN,streamer_symbol)
    ticker = matched.group(1)
    expiration = datetime.datetime.strptime(matched.group(2),'%y%m%d').date()
    contract_type = matched.group(3)
    strike = float(matched.group(4))
    return ticker,expiration,contract_type,strike


async def get_json(json_file):

    event_dict = {"json_file":json_file}

    file_split = json_file.split("/")
    tstamp, uid = file_split[-1].replace(".json","").split("-uid-")
    event_type = file_split[-2]
    streamer_symbol = file_split[-3]
    ## TODO: parse later?
    #tstamp = datetime.datetime.strptime(tstamp,'%Y-%m-%d-%H-%M-%S.%f')

    async with aiofiles.open(json_file, mode='r') as f:
        content = json.loads(await f.read())

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
    return df

if __name__ == "__main__":

    ticker = 'SPX'
    tstamp = datetime.datetime(2024,12,31)
    date_stamp_str = tstamp.strftime("%Y-%m-%d")

    if ticker == 'SPX':
        ticker_variants = ["SPX","SPXW"]
    else:
        ticker_variants = [ticker]

    os.makedirs('tmp/pngs',exist_ok=True)
    pq_file = f'tmp/{ticker}-{date_stamp_str}.parquet.gzip'
    gex_csv_file = f'tmp/{ticker}-{date_stamp_str}-gex.csv'
    gex_png_file = f'tmp/{ticker}-{date_stamp_str}-gex.png'

    if not os.path.exists(pq_file):
        df = asyncio.run(main(ticker,tstamp,pq_file))
    else:
        df = pd.read_parquet(pq_file)
    df.tstamp = df.tstamp.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d-%H-%M-%S.%f'))
    df['tstamp_reduced'] = df.tstamp.apply(lambda x: x.replace(microsecond=0))
    print(df.shape)
    tstamp_list = pd.date_range(start="2024-12-31 09:30:00",end="2024-12-31 16:30:00",freq='s')

    # df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce')
    # df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    # df['bid_volume'] = pd.to_numeric(df['bid_volume'], errors='coerce')
    # df['ask_volume'] = pd.to_numeric(df['ask_volume'], errors='coerce')
    # df['gamma'] = pd.to_numeric(df['gamma'], errors='coerce')
    # df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    for tstamp in tstamp_list:
        print(tstamp)

        u_df = df[(df.event_type=='candle')&(df.strike.notnull())&(df.tstamp_reduced==tstamp)]
        print(len(u_df))

        c_df = df[(c_df.event_type=="candle")&(df.strike.notnull())&(df.tstamp_reduced==tstamp)]
        print(len(cdf))

        s_df = df[(df.event_type=="summary")&(df.strike.notnull())&(df.tstamp_reduced==tstamp)]
        print(len(s_df))

        ts_df = df[(df.event_type=="timeandsale")&(df.strike.notnull())&(df.tstamp_reduced==tstamp)]
        print(len(ts_df))
