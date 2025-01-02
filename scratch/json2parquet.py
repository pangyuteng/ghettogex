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


#async def get_oi(event_symbol):
def get_oi(event_symbol,df):
    # tstamp,event_symbol,oi

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
    oi_csv_file = 'tmp/oi.csv'
    gex_csv_file = f'tmp/{ticker}-{date_stamp_str}-gex.csv'
    gex_png_file = f'tmp/{ticker}-{date_stamp_str}-gex.png'

    tstamp_list = pd.date_range(start="2024-12-31 09:30:00",end="2024-12-31 16:30:00",freq='s')
    tstamp_list = sorted(list(set([x.replace(second=0) for x in tstamp_list])))

    if not os.path.exists(pq_file):
        df = asyncio.run(main(ticker,tstamp,pq_file))
    else:
        df = pd.read_parquet(pq_file)

    options_df = df[df.strike.notnull()]
    event_symbol_list = sorted(list(options_df.event_symbol.unique()))

    df.tstamp = df.tstamp.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d-%H-%M-%S.%f'))
    df['tstamp_reduced'] = df.tstamp.apply(lambda x: x.replace(second=0,microsecond=0))

    df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df['bid_volume'] = pd.to_numeric(df['bid_volume'], errors='coerce')
    df['ask_volume'] = pd.to_numeric(df['ask_volume'], errors='coerce')
    df['gamma'] = pd.to_numeric(df['gamma'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['size'] = pd.to_numeric(df['size'], errors='coerce')

    def get_side_int(x):
        if x == "BUY":
            return 1
        elif x == "SELL":
            return -1
        else:
            return np.nan
    df['side_int'] = df.aggressor_side.apply(lambda x: get_side_int(x))
    
    open_interest_dict = {}
    oi_list = []
    for event_symbol in tqdm(event_symbol_list):
        open_interest_dict[event_symbol]={}

        for tstamp_idx,tstamp in enumerate(tstamp_list):

            u_df = df[(df.event_type=='candle')&(df.strike.isnull())&(df.tstamp_reduced==tstamp)]
            #print(len(u_df))

            c_df = df[(df.event_type=="candle")&(df.strike.notnull())&(df.tstamp_reduced==tstamp)]
            #print(len(c_df))

            s_df = df[(df.event_type=="summary")&(df.strike.notnull())&(df.tstamp_reduced==tstamp)]
            #print(len(s_df))

            ts_df = df[(df.event_type=="timeandsale")&(df.strike.notnull())&(df.tstamp_reduced==tstamp)]
            #print(len(ts_df))
        
            if tstamp_idx == 0:
                oi_list = s_df['open_interest'].to_list()
                if len(oi_list) > 0:
                    open_interest = oi_list[0]
                else:
                    open_interest = 0
                    print(event_symbol,tstamp,"???")
                open_interest_dict[event_symbol][tstamp] = open_interest
            else:
                prior_tstamp = tstamp_list[tstamp_idx-1]
                prior_open_interest = open_interest_dict[event_symbol][prior_tstamp]
                size_sum = (ts_df.size*ts_df.side_int).sum()
                open_interest = prior_open_interest+size_sum
                open_interest_dict[event_symbol][tstamp] = open_interest

            item = dict(
                event_symbol=event_symbol,
                tstamp=tstamp,
                open_interest=open_interest,
            )
            oi_list.append(item)
    oi_list.to_csv(oi_csv_file,index=False)