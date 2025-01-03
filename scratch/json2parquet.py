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
    pass

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
    df['size_signed'] = df['size'].where(df.aggressor_side == 'BUY', other=-1*df['size']) #"BUY","SELL":

    #event_symbol_list = [".SPXW241231P5700"]
    oi_list = []
    for event_symbol in tqdm(event_symbol_list):
        print(len(tstamp_list))
        print(event_symbol)
        u_df = df[(df.event_type=='candle')&(df.event_symbol==ticker)&(df.strike.isnull())]
        print(len(u_df))
        c_df = df[(df.event_type=="candle")&(df.event_symbol==event_symbol)&(df.strike.notnull())]
        print(len(c_df))
        s_df = df[(df.event_type=="summary")&(df.event_symbol==event_symbol)&(df.strike.notnull())]
        print(len(s_df))
        ts_df = df[(df.event_type=="timeandsale")&(df.event_symbol==event_symbol)&(df.strike.notnull())]
        print(len(ts_df))
        if len(s_df) > 0:
            open_interest = s_df['open_interest'].to_numpy()[0]
        else:
            open_interest = 0

        print(event_symbol,open_interest)
        print(len(ts_df))

        if len(ts_df) > 0:
            # create new df
            tmp_cols = [
                'tstamp_reduced','tstamp','event_symbol','event_type','size_signed', # 'aggressor_side','size','uid','json_file'
                'expiration','contract_type','strike'
            ]
            tmp_df = ts_df.copy(deep=True).reset_index()
            tmp_df = tmp_df[tmp_cols]
            tmp_df = tmp_df.sort_values(["tstamp"],ascending=True)
            tmp_df['latest_open_interest'] = open_interest+tmp_df.size_signed.cumsum()
            oi_cols = ['tstamp_reduced','event_symbol','expiration','contract_type','strike','latest_open_interest']
            tmp_df = tmp_df[oi_cols]
            tmp_df = tmp_df.groupby(oi_cols).last().reset_index()
            oi_list.append(tmp_df)

    oi_pd = pd.concat(oi_list)
    oi_pd.to_csv("oi.csv",index=False)
        
            