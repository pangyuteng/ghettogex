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

def hola_tasty():

    ticker = 'SPX'
    tstamp = datetime.datetime(2024,12,30)
    date_stamp_str = tstamp.strftime("%Y-%m-%d")
    print(ticker,tstamp)
    pq_file = f'{ticker}-{date_stamp_str}.parquet.gzip'
    if not os.path.exists(pq_file):
        df = asyncio.run(main(ticker,tstamp,pq_file))
    else:
        df = pd.read_parquet(pq_file)
    print(df.shape)
    print(df.columns)
    print(df.tstamp)
    #df.tstamp = df.tstamp.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d-%H-%M-%S.%f'))
    # df.ticker in SPX or SPXW

    for x in ['candle','quote']:
        tmpdf = df[(df.event_type==x)&(df.strike.isnull())]
        print(x,tmpdf.shape)
    for x in ['greeks','profile','trade','summary']:
        #tmpdf = df[(df.strike.notnull())&(df.ticker==ticker)&(df.event_type==x)]
        tmpdf = df[(df.event_type==x)&(df.strike.notnull())]
        print(x,tmpdf.shape)

    """
    candle_df = candle_df[["eventsymbol","strike","ticker", "expiration", "contracttype","volume","askvolume","bidvolume"]]
    candle_df = candle_df.groupby(["eventsymbol","strike","ticker", "expiration", "contracttype"]).sum()

    greeks_df = greeks_df[["eventsymbol","strike","ticker", "expiration", "contracttype","gamma"]]
    greeks_df = greeks_df.groupby(["eventsymbol","strike","ticker", "expiration", "contracttype"]).last()

    candle_df.reset_index(inplace=True)
    greeks_df.reset_index(inplace=True)
    
    df = greeks_df.merge(candle_df,how='left',on=["eventsymbol","strike","ticker", "expiration", "contracttype"])

    df['contract_type_int'] = df.contracttype.apply(lambda x: 1 if x=='C' else -1)
    df['gexCandleVolume'] = df['gamma'].astype(float) * df['volume'].astype(float) * 100 * price_close * price_close * 0.01 * df['contract_type_int']

    # Open Interest, but note we are using volume.
    # https://perfiliev.com/blog/how-to-calculate-gamma-exposure-and-zero-gamma-level
    #Bid Price - highest price that buyers are willing to pay for
    #Ask Price - lowest price that sellers are willing to sell at
    #Bid volume refers to transactions that happen at the bid price
    #Ask volume refers to transactions that happen at the ask price

    # i think below is what we want:
    # contract_type_int 1 , contracttype C, factor_bid: 1 factor_ask: 1
    # contract_type_int -1 , contracttype P, factor_bid: -1 factor_ask: 1
    df['factor_bid'] = df.contracttype.apply(lambda x: 1 if x=='C' else -1)
    df['factor_ask'] = df.contracttype.apply(lambda x: -1 if x=='P' else 1)
    df['gexCandleBidAskVolume'] = \
        df['gamma'].astype(float) * df['bidvolume'].astype(float) * 100 * price_close * price_close * 0.01 * df['factor_bid'] + \
        df['gamma'].astype(float) * df['askvolume'].astype(float) * 100 * price_close * price_close * 0.01 * df['factor_ask']

    gex_df = df[ (df.strike > price_close*min_prct) & (df.strike < price_close*max_prct) ]
    naive_gex = gex_df.gexCandleVolume[gex_df.gexCandleVolume.notnull()].sum()
    bidask_gex = gex_df.gexCandleBidAskVolume[gex_df.gexCandleBidAskVolume.notnull()].sum()
    # rename 
    df['naive_gex'] = df.gexCandleVolume
    df['bidask_gex'] = df.gexCandleBidAskVolume

    """

if __name__ == "__main__":
    hola_tasty()
    