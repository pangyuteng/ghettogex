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

min_prct,max_prct = 0.96,1.04
def get_gex(df,tstamp_reduced,ticker,ticker_variants):
    
    # ['candle','quote']
    # underlying spot price via candle events
    udf = df[(df.tstamp_reduced==tstamp_reduced)&(df.event_type=='candle')&(df.strike.isnull())&(df.streamer_symbol==ticker)]
    udf = udf.sort_values(['tstamp']).reset_index()
    spot_price = np.nan
    if len(udf) > 0:
        spot_price = float(udf.iloc[-1].close)
        # print(tstamp_reduced,spot_price)

    # ['greeks','profile','trade','summary']
    # sum options volumes via candle events
    event_type = 'candle'
    cdf = df[(df.tstamp_reduced==tstamp_reduced)&(df.event_type==event_type)&(df.strike.notnull())&(df.ticker.apply(lambda x: x in ticker_variants))]
    cdf = cdf[["streamer_symbol","strike","ticker", "expiration", "contract_type","volume","ask_volume","bid_volume"]]
    cdf = cdf.groupby(["streamer_symbol","strike","ticker", "expiration", "contract_type"]).sum()
    cdf = cdf.reset_index()

    event_type = 'greeks'
    gdf = df[(df.tstamp_reduced==tstamp_reduced)&(df.event_type==event_type)&(df.strike.notnull())&(df.ticker.apply(lambda x: x in ticker_variants))]
    gdf = gdf.sort_values(['tstamp'])
    gdf = gdf[["streamer_symbol","strike","ticker", "expiration", "contract_type","gamma"]]
    gdf = gdf.groupby(["streamer_symbol","strike","ticker", "expiration", "contract_type"]).last()
    gdf = gdf.reset_index()

    idf = gdf.merge(cdf,how='left',on=["streamer_symbol","strike","ticker", "expiration", "contract_type"])
    idf['contract_type_int'] = idf.contract_type.apply(lambda x: 1 if x=='C' else -1)
    idf['gex_volume'] = idf['gamma'].astype(float) * idf['volume'].astype(float) * 100 * spot_price * spot_price * 0.01 * idf['contract_type_int']

    # Open Interest, but note we are using volume.
    # https://perfiliev.com/blog/how-to-calculate-gamma-exposure-and-zero-gamma-level
    # A crude approximation is that the dealers are long the calls and short the puts,

    #Ask Price - lowest price that sellers are willing to sell at
    #Bid Price - highest price that buyers are willing to pay for
    #Ask volume refers to transactions that happen at the ask price
    #Bid volume refers to transactions that happen at the bid price

    #assumption is dealer long call, 1, (near ask)
    #assumption is dealer short put, -1 (near bid)

    # i think below is what we want:
    # contract_type_int 1 , contract_type C, factor_bid: 1 factor_ask: 1
    # contract_type_int -1 , contract_type P, factor_bid: -1 factor_ask: 1
    idf['factor_bid_volume'] = idf.contract_type.apply(lambda x: -1 if x=='P' else 1)
    idf['factor_ask_volume'] = idf.contract_type.apply(lambda x: 1 if x=='C' else -1)
    idf['gex_bid_ask_volume'] = \
        idf['gamma'].astype(float) * idf['ask_volume'].astype(float) * 100 * spot_price * spot_price * 0.01 * idf['factor_ask_volume'] + \
        idf['gamma'].astype(float) * idf['bid_volume'].astype(float) * 100 * spot_price * spot_price * 0.01 * idf['factor_bid_volume']

    tmpdf = idf[ (idf.strike > spot_price*min_prct) & (idf.strike < spot_price*max_prct) ]
    naive_gex = tmpdf.gex_volume[tmpdf.gex_volume.notnull()].sum()
    bidask_gex = tmpdf.gex_bid_ask_volume[tmpdf.gex_bid_ask_volume.notnull()].sum()
    print(naive_gex,bidask_gex)
    return dict(
        tstamp_reduced=tstamp_reduced,
        spot_price=spot_price,
        naive_gex=naive_gex,
        bidask_gex=bidask_gex,
    )

def hola_tasty():

    ticker = 'SPX'
    tstamp = datetime.datetime(2024,12,30)
    date_stamp_str = tstamp.strftime("%Y-%m-%d")

    if ticker == 'SPX':
        ticker_variants = ["SPX","SPXW"]
    else:
        ticker_variants = [ticker]

    pq_file = f'{ticker}-{date_stamp_str}.parquet.gzip'
    print(pq_file)
    if not os.path.exists(pq_file):
        df = asyncio.run(main(ticker,tstamp,pq_file))
    else:
        df = pd.read_parquet(pq_file)

    gex_csv_file = f'{ticker}-{date_stamp_str}-gex.csv'
    if not os.path.exists(gex_csv_file):
        df.tstamp = df.tstamp.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d-%H-%M-%S.%f'))
        df['tstamp_reduced'] = df.tstamp.apply(lambda x: x.replace(second=0,microsecond=0))
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df['bid_volume'] = pd.to_numeric(df['bid_volume'], errors='coerce')
        df['ask_volume'] = pd.to_numeric(df['ask_volume'], errors='coerce')
        df['gamma'] = pd.to_numeric(df['gamma'], errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')


        mylist = []
        for tstamp_reduced in sorted(df.tstamp_reduced.unique()):
            try:
                row_dict = get_gex(df,tstamp_reduced,ticker,ticker_variants)
                print(row_dict)
                mylist.append(row_dict)
            except KeyboardInterrupt:
                sys.exit(1)
            except:
                traceback.print_exc()
                pass
        gex_df = pd.DataFrame(mylist)
        gex_df.to_csv(gex_csv_file,index=False)

    """
    candle_df = candle_df[["streamer_symbol","strike","ticker", "expiration", "contract_type","volume","askvolume","bidvolume"]]
    candle_df = candle_df.groupby(["streamer_symbol","strike","ticker", "expiration", "contract_type"]).sum()

    greeks_df = greeks_df[["streamer_symbol","strike","ticker", "expiration", "contract_type","gamma"]]
    greeks_df = greeks_df.groupby(["streamer_symbol","strike","ticker", "expiration", "contract_type"]).last()

    candle_df.reset_index(inplace=True)
    greeks_df.reset_index(inplace=True)
    
    df = greeks_df.merge(candle_df,how='left',on=["streamer_symbol","strike","ticker", "expiration", "contract_type"])

    df['contract_type_int'] = df.contract_type.apply(lambda x: 1 if x=='C' else -1)
    df['gex_volume'] = df['gamma'].astype(float) * df['volume'].astype(float) * 100 * spot_price * spot_price * 0.01 * df['contract_type_int']

    # Open Interest, but note we are using volume.
    # https://perfiliev.com/blog/how-to-calculate-gamma-exposure-and-zero-gamma-level
    #Bid Price - highest price that buyers are willing to pay for
    #Ask Price - lowest price that sellers are willing to sell at
    #Bid volume refers to transactions that happen at the bid price
    #Ask volume refers to transactions that happen at the ask price

    # i think below is what we want:
    # contract_type_int 1 , contract_type C, factor_bid: 1 factor_ask: 1
    # contract_type_int -1 , contract_type P, factor_bid: -1 factor_ask: 1
    df['factor_bid'] = df.contract_type.apply(lambda x: 1 if x=='C' else -1)
    df['factor_ask'] = df.contract_type.apply(lambda x: -1 if x=='P' else 1)
    df['gex_bid_ask_volume'] = \
        df['gamma'].astype(float) * df['bidvolume'].astype(float) * 100 * spot_price * spot_price * 0.01 * df['factor_bid'] + \
        df['gamma'].astype(float) * df['askvolume'].astype(float) * 100 * spot_price * spot_price * 0.01 * df['factor_ask']

    gex_df = df[ (df.strike > spot_price*min_prct) & (df.strike < spot_price*max_prct) ]
    naive_gex = gex_df.gex_volume[gex_df.gex_volume.notnull()].sum()
    bidask_gex = gex_df.gex_bid_ask_volume[gex_df.gex_bid_ask_volume.notnull()].sum()
    # rename 
    df['naive_gex'] = df.gex_volume
    df['bidask_gex'] = df.gex_bid_ask_volume

    """

if __name__ == "__main__":
    hola_tasty()
    