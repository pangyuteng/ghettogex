import traceback
import os
import sys
import datetime
from datetime import timezone
import pytz
import time
import pathlib
import tempfile
from tqdm import tqdm
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
import pandas_market_calendars as mcal

def get_market_open_close(day_stamp,no_tzinfo=True):
    nyse = mcal.get_calendar('NYSE')
    early = nyse.schedule(start_date=day_stamp, end_date=day_stamp)
    market_open = list(early.to_dict()['market_open'].values())[0]
    market_close = list(early.to_dict()['market_close'].values())[0]
    if no_tzinfo:
        return market_open.replace(tzinfo=None),market_close.replace(tzinfo=None)
    else:
        return market_open,market_close

TOTAL_SECONDS_ONE_YEAR = 365*24*60*60 # total seconds

BOT_EOD_ROOT = "/mnt/hd2/data/finance/bot-eod-zip"
CACHE_FOLDER = "/mnt/hd1/data/uw-options-cache"

def get_open_close(pq_file):
    ticker = 'SPX'
    day_stamp = os.path.basename(pq_file).replace(".parquet.gzip","")
    market_open,market_close = get_market_open_close(day_stamp,no_tzinfo=True)
    df = pd.read_parquet(pq_file)

    df = df[(df.tstamp_sec >= market_open)&(df.tstamp_sec <= market_close)&(df.expiry==day_stamp)]
    df = df.sort_values(['tstamp']).reset_index()

    price_df = df[['underlying_price','tstamp_sec']]
    price_df = price_df.groupby(['tstamp_sec']).agg(
        underlying_price=pd.NamedAgg(column="underlying_price", aggfunc="last"),
    ).resample('1s').ffill().reset_index()
    price_list = price_df.underlying_price.to_list()
    price_open = price_list[0]
    price_high = np.nanmax(price_list)
    price_low = np.nanmin(price_list)
    price_close = price_list[-1]

    first_few_min = market_close - datetime.timedelta(minutes=1)
    first_few_df = df[(df.tstamp_sec<first_few_min)]
    idx = np.abs(first_few_df.strike-price_open).argmin()
    row = df.loc[idx,:]
    atm_df = first_few_df[first_few_df.strike==row.strike]
    atm_iv = np.nanmean(atm_df.implied_volatility)

    vix_pq_file = pq_file.replace("SPX","VIX")
    vix_df = pd.read_parquet(vix_pq_file)
    vix_df = vix_df[(vix_df.tstamp_sec >= market_open)&(vix_df.tstamp_sec <= market_close)]
    vix_df = vix_df.sort_values(['tstamp']).reset_index()
    vix_df = vix_df.sort_values(['tstamp']).reset_index()
    vix_price_list = vix_df.underlying_price.to_list()
    vix_open = vix_price_list[0]
    vix_high = np.nanmax(price_list)
    vix_low = np.nanmin(price_list)
    vix_close = vix_price_list[-1]

    return day_stamp,price_open,price_high,price_low,price_close,vix_open,vix_high,vix_low,vix_close

def process(pq_file):
    ticker = 'SPX'
    day_stamp = os.path.basename(pq_file).replace(".parquet.gzip","")
    market_open,market_close = get_market_open_close(day_stamp,no_tzinfo=True)
    print(market_open,market_close)
    df = pd.read_parquet(pq_file)
    print(df.shape)

    df = df[(df.tstamp_sec >= market_open)&(df.tstamp_sec <= market_close)&(df.expiry==day_stamp)]
    df = df.sort_values(['tstamp']).reset_index()
    print(df.shape)

    price_df = df[['underlying_price','tstamp_sec']]
    price_df = price_df.groupby(['tstamp_sec']).agg(
        underlying_price=pd.NamedAgg(column="underlying_price", aggfunc="last"),
    ).resample('1s').ffill().reset_index()
    price_open = price_df.underlying_price.to_list()[0]
    price_close = price_df.underlying_price.to_list()[-1]
    print(f'price_open: {price_open}, price_close: {price_close}')

    first_few_min = market_close - datetime.timedelta(minutes=1)
    last_few_df = df[(df.tstamp_sec<first_few_min)]
    idx = np.abs(last_few_df.strike-price_open).argmin()
    row = df.loc[idx,:]

    last_few_min = market_close - datetime.timedelta(minutes=5)
    last_few_df = df[(df.tstamp_sec>last_few_min)&(df.nbbo_ask <= 0.05)&(df.nbbo_bid <= 0.05)]
    print(last_few_min)

    worthless_call_strike = last_few_df[(last_few_df.option_type=='call')].strike.min()
    worthless_put_strike = last_few_df[(last_few_df.option_type=='put')].strike.max()
    print(f'worthless_call_strike {worthless_call_strike}')
    print(f'worthless_put_strike {worthless_put_strike}')

    worthless_call_df = df[(df.option_type=='call')&(df.strike==worthless_call_strike)].reset_index()
    worthless_call_df = worthless_call_df[['price','tstamp_sec']]
    worthless_call_df = worthless_call_df.groupby(['tstamp_sec']).agg(
        price=pd.NamedAgg(column="price", aggfunc="last"),
    ).resample('1s').ffill().reset_index()

    worthless_put_df = df[(df.option_type=='put')&(df.strike==worthless_put_strike)].reset_index()
    worthless_put_df = worthless_put_df[['price','tstamp_sec']]
    worthless_put_df = worthless_put_df.groupby(['tstamp_sec']).agg(
        price=pd.NamedAgg(column="price", aggfunc="last"),
    ).resample('1s').ffill().reset_index()
    print(worthless_call_df.shape)
    print(worthless_put_df.shape)
    if True:
        sns.lineplot(data=worthless_call_df,x='tstamp_sec',y='price')
        sns.lineplot(data=worthless_put_df,x='tstamp_sec',y='price')
        title=f'{day_stamp} {ticker} price_open: {price_open}, price_close: {price_close}'
        plt.grid(True)
        plt.title(title)
        plt.show()
        plt.savefig("price.png")
        plt.close()
    
    if False:
        sns.lineplot(data=price_df,x='tstamp_sec',y='underlying_price')
        title=f'{day_stamp} {ticker} price_open: {price_open}, price_close: {price_close}'
        plt.grid(True)
        plt.title(title)
        plt.show()
        plt.savefig("price.png")
        plt.close()

    # executed_at', 'underlying_symbol', 'option_chain_id', 'side', 'strike',
    #        'option_type', 'expiry', 'underlying_price', 'nbbo_bid', 'nbbo_ask',
    #        'ewma_nbbo_bid', 'ewma_nbbo_ask', 'price', 'size', 'premium', 'volume',
    #        'open_interest', 'implied_volatility', 'delta', 'theta', 'gamma',
    #        'vega', 'rho', 'theo', 'sector', 'exchange', 'report_flags', 'canceled',
    #        'upstream_condition_detail', 'equity_type', 'tstamp', 'tstamp_sec'],
    #       dtype='object'

def main():
    myfolder = "/mnt/hd1/data/uw-options-cache/SPX"
    pq_file_list = sorted([str(x) for x in pathlib.Path(myfolder).rglob("*parquet.gzip")])
    mylist = []
    for pq_file in pq_file_list:
        day_stamp,price_open,price_high,price_low,price_close,vix_open,vix_high,vix_low,vix_close = get_open_close(pq_file)

        item = dict(
            tstamp=day_stamp,
            spx_open=price_open,
            spx_high=price_high,
            spx_low=price_low,
            spx_close=price_close,
            vix_open=vix_open,
            vix_high=vix_high,
            vix_low=vix_low,
            vix_close=vix_close
            )
        mylist.append(item)
    df = pd.DataFrame(mylist)
    df.to_csv("ohlc-spx-vix.price")

if __name__ == "__main__":
    main()
    # pq_file = "/mnt/hd1/data/uw-options-cache/SPX/2025-08-07.parquet.gzip"
    # process(pq_file)
    

"""
def format_stamp(x):
    if '.' in x:
        return datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S.%f+00')
    else:
        return datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S+00')

et_tz = "US/Eastern"
day_stamp
day_stamp_str
pq_file = os.path.join(CACHE_FOLDER,ticker,f"{tstamp_from_file}.parquet.gzip")
tstamp_from_file = os.path.basename(zip_file).replace("bot-eod-report-","").replace(".zip","")
"""

"""

docker run -it -w $PWD -v /mnt:/mnt -p 8888:8888 fi-notebook:latest bash

python locate_expiring_contracts_uw.py SPX

"""