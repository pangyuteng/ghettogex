
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
    vix_high = np.nanmax(vix_price_list)
    vix_low = np.nanmin(vix_price_list)
    vix_close = vix_price_list[-1]

    return day_stamp,price_open,price_high,price_low,price_close,vix_open,vix_high,vix_low,vix_close

csv_file = "ohlc-spx-vix.csv"
def generate():
    myfolder = "/mnt/hd1/data/uw-options-cache/SPX"
    pq_file_list = sorted([str(x) for x in pathlib.Path(myfolder).rglob("*parquet.gzip")])
    mylist = []
    for pq_file in tqdm(pq_file_list):
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
    df.to_csv(csv_file,index=False)

def main():
    df = pd.read_csv(csv_file)
    df = df.sort_values(['tstamp']).reset_index()
    df = df.dropna()
    min_tstamp = df.tstamp.to_list()[0]
    max_tstamp = df.tstamp.to_list()[1]
    print(df.head())
    print(df.shape)

    df['prct_change'] = 100*(df.spx_close-df.spx_open)/df.spx_open

    # # https://www.tastylive.com/concepts-strategies/implied-volatility-rank-percentile
    def iv_rank(w):
        return 100* ( w.iloc[-1] - np.min(w) ) / (np.max(w)-np.min(w))

    df['iv_rank'] =df.vix_open.rolling(252).apply(iv_rank)


    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    sns.scatterplot(df,x='vix_open',y='prct_change',alpha=0.5,size=0.2,ax=ax,legend=False)
    
    historicaldf = pd.read_csv("SPX.csv")
    hist_prct_change = (historicaldf.Close-historicaldf.Open)/historicaldf.Open
    #np.percentile(hist_prct_change,[0.5])
    # 67,49,38,30,23,15
    # plt.plot([15,20,25,30,35,40],[])
    ax.set_yscale('symlog')

    plt.xlabel('vix open price (same day)')
    plt.ylabel('spx daily prct change')
    plt.title(f"n={len(df)}, {min_tstamp} to {max_tstamp} \n h/t Jiajun & LIZJNY Tastylive")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("prct_change_uw.png")

    if False:
        ax = fig.add_subplot(2,1,2)
        sns.scatterplot(df,x='iv_rank',y='prct_change',alpha=0.5,size=0.2,ax=ax)
        ax.set_yscale('symlog')
        plt.xlabel('IV rank')
        plt.ylabel('spx daily prct change')
        plt.grid(True)
        plt.tight_layout()

        plt.savefig("prct_change_uw.png")
        plt.close()


        fig = plt.figure()
        plt.subplot(311)
        plt.plot(df.tstamp,df.spx_close)
        plt.title("SPX")
        plt.grid(True)
        plt.subplot(312)
        plt.plot(df.tstamp,df.vix_close)
        plt.title("VIX")
        plt.grid(True)
        plt.subplot(313)
        plt.plot(df.tstamp,df.iv_rank)
        plt.title("IV rank")
        plt.grid(True)
        plt.savefig("price.png")
        plt.close()



if __name__ == "__main__":
    if not os.path.exists(csv_file):
        generate()
    main()


"""

docker run -it -w $PWD -v /mnt:/mnt -p 8888:8888 fi-notebook:latest bash

"""
