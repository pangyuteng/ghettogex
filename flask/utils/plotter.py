import sys
import pathlib
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from .data_yahoo import (
    CACHE_FOLDER,
    BTC_TICKER,
    INDEX_TICKER_LIST,
    BTC_TICKER_LIST,
    get_cache_latest
)

def get_iv_df(ticker,option_type,tstamp=None):
    
    underlying_dict,options_df,last_json_file,csv_json_file = get_cache_latest(ticker,tstamp=tstamp)

    df = options_df[options_df.option_type==option_type]
    df = df[['expiry','strike','impliedVolatility']]
    df = df.sort_values(by=['expiry', 'strike'])

    iv_df = df.pivot(index='expiry', columns='strike', values='impliedVolatility')
    
    return iv_df

    #contractSymbol,lastTradeDate,strike,lastPrice,bid,ask,change,percentChange,
    #volume,openInterest,impliedVolatility,inTheMoney,contractSize,currency,ticker,option_type,expiry

if __name__ == "__main__":
    ticker = sys.argv[1]
    iv_df = get_iv_df(ticker,'call')
    iv_df.to_csv("ok.csv")
    for n,row in iv_df.iterrows():
        print(row)
    print(iv_df.shape)
    plt.imshow(iv_df)
    plt.title(ticker)
    plt.colorbar()
    plt.savefig("ok.png")

"""

python -m utils.plotter MSTR

"""