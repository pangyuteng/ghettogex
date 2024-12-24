import sys
import pathlib
import datetime
import traceback
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from py_vollib.ref_python.black_scholes_merton import black_scholes_merton
from .data_yahoo import (
    CACHE_FOLDER,
    BTC_TICKER,
    INDEX_TICKER_LIST,
    BTC_TICKER_LIST,
    get_cache_latest
)

def get_iv_df(ticker,option_type,tstamp=None):
    
    underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(ticker,tstamp=tstamp)
    if "SPX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "SPXW" in x)]
    if "NDX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "NDXP" in x)]
    df = df[df.option_type==option_type]
    # TODO: fill NA with theoretical IV
    df.to_csv("ok.csv")
    df = df.sort_values(by=['expiration', 'strike','iv'])
    df = df[['expiration','strike','iv']]
    print(df.strike)
    try:
        iv_df = df.pivot(index='expiration', columns='strike', values='iv')
    except:
        traceback.print_exc()
    return iv_df

    #contractSymbol,lastTradeDate,strike,lastPrice,bid,ask,change,percentChange,
    #volume,openInterest,iv,inTheMoney,contractSize,currency,ticker,option_type,expiration

if __name__ == "__main__":
    ticker = sys.argv[1]
    option_type = sys.argv[2]
    iv_df = get_iv_df(ticker,option_type)
    iv_df.to_csv("ok.csv")
    for n,row in iv_df.iterrows():
        print(row)
    print(iv_df.shape)
    plt.imshow(iv_df)
    plt.title(ticker)
    plt.colorbar()
    plt.savefig("ok.png")

"""

python -m utils.plotter MSTR C

"""