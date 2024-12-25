import sys
import pathlib
import datetime
import traceback
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from py_vollib.ref_python.black_scholes_merton.implied_volatility import implied_volatility
from .misc import now_in_new_york
from .data_yahoo import (
    CACHE_FOLDER,
    BTC_TICKER,
    INDEX_TICKER_LIST,
    BTC_TICKER_LIST,
    get_cache_latest
)
from .data_cboe import (
    compute_total_gex,
    compute_gex_by_strike,
    compute_gex_by_expiration,
)

def get_iv_df(ticker,option_type,tstamp=None,is_pivot=True):
    
    underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(ticker,tstamp=tstamp)
    if "SPX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "SPXW" in x)]
    elif "NDX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "NDXP" in x)]
    else:
        df = options_df

    df = df[df.option_type==option_type]
    # TODO: fill NA with theoretical IV
    df = df.sort_values(by=['expiration', 'strike','iv'])
    df = df[['expiration','strike','iv']]
    if is_pivot:
        try:
            iv_df = df.pivot(index='expiration', columns='strike', values='iv')
        except:
            traceback.print_exc()
        return iv_df
    else:
        return df

    #contractSymbol,lastTradeDate,strike,lastPrice,bid,ask,change,percentChange,
    #volume,openInterest,iv,inTheMoney,contractSize,currency,ticker,option_type,expiration

def compute_iv(row,today):
    # option,bid,bid_size,ask,ask_size,iv,
    # open_interest,volume,delta,gamma,vega,theta,rho,theo,change,
    # open,high,low,tick,last_trade_price,last_trade_time,
    # percent_change,prev_day_close,spot_price,option_type,strike,expiration
    price = row.last_trade_price
    # :param S: underlying asset price
    S = row.spot_price
    # :param K: strike price
    K = row.strike
    # :param t: time to expiration in years
    t = (row.expiration-today).days/365
    #:param r: risk-free interest rate
    r = 0.01
    # :param q: annualized continuous dividend rate
    q = 0
    #:param flag: 'c' or 'p' for call or put.
    flag = row.option_type.lower()
    iv = implied_volatility(price, S, K, t, r, q, flag)
    return iv


def get_theo_iv_df(ticker,option_type,tstamp=None,is_pivot=True):
    today = now_in_new_york().replace(tzinfo=None)
    underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(ticker,tstamp=tstamp)
    if "SPX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "SPXW" in x)]
    elif "NDX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "NDXP" in x)]
    else:
        df = options_df
    df = df.copy(deep=True)
    df.expiration = df.expiration.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d'))
    df = df[df.option_type==option_type]
    df['theo_iv'] = df.apply(lambda row: compute_iv(row,today), axis=1)

    df = df.sort_values(by=['expiration', 'strike','iv'])
    df = df[['expiration','strike','iv']]
    if is_pivot:
        try:
            iv_df = df.pivot(index='expiration', columns='strike', values='iv')
        except:
            traceback.print_exc()
        return iv_df
    else:
        return df

def get_gex_df(ticker,tstamp=None):
    
    underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(ticker,tstamp=tstamp)
    if "SPX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "SPXW" in x)]
    elif "NDX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "NDXP" in x)]
    else:
        df = options_df
    df.expiration = df.expiration.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d'))
    
    try:
        spot = df.spot_price[0]
    except:
        print(underlying_dict['currentPrice'])
        traceback.print_exc()
    total_gex = compute_total_gex(spot,df)
    print(f'total gex {total_gex}')
    compute_gex_by_strike(spot,df)
    compute_gex_by_expiration(df)
    return df

if __name__ == "__main__":
    # for ticker in INDEX_TICKER_LIST:
    # for option_type in ["C","P"]:
    ticker = sys.argv[1]
    option_type = sys.argv[2]

    #df = get_iv_df(ticker,option_type,is_pivot=False)
    df = get_theo_iv_df(ticker,option_type)
    df.to_csv("ok.csv")
    for x in sorted(df.expiration.unique()):
        rowdf = df[df.expiration==x]
        plt.scatter(rowdf.strike,rowdf.iv,label=x)
    plt.grid(True)
    plt.legend()
    plt.title(f'{ticker} {option_type}')
    plt.savefig(f'ok-{ticker}-{option_type}')

"""

python -m utils.compute MSTR C

"""
