import sys
import pathlib
import datetime
import traceback
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.interpolate import griddata

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

def _get_iv_df(ticker,option_type,tstamp=None,is_pivot=True):
    
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


def myinterp(array, scale=1, method='cubic'):
    x = np.arange(array.shape[1]*scale)[::scale]
    y = np.arange(array.shape[0]*scale)[::scale]
    x_in_grid, y_in_grid = np.meshgrid(x,y)
    x_out, y_out = np.meshgrid(np.arange(max(x)+1),np.arange(max(y)+1))
    array = np.ma.masked_invalid(array)
    x_in = x_in_grid[~array.mask]
    y_in = y_in_grid[~array.mask]
    zi = griddata((x_in, y_in), array[~array.mask].reshape(-1),(x_out, y_out), method=method)
    return zi

def get_iv_df(ticker,option_type,tstamp=None,is_pivot=True,interp=True):
    df = _get_iv_df(ticker,option_type,tstamp=tstamp,is_pivot=is_pivot)
    if is_pivot is True and interp is True:
        arr = df.to_numpy()
        arr = myinterp(arr)
        df = pd.DataFrame(data=arr,index=df.index,columns=df.columns)
    return df

def gex_test(ticker):
    print("ok")

def iv_test(ticker,option_type):
    df = get_iv_df(ticker,option_type,is_pivot=True,interp=False)
    df.to_csv(f"ok-{ticker}-{option_type}.csv")
    arr = df.to_numpy()
    arr[arr==0]=np.nan
    print(np.nanmax(arr),np.nanmedian(arr),np.nanmin(arr),np.sum(np.isnan(arr)))
    print(arr.shape)
    df.to_csv(f"ok-{ticker}-{option_type}-interp.csv")
    df = get_iv_df(ticker,option_type,is_pivot=True,interp=True)
    arr = df.to_numpy()
    print(np.nanmax(arr),np.nanmedian(arr),np.nanmin(arr),np.sum(np.isnan(arr)))
    print(arr.shape)
    print("---")
    plt.imshow(arr)
    plt.grid(True)
    plt.colorbar()
    plt.title(f'{ticker} {option_type}')
    plt.savefig(f'ok-{ticker}-{option_type}.png')

if __name__ == "__main__":
    ticker = sys.argv[1]
    option_type = sys.argv[2]
    action = sys.argv[3]
    if action == 'iv':
        iv_test(ticker,option_type)
    if action == 'gex':
        gex_test(ticker)
"""

python -m utils.compute MSTR C iv
python -m utils.compute MSTR C gex

"""
