import logging
logger = logging.getLogger(__file__)
import sys
import pathlib
import datetime
import traceback
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.interpolate import griddata

from py_vollib.ref_python.black_scholes_merton.implied_volatility import implied_volatility

from .data_cache import (
    BTC_TICKER,
    INDEX_TICKER_LIST,
    BTC_TICKER_LIST,
    BTC_MSTR_TICKER_LIST,
    get_cache_latest
)
from .data_cboe import (
    compute_total_gex,
    compute_gex_by_strike,
    compute_gex_by_expiration,
    compute_gex_surface,
)


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

def iv_test(ticker,option_type):
    df = get_iv_df(ticker,option_type,is_pivot=True,interp=False)
    df.to_csv(f"ok-{ticker}-{option_type}.csv")
    arr = df.to_numpy()
    arr[arr==0]=np.nan
    logger.debug(np.nanmax(arr),np.nanmedian(arr),np.nanmin(arr),np.sum(np.isnan(arr)))
    logger.debug(arr.shape)
    df.to_csv(f"ok-{ticker}-{option_type}-interp.csv")
    df = get_iv_df(ticker,option_type,is_pivot=True,interp=True)
    arr = df.to_numpy()
    logger.debug(np.nanmax(arr),np.nanmedian(arr),np.nanmin(arr),np.sum(np.isnan(arr)))
    logger.debug(arr.shape)
    logger.debug("---")
    plt.imshow(arr)
    plt.grid(True)
    plt.colorbar()
    plt.title(f'{ticker} {option_type}')
    plt.savefig(f'ok-{ticker}-{option_type}.png')

def get_gex_df(ticker,tstamp=None,save_png=False):
    
    underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(ticker,tstamp=tstamp)
    if "SPX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "SPXW" in x)].copy(deep=True)
    elif "NDX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "NDXP" in x)].copy(deep=True)
    else:
        df = options_df.copy(deep=True)
    df.expiration = df.expiration.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d'))
    df = df.reset_index()
    try:
        spot_price = df.loc[0,'spot_price']
    except:
        traceback.print_exc()
        spot_price = np.nan

    total_gex = compute_total_gex(spot_price,df)
    if save_png:
        logger.debug(total_gex)
    gex_by_strike = compute_gex_by_strike(spot_price,df,ticker=ticker,save_png=save_png)
    gex_by_expiration = compute_gex_by_expiration(df,ticker=ticker,save_png=save_png)
    gex_df = compute_gex_surface(spot_price,df,ticker=ticker,save_png=save_png)

    return spot_price, gex_by_strike, gex_by_expiration, gex_df

def gex_test(ticker):
    spot_price, gex_by_strike, gex_by_expiration, gex_df = get_gex_df(ticker,save_png=True)
    logger.debug(gex_by_strike)
    logger.debug(gex_by_strike.shape)
    logger.debug(gex_by_expiration.shape)
    logger.debug(gex_df.shape)
    gex_by_strike.plot()
    plt.savefig(f'ok-hohoho-{ticker}.png')

def round_nearest(x, a):
    return np.round(x / a) * a

ROUND_UP_UNIT = 500
def compute_btc_gex(tstamp=None,save_png=False):
    underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(BTC_TICKER,tstamp=tstamp)
    btc_spot_price = underlying_dict['last_price']
    ticker_list = BTC_MSTR_TICKER_LIST
    gex_by_strike_list = []
    for ticker in ticker_list:
        underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(ticker,tstamp=tstamp)
        row_df = options_df
        row_df.expiration = row_df.expiration.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d'))
        row_df = row_df.reset_index()
        try:
            spot_price = row_df.loc[0,'spot_price']
            compute_total_gex(spot_price, row_df)
            gex_by_strike = compute_gex_by_strike(spot_price,row_df,lim='large',save_png=save_png)
            logger.debug(f"---- {ticker}")
            strike_list = gex_by_strike['strike'].values
            gex_list = gex_by_strike['gex'].values
            moneyness_list = strike_list/spot_price
            btc_moneyness_list = round_nearest(moneyness_list*btc_spot_price, ROUND_UP_UNIT)
            
            for strike,gex in zip(btc_moneyness_list,gex_list):
                gex_by_strike_list.append(dict(
                    ticker=ticker,
                    strike=strike,
                    gex=gex,
                ))

            # TODO :
            # normalize expiration to year ??
            # then sum up gex per (strike,expiration)
            expiration_list = []
            gex_by_expiration = compute_gex_by_expiration(row_df,ticker=ticker,save_png=save_png)
            gex_df = compute_gex_surface(spot_price,row_df,ticker=ticker,save_png=save_png)
            logger.debug(f'{gex_df.shape}')
            logger.debug(gex_df.columns)
            logger.debug(gex_df.index)

        except:
            traceback.print_exc()

    df = pd.DataFrame(gex_by_strike_list)
    df = df[['strike','gex']]
    df = df.groupby(['strike'],as_index=False).sum()
    total_gex = df['gex'].sum()
    if save_png:
        df.to_csv("ok.csv",index=False)
        for n,row in df.iterrows():
            plt.plot([0,row.gex],[row.strike,row.strike], linewidth=2, color='blue')

        plt.axhline(btc_spot_price,color='red',linewidth=1)
        plt.locator_params(axis='y', nbins=20)
        plt.locator_params(axis='x', nbins=20)
        plt.xticks(rotation=45)
        plt.title(f'total_gex: {total_gex:1.3f} Bn\ncombined {BTC_MSTR_TICKER_LIST}\n spot: {btc_spot_price:1.2f}(red)')
        plt.grid(True)
        plt.ylabel("BTC strike")
        plt.xlabel("GEX (Bn)")
        plt.tight_layout()
        plt.savefig("ok.png")
    return btc_spot_price, df

if __name__ == "__main__":
    ticker = sys.argv[1]
    option_type = sys.argv[2]
    action = sys.argv[3]
    if action == 'iv':
        iv_test(ticker,option_type)
    if action == 'gex':
        gex_test(ticker)
    if action == 'btcgex':
        compute_btc_gex(save_png=True)
"""

python -m utils.compute MSTR C iv
python -m utils.compute MSTR C gex
python -m utils.compute null null btcgex

"""
