
import datetime
import pytz
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

import py_vollib.black_scholes_merton.implied_volatility
import py_vollib.black_scholes_merton
import py_vollib_vectorized

import scipy.interpolate as interpolate

from .misc import get_market_open_close


TOTAL_SECONDS_ONE_YEAR = 365*24*60*60 # total seconds

def get_expiry_tstamp(expiry):
    if not isinstance(expiry,str):
        return np.nan
    expiry = datetime.datetime.strptime(expiry,"%Y-%m-%d")
    _,expiry_tstamp = get_market_open_close(expiry)
    return expiry_tstamp.replace(tzinfo=None)

def get_annualized_time_to_expiration(row,expiry_mapper):
    if isinstance(row.expiration,str):
        expiry = row.expiration
    else:
        expiry = row.expiration.strftime("%Y-%m-%d")
    expiry_tstamp = expiry_mapper[expiry]
    sec_to_expiration = (expiry_tstamp-row.tstamp).total_seconds()
    atte = sec_to_expiration/TOTAL_SECONDS_ONE_YEAR
    return atte

#
# https://www.stephendiehl.com/posts/volatility_surface
# 
# NOTE: THIS METHOD interp_implied_volatility IS CRUDE AND VERY WRONG
# 
# see doc/hau.0fcbcd78dd6272834a38.pdf
# see doc/vol-surface 


def interp_implied_volatility(df,s=None,return_fine=False):
    # TODO: replace with domokane/FinancePy, support multi expiry
    raise ValueError("replace with domokane/FinancePy")
    assert(len(df.contract_type.unique())==1)
    assert(len(df.expiration.unique())==1)

    df = df.sort_values(["strike"])
    # Prepare interpolation data
    y = df.tte
    x = df.strike
    z = df.iv
    # Perform interpolation
    spline = interpolate.UnivariateSpline(
        x, z, s=s
    )
    if return_fine:
        x = df.strike
        x_new = np.linspace(x.min(), x.max(), 100)
        z_smooth = spline(x_new)
        iv_df = pd.DataFrame({'iv':z_smooth,'strike':x_new})
        iv_df['contract_type']=contract_type
        return iv_df
    else:
        df['iv']=spline(df.strike)
        return df

def compute_theo_price(df,df_call_symbol='C'):
    flag = df.contract_type.apply(lambda x: 'c' if x == df_call_symbol else 'p')
    S = df.spot_price.astype(np.float16)
    K = df.strike.astype(np.float16)
    t = df.tte.astype(np.float16)
    r = 0.0 # interest rate
    sigma = df.iv
    q = 0 # annualized continuous dividend yield.
    theo_price = py_vollib.black_scholes_merton.black_scholes_merton(flag, S, K, t, r, sigma, q, return_as='numpy')
    df['theo_price'] = theo_price
    return df

def compute_iv(df):
    price = df.price.astype(np.float16)
    flag = df.contract_type.apply(lambda x: 'c' if x == 'C' else 'p')
    S = df.spot_price.astype(np.float16)
    K = df.strike.astype(np.float16)
    t = df.tte.astype(np.float16)
    r = 0.0 # interest rate
    bsm_iv = py_vollib.black_scholes_merton.implied_volatility.implied_volatility(price, S, K, t, r, flag, q=0, return_as='numpy')
    df['iv'] = bsm_iv
    return df
    
"""

from .iv_utils import get_expiry_tstamp,get_annualized_time_to_expiration,compute_theo_price,compute_iv,interp_implied_volatility
# compute and interpolate IV from price for put and calls
# then determine side by checking if price is above or below theoretical price
expiration_series = ts_df.expiration[ts_df.expiration.notnull()]
expiry_mapper = {x.strftime("%Y-%m-%d"):get_expiry_tstamp(x.strftime("%Y-%m-%d"))  for x in list(expiration_series.unique())}
ts_df['tte'] = ts_df.apply(lambda x: get_annualized_time_to_expiration(x,expiry_mapper),axis=1)
ts_df['spot_price'] = spot_price
ts_df = compute_iv(ts_df)
call_ts_df = interp_implied_volatility(ts_df[ts_df.contract_type=='C'].copy())
puts_ts_df = interp_implied_volatility(ts_df[ts_df.contract_type=='P'].copy())
ts_df = pd.concat([call_ts_df,puts_ts_df])
ts_df = compute_theo_price(ts_df)
ts_df['theo_aggressor_side'] = np.where(ts_df['price']>=ts_df['theo_price'], 'BUY', 'SELL')

"""