
import datetime
import pytz
import numpy as np

import pandas_market_calendars as mcal

import py_vollib.black_scholes_merton.implied_volatility
import py_vollib.black_scholes_merton
import py_vollib_vectorized

import scipy.interpolate as interpolate



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

def get_expiry_tstamp(expiry):
    if not isinstance(expiry,str):
        return np.nan
    expiry = datetime.datetime.strptime(expiry,"%Y-%m-%d")
    _,expiry_tstamp = get_market_open_close(expiry)
    return expiry_tstamp.replace(tzinfo=None)

def get_annualized_time_to_expiration(expiration,tstamp_sec,expiry_mapper):
    expiry_tstamp = expiry_mapper[expiration]
    sec_to_expiration = (expiry_tstamp-tstamp_sec).total_seconds()
    atte = sec_to_expiration/TOTAL_SECONDS_ONE_YEAR
    return atte



#
# https://www.stephendiehl.com/posts/volatility_surface
# 
# NOTE: THIS METHOD interp_implied_volatility IS CRUDE AND VERY WRONG
# 
# see doc/hau.0fcbcd78dd6272834a38.pdf
# 
def interp_implied_volatility(df,s=None,return_fine=False):
    # Prepare interpolation data
    df = df.sort_values(["strike"])
    y = df.tte # only 1 kind of expiration.
    x = df.strike
    z = df.iv
    x_new = x
    # Perform interpolation
    spline = interpolate.UnivariateSpline(
        x, z, s=s
    )
    if return_fine:
        x_new = np.linspace(x.min(), x.max(), 100)
        z_smooth = spline(x_new)
        iv_df = pd.DataFrame({'iv':z_smooth,'strike':x_new,'tte':np.nan})
        return iv_df
    else:
        z_smooth = spline(x)
        df['interp_iv'] = spline(x)
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
    df['theo_aggressor_side'] = np.where(df['price']>=df['theo_price'], 'BUY', 'SELL')
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
    
