
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

from .gflow_stats import calc_dp_cdf_pdf,calc_delta_ex,calc_gamma_ex,calc_vanna_ex,calc_charm_ex

import ctypes
from numba import vectorize, njit
from numba.types import float64, string

@njit(
    float64[:, :](
        float64[:, :],
        float64[:],
        float64[:],
        float64[:],
        float64[:, :],
        float64[:, :],
    )
)
def calc_vanna(S, vol, T, q, dp, pdf_dp):
    dm = dp - vol * np.sqrt(T)
    vanna = -np.exp(-q * T) * pdf_dp * (dm / vol)
    # change in delta per one percent move in IV
    # or change in vega per one percent move in underlying
    return vanna

@njit(
    float64[:, :](
        float64[:, :],
        float64[:],
        float64[:],
        float64,
        float64,
        string,
        float64[:, :],
        float64[:, :],
        float64[:, :],
    )
)
def calc_charm(S, vol, T, r, q, opt_type, dp, cdf_dp, pdf_dp):
    dm = dp - vol * np.sqrt(T)
    if opt_type == "call":
        charm = (q * np.exp(-q * T) * cdf_dp) - np.exp(-q * T) * pdf_dp * (
            2 * (r - q) * T - dm * vol * np.sqrt(T)
        ) / (2 * T * vol * np.sqrt(T))
    else:
        charm = (-q * np.exp(-q * T) * (1 - cdf_dp)) - np.exp(-q * T) * pdf_dp * (
            2 * (r - q) * T - dm * vol * np.sqrt(T)
        ) / (2 * T * vol * np.sqrt(T))
    # change in delta per day until expiration
    return charm
    

def compute_vanna_charm(
    spot_price,
    strike_prices,
    opt_ivs,
    time_till_exp,
    yield_10yr,
    dividend_yield,
    contract_type): # contract_type = 'call' or 'put'

    if not contract_type in ['call','put']:
        raise ValueError()

    np_spot_price = np.array([[spot_price]]).astype(np.float64)
    strike_prices = strike_prices.to_numpy().astype(np.float64)
    opt_ivs = opt_ivs.to_numpy().astype(np.float64)
    time_till_exp = time_till_exp.to_numpy().astype(np.float64)
    yield_10yr = np.float64(yield_10yr)
    dividend_yield = np.float64(dividend_yield)
    np_dividend_yield = np.array([dividend_yield])
    
    dp, cdf_dp, pdf_dp = calc_dp_cdf_pdf(
        np_spot_price,
        strike_prices,
        opt_ivs,
        time_till_exp,
        yield_10yr,
        dividend_yield,
    )

    vanna = calc_vanna(
        np_spot_price,
        opt_ivs,
        time_till_exp,
        np_dividend_yield,
        dp,
        pdf_dp,
    )

    charm = calc_charm(
        np_spot_price,
        opt_ivs,
        time_till_exp,
        yield_10yr,
        dividend_yield,
        contract_type,
        dp,
        cdf_dp,
        pdf_dp
    )
    return vanna,charm

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



def compute_exposure(tstamp,spot_price,spot_volatility,df):

    # TODO get yield?
    yield_10yr = 0.01
    dividend_yield = 0.0

    np_spot_price = np.array([[spot_price]]).astype(np.float64)
    yield_10yr = np.float64(yield_10yr)
    dividend_yield = np.float64(dividend_yield)
    np_dividend_yield = np.array([dividend_yield])

    df = df.copy()
    # S is spot price, K is strike price, vol is implied volatility
    # T is time to expiration, r is risk-free rate, q is dividend yield
    call_idx = df.index[df.contract_type=='C'].tolist()
    if len(call_idx) > 0:
        call_opt_type = 'call'
        call_df = df[df.contract_type=='C']
        call_delta = call_df.delta.to_numpy().astype(np.float64)
        call_gamma = call_df.gamma.to_numpy().astype(np.float64)
        call_k = call_df.strike.to_numpy().astype(np.float64)
        call_v = call_df.volatility.to_numpy().astype(np.float64)
        call_t = call_df.time_till_exp.to_numpy().astype(np.float64)
        call_oi = call_df.true_oi.to_numpy().astype(np.float64)

        call_dp, call_cdf_dp, call_pdf_dp = calc_dp_cdf_pdf(
            np_spot_price,
            call_k,
            call_v,
            call_t,
            yield_10yr,
            dividend_yield,
        )

        df.loc[call_idx,'dex'] = call_delta*call_oi*spot_price
        df.loc[call_idx,'state_gex'] = call_gamma*call_oi*spot_price*spot_price
        df.loc[call_idx,'vex'] = calc_vanna_ex(np_spot_price, call_v, call_t, dividend_yield, call_oi, call_dp, call_pdf_dp).squeeze().astype(np.float32)
        df.loc[call_idx,'cex'] = calc_charm_ex(np_spot_price, call_v, call_t, yield_10yr, dividend_yield, call_opt_type, call_oi, call_dp, call_cdf_dp, call_pdf_dp).squeeze().astype(np.float32)
    
    put_idx = df.index[df.contract_type=='P'].tolist()
    if len(put_idx) > 0:
        put_opt_type = 'put'
        put_df = df[df.contract_type=='P']
        put_delta = put_df.delta.to_numpy().astype(np.float64)
        put_gamma = put_df.gamma.to_numpy().astype(np.float64)
        put_k = put_df.strike.to_numpy().astype(np.float64)
        put_v = put_df.volatility.to_numpy().astype(np.float64)
        put_t = put_df.time_till_exp.to_numpy().astype(np.float64)
        put_oi = put_df.true_oi.to_numpy().astype(np.float64)

        put_dp, put_cdf_dp, put_pdf_dp = calc_dp_cdf_pdf(
            np_spot_price,
            put_k,
            put_v,
            put_t,
            yield_10yr,
            dividend_yield,
        )

        df.loc[put_idx,'dex'] = put_delta*put_oi*spot_price
        df.loc[put_idx,'state_gex'] = put_gamma*put_oi*spot_price*spot_price*-1
        df.loc[put_idx,'vex'] = calc_vanna_ex(np_spot_price, put_v, put_t, dividend_yield, put_oi, put_dp, put_pdf_dp).squeeze().astype(np.float32)
        df.loc[put_idx,'cex'] = calc_charm_ex(np_spot_price, put_v, put_t, yield_10yr, dividend_yield, put_opt_type, put_oi, put_dp, put_cdf_dp, put_pdf_dp).squeeze().astype(np.float32)
        
    return df