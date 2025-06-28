
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

def interpolate_quote_price(df,s=0.1):
    # quote_df is presorted sorted by strike

    df['mid_price'] = (df.ask_price + df.bid_price)/2
    df['interp_price'] = None
    call_idx = df.index[df.contract_type=='C'].tolist()
    if len(call_idx) > 0:
        strike = df.loc[call_idx,'strike']
        mid_price = df.loc[call_idx,'mid_price']
        spline = interpolate.UnivariateSpline(strike,mid_price,s=s)
        interp_price = spline(strike)
        df.loc[call_idx,'interp_price'] = interp_price

    put_idx = df.index[df.contract_type=='P'].tolist()
    if len(put_idx) > 0:
        strike = df.loc[put_idx,'strike']
        mid_price = df.loc[put_idx,'mid_price']
        spline = interpolate.UnivariateSpline(strike,mid_price,s=s)
        interp_price = spline(strike)
        df.loc[put_idx,'interp_price'] = interp_price

def compute_theo_price():
    raise NotImplementedError("not tested")
    flag = df.contract_type.apply(lambda x: 'c' if x == df_call_symbol else 'p')
    S = df.spot_price.astype(np.float16)
    K = df.strike.astype(np.float16)
    t = df.tte.astype(np.float16)
    r = 0.0 # interest rate
    sigma = df.iv
    q = 0 # annualized continuous dividend yield.
    theo_price = py_vollib.black_scholes_merton.black_scholes_merton(flag, S, K, t, r, sigma, q, return_as='numpy')
    gamma = py_vollib.black_scholes.greeks.numerical.gamma(flag, S, K, t, r, sigma, return_as='numpy')
    df['theo_price'] = theo_price
    df['gamma'] = gamma

def compute_implied_volatility(df,price_column='price'):
    yield_10yr = 1e-5
    dividend_yield = 0.0
    price = df[price_column].astype(np.float16)
    flag = df.contract_type.apply(lambda x: 'c' if x == 'C' else 'p')
    S = df.spot_price.astype(np.float16)
    K = df.strike.astype(np.float16)
    
    # shitty hack to boost iv to match volatility from dxlink greeks event
    # pad more and more time as time approach zero, max 1 hr
    t = df.time_till_exp.astype(np.float16)
    t += ((np.log1p(t+1000)*-1+1000)/1000)/(24*365)

    r = np.float64(yield_10yr)
    bsm_iv = py_vollib.black_scholes_merton.implied_volatility.implied_volatility(price, S, K, t, r, flag, q=dividend_yield, return_as='numpy')
    df['bsm_iv'] = bsm_iv

def compute_exposure(tstamp,spot_price,spot_volatility,df):

    yield_10yr = 1e-5
    dividend_yield = 0.0

    np_spot_price = np.array([[spot_price]]).astype(np.float64)
    yield_10yr = np.float64(yield_10yr)
    dividend_yield = np.float64(dividend_yield)
    np_dividend_yield = np.array([dividend_yield])

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





