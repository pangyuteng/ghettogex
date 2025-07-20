import os
import sys
import warnings
import logging
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

import traceback
import time
import pytz
import datetime
import numpy as np
import pandas as pd
from tqdm import tqdm
import asyncio

from .postgres_utils import (
    cpostgres_execute, cpostgres_copy,
    psycopg_pool,postgres_uri,
)

from .data_tasty import background_subscribe, is_market_open, now_in_new_york
from .misc import timedelta_from_market_open
from .iv_utils import (
    get_expiry_tstamp,
    TOTAL_SECONDS_ONE_YEAR,
    compute_greeks,
    compute_exposure,
    compute_theo_price,
)

async def get_events_df_from_scratch(aconn,ticker,utc_tstamp,max_utc_tstamp,future_utc_tstamp,min_utc_tstamp):
    if ticker == 'SPX':
        ticker_alt = 'SPXW'
    elif ticker == 'NDX':
        ticker_alt = 'NDXP'
    elif ticker == 'VIX':
        ticker_alt = 'VIXW'
    else:
        ticker_alt = ticker
    expiration = utc_tstamp.date()

    # the first minute, grab everything
    columns = [
        'event_type','event_symbol','time',
        'spot_price','open','high','low','close','volume','ask_volume','bid_volume',
        'open_interest','true_oi','price','bid_price','ask_price','volatility','delta','gamma','theta','rho','vega',
        'bid_time','ask_time','bid_size','ask_size',
        'size','aggressor_side','ticker','expiration','contract_type','strike','tstamp',
    ]

    query_str = """
    select 'vix_candle' as event_type,event_symbol,close as spot_price,time,tstamp from candle
    where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,'VIX') # use vix as 
    uv = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'underlying_candle' as event_type,event_symbol,close as spot_price,time,tstamp from candle
    where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker)
    uc = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'candle' as event_type,event_symbol,open,high,low,close,volume,ask_volume,bid_volume,tstamp,ticker,expiration,contract_type,time,strike from candle
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration)
    oc = cpostgres_execute(aconn,query_str,query_args)

    # TODO: get prior day open_interest and true_oi by creating a new event_agg row.
    query_str = """
    --select 'summary' as event_type,event_symbol,open_interest,tstamp,ticker,expiration,contract_type,strike from summary
    select 'summary' as event_type,event_symbol,0 as open_interest, 0 as true_oi,tstamp,ticker,expiration,contract_type,strike from summary
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration)
    os = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'greeks' as event_type,event_symbol,price,volatility,delta,gamma,theta,rho,vega,time,tstamp,ticker,expiration,contract_type,strike from greeks
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration)
    og = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'timeandsale' as event_type,event_symbol,size,price,bid_price,ask_price,aggressor_side,time,tstamp,ticker,expiration,contract_type,strike from timeandsale
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration)
    ot = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'quotehist' as event_type,event_symbol,bid_time,ask_time,bid_price,ask_price,bid_size,ask_size,tstamp,ticker,expiration,contract_type,strike from quote
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (utc_tstamp,future_utc_tstamp,ticker_alt,expiration) # quote
    oqh = cpostgres_execute(aconn,query_str,query_args)

    quote_query_str = """
    select distinct 'quote' as event_type,event_symbol,ticker,expiration,contract_type,strike,
        last(ask_price,tstamp) as ask_price,last(bid_price,tstamp) as bid_price,last(tstamp,tstamp) as tstamp
    FROM quote WHERE
    tstamp <= %s
    AND tstamp > %s - interval '180 second'
    AND ticker = %s AND expiration = %s 
    GROUP BY event_symbol,contract_type,ticker,strike,expiration
    """
    candle_query_str = """
    select distinct 'quote' as event_type,event_symbol,ticker,expiration,contract_type,strike,
        last(close,tstamp) as price,last(tstamp,tstamp) as tstamp
    FROM candle WHERE
    tstamp <= %s
    AND tstamp > %s - interval '180 second'
    AND ticker = %s AND expiration = %s 
    GROUP BY event_symbol,contract_type,ticker,strike,expiration
    """
    query_args = (utc_tstamp,utc_tstamp,ticker_alt,expiration) # quote
    oq = cpostgres_execute(aconn,quote_query_str,query_args)

    all_groups = await asyncio.gather(uv,uc,oc,os,og,ot,oqh,oq)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        pd_list = [pd.DataFrame(x,columns=columns) for x in all_groups if x is not None]
        df = pd.concat(pd_list,ignore_index=True)
    return df

async def get_events_df(aconn,ticker,utc_tstamp,max_utc_tstamp,future_utc_tstamp,min_utc_tstamp):

    if ticker == 'SPX':
        ticker_alt = 'SPXW'
    elif ticker == 'NDX':
        ticker_alt = 'NDXP'
    elif ticker == 'VIX':
        ticker_alt = 'VIXW'
    else:
        ticker_alt = ticker
    expiration = utc_tstamp.date()
    columns = [
        'event_type','event_symbol','time',
        'spot_price','open','high','low','close','volume','ask_volume','bid_volume',
        'true_oi','open_interest','price','bid_price','ask_price','volatility','delta','gamma','theta','rho','vega',
        'bid_time','ask_time','bid_size','ask_size',
        'size','aggressor_side','ticker','expiration','contract_type','strike','tstamp',
    ]

    query_str = """
    select 'vix_candle' as event_type,event_symbol,close as spot_price,time,tstamp from candle
    where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null and close != 0
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,'VIX') # use vix as 
    uv = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'underlying_candle' as event_type,event_symbol,close as spot_price,time,tstamp from candle
    where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null and close != 0
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker) # underlying_candle
    uc = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'candle' as event_type,event_symbol,open,high,low,close,volume,ask_volume,bid_volume,time,tstamp,ticker,expiration,contract_type,strike from candle
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (utc_tstamp,max_utc_tstamp,ticker_alt,expiration) # candle
    oc = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select distinct 'summary' as event_type, event_symbol,
        last(true_oi,tstamp) as true_oi,
        last(open_interest,tstamp) as open_interest,
        last(tstamp,tstamp) as tstamp,
        ticker,expiration,contract_type,strike
        from event_agg
    where tstamp < %s 
    AND tstamp > %s - interval '5 second'
    and ticker = %s and expiration = %s
    GROUP BY event_type,event_symbol,ticker,expiration,contract_type,strike
    """
    query_args = (utc_tstamp,utc_tstamp,ticker_alt,expiration) # greeks
    os = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'greeks' as event_type,event_symbol,price,volatility,delta,gamma,theta,rho,vega,time,tstamp,ticker,expiration,contract_type,strike from greeks
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration) # greeks
    og = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'timeandsale' as event_type,event_symbol,size,price,bid_price,ask_price,aggressor_side,time,tstamp,ticker,expiration,contract_type,strike from timeandsale
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (utc_tstamp,max_utc_tstamp,ticker_alt,expiration) # timeandsale
    ot = cpostgres_execute(aconn,query_str,query_args)

    query_str = """
    select 'quotehist' as event_type,event_symbol,bid_time,ask_time,bid_price,ask_price,bid_size,ask_size,tstamp,ticker,expiration,contract_type,strike from quote
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (utc_tstamp,future_utc_tstamp,ticker_alt,expiration) # quote history
    oqh = cpostgres_execute(aconn,query_str,query_args)

    quote_query_str = """
    select distinct 'quote' as event_type,event_symbol,ticker,expiration,contract_type,strike,
        last(ask_price,tstamp) as ask_price,last(bid_price,tstamp) as bid_price,last(tstamp,tstamp) as tstamp
    FROM quote WHERE
    tstamp <= %s
    AND tstamp > %s - interval '180 second'
    AND ticker = %s AND expiration = %s 
    GROUP BY event_type,event_symbol,contract_type,ticker,strike,expiration
    """
    candle_query_str = """
    select distinct 'quote' as event_type,event_symbol,ticker,expiration,contract_type,strike,
        last(close,tstamp) as price,last(tstamp,tstamp) as tstamp
    FROM candle WHERE
    tstamp <= %s
    AND tstamp > %s - interval '180 second'
    AND ticker = %s AND expiration = %s 
    GROUP BY event_type,event_symbol,contract_type,ticker,strike,expiration
    """
    query_args = (utc_tstamp,utc_tstamp,ticker_alt,expiration) # quote
    oq = cpostgres_execute(aconn,quote_query_str,query_args)

    all_groups = await asyncio.gather(uv,uc,oc,os,og,ot,oqh,oq)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        pd_list = [pd.DataFrame(x,columns=columns) for x in all_groups if x is not None]
        df = pd.concat(pd_list,ignore_index=True)
    return df

def get_side_mod(row,quotehist_df=None):
    try:
        side_mod = None
        cond_met = False

        if row.large_order:
            tmp_df = quotehist_df[quotehist_df.event_symbol==row.event_symbol]
            cond_met = True if len(tmp_df) > 2 else False
        else:
            tmp_df = None

        if row.large_order and cond_met:
            ask_price_list = tmp_df.ask_price.to_list()
            bid_price_list = tmp_df.bid_price.to_list()
            # TODO: hau volatility also uses ask_size and bid_size
            if ask_price_list[-1] > ask_price_list[0]:
                side_mod = 'likely_ask'
            elif bid_price_list[-1] > bid_price_list[0]:
                side_mod = 'likely_ask'
            else:
                side_mod = 'likely_bid' #???
        elif row.aggressor_side == 'BUY':
            side_mod = 'ask' # BUY or near ask
        elif row.aggressor_side == 'SELL':
            side_mod = 'bid' # SELL or near bid
        elif row.aggressor_side == 'UNDEFINED':
            if not np.isnan(row.theo_price):
                if (row.price - row.theo_price) > 1E-3:
                    side_mod = 'likely_ask'
                elif (row.price - row.theo_price) < -1E-3:
                    side_mod = 'likely_bid'
                else:
                    pass # assume mid is matched.
        return side_mod
    except:
        traceback.print_exc()
        return "exception"

def get_size_signed(row):
    if row.side_mod in ['ask','likely_ask']: # near ask, client bought, dealer short
        return -1*row['size'] 
    elif row.side_mod in ['bid','likely_bid']: # near bid, client sold, dealer long
        return row['size']
    else:
        return 0 # assume mid is matched
    
def compute_gex_core(utc_tstamp,df,from_scratch,first_minute=False):
    tstamp = utc_tstamp.replace(tzinfo=None) # postgres tsamp have no tzinfo
    # NOTE: we sort by time first, since tstamp col is postgres insert time.
    df = df.sort_values(by=['event_type','time','tstamp'])

    underlying_candle_df = df[(df.event_type=='underlying_candle')]
    if len(underlying_candle_df)>0:
        spot_price = underlying_candle_df.spot_price.to_list()[-1]
        #avg_spot_price = np.mean(underlying_candle_df.spot_price.to_list()[-5:])
    else:
        spot_price = np.nan
        #avg_spot_price = np.nan

    vix_candle_df = df[(df.event_type=='vix_candle')]
    if len(vix_candle_df)>0:
        spot_volatility = vix_candle_df.spot_price.to_list()[-1]
    else:
        spot_volatility = np.nan

    candle_df = df[df.event_type=='candle']
    summary_df = df[df.event_type=='summary']
    greeks_df = df[df.event_type=='greeks']
    quotehist_df = df[df.event_type=='quotehist']

    # use copy since will be adding columns to the df
    ts_df = df[df.event_type=='timeandsale'].copy()

    # NOTE:
    # quote data not really usable, as it is not synced with timeandsale...
    # once synced with timeandsale, can derive vol surface for better determine ddoi
    quote_df = df[df.event_type=='quote'].copy()
    quote_df = quote_df.sort_values(by=['contract_type','strike'])
    quote_df['price'] = (quote_df['ask_price']+quote_df['bid_price'])/2.0

    # flag large orders using timeandsale (NOTE: alternatively use size relative to bid/ask size in quote event)
    ts_df['size'] = ts_df['size'].astype(float)

    large_order_th = ts_df['size'].mean()+3*ts_df['size'].std()
    if not np.isnan(large_order_th):
        ts_df['large_order'] = ts_df['size'].apply(lambda x: x > large_order_th)
    else:
        ts_df['large_order'] = False

    # time_till_exp ####################################
    ts_df['theo_price'] = np.nan
    try:
        if len(ts_df) > 0 and False:
            expiration_mapper = {x:get_expiry_tstamp(x.strftime("%Y-%m-%d")) for x in list(ts_df.expiration.unique())}
            ts_df['time_till_exp'] = ts_df.expiration.apply(lambda x: (expiration_mapper[x]-tstamp).total_seconds()/TOTAL_SECONDS_ONE_YEAR )
            epsilon = 1e-5
            ts_df.loc[ts_df.time_till_exp==0,'time_till_exp'] = epsilon
            ts_df['spot_price'] = spot_price
            if False:
                # comment: we now are cmoparing using lagged data from qute event!
                # even if you query the lagged 1sec quote it gex profile still looks wrong!
                ts_df.drop(['price'], axis=1,inplace=True)
                ts_df = ts_df.merge(quote_df[['event_symbol','price']],how='left',on=['event_symbol'])
            # TDOO: compute IV merge with quote_df, compare IV?
            compute_theo_price(ts_df,spot_price,spot_volatility)
            ts_df.loc[ts_df.theo_price==0.0,'theo_price']=np.nan
    except:
        traceback.print_exc()
    ts_df['side_mod'] = ts_df.apply(lambda x: get_side_mod(x,quotehist_df=quotehist_df),axis=1)
    ts_df['size_signed'] = ts_df.apply(lambda x: get_size_signed(x),axis=1)

    candle_df = candle_df[['event_symbol','open','high','low','close','volume','bid_volume','ask_volume']]
    candle_df = candle_df.groupby(['event_symbol']).agg(
        open=pd.NamedAgg(column="open", aggfunc="last"),
        high=pd.NamedAgg(column="high", aggfunc="last"),
        low=pd.NamedAgg(column="low", aggfunc="last"),
        close=pd.NamedAgg(column="close", aggfunc="last"),
        volume=pd.NamedAgg(column="volume", aggfunc="sum"),
        bid_volume=pd.NamedAgg(column="bid_volume", aggfunc="sum"),
        ask_volume=pd.NamedAgg(column="ask_volume", aggfunc="sum"),
    ).reset_index()

    summary_df = summary_df[['event_symbol','ticker','strike','contract_type','expiration','open_interest','true_oi']]
    summary_df = summary_df.groupby(['event_symbol','ticker','strike','contract_type','expiration']).last().reset_index()

    greeks_df = greeks_df[['event_symbol','volatility','delta','gamma','theta','rho','vega']] # 'price',
    greeks_df = greeks_df.groupby(['event_symbol']).last().reset_index()

    timeandsale_df = ts_df[['event_symbol','size_signed']]
    timeandsale_df = timeandsale_df.groupby(['event_symbol']).sum().reset_index()

    # NOTE: start with summary since you always have summary
    merged_df = summary_df.merge(greeks_df,how='left',on=['event_symbol'])
    merged_df = merged_df.merge(timeandsale_df,how='left',on=['event_symbol'])
    merged_df = merged_df.merge(candle_df,how='left',on=['event_symbol'])

    quote_df = quote_df[['event_symbol','price']]
    merged_df = merged_df.merge(quote_df,how='left',on=['event_symbol'])

    merged_df['spot_volatility'] = spot_volatility
    merged_df['spot_price'] = spot_price
    merged_df['gamma_sign'] = merged_df.contract_type.apply(lambda x: -1 if x == 'P' else 1)

    for col_name in [
        'spot_volatility','delta','gamma','volatility',
        'open_interest','true_oi','spot_price',
        'size_signed','price','volume','ask_volume','bid_volume',
        'gamma_sign']:

        merged_df[col_name] = pd.to_numeric(merged_df[col_name], errors='coerce')

    merged_df.true_oi = merged_df.true_oi.fillna(value=0)
    merged_df.open_interest = merged_df.open_interest.fillna(value=0)
    merged_df.price = merged_df.price.fillna(value=0)
    merged_df.size_signed = merged_df.size_signed.fillna(value=0)
    merged_df.volume = merged_df.volume.fillna(value=0)
    merged_df.ask_volume = merged_df.ask_volume.fillna(value=0)
    merged_df.bid_volume = merged_df.bid_volume.fillna(value=0)
    merged_df.volatility = merged_df.volatility.fillna(value=0)

    # NOTE: ask means buy, dealer is short, bid means sell, dealer is long
    # open_interest aggregated from bid/ask volume from candle events
    merged_df.open_interest = merged_df.open_interest-merged_df.ask_volume+merged_df.bid_volume
    # true_oi aggregated from size in timeandsale events, and tweaked in get_side_mod (work-in-progress)
    merged_df.true_oi = merged_df.true_oi + merged_df.size_signed

    # volume_gex is the vanilla flavor using bid/ask volume from candle event
    merged_df['volume_gex'] = merged_df.gamma * merged_df.open_interest * 100 * merged_df.spot_price * merged_df.spot_price * 0.01 * merged_df.gamma_sign
    if False:
        # state_gex is computed below with updated greeks
        merged_df['state_gex'] = merged_df.gamma * merged_df.true_oi * merged_df.spot_price * merged_df.spot_price * merged_df.gamma_sign

    # time_till_exp ####################################
    try:
        expiration_mapper = {x:get_expiry_tstamp(x.strftime("%Y-%m-%d")) for x in list(merged_df.expiration.unique())}
        merged_df['time_till_exp'] = merged_df.expiration.apply(lambda x: (expiration_mapper[x]-tstamp).total_seconds()/TOTAL_SECONDS_ONE_YEAR )
        epsilon = 1e-5
        merged_df.loc[merged_df.time_till_exp==0,'time_till_exp'] = epsilon
    except:
        traceback.print_exc()

    # greeks ####################################
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            compute_greeks(merged_df,spot_price,spot_volatility,price_column='price')
        if not first_minute:
            #merged_df['volatility'] = merged_df['bsm_iv'] disable for now, too volatile.
            merged_df['delta'] = merged_df['bsm_delta']
            merged_df['gamma'] = merged_df['bsm_gamma']
    except:
        traceback.print_exc()

    # delta gamma charm vanna exposure ####################################
    try:

        merged_df['convexity'] = 0.0
        merged_df['state_gex'] = 0.0
        merged_df['dex'] = 0.0
        merged_df['vex'] = 0.0
        merged_df['cex'] = 0.0
        compute_exposure(merged_df,spot_price,spot_volatility)

    except:
        raise ValueError()
        traceback.print_exc()

    merged_df.volume_gex = merged_df.volume_gex.fillna(value=0)
    merged_df.state_gex = merged_df.state_gex.fillna(value=0)
    merged_df.convexity = merged_df.convexity.fillna(value=0)
    merged_df.dex = merged_df.dex.fillna(value=0)
    merged_df.vex = merged_df.vex.fillna(value=0)
    merged_df.cex = merged_df.cex.fillna(value=0)

    if from_scratch:
        # quality check
        reqd_event_list = ['summary','greeks','timeandsale','candle']
        if np.isnan(spot_price) or not all([event_type in df.event_type.unique() for event_type in reqd_event_list]):
            qc_pass = False
        else:
            qc_pass = True
    else:
        qc_pass = True

    return merged_df, qc_pass

async def compute_gex(ticker,et_tstamp,from_scratch=None,persist_to_postgres=True,overwrite=False):
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        #await apool.check() # <-- this is slow
        async with apool.connection() as aconn:
            #async with aconn.pipeline() as apipeline: # <-- using copy, dont use pipeline
            return await _compute_gex(aconn,ticker,et_tstamp,from_scratch=from_scratch,persist_to_postgres=persist_to_postgres,overwrite=overwrite)

async def _compute_gex(aconn,ticker,et_tstamp,from_scratch=None,persist_to_postgres=True,overwrite=False):
    time_a = time.time()

    agg_df = None
    csv_file = f"tmp/volume_gex-{et_tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    csv_file = None # TOO SLOW! FOR DEBUG
    utc = pytz.timezone('UTC')
    utc_tstamp = et_tstamp.astimezone(tz=utc)
    max_utc_tstamp = utc_tstamp+datetime.timedelta(seconds=1)
    future_utc_tstamp = utc_tstamp+datetime.timedelta(seconds=2) # grab quotes
    prior_minute_utc_tstamp = utc_tstamp-datetime.timedelta(seconds=300)
    
    marketopendelta, market_open_tstamp_et = timedelta_from_market_open(et_tstamp)
    market_open_tstamp_utc = market_open_tstamp_et.astimezone(tz=utc)
    lookback_tstamp_utc = market_open_tstamp_utc - datetime.timedelta(days=30)
    if marketopendelta < datetime.timedelta(minutes=1):
        first_minute = True
    else:
        first_minute = False

    if from_scratch is None:
        from_scratch = first_minute
    # the first minute, grab everything
    event_agg_columns = [
        'event_symbol',
        'tstamp',
        'spot_price',
        'delta','gamma','volatility',
        'ticker','expiration','contract_type','strike',
        'open_interest','true_oi','price',
        'volume_gex','state_gex',
        'dex','convexity','vex','cex'
    ]

    query_str = "SELECT * FROM gex_net WHERE ticker = %s and tstamp = %s"
    query_args = (ticker,utc_tstamp)
    fetched = await cpostgres_execute(aconn,query_str,query_args)

    time_b = time.time()
    logger.debug(f'pg select {time_b-time_a} {len(fetched)}')

    if len(fetched) == 0 or overwrite is True:
        if from_scratch:
            time_a = time.time()
            event_df = await get_events_df_from_scratch(aconn,ticker,utc_tstamp,max_utc_tstamp,future_utc_tstamp,market_open_tstamp_utc)
            time_b = time.time()
            logger.info(f'get_events_df {time_b-time_a}')
            agg_df, qc_pass = compute_gex_core(utc_tstamp,event_df.copy(deep=True),from_scratch,first_minute=first_minute)
            agg_df['tstamp']=utc_tstamp
            logger.debug(f'{from_scratch},{et_tstamp},{qc_pass},{len(event_df)},{len(agg_df)},{agg_df.state_gex.sum()}')

            time_c = time.time()
            logger.info(f'compute_gex_core {time_c-time_b}')

        else:
            time_a = time.time()
            event_df = await get_events_df(aconn,ticker,utc_tstamp,max_utc_tstamp,future_utc_tstamp,prior_minute_utc_tstamp)

            time_b = time.time()
            logger.info(f'get_events_df {time_b-time_a}')
            agg_df, qc_pass = compute_gex_core(utc_tstamp,event_df.copy(deep=True),from_scratch,first_minute=first_minute)
            agg_df['tstamp']=utc_tstamp
            logger.debug(f'{from_scratch},{et_tstamp},{qc_pass},{len(event_df)},{len(agg_df)},{agg_df.state_gex.sum()}')

            time_c = time.time()
            logger.info(f'compute_gex_core {time_c-time_b}')

        if persist_to_postgres and qc_pass:
            time_d = time.time()
            
            agg_df = agg_df[event_agg_columns]

            if len(agg_df) == 0:
                logger.warning(("len(agg_df) is 0!!!"))
                return

            if csv_file:
                agg_df.to_csv(csv_file,index=False)

            query_dict = {}

            event_agg_query_str = """
                COPY event_agg (event_symbol,tstamp,
                delta,gamma,volatility,price,open_interest,true_oi,ticker,expiration,contract_type,strike) FROM STDIN
            """
            async def insert_event_agg(row):
                query_args = [
                    row.event_symbol,row.tstamp,
                    row.delta,row.gamma,row.volatility,row.price,row.open_interest,row.true_oi,row.ticker,row.expiration,row.contract_type,row.strike
                ]
                return query_args

            query_dict[event_agg_query_str] = await asyncio.gather(*(insert_event_agg(row) for n,row in agg_df.iterrows()))

            table_cols = ['ticker','strike','tstamp','spot_price','volume_gex','state_gex','dex','convexity','vex','cex','true_oi']
            agg_df['ticker'] = ticker
            strike_ex_df = agg_df[table_cols].copy()
            strike_ex_df = strike_ex_df.groupby(['ticker','strike','tstamp']).agg(
                spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
                volume_gex=pd.NamedAgg(column="volume_gex", aggfunc="sum"),
                state_gex=pd.NamedAgg(column="state_gex", aggfunc="sum"),
                dex=pd.NamedAgg(column="dex", aggfunc="sum"),
                convexity=pd.NamedAgg(column="convexity", aggfunc="sum"),
                vex=pd.NamedAgg(column="vex", aggfunc="sum"),
                cex=pd.NamedAgg(column="cex", aggfunc="sum"),
            ).reset_index()
            
            call_strike_ex_df = agg_df[agg_df.contract_type=="C"][table_cols].copy()
            call_strike_ex_df = call_strike_ex_df.groupby(['ticker','strike','tstamp']).agg(
                call_convexity=pd.NamedAgg(column="convexity", aggfunc="sum"),
                call_oi=pd.NamedAgg(column="true_oi", aggfunc="sum"),
                call_dex=pd.NamedAgg(column="dex", aggfunc="sum"),
                call_gex=pd.NamedAgg(column="state_gex", aggfunc="sum"),
                call_vex=pd.NamedAgg(column="vex", aggfunc="sum"),
                call_cex=pd.NamedAgg(column="cex", aggfunc="sum"),
            ).reset_index()
            put_strike_ex_df = agg_df[agg_df.contract_type=="P"][table_cols].copy()
            put_strike_ex_df = put_strike_ex_df.groupby(['ticker','strike','tstamp']).agg(
                put_convexity=pd.NamedAgg(column="convexity", aggfunc="sum"),
                put_oi=pd.NamedAgg(column="true_oi", aggfunc="sum"),
                put_dex=pd.NamedAgg(column="dex", aggfunc="sum"),
                put_gex=pd.NamedAgg(column="state_gex", aggfunc="sum"),
                put_vex=pd.NamedAgg(column="vex", aggfunc="sum"),
                put_cex=pd.NamedAgg(column="cex", aggfunc="sum"),
            ).reset_index()

            strike_ex_df = strike_ex_df.merge(call_strike_ex_df,how='left',on=['ticker','strike','tstamp'])
            strike_ex_df = strike_ex_df.merge(put_strike_ex_df,how='left',on=['ticker','strike','tstamp'])

            gex_strike_query_str = """
                COPY gex_strike (ticker,strike,tstamp,volume_gex,state_gex,dex,convexity,vex,cex,
                call_convexity,call_oi,call_dex,call_gex,call_vex,call_cex,
                put_convexity,put_oi,put_dex,put_gex,put_vex,put_cex) FROM STDIN
            """
            async def insert_gex_strike(row):
                query_args = [row.ticker,row.strike,row.tstamp,row.volume_gex,row.state_gex,row.dex,row.convexity,row.vex,row.cex,
                row.call_convexity,row.call_oi,row.call_dex,row.call_gex,row.call_vex,row.call_cex,
                row.put_convexity,row.put_oi,row.put_dex,row.put_gex,row.put_vex,row.put_cex
                ]
                return query_args
            query_dict[gex_strike_query_str] = await asyncio.gather(*(insert_gex_strike(row) for n,row in strike_ex_df.iterrows()))
            
            table_cols = [
                'ticker','tstamp','spot_price','volume_gex','state_gex','dex','convexity','vex','cex',
                'call_convexity','call_oi','call_dex','call_gex','call_vex','call_cex',
                'put_convexity','put_oi','put_dex','put_gex','put_vex','put_cex'
            ]

            net_gex_df = strike_ex_df[table_cols].copy()
            net_gex_df = net_gex_df.groupby(['ticker','tstamp']).agg(
                spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
                volume_gex=pd.NamedAgg(column="volume_gex", aggfunc="sum"),
                state_gex=pd.NamedAgg(column="state_gex", aggfunc="sum"),
                convexity=pd.NamedAgg(column="convexity", aggfunc="sum"),
                dex=pd.NamedAgg(column="dex", aggfunc="sum"),
                vex=pd.NamedAgg(column="vex", aggfunc="sum"),
                cex=pd.NamedAgg(column="cex", aggfunc="sum"),
                call_oi=pd.NamedAgg(column="call_oi", aggfunc="sum"),
                call_convexity=pd.NamedAgg(column="call_convexity", aggfunc="sum"),
                call_dex=pd.NamedAgg(column="call_dex", aggfunc="sum"),
                call_gex=pd.NamedAgg(column="call_gex", aggfunc="sum"),
                call_vex=pd.NamedAgg(column="call_vex", aggfunc="sum"),
                call_cex=pd.NamedAgg(column="call_cex", aggfunc="sum"),
                put_oi=pd.NamedAgg(column="put_oi", aggfunc="sum"),
                put_convexity=pd.NamedAgg(column="put_convexity", aggfunc="sum"),
                put_dex=pd.NamedAgg(column="put_dex", aggfunc="sum"),
                put_gex=pd.NamedAgg(column="put_gex", aggfunc="sum"),
                put_vex=pd.NamedAgg(column="put_vex", aggfunc="sum"),
                put_cex=pd.NamedAgg(column="put_cex", aggfunc="sum"),
            ).reset_index()

            gex_net_query_str = """
                COPY gex_net (ticker,tstamp,volume_gex,state_gex,spot_price,dex,convexity,vex,cex,
                    call_convexity,call_oi,call_dex,call_gex,call_vex,call_cex,
                    put_convexity,put_oi,put_dex,put_gex,put_vex,put_cex) FROM STDIN
            """
            async def insert_gex_net(row):
                query_args = [row.ticker,row.tstamp,row.volume_gex,row.state_gex,row.spot_price,row.dex,row.convexity,row.vex,row.cex,
                row.call_convexity,row.call_oi,row.call_dex,row.call_gex,row.call_vex,row.call_cex,
                row.put_convexity,row.put_oi,row.put_dex,row.put_gex,row.put_vex,row.put_cex,
                ]
                return query_args

            query_dict[gex_net_query_str] = await asyncio.gather(*(insert_gex_net(row) for n,row in net_gex_df.iterrows()))

            time_c = time.time()
            logger.info(f'query prep {time_c-time_d}')
            await cpostgres_copy(aconn,query_dict)

            time_e = time.time()
            logger.info(f'postgres_execute_many {time_e-time_c}')

        else:
            logger.debug(f'qc_pass {qc_pass}, {len(fetched)}')
            if first_minute:
                time_b = time.time()
                query_str = "INSERT INTO gex_net (ticker,tstamp) VALUES (%s,%s) ON CONFLICT DO NOTHING;"
                query_args = (ticker,utc_tstamp)
                await cpostgres_execute(aconn,query_str,query_args,is_commit=True)
                time_c = time.time()
                logger.info(f'postgres_execute {time_c-time_b}')
    else:
        logger.debug(f'{utc_tstamp} {len(fetched)} found!')
    return agg_df

def main(ticker,my_date):
    tstamp_list = pd.date_range(start=my_date+" 09:30:00",end=my_date+" 16:00:00",freq='s',tz=pytz.timezone('US/Eastern'))
    for tstamp in tqdm(tstamp_list):
        logger.debug(f'...')
        if tstamp > now_in_new_york():
            break
        try:
            agg_df = asyncio.run(compute_gex(ticker,tstamp,from_scratch=None,persist_to_postgres=True,overwrite=True))
            if agg_df is not None:
                logger.debug(f'volume_gex {agg_df.volume_gex.sum()} state_gex {agg_df.state_gex.sum()}')
        except KeyboardInterrupt:
            sys.exit(1)
        except:
            traceback.print_exc()
            sys.exit(1)

def tryone(ticker,tstampstr):
    tstamp = datetime.datetime.strptime(tstampstr,'%Y-%m-%d-%H-%M-%S')
    tstamp = pytz.timezone('US/Eastern').localize(tstamp)
    print(ticker,tstamp)
    try:
        agg_df = asyncio.run(compute_gex(ticker,tstamp,from_scratch=None,persist_to_postgres=False,overwrite=True))
        if agg_df is not None:
            logger.debug(agg_df.head())
            logger.debug(f'volume_gex {agg_df.volume_gex.sum()} state_gex {agg_df.state_gex.sum()}')

    except KeyboardInterrupt:
        sys.exit(1)
    except:
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    ticker = sys.argv[1]
    et_tstamp_str = sys.argv[2]
    if len(et_tstamp_str) == 10:
        main(ticker,et_tstamp_str)
    else:
        tryone(ticker,et_tstamp_str)

"""

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-554bc784bf-xrgkg 5432:5432

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres

python -m utils.compute_intraday SPX 2025-06-23-09-00-01

python -m utils.compute_intraday SPX 2025-07-11

python -m utils.compute_intraday SPX 2025-07-02-10-00-00

/uplot?ticker=SPX&tstamp=2025-07-02-14-00-00

"""