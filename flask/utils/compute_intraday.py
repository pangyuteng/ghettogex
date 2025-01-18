import os
import sys
import warnings
import logging
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
#logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

import traceback
import time
import pytz
import datetime
import numpy as np
import pandas as pd
from tqdm import tqdm
import asyncio

from .postgres_utils import (
    postgres_execute, postgres_execute_many,
    apostgres_execute, apostgres_execute_many
)

from .data_tasty import background_subscribe, is_market_open, now_in_new_york
from .misc import timedelta_from_market_open

async def get_events_df_from_scratch(ticker,utc_tstamp,max_utc_tstamp,min_utc_tstamp):
    if ticker == 'SPX':
        ticker_alt = 'SPXW'
    elif ticker == 'NDX':
        ticker_alt = 'NDXP'
    elif ticker == 'VIX':
        ticker_alt = 'VIXW'
    else:
        ticker_alt = ticker


    # the first minute, grab everything
    columns = [
        'event_type','event_symbol',
        'spot_price','open','high','low','close','volume','ask_volume','bid_volume',
        'open_interest','price','volatility','delta','gamma','theta','rho','vega',
        'size','aggressor_side','ticker','expiration','contract_type','strike','tstamp',
    ]

    if False:
        query_str = """
        (select 'underlying_candle' as event_type,event_symbol,close as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,null as ticker,null as expiration,null as contract_type,null as strike from candle
        where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
        ) union all (
        select 'candle' as event_type,event_symbol,null::float as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from candle
        where tstamp >= %s and tstamp < %s and ticker = %s
        ) union all (
        select 'summary' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp ,ticker,expiration,contract_type,strike from summary
        where tstamp >= %s and tstamp < %s and ticker = %s
        ) union all (
        select 'greeks' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, price,volatility,delta,gamma,theta,rho,vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from greeks
        where tstamp >= %s and tstamp < %s and ticker = %s
        ) union all (
        select 'timeandsale' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
        where tstamp >= %s and tstamp < %s and ticker = %s
        )
        """
        query_args = (
            min_utc_tstamp,max_utc_tstamp,ticker,
            min_utc_tstamp,max_utc_tstamp,ticker_alt,
            min_utc_tstamp,max_utc_tstamp,ticker_alt,
            min_utc_tstamp,max_utc_tstamp,ticker_alt,
            min_utc_tstamp,max_utc_tstamp,ticker_alt,
        )

        fetched = await apostgres_execute(query_str,query_args)
        
        if fetched is None:
            df = pd.DataFrame([],columns=columns)
        else:
            df = pd.DataFrame(fetched,columns=columns)
        return df
    else:

        query_str = """
        select 'underlying_candle' as event_type,event_symbol,close as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,null as ticker,null as expiration,null as contract_type,null as strike from candle
        where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
        """
        query_args = (min_utc_tstamp,max_utc_tstamp,ticker)
        uc = apostgres_execute(query_str,query_args)

        query_str = """
        select 'candle' as event_type,event_symbol,null::float as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from candle
        where tstamp >= %s and tstamp < %s and ticker = %s
        """
        query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt)
        oc = apostgres_execute(query_str,query_args)

        query_str = """
        select 'summary' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp ,ticker,expiration,contract_type,strike from summary
        where tstamp >= %s and tstamp < %s and ticker = %s
        """
        query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt)
        os = apostgres_execute(query_str,query_args)

        query_str = """
        select 'greeks' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, price,volatility,delta,gamma,theta,rho,vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from greeks
        where tstamp >= %s and tstamp < %s and ticker = %s
        """
        query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt)
        og = apostgres_execute(query_str,query_args)

        query_str = """
        select 'timeandsale' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
        where tstamp >= %s and tstamp < %s and ticker = %s
        """
        query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt)
        ot = apostgres_execute(query_str,query_args)

        all_groups = await asyncio.gather(uc,oc,os,og,ot)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            pd_list = [pd.DataFrame(x,columns=columns) for x in all_groups if x is not None]
            df = pd.concat(pd_list,ignore_index=True)
        return df

async def get_events_df(ticker,utc_tstamp,max_utc_tstamp,min_utc_tstamp):

    if ticker == 'SPX':
        ticker_alt = 'SPXW'
    elif ticker == 'NDX':
        ticker_alt = 'NDXP'
    elif ticker == 'VIX':
        ticker_alt = 'VIXW'
    else:
        ticker_alt = ticker

    columns = [
        'event_type','event_symbol',
        'spot_price','open','high','low','close','volume','ask_volume','bid_volume',
        'open_interest','price','volatility','delta','gamma','theta','rho','vega',
        'size','aggressor_side','ticker','expiration','contract_type','strike','tstamp',
    ]

    if False:
        query_str = """
        (select 'underlying_candle' as event_type,event_symbol,close as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,null as ticker,null as expiration,null as contract_type,null as strike from candle
        where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
        ) union all (
        select 'candle' as event_type,event_symbol,null::float as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from candle
        where tstamp >= %s and tstamp < %s and ticker = %s
        ) union all (
        select 'summary' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp ,ticker,expiration,contract_type,strike from event_agg
        where dstamp = %s and ticker = %s
        ) union all (
        select 'greeks' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, price,volatility,delta,gamma,theta,rho,vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from greeks
        where tstamp >= %s and tstamp < %s and ticker = %s
        ) union all (
        select 'timeandsale' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
        where tstamp >= %s and tstamp < %s and ticker = %s
        )
        """
        query_args = (
            min_utc_tstamp,max_utc_tstamp,ticker, # underlying_candle
            utc_tstamp,max_utc_tstamp,ticker_alt, # candle
            utc_tstamp.date(),ticker_alt, # event_agg
            min_utc_tstamp,max_utc_tstamp,ticker_alt, # greeks
            utc_tstamp,max_utc_tstamp,ticker_alt, # timeandsale
        )

        fetched = await apostgres_execute(query_str,query_args)
        if fetched is None:
            df = pd.DataFrame([],columns=columns)
        else:
            df = pd.DataFrame(fetched,columns=columns)

        return df
    else:
            
        query_str = """
        select 'underlying_candle' as event_type,event_symbol,close as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,null as ticker,null as expiration,null as contract_type,null as strike from candle
        where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
        """
        query_args = (min_utc_tstamp,max_utc_tstamp,ticker) # underlying_candle
        uc = apostgres_execute(query_str,query_args)

        query_str = """
        select 'candle' as event_type,event_symbol,null::float as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from candle
        where tstamp >= %s and tstamp < %s and ticker = %s
        """
        query_args = (utc_tstamp,max_utc_tstamp,ticker_alt) # candle
        oc = apostgres_execute(query_str,query_args)

        query_str = """
        select 'summary' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp ,ticker,expiration,contract_type,strike from event_agg
        where dstamp = %s and ticker = %s
        """
        query_args = (utc_tstamp.date(),ticker_alt) # event_agg
        os = apostgres_execute(query_str,query_args)

        query_str = """
        select 'greeks' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, price,volatility,delta,gamma,theta,rho,vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from greeks
        where tstamp >= %s and tstamp < %s and ticker = %s
        """
        query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt) # greeks
        og = apostgres_execute(query_str,query_args)

        query_str = """
        select 'timeandsale' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
        where tstamp >= %s and tstamp < %s and ticker = %s
        """
        query_args = (utc_tstamp,max_utc_tstamp,ticker_alt) # timeandsale
        ot = apostgres_execute(query_str,query_args)

        all_groups = await asyncio.gather(uc,oc,os,og,ot)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            pd_list = [pd.DataFrame(x,columns=columns) for x in all_groups if x is not None]
            df = pd.concat(pd_list,ignore_index=True)
        return df

# event: candle
# volume, bid_volume,ask_volume, --> sum, open,high,low,close
# event: summary
# open_interest
# event: greeks
# price	volatility	delta	gamma	theta	rho	vega
# event: timeandsale
# size, aggressor_side

# aggressor_side_int =
# latest_open_interest = 
#event_type,event_symbol,close,spot_price,open_interest,gamma,size,aggressor_side,ticker,expiration,contract_type,strike,tstamp
# grab last close,spot_price, 
# observations
# + greeks needs to be updated if no greeks and options candle exists
# + spot needs to be updated if candle, and you got underlying quotes.
# + summary event seems to be only once a day?


def compute_gex_core(df,from_scratch):
    df = df.sort_values(by=['event_type','tstamp'])
    # size from timeandsale event
    df['size_signed'] = df['size'].where(df.aggressor_side == 'BUY', other=-1*df['size'])
    underlying_candle_df = df[df.event_type=='underlying_candle']

    if len(underlying_candle_df)>0:
        underlying_candle_df = df[df.event_type=='underlying_candle']
        spot_price = underlying_candle_df.spot_price.to_list()[-1]
    else:
        spot_price = np.nan

    candle_df = df[df.event_type=='candle']
    summary_df = df[df.event_type=='summary']
    greeks_df = df[df.event_type=='greeks']
    timeandsale_df = df[df.event_type=='timeandsale']

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

    oi_df = summary_df[['event_symbol','ticker','strike','contract_type','expiration','open_interest']]
    oi_df = oi_df.groupby(['event_symbol','ticker','strike','contract_type','expiration']).last().reset_index()
    
    greeks_df = greeks_df[['event_symbol','price','volatility','delta','gamma','theta','rho','vega']]
    greeks_df = greeks_df.groupby(['event_symbol']).last().reset_index()

    timeandsale_df = timeandsale_df[['event_symbol','size_signed']]
    timeandsale_df = timeandsale_df.groupby(['event_symbol']).sum().reset_index()

    merged_df = oi_df.merge(greeks_df,how='left',on=['event_symbol'])
    merged_df = merged_df.merge(timeandsale_df,how='left',on=['event_symbol'])
    merged_df = merged_df.merge(candle_df,how='left',on=['event_symbol'])

    merged_df['contract_type_int'] = merged_df.contract_type.apply(lambda x: -1 if x == 'P' else 1)
    merged_df['spot_price']=spot_price


    # TODO: THIS IS WHERE YOU WANT TO PLAY WITH gex compute...
    #ok.oi_timeandsale = ok.oi_timeandsale.cumsum().astype(float)+init_oi
    #ok.oi_volume = ok.oi_volume.cumsum().astype(float)+init_oi
    # oi_timeandsale = merged_df.open_interest+merged_df.ask_volume-merged_df.bid_volume
    # oi_volume = merged_df.open_interest+merged_df.size_signed

    for col_name in ['gamma','open_interest','spot_price','contract_type_int','size_signed']:
        merged_df[col_name] = pd.to_numeric(merged_df[col_name], errors='coerce')

    merged_df['open_interest']=merged_df.open_interest+merged_df.size_signed
    merged_df['naive_gex'] = merged_df.gamma * merged_df.open_interest * 100 * merged_df.spot_price * merged_df.spot_price * 0.01 * merged_df.contract_type_int
    merged_df.open_interest = merged_df.open_interest.fillna(value=0)
    merged_df.naive_gex = merged_df.naive_gex.fillna(value=0)
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


# TODO: create from_scratch cron job every 5 minute 

async def compute_gex(ticker,et_tstamp,from_scratch=None,persist_to_postgres=True):
    time_a = time.time()

    gex_df = None
    csv_file = f"tmp/naive_gex-{et_tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    csv_file = None
    utc = pytz.timezone('UTC')
    utc_tstamp = et_tstamp.astimezone(tz=utc)
    max_utc_tstamp = utc_tstamp+datetime.timedelta(seconds=1)
    prior_minute_utc_tstamp = utc_tstamp-datetime.timedelta(seconds=60)
    
    delta, market_open_tstamp_et = timedelta_from_market_open(et_tstamp)
    market_open_tstamp_utc = market_open_tstamp_et.astimezone(tz=utc)

    if delta < datetime.timedelta(minutes=1):
        first_minute = True
    else:
        first_minute = False

    if from_scratch is None:
        from_scratch = first_minute
    # the first minute, grab everything
    event_agg_columns = [
        'event_symbol',
        'dstamp',
        'tstamp',
        'spot_price','gamma',
        'ticker','expiration','contract_type','strike',
        'open_interest',
        'naive_gex',
    ]
    # 'open','high','low','close','volume','ask_volume','bid_volume',
    # 'price','volatility','delta','gamma','theta','rho','vega',
    query_str = "SELECT * FROM gex_net WHERE ticker = %s and tstamp = %s"
    query_args = (ticker,utc_tstamp)
    fetched = postgres_execute(query_str,query_args)

    time_b = time.time()
    logger.debug(f'pg select {time_b-time_a} {len(fetched)}')

    if len(fetched) == 0:
        if from_scratch:
            time_a = time.time()
            event_df = await get_events_df_from_scratch(ticker,utc_tstamp,max_utc_tstamp,market_open_tstamp_utc)

            time_b = time.time()
            logger.info(f'get_events_df {time_b-time_a}')

            agg_df, qc_pass = compute_gex_core(event_df.copy(deep=True),from_scratch)
            agg_df['dstamp']=utc_tstamp.date()
            agg_df['tstamp']=utc_tstamp
            logger.debug(f'{from_scratch},{et_tstamp},{qc_pass},{len(event_df)},{len(agg_df)},{agg_df.naive_gex.sum()}')

            time_c = time.time()
            logger.info(f'compute_gex_core {time_c-time_b}')

        else:
            time_a = time.time()
            event_df = await get_events_df(ticker,utc_tstamp,max_utc_tstamp,prior_minute_utc_tstamp)

            time_b = time.time()
            logger.info(f'get_events_df {time_b-time_a}')

            agg_df, qc_pass = compute_gex_core(event_df.copy(deep=True),from_scratch)
            agg_df['dstamp']=utc_tstamp.date()
            agg_df['tstamp']=utc_tstamp
            logger.debug(f'{from_scratch},{et_tstamp},{qc_pass},{len(event_df)},{len(agg_df)},{agg_df.naive_gex.sum()}')

            time_c = time.time()
            logger.info(f'compute_gex_core {time_c-time_b}')

        if persist_to_postgres and qc_pass:
            time_d = time.time()
            agg_df = agg_df[event_agg_columns]
            if csv_file:
                agg_df.to_csv(csv_file,index=False)

            query_dict = {}
            # pkey event_symbol and dstamp   
            query_str = "INSERT INTO event_agg (event_symbol,dstamp,open_interest,naive_gex,tstamp,ticker,expiration,contract_type,strike) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict (event_symbol,dstamp) do update set open_interest = %s, naive_gex = %s, tstamp = %s, ticker = %s, expiration = %s, contract_type = %s, strike = %s;"
            query_dict[query_str]=[]
            for n,row in agg_df.iterrows():
                query_args = [row.event_symbol,row.dstamp,row.open_interest,row.naive_gex,row.tstamp,row.ticker,row.expiration,row.contract_type,row.strike,row.open_interest,row.naive_gex,row.tstamp,row.ticker,row.expiration,row.contract_type,row.strike]
                query_dict[query_str].append(query_args)
            
            table_cols = ['ticker','strike','tstamp','naive_gex']
            agg_df['ticker'] = ticker
            strike_gex_df = agg_df[table_cols]
            strike_gex_df = strike_gex_df.groupby(['ticker','strike','tstamp']).agg(
                naive_gex=pd.NamedAgg(column="naive_gex", aggfunc="sum"),
            ).reset_index()
            query_str = "INSERT INTO gex_strike (ticker,strike,tstamp,naive_gex) VALUES (%s,%s,%s,%s) on conflict (ticker,strike,tstamp) do update set naive_gex = %s;"
            def get_args(row):
                return [row.ticker,row.strike,row.tstamp,row.naive_gex,row.naive_gex]
            query_args = strike_gex_df.apply(lambda row: get_args(row), axis=1)
            query_dict[query_str]=query_args.to_list()

            table_cols = ['ticker','tstamp','spot_price','naive_gex']
            agg_df['ticker'] = ticker
            net_gex_df = agg_df[table_cols]
            net_gex_df = net_gex_df.groupby(['ticker','tstamp']).agg(
                spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
                naive_gex=pd.NamedAgg(column="naive_gex", aggfunc="sum"),
            ).reset_index()
            query_str = "INSERT INTO gex_net (ticker,tstamp,naive_gex,spot_price) VALUES (%s,%s,%s,%s) on conflict (ticker,tstamp) do update set naive_gex = %s, spot_price = %s;"
            def get_args(row):
                return [row.ticker,row.tstamp,row.naive_gex,row.spot_price,row.naive_gex,row.spot_price]
            query_args = net_gex_df.apply(lambda row: get_args(row), axis=1)
            query_dict[query_str] = query_args.to_list()

            time_b = time.time()
            logger.info(f'text append {time_b-time_d}')

            postgres_execute_many(query_dict)
            time_c = time.time()
            logger.info(f'postgres_execute_many {time_c-time_b}')
        else:
            logger.debug(f'qc_pass {qc_pass}, {len(fetched)}')
            if first_minute:
                time_b = time.time()
                query_str = "INSERT INTO gex_net (ticker,tstamp) VALUES (%s,%s) ON CONFLICT DO NOTHING;"
                query_args = (ticker,utc_tstamp)
                postgres_execute(query_str,query_args,is_commit=True)
                time_c = time.time()
                logger.info(f'postgres_execute {time_c-time_b}')
    else:
        logger.debug(f'{utc_tstamp} {len(fetched)} found!')
    return gex_df

def main(ticker,my_date):
    tstamp_list = pd.date_range(start=my_date+" 09:30:00",end=my_date+" 16:00:00",freq='s',tz=pytz.timezone('US/Eastern'))
    for tstamp in tqdm(tstamp_list):
        logger.info(f'...')
        if tstamp > now_in_new_york():
            break
        try:
            get_df = asyncio.run(compute_gex(ticker,tstamp,from_scratch=None,persist_to_postgres=True))
        except KeyboardInterrupt:
            sys.exit(1)
        except:
            traceback.print_exc()
            sys.exit(1)

def tryone(ticker):
    for x in range(1):
        tstamp = now_in_new_york()
        try:
            get_df = asyncio.run(compute_gex(ticker,tstamp,from_scratch=True,persist_to_postgres=True))
        except KeyboardInterrupt:
            sys.exit(1)
        except:
            traceback.print_exc()
            sys.exit(1)

if __name__ == "__main__":
    ticker = sys.argv[1]
    my_date = sys.argv[2]
    main(ticker,my_date)

"""
select * from candle 
where event_symbol = 'SPX'
order by tstamp asc limit 10

select * from candle 
where event_symbol = 'SPX'
order by tstamp desc limit 10

SELECT * from candle
WHERE event_symbol = 'SPX'
and tstamp >= '2025-01-07 14:31:00' and tstamp < '2025-01-07 14:32:00'
-- 66 rows

https://www.pgmustard.com/blog/max-parallel-workers-per-gather

https://stackoverflow.com/questions/63680444/why-less-than-has-much-better-performance-than-greater-than-in-a-query

EXPLAIN ANALYZE
(select 'underlying_candle' as event_type,event_symbol,close as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,null as ticker,null as expiration,null as contract_type,null as strike from candle
where tstamp >= '2025-01-07 15:00:00' and tstamp < '2025-01-07 15:01:00' and event_symbol = 'SPX'
) union all (
select 'candle' as event_type,event_symbol,null::float as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from candle
where tstamp >= '2025-01-07 15:00:59' and tstamp < '2025-01-07 15:01:00' and event_symbol like '.SPX%'
) union all (
select 'summary' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp ,ticker,expiration,contract_type,strike from event_agg
where tstamp >= '2025-01-07 15:00:00' and tstamp < '2025-01-07 15:01:00' and event_symbol like '.SPX%'
) union all (
select 'greeks' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, price,volatility,delta,gamma,theta,rho,vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from greeks
where tstamp >= '2025-01-07 15:00:00' and tstamp < '2025-01-07 15:01:00' and event_symbol like '.SPX%'
) union all (
select 'timeandsale' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
where tstamp >= '2025-01-07 15:00:59' and tstamp < '2025-01-07 15:01:00' and event_symbol like '.SPX%'
)

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-554bc784bf-xrgkg 5432:5432

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres

python -m utils.compute_intraday SPX 2025-01-07 && \
python -m utils.compute_intraday SPX 2025-01-08

"""