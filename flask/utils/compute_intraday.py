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
    apostgres_execute, apostgres_execute_many,
    psycopg_pool,postgres_uri,
)

from .data_tasty import background_subscribe, is_market_open, now_in_new_york
from .misc import timedelta_from_market_open

async def get_events_df_from_scratch(apool,ticker,utc_tstamp,max_utc_tstamp,min_utc_tstamp,lookback_utc_tstamp):
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

    query_str = """
    select 'underlying_candle' as event_type,event_symbol,close as spot_price,tstamp from candle
    where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker)
    uc = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'candle' as event_type,event_symbol,open,high,low,close,volume,ask_volume,bid_volume,tstamp,ticker,expiration,contract_type,strike from candle
    where tstamp >= %s and tstamp < %s and ticker = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt)
    oc = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'summary' as event_type,event_symbol,open_interest,tstamp,ticker,expiration,contract_type,strike from summary
    where tstamp >= %s and tstamp < %s and ticker = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt)
    os = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'greeks' as event_type,event_symbol,price,volatility,delta,gamma,theta,rho,vega,tstamp,ticker,expiration,contract_type,strike from greeks
    where tstamp >= %s and tstamp < %s and ticker = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt)
    og = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'timeandsale' as event_type,event_symbol,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
    where tstamp >= %s and tstamp < %s and ticker = %s
    """
    query_args = (lookback_utc_tstamp,max_utc_tstamp,ticker_alt)
    ot = apostgres_execute(apool,query_str,query_args)

    all_groups = await asyncio.gather(uc,oc,os,og,ot)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        pd_list = [pd.DataFrame(x,columns=columns) for x in all_groups if x is not None]
        df = pd.concat(pd_list,ignore_index=True)
        df['true_oi']=None
    return df

async def get_events_df(apool,ticker,utc_tstamp,max_utc_tstamp,min_utc_tstamp):

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
        'true_oi','open_interest','price','volatility','delta','gamma','theta','rho','vega',
        'size','aggressor_side','ticker','expiration','contract_type','strike','tstamp',
    ]

    query_str = """
    select 'underlying_candle' as event_type,event_symbol,close as spot_price,tstamp from candle
    where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker) # underlying_candle
    uc = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'candle' as event_type,event_symbol,open,high,low,close,volume,ask_volume,bid_volume,tstamp,ticker,expiration,contract_type,strike from candle
    where tstamp >= %s and tstamp < %s and ticker = %s
    """
    query_args = (utc_tstamp,max_utc_tstamp,ticker_alt) # candle
    oc = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'summary' as event_type,event_symbol,true_oi,open_interest,tstamp,ticker,expiration,contract_type,strike from event_agg
    where dstamp = %s and ticker = %s
    """
    query_args = (utc_tstamp.date(),ticker_alt) # event_agg
    os = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'greeks' as event_type,event_symbol,price,volatility,delta,gamma,theta,rho,vega,tstamp,ticker,expiration,contract_type,strike from greeks
    where tstamp >= %s and tstamp < %s and ticker = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt) # greeks
    og = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'timeandsale' as event_type,event_symbol,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
    where tstamp >= %s and tstamp < %s and ticker = %s
    """
    query_args = (utc_tstamp,max_utc_tstamp,ticker_alt) # timeandsale
    ot = apostgres_execute(apool,query_str,query_args)

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

def get_size_signed(row):
    if row.aggressor_side == 'BUY':
        return -1*row['size'] # buy means dealer is short the contract
    elif row.aggressor_side == 'SELL':
        return row['size'] # sell means dealer is long the contract
    else:
        return 0 # hau voaltility 2021 ????

def compute_gex_core(df,from_scratch):
    df = df.sort_values(by=['event_type','tstamp'])
    df['size_signed'] = df.apply(lambda x: get_size_signed(x),axis=1)

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

    summary_df = summary_df[['event_symbol','ticker','strike','contract_type','expiration','open_interest','true_oi']]
    summary_df = summary_df.groupby(['event_symbol','ticker','strike','contract_type','expiration']).last().reset_index()
    
    greeks_df = greeks_df[['event_symbol','price','volatility','delta','gamma','theta','rho','vega']]
    greeks_df = greeks_df.groupby(['event_symbol']).last().reset_index()

    timeandsale_df = timeandsale_df[['event_symbol','size_signed']]
    timeandsale_df = timeandsale_df.groupby(['event_symbol']).sum().reset_index()

    merged_df = greeks_df.merge(summary_df,how='left',on=['event_symbol'])
    merged_df = merged_df.merge(timeandsale_df,how='left',on=['event_symbol'])
    merged_df = merged_df.merge(candle_df,how='left',on=['event_symbol'])

    # contract_type_int is the naive gex assumption. dealer is long call, short put
    merged_df['contract_type_int'] = merged_df.contract_type.apply(lambda x: -1 if x == 'P' else 1)
    merged_df['spot_price']=spot_price

    for col_name in ['gamma','open_interest','true_oi','spot_price','contract_type_int','size_signed','volume','ask_volume','bid_volume']:
        merged_df[col_name] = pd.to_numeric(merged_df[col_name], errors='coerce')

    merged_df.true_oi = merged_df.true_oi.fillna(value=0)
    merged_df.open_interest = merged_df.open_interest.fillna(value=0)
    merged_df.size_signed = merged_df.size_signed.fillna(value=0)
    merged_df.volume = merged_df.volume.fillna(value=0)
    merged_df.ask_volume = merged_df.ask_volume.fillna(value=0)
    merged_df.bid_volume = merged_df.bid_volume.fillna(value=0)


    # let naive_gex open_interest be updated using 
    # ask means buy, dealer is short, bid means sell, dealer is long
    merged_df.open_interest = merged_df.open_interest-merged_df.ask_volume+merged_df.bid_volume
    if from_scratch:
        merged_df['true_oi'] = merged_df.size_signed
    
    else:
        # update oi
        merged_df['true_oi'] += merged_df.size_signed

    # UNSURE HERE... stil at debug phase
    # KISS.
    # `naive_gex` update summary based on bid-ask volume using summary open_interest
    # `true_gex` uses timeandsale and true_oi
    
    # naive_gex is wrong
    merged_df['naive_gex'] = merged_df.gamma * merged_df.open_interest * 100 * merged_df.spot_price * merged_df.spot_price * 0.01 * merged_df.contract_type_int

    merged_df['true_gex'] = merged_df.gamma * merged_df.true_oi * 100 * merged_df.spot_price * merged_df.spot_price * 0.01

    merged_df.naive_gex = merged_df.naive_gex.fillna(value=0)
    merged_df.true_gex = merged_df.true_gex.fillna(value=0)
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
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=500,open=False) as apool:
        await _compute_gex(apool,ticker,et_tstamp,from_scratch=from_scratch,persist_to_postgres=persist_to_postgres)

async def _compute_gex(apool,ticker,et_tstamp,from_scratch=None,persist_to_postgres=True):
    time_a = time.time()

    gex_df = None
    csv_file = f"tmp/naive_gex-{et_tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    csv_file = None # TOO SLOW! FOR DEBUG
    utc = pytz.timezone('UTC')
    utc_tstamp = et_tstamp.astimezone(tz=utc)
    max_utc_tstamp = utc_tstamp+datetime.timedelta(seconds=1)
    prior_minute_utc_tstamp = utc_tstamp-datetime.timedelta(seconds=60)
    
    delta, market_open_tstamp_et = timedelta_from_market_open(et_tstamp)
    market_open_tstamp_utc = market_open_tstamp_et.astimezone(tz=utc)
    lookback_tstamp_utc = market_open_tstamp_utc - datetime.timedelta(days=30)
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
        'open_interest','true_oi',
        'naive_gex','true_gex',
    ]
    # 'open','high','low','close','volume','ask_volume','bid_volume',
    # 'price','volatility','delta','gamma','theta','rho','vega',
    query_str = "SELECT * FROM gex_net WHERE ticker = %s and tstamp = %s"
    query_args = (ticker,utc_tstamp)
    fetched = await apostgres_execute(apool,query_str,query_args)

    time_b = time.time()
    logger.debug(f'pg select {time_b-time_a} {len(fetched)}')

    if len(fetched) == 0:
        if from_scratch:
            time_a = time.time()
            event_df = await get_events_df_from_scratch(apool,ticker,utc_tstamp,max_utc_tstamp,market_open_tstamp_utc,lookback_tstamp_utc)

            time_b = time.time()
            logger.info(f'get_events_df {time_b-time_a}')
            agg_df, qc_pass = compute_gex_core(event_df.copy(deep=True),from_scratch)
            agg_df['dstamp']=utc_tstamp.date()
            agg_df['tstamp']=utc_tstamp
            logger.debug(f'{from_scratch},{et_tstamp},{qc_pass},{len(event_df)},{len(agg_df)},{agg_df.true_gex.sum()}')

            time_c = time.time()
            logger.info(f'compute_gex_core {time_c-time_b}')

        else:
            time_a = time.time()
            event_df = await get_events_df(apool,ticker,utc_tstamp,max_utc_tstamp,prior_minute_utc_tstamp)

            time_b = time.time()
            logger.info(f'get_events_df {time_b-time_a}')
            agg_df, qc_pass = compute_gex_core(event_df.copy(deep=True),from_scratch)
            agg_df['dstamp']=utc_tstamp.date()
            agg_df['tstamp']=utc_tstamp
            logger.debug(f'{from_scratch},{et_tstamp},{qc_pass},{len(event_df)},{len(agg_df)},{agg_df.true_gex.sum()}')

            time_c = time.time()
            logger.info(f'compute_gex_core {time_c-time_b}')

        if persist_to_postgres and qc_pass:
            time_d = time.time()
            
            agg_df = agg_df[event_agg_columns]
            if csv_file:
                agg_df.to_csv(csv_file,index=False)

            query_dict = {}

            event_agg_query_str = "INSERT INTO event_agg (event_symbol,dstamp,open_interest,naive_gex,true_oi,true_gex,tstamp,ticker,expiration,contract_type,strike) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict (event_symbol,dstamp) do update set open_interest = %s, naive_gex = %s, true_oi = %s, true_gex = %s, tstamp = %s, ticker = %s, expiration = %s, contract_type = %s, strike = %s;"
            async def insert_event_agg(row):
                query_args = [row.event_symbol,row.dstamp,row.open_interest,row.naive_gex,row.true_oi,row.true_gex,row.tstamp,row.ticker,row.expiration,row.contract_type,row.strike,row.open_interest,row.naive_gex,row.true_oi,row.true_gex,row.tstamp,row.ticker,row.expiration,row.contract_type,row.strike]
                return query_args
            query_dict[event_agg_query_str] = await asyncio.gather(*(insert_event_agg(row) for n,row in agg_df.iterrows()))

            table_cols = ['ticker','strike','tstamp','naive_gex','true_gex']
            agg_df['ticker'] = ticker
            strike_gex_df = agg_df[table_cols]
            strike_gex_df = strike_gex_df.groupby(['ticker','strike','tstamp']).agg(
                naive_gex=pd.NamedAgg(column="naive_gex", aggfunc="sum"),
                true_gex=pd.NamedAgg(column="true_gex", aggfunc="sum"),
            ).reset_index()

            gex_strike_query_str = "INSERT INTO gex_strike (ticker,strike,tstamp,naive_gex,true_gex) VALUES (%s,%s,%s,%s,%s) on conflict (ticker,strike,tstamp) do update set naive_gex = %s,true_gex = %s;"
            async def insert_gex_strike(row):
                query_args = [row.ticker,row.strike,row.tstamp,row.naive_gex,row.true_gex,row.naive_gex,row.true_gex]
                return query_args
            query_dict[gex_strike_query_str] = await asyncio.gather(*(insert_gex_strike(row) for n,row in strike_gex_df.iterrows()))

            table_cols = ['ticker','tstamp','spot_price','naive_gex','true_gex']
            agg_df['ticker'] = ticker
            net_gex_df = agg_df[table_cols]
            net_gex_df = net_gex_df.groupby(['ticker','tstamp']).agg(
                spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
                naive_gex=pd.NamedAgg(column="naive_gex", aggfunc="sum"),
                true_gex=pd.NamedAgg(column="true_gex", aggfunc="sum"),
            ).reset_index()

            gex_net_query_str = "INSERT INTO gex_net (ticker,tstamp,naive_gex,true_gex,spot_price) VALUES (%s,%s,%s,%s,%s) on conflict (ticker,tstamp) do update set naive_gex = %s,true_gex = %s, spot_price = %s;"
            async def insert_gex_net(row):
                query_args = [row.ticker,row.tstamp,row.naive_gex,row.true_gex,row.spot_price,row.naive_gex,row.true_gex,row.spot_price]
                return query_args
            query_dict[gex_net_query_str] = await asyncio.gather(*(insert_gex_net(row) for n,row in net_gex_df.iterrows()))

            time_c = time.time()
            logger.info(f'query prep {time_c-time_d}')
            await apostgres_execute_many(apool,query_dict)

            time_e = time.time()
            logger.info(f'postgres_execute_many {time_e-time_c}')

        else:
            logger.debug(f'qc_pass {qc_pass}, {len(fetched)}')
            if first_minute:
                time_b = time.time()
                query_str = "INSERT INTO gex_net (ticker,tstamp) VALUES (%s,%s) ON CONFLICT DO NOTHING;"
                query_args = (ticker,utc_tstamp)
                await apostgres_execute(apool,query_str,query_args,is_commit=True)
                time_c = time.time()
                logger.info(f'postgres_execute {time_c-time_b}')
    else:
        logger.debug(f'{utc_tstamp} {len(fetched)} found!')
    return gex_df

def main(ticker,my_date):
    tstamp_list = pd.date_range(start=my_date+" 09:30:00",end=my_date+" 16:00:00",freq='s',tz=pytz.timezone('US/Eastern'))
    tstamp_list = pd.date_range(start=my_date+" 14:30:00",end=my_date+" 16:00:00",freq='s',tz=pytz.timezone('US/Eastern'))
    for tstamp in tqdm(tstamp_list):
        logger.debug(f'...')
        if tstamp > now_in_new_york():
            break
        try:
            from_scratch = None
            get_df = asyncio.run(compute_gex(ticker,tstamp,from_scratch=from_scratch,persist_to_postgres=True))
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
    #tryone(ticker)

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
where tstamp >= '2025-02-14 17:00:00' and tstamp < '2025-02-14 17:01:00' and event_symbol = 'SPX'
) union all (
select 'candle' as event_type,event_symbol,null::float as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from candle
where tstamp >= '2025-02-14 17:00:59' and tstamp < '2025-02-14 17:01:00' and ticker = 'SPXW'
) union all (
select 'summary' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp ,ticker,expiration,contract_type,strike from event_agg
where tstamp >= '2025-02-14 17:00:00' and tstamp < '2025-02-14 17:01:00' and ticker = 'SPXW'
) union all (
select 'greeks' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, price,volatility,delta,gamma,theta,rho,vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from greeks
where tstamp >= '2025-02-14 17:00:00' and tstamp < '2025-02-14 17:01:00' and ticker = 'SPXW'
) union all (
select 'timeandsale' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
where tstamp >= '2025-02-14 17:00:59' and tstamp < '2025-02-14 17:01:00' and ticker = 'SPXW'
)

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-554bc784bf-xrgkg 5432:5432

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres

python -m utils.compute_intraday SPX 2025-01-07 && \
python -m utils.compute_intraday SPX 2025-01-08

"""