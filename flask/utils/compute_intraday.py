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

async def get_events_df_from_scratch(apool,ticker,utc_tstamp,max_utc_tstamp,future_utc_tstamp,min_utc_tstamp):
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
        'open_interest','price','bid_price','ask_price','volatility','delta','gamma','theta','rho','vega',
        'bid_time','ask_time','bid_size','ask_size',
        'size','aggressor_side','ticker','expiration','contract_type','strike','tstamp',
    ]

    query_str = """
    select 'underlying_candle' as event_type,event_symbol,close as spot_price,time,tstamp from candle
    where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker)
    uc = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'candle' as event_type,event_symbol,open,high,low,close,volume,ask_volume,bid_volume,tstamp,ticker,expiration,contract_type,time,strike from candle
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration)
    oc = apostgres_execute(apool,query_str,query_args)

    # TODO: get prior day open_interest and true_oi by creating a new event_agg row.
    query_str = """
    --select 'summary' as event_type,event_symbol,open_interest,tstamp,ticker,expiration,contract_type,strike from summary
    select 'summary' as event_type,event_symbol,0 as open_interest, 0 as true_oi,tstamp,ticker,expiration,contract_type,strike from summary
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration)
    os = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'greeks' as event_type,event_symbol,price,volatility,delta,gamma,theta,rho,vega,time,tstamp,ticker,expiration,contract_type,strike from greeks
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration)
    og = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'timeandsale' as event_type,event_symbol,size,price,bid_price,ask_price,aggressor_side,time,tstamp,ticker,expiration,contract_type,strike from timeandsale
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration)
    ot = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'quote' as event_type,event_symbol,bid_time,ask_time,bid_price,ask_price,bid_size,ask_size,tstamp,ticker,expiration,contract_type,strike from quote
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (utc_tstamp,future_utc_tstamp,ticker_alt,expiration) # quote
    oq = apostgres_execute(apool,query_str,query_args)

    all_groups = await asyncio.gather(uc,oc,os,og,ot,oq)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        pd_list = [pd.DataFrame(x,columns=columns) for x in all_groups if x is not None]
        df = pd.concat(pd_list,ignore_index=True)
    return df

async def get_events_df(apool,ticker,utc_tstamp,max_utc_tstamp,future_utc_tstamp,min_utc_tstamp):

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
    select 'underlying_candle' as event_type,event_symbol,close as spot_price,time,tstamp from candle
    where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker) # underlying_candle
    uc = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'candle' as event_type,event_symbol,open,high,low,close,volume,ask_volume,bid_volume,time,tstamp,ticker,expiration,contract_type,strike from candle
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (utc_tstamp,max_utc_tstamp,ticker_alt,expiration) # candle
    oc = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'summary' as event_type,event_symbol,true_oi,open_interest,tstamp,ticker,expiration,contract_type,strike from event_agg
    where dstamp = %s and ticker = %s and expiration = %s
    """
    query_args = (utc_tstamp.date(),ticker_alt,expiration) # event_agg
    os = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'greeks' as event_type,event_symbol,price,volatility,delta,gamma,theta,rho,vega,time,tstamp,ticker,expiration,contract_type,strike from greeks
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (min_utc_tstamp,max_utc_tstamp,ticker_alt,expiration) # greeks
    og = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'timeandsale' as event_type,event_symbol,size,price,bid_price,ask_price,aggressor_side,time,tstamp,ticker,expiration,contract_type,strike from timeandsale
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (utc_tstamp,max_utc_tstamp,ticker_alt,expiration) # timeandsale
    ot = apostgres_execute(apool,query_str,query_args)

    query_str = """
    select 'quote' as event_type,event_symbol,bid_time,ask_time,bid_price,ask_price,bid_size,ask_size,tstamp,ticker,expiration,contract_type,strike from quote
    where tstamp >= %s and tstamp < %s and ticker = %s and expiration = %s
    """
    query_args = (utc_tstamp,future_utc_tstamp,ticker_alt,expiration) # quote
    oq = apostgres_execute(apool,query_str,query_args)

    all_groups = await asyncio.gather(uc,oc,os,og,ot,oq)
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

# observations
# + greeks needs to be updated if no greeks and options candle exists
# + spot needs to be updated if candle, and you got underlying quotes.
# + summary event seems to be only once a day?

# NOTE:
# kinda want to go with below
# and then you can confirm with prior day summary.
# + for relatively normal sizsed orders, use price vs bid/ask
# + for relatively large order, go with orderbook liquidity (quote history, change in bid/ask size and price? so with 3 sec lag?)
# + unsure about volatility surface... dont have a method for now.
#
# use quote or timeandsale to bid/ask trend to determine side
# if mid, assume buy/sell is matched, return 0

def get_side_mod(row,quote_df=None,datasource='tasty'):
    try:
        side_mod = None
        if datasource == 'tasty':
            if row.large_order:
                tmp_df = quote_df[quote_df.event_symbol==row.event_symbol]
                cond_met = True if len(tmp_df) > 2 else False
                if cond_met:
                    #cols = ['bid_size','bid_price','ask_price','ask_size','ask_time','bid_time','tstamp']
                    #print(tmp_df[cols],'!!!!!!!!!!!!!!!!!!!1')
                    ask_price_list = tmp_df.ask_price.to_list()
                    bid_price_list = tmp_df.bid_price.to_list()
                    # TODO: hau volatility also uses ask_size and bid_size
                    if ask_price_list[-1] > ask_price_list[0]:
                        side_mod = 'likely_ask'
                    elif bid_price_list[-1] > bid_price_list[0]:
                        side_mod = 'likely_ask'
                    else:
                        side_mod = 'likely_bid' #???
                else:
                    if row.aggressor_side == 'BUY':
                        side_mod = 'ask' # BUY or near ask
                    elif row.aggressor_side == 'SELL':
                        side_mod = 'bid' # SELL or near bid
                    elif row.aggressor_side == 'UNDEFINED':
                        pass # assume mid is matched.
            else:
                if row.aggressor_side == 'BUY':
                    side_mod = 'ask' # BUY or near ask
                elif row.aggressor_side == 'SELL':
                    side_mod = 'bid' # SELL or near bid
                elif row.aggressor_side == 'UNDEFINED':
                    pass # assume mid is matched.
        elif datasource == 'uw':
            # uw data have no quote event, instead, we use the nbbo_ask,nbbo_bid from flow data.
            idx = row['index']
            cond_met = row.strike == quote_df.at[idx+1,"strike"]
            if row.large_order and cond_met:
                if quote_df.at[idx+1,"nbbo_ask"] > quote_df.at[idx,"nbbo_ask"]:
                    side_mod = 'likely_ask'
                elif arg_df.at[idx+1,"nbbo_bid"] > arg_df.at[idx,"nbbo_bid"]:
                    side_mod = 'likely_ask'
                else:
                    side_mod = 'likely_bid' #???
            else:
                if row.side == 'ask': # near ask, client bought, dealer short
                    side_mod = 'ask'
                elif row.side == 'bid': # near bid, client sold, dealer long
                    side_mod = 'bid'
                else:
                    pass # assume mid is matched.
                    # TODO: volatility surface fitting
        else:
            raise NotImplementedError()

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

def compute_gex_core(df,from_scratch):
    # NOTE: we sort by time first, since tstamp is postgres insert time.
    df = df.sort_values(by=['event_type','time','tstamp'])

    underlying_candle_df = df[df.event_type=='underlying_candle']
    if len(underlying_candle_df)>0:
        spot_price = underlying_candle_df.spot_price.to_list()[-1]
    else:
        spot_price = np.nan

    candle_df = df[df.event_type=='candle']
    summary_df = df[df.event_type=='summary']
    greeks_df = df[df.event_type=='greeks']
    quote_df = df[df.event_type=='quote']
    ts_df = df[df.event_type=='timeandsale'].copy()

    # flag large orders using timeandsale (NOTE: alternatively use size relative to bid/ask size in quote event)
    ts_df['size'] = ts_df['size'].astype(float)
    large_order_th = ts_df['size'].mean()+3*ts_df['size'].std()
    ts_df['large_order'] = ts_df['size'].apply(lambda x:x > large_order_th)
    ts_df['side_mod'] = ts_df.apply(lambda x: get_side_mod(x,quote_df=quote_df),axis=1)
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
    
    # TODO: compute vanna and charm
    greeks_df = greeks_df[['event_symbol','price','volatility','delta','gamma','theta','rho','vega']]
    greeks_df = greeks_df.groupby(['event_symbol']).last().reset_index()

    timeandsale_df = ts_df[['event_symbol','size_signed']]
    timeandsale_df = timeandsale_df.groupby(['event_symbol']).sum().reset_index()

    merged_df = greeks_df.merge(summary_df,how='left',on=['event_symbol'])
    merged_df = merged_df.merge(timeandsale_df,how='left',on=['event_symbol'])
    merged_df = merged_df.merge(candle_df,how='left',on=['event_symbol'])

    # contract_type_int is the naive gex assumption. dealer is long call, short put
    merged_df['contract_type_int'] = merged_df.contract_type.apply(lambda x: -1 if x == 'P' else 1)
    merged_df['spot_price']=spot_price

    for col_name in ['gamma','open_interest','true_oi','spot_price','contract_type_int','size_signed','price','volume','ask_volume','bid_volume']:
        merged_df[col_name] = pd.to_numeric(merged_df[col_name], errors='coerce')

    merged_df.true_oi = merged_df.true_oi.fillna(value=0)
    merged_df.open_interest = merged_df.open_interest.fillna(value=0)
    merged_df.price = merged_df.price.fillna(value=0)
    merged_df.size_signed = merged_df.size_signed.fillna(value=0)
    merged_df.volume = merged_df.volume.fillna(value=0)
    merged_df.ask_volume = merged_df.ask_volume.fillna(value=0)
    merged_df.bid_volume = merged_df.bid_volume.fillna(value=0)

    # NOTE: let `volume_gex` open_interest be updated using
    # ask means buy, dealer is short, bid means sell, dealer is long
    merged_df.open_interest = merged_df.open_interest-merged_df.ask_volume+merged_df.bid_volume
    # NOTE: let `true_oi` be computed using theo_aggressor_side
    merged_df.true_oi = merged_df.true_oi + merged_df.size_signed

    # UNSURE HERE... stil at debug phase
    # KISS.
    # `volume_gex` update summary based on bid-ask volume using summary open_interest
    # `state_gex` uses timeandsale and true_oi (for now starts from 0 at start of day)
    
    # volume_gex is the vanilla flavor
    merged_df['volume_gex'] = merged_df.gamma * merged_df.open_interest * 100 * merged_df.spot_price * merged_df.spot_price * 0.01 * merged_df.contract_type_int
    # state gex is a WIP.
    merged_df['state_gex'] = merged_df.gamma * merged_df.true_oi * 100 * merged_df.spot_price * merged_df.spot_price * 0.01 * merged_df.contract_type_int
    
    #merged_df['convexity'] = merged_df.gamma * merged_df.true_oi * 100 * merged_df.spot_price * merged_df.spot_price * 0.01
    #merged_df['dex'] = merged_df.delta * merged_df.true_oi * 100 * merged_df.spot_price * merged_df.spot_price * 0.01
    #merged_df['vanna'] = 
    #merged_df['charm'] = 

    merged_df.volume_gex = merged_df.volume_gex.fillna(value=0)
    merged_df.state_gex = merged_df.state_gex.fillna(value=0)
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

async def compute_gex(ticker,et_tstamp,from_scratch=None,persist_to_postgres=True):
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=30,open=False) as apool:
        await _compute_gex(apool,ticker,et_tstamp,from_scratch=from_scratch,persist_to_postgres=persist_to_postgres)

async def _compute_gex(apool,ticker,et_tstamp,from_scratch=None,persist_to_postgres=True):
    time_a = time.time()

    gex_df = None
    csv_file = f"tmp/volume_gex-{et_tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
    csv_file = None # TOO SLOW! FOR DEBUG
    utc = pytz.timezone('UTC')
    utc_tstamp = et_tstamp.astimezone(tz=utc)
    max_utc_tstamp = utc_tstamp+datetime.timedelta(seconds=1)
    future_utc_tstamp = utc_tstamp+datetime.timedelta(seconds=2) # grab quotes
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
        'volume_gex','state_gex',
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
            event_df = await get_events_df_from_scratch(apool,ticker,utc_tstamp,max_utc_tstamp,future_utc_tstamp,market_open_tstamp_utc)
            time_b = time.time()
            logger.info(f'get_events_df {time_b-time_a}')
            agg_df, qc_pass = compute_gex_core(event_df.copy(deep=True),from_scratch)
            agg_df['dstamp']=utc_tstamp.date()
            agg_df['tstamp']=utc_tstamp
            logger.debug(f'{from_scratch},{et_tstamp},{qc_pass},{len(event_df)},{len(agg_df)},{agg_df.state_gex.sum()}')

            time_c = time.time()
            logger.info(f'compute_gex_core {time_c-time_b}')

        else:
            time_a = time.time()
            event_df = await get_events_df(apool,ticker,utc_tstamp,max_utc_tstamp,future_utc_tstamp,prior_minute_utc_tstamp)

            time_b = time.time()
            logger.info(f'get_events_df {time_b-time_a}')
            agg_df, qc_pass = compute_gex_core(event_df.copy(deep=True),from_scratch)
            agg_df['dstamp']=utc_tstamp.date()
            agg_df['tstamp']=utc_tstamp
            logger.debug(f'{from_scratch},{et_tstamp},{qc_pass},{len(event_df)},{len(agg_df)},{agg_df.state_gex.sum()}')

            time_c = time.time()
            logger.info(f'compute_gex_core {time_c-time_b}')

        if persist_to_postgres and qc_pass:
            time_d = time.time()
            
            agg_df = agg_df[event_agg_columns]
            if csv_file:
                agg_df.to_csv(csv_file,index=False)

            query_dict = {}

            event_agg_query_str = "INSERT INTO event_agg (event_symbol,dstamp,open_interest,volume_gex,true_oi,state_gex,tstamp,ticker,expiration,contract_type,strike) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict (event_symbol,dstamp) do update set open_interest = %s, volume_gex = %s, true_oi = %s, state_gex = %s, tstamp = %s, ticker = %s, expiration = %s, contract_type = %s, strike = %s;"
            async def insert_event_agg(row):
                query_args = [row.event_symbol,row.dstamp,row.open_interest,row.volume_gex,row.true_oi,row.state_gex,row.tstamp,row.ticker,row.expiration,row.contract_type,row.strike,row.open_interest,row.volume_gex,row.true_oi,row.state_gex,row.tstamp,row.ticker,row.expiration,row.contract_type,row.strike]
                return query_args
            query_dict[event_agg_query_str] = await asyncio.gather(*(insert_event_agg(row) for n,row in agg_df.iterrows()))
            
            # TODO: grab call_oi,put_oi,naive_dex,true_dex,

            table_cols = ['ticker','strike','tstamp','volume_gex','state_gex']
            agg_df['ticker'] = ticker
            strike_gex_df = agg_df[table_cols]
            strike_gex_df = strike_gex_df.groupby(['ticker','strike','tstamp']).agg(
                volume_gex=pd.NamedAgg(column="volume_gex", aggfunc="sum"),
                state_gex=pd.NamedAgg(column="state_gex", aggfunc="sum"),
            ).reset_index()
            
            """
            dex double precision,
            convexity double precision,
            charm double precision,
            vanna double precision,
            call_oi double precision,
            call_dex double precision,
            call_gex double precision,
            call_vanna double precision,
            call_charm double precision,
            put_oi double precision,
            put_dex double precision,
            put_gex double precision,
            put_vanna double precision,
            put_charm double precision,
            """

            # 'call_oi', 'put_oi', 'call_gex','put_gex', 'dex', 'vanna'?
            gex_strike_query_str = "INSERT INTO gex_strike (ticker,strike,tstamp,volume_gex,state_gex) VALUES (%s,%s,%s,%s,%s) on conflict (ticker,strike,tstamp) do update set volume_gex = %s,state_gex = %s;"
            async def insert_gex_strike(row):
                query_args = [row.ticker,row.strike,row.tstamp,row.volume_gex,row.state_gex,row.volume_gex,row.state_gex]
                return query_args
            query_dict[gex_strike_query_str] = await asyncio.gather(*(insert_gex_strike(row) for n,row in strike_gex_df.iterrows()))
            
            # 'true_dex','call_gex','put_gex', major_call_gex_strike, major_put_gex_strike
            table_cols = ['ticker','tstamp','spot_price','volume_gex','state_gex']
            agg_df['ticker'] = ticker
            net_gex_df = agg_df[table_cols]
            net_gex_df = net_gex_df.groupby(['ticker','tstamp']).agg(
                spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
                volume_gex=pd.NamedAgg(column="volume_gex", aggfunc="sum"),
                state_gex=pd.NamedAgg(column="state_gex", aggfunc="sum"),
            ).reset_index()

            gex_net_query_str = "INSERT INTO gex_net (ticker,tstamp,volume_gex,state_gex,spot_price) VALUES (%s,%s,%s,%s,%s) on conflict (ticker,tstamp) do update set volume_gex = %s,state_gex = %s, spot_price = %s;"
            async def insert_gex_net(row):
                query_args = [row.ticker,row.tstamp,row.volume_gex,row.state_gex,row.spot_price,row.volume_gex,row.state_gex,row.spot_price]
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

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-554bc784bf-xrgkg 5432:5432

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres

python -m utils.compute_intraday SPX 2025-06-05

"""