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

from .pg_queries import ORDER_IMBALANCE_GEX_QUERY
from .postgres_utils import (
    cpostgres_execute, cpostgres_copy,
    psycopg_pool,postgres_uri,
)

from .misc import timedelta_from_market_open, now_in_new_york
from .iv_utils import (
    TOTAL_SECONDS_ONE_YEAR,
    get_expiry_tstamp,
    compute_greeks,
    compute_exposure,
)

async def compute_gex(ticker,et_tstamp,persist_to_postgres=True):
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        async with apool.connection() as aconn:
            try:
                return await _compute_og_gex(aconn,ticker,et_tstamp,persist_to_postgres=persist_to_postgres)
            except:
                logger.error(traceback.format_exc())
            try:
                return await _compute_ghetto_gex(aconn,ticker,et_tstamp,persist_to_postgres=persist_to_postgres)
            except:
                logger.error(traceback.format_exc())

"""
    mm_order_imbalance NOTE:
    In postgres tables `order_imbalance` we have:
    `sum(ask_volume)-sum(bid_volume) as order_imbalance`
    and then we sum `order_imbalance` up in table `order_imbalance_1day`,
    Since `order_imbalance` is customer perspective, 
    for market maker `mm_order_imbalance` we flip it via *-1.
"""

async def _compute_og_gex(aconn,ticker,et_tstamp,persist_to_postgres=True):
    time_c = time.time()
    utc = pytz.timezone('UTC')
    utc_tstamp = et_tstamp.astimezone(tz=utc)

    if ticker == 'SPX':
        ticker_alt = 'SPXW'
    elif ticker == 'NDX':
        ticker_alt = 'NDXP'
    elif ticker == 'VIX':
        ticker_alt = 'VIXW'
    else:
        ticker_alt = ticker

    expiration = utc_tstamp.date()

    query_args = (ticker_alt,expiration,expiration,ticker_alt,expiration,expiration,ticker_alt,expiration,expiration,ticker,expiration,'VIX',expiration)
    fetched = await cpostgres_execute(aconn,ORDER_IMBALANCE_GEX_QUERY,query_args)
    df = pd.DataFrame([dict(x) for x in fetched])
    df['mm_order_imbalance'] = -1 * df.order_imbalance # see above mm_order_imbalance note
    df['hedge_sign'] = df.contract_type.apply(lambda x: -1 if x == 'P' else 1)

    # NOTE: we are using dxlink gamma which is lagged by 1 min!!!
    df['volume_gex'] = df.gamma * df.mm_order_imbalance * df.spot_price * df.spot_price * df.hedge_sign
    df['state_gex'] = df.volume_gex
    df['tstamp'] = utc_tstamp.replace(tzinfo=None) # postgres tsamp have no tzinfo
    df['ticker'] = ticker

    query_dict = {}

    table_cols = ['ticker','strike','tstamp','spot_price','volume_gex','state_gex','mm_order_imbalance']

    strike_ex_df = df[table_cols].copy()
    strike_ex_df = strike_ex_df.groupby(['ticker','strike','tstamp']).agg(
        spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
        volume_gex=pd.NamedAgg(column="volume_gex", aggfunc="sum"),
        state_gex=pd.NamedAgg(column="state_gex", aggfunc="sum"),
    ).reset_index()

    gex_strike_query_str = """
        COPY gex_strike (ticker,strike,tstamp,volume_gex,state_gex) FROM STDIN
    """
    async def insert_gex_strike(row):
        query_args = [row.ticker,row.strike,row.tstamp,row.volume_gex,row.state_gex]
        return query_args
    query_dict[gex_strike_query_str] = await asyncio.gather(*(insert_gex_strike(row) for n,row in strike_ex_df.iterrows()))

    table_cols = [
        'ticker','tstamp','spot_price','volume_gex','state_gex',
    ]

    net_gex_df = strike_ex_df[table_cols].copy()
    net_gex_df = net_gex_df.groupby(['ticker','tstamp']).agg(
        spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
        volume_gex=pd.NamedAgg(column="volume_gex", aggfunc="sum"),
        state_gex=pd.NamedAgg(column="state_gex", aggfunc="sum"),
    ).reset_index()

    gex_net_query_str = """
        COPY gex_net (ticker,tstamp,volume_gex,state_gex,spot_price) FROM STDIN
    """
    async def insert_gex_net(row):
        query_args = [row.ticker,row.tstamp,row.volume_gex,row.state_gex,row.spot_price]
        return query_args

    query_dict[gex_net_query_str] = await asyncio.gather(*(insert_gex_net(row) for n,row in net_gex_df.iterrows()))

    if persist_to_postgres:
        try:
            await cpostgres_copy(aconn,query_dict)
        except:
            logger.error(f"{query_dict}")
            traceback.print_exc()

    time_e = time.time()
    logger.info(f'_compute_og_gex done {time_e-time_c}')
    return net_gex_df

"""
NOTE: goal of _compute_ghetto_gex is to ultimately replace _compute_og_gex
"""
async def _compute_ghetto_gex(aconn,ticker,et_tstamp,persist_to_postgres=True):
    time_c = time.time()
    utc = pytz.timezone('UTC')
    utc_tstamp = et_tstamp.astimezone(tz=utc)

    if ticker == 'SPX':
        ticker_alt = 'SPXW'
    elif ticker == 'NDX':
        ticker_alt = 'NDXP'
    elif ticker == 'VIX':
        ticker_alt = 'VIXW'
    else:
        ticker_alt = ticker

    expiration = utc_tstamp.date()

    query_args = (ticker_alt,expiration,expiration,ticker_alt,expiration,expiration,ticker_alt,expiration,expiration,ticker,expiration,'VIX',expiration)
    fetched = await cpostgres_execute(aconn,ORDER_IMBALANCE_GEX_QUERY,query_args)
    
    df = pd.DataFrame([dict(x) for x in fetched])

    df['ticker'] = ticker
    df['ticker_alt'] = ticker_alt
    df['tstamp'] = utc_tstamp.replace(tzinfo=None) # postgres tsamp have no tzinfo
    df['price'] = (df.ask_price+df.bid_price)/2
    df['mm_order_imbalance'] = -1 * df.order_imbalance # see above mm_order_imbalance note

    try:

        df['convexity'] = df.gamma*df.order_imbalance
        expiration_mapper = {x:get_expiry_tstamp(x.strftime("%Y-%m-%d")) for x in list(df.expiration.unique())}
        df['time_till_exp'] = df.apply(lambda x: (expiration_mapper[x.expiration]-x.tstamp).total_seconds()/TOTAL_SECONDS_ONE_YEAR, axis=1)
        epsilon = 1e-5
        df.loc[df.time_till_exp==0,'time_till_exp'] = epsilon

        # ignore py_vollib_vectorized intrinsic warning
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df['open_interest']=df.mm_order_imbalance
            compute_greeks(df)
            compute_exposure(df)

    except:
        logger.error(traceback.format_exc())
        raise ValueError("time_till_exp error")

    query_dict = {}

    # event_contract ####################################

    event_contract_query_str = """
        COPY event_contract (tstamp,event_symbol,ticker,expiration,contract_type,strike,mm_order_imbalance,ask_price,bid_price,volatility,delta,gamma,dex,gex,vex,cex,convexity) FROM STDIN
    """
    async def insert_event_contract(row):
        query_args = [
            row.tstamp,row.event_symbol,row.ticker_alt,row.expiration,row.contract_type,row.strike,
            row.mm_order_imbalance,row.ask_price,row.bid_price,
            row.volatility,row.delta,row.gamma,row.dex,row.gex,row.vex,row.cex,row.convexity]
        return query_args

    query_dict[event_contract_query_str] = await asyncio.gather(*(insert_event_contract(row) for n,row in df.iterrows()))

    # event_strike ####################################

    table_cols = ['ticker','strike','tstamp','gex','dex','vex','cex','convexity']
    strike_df = df[table_cols].copy()
    strike_df = strike_df.groupby(['ticker','strike','tstamp']).agg(
        dex=pd.NamedAgg(column="dex", aggfunc="sum"),
        gex=pd.NamedAgg(column="gex", aggfunc="sum"),
        vex=pd.NamedAgg(column="vex", aggfunc="sum"),
        cex=pd.NamedAgg(column="cex", aggfunc="sum"),
        convexity=pd.NamedAgg(column="convexity", aggfunc="sum"),
    ).reset_index()
    event_strike_query_str = """
        COPY event_strike (ticker,strike,tstamp,dex,gex,vex,cex,convexity) FROM STDIN
    """
    async def insert_event_strike(row):
        query_args = [row.ticker,row.strike,row.tstamp,row.dex,row.gex,row.vex,row.cex,row.convexity]
        return query_args

    query_dict[event_strike_query_str] = await asyncio.gather(*(insert_event_strike(row) for n,row in strike_df.iterrows()))

    # event_underlying ####################################

    table_cols = ['ticker','tstamp','spot_price','gex','dex','vex','cex','convexity']
    underlying_df = df[table_cols].copy()
    underlying_df = underlying_df.groupby(['ticker','tstamp']).agg(
        spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
        dex=pd.NamedAgg(column="dex", aggfunc="sum"),
        gex=pd.NamedAgg(column="gex", aggfunc="sum"),
        vex=pd.NamedAgg(column="vex", aggfunc="sum"),
        cex=pd.NamedAgg(column="cex", aggfunc="sum"),
        convexity=pd.NamedAgg(column="convexity", aggfunc="sum"),
    ).reset_index()

    event_underlying_query_str = """
        COPY event_underlying (ticker,tstamp,spot_price,dex,gex,vex,cex,convexity) FROM STDIN
    """
    async def insert_underlying(row):
        query_args = [row.ticker,row.tstamp,row.spot_price,row.dex,row.gex,row.vex,row.cex,row.convexity]
        return query_args

    query_dict[event_underlying_query_str] = await asyncio.gather(*(insert_underlying(row) for n,row in underlying_df.iterrows()))

    if persist_to_postgres:
        try:
            await cpostgres_copy(aconn,query_dict)
        except:
            logger.error(f"{query_dict}")
            traceback.print_exc()

    time_e = time.time()
    logger.info(f'_compute_ghetto_gex done {time_e-time_c}')
    return underlying_df


def main(ticker,my_date):
    tstamp_list = pd.date_range(start=my_date+" 09:30:00",end=my_date+" 16:00:00",freq='s',tz=pytz.timezone('US/Eastern'))
    for tstamp in tqdm(tstamp_list):
        logger.debug(f'...')
        if tstamp > now_in_new_york():
            break
        try:
            out_df = asyncio.run(compute_gex(ticker,tstamp,persist_to_postgres=False))
            if out_df is not None:
                logger.debug(f'gex {out_df.gex.sum()} convexity {out_df.convexity.sum()}')
                #logger.debug(f'volume_gex {out_df.volume_gex.sum()} state_gex {out_df.state_gex.sum()}')
        except KeyboardInterrupt:
            sys.exit(1)
        except:
            traceback.print_exc()
            sys.exit(1)

def tryone(ticker,tstampstr):
    tstamp = datetime.datetime.strptime(tstampstr,'%Y-%m-%d-%H-%M-%S')
    tstamp = pytz.timezone('US/Eastern').localize(tstamp)
    logger.debug(f"{ticker} {tstamp}")
    try:
        out_df = asyncio.run(compute_gex(ticker,tstamp,persist_to_postgres=True))
        if out_df is not None:
            logger.debug(out_df.head())
            logger.debug(f'gex {out_df.gex.sum()} convexity {out_df.convexity.sum()}')
            #logger.debug(f'volume_gex {out_df.volume_gex.sum()} state_gex {out_df.state_gex.sum()}')

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

python -m utils.compute_intraday SPX 2025-11-21

python -m utils.compute_intraday SPX 2025-11-21-15-59-59

"""