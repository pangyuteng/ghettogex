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
        agg_df = asyncio.run(compute_gex(ticker,tstamp,from_scratch=None,persist_to_postgres=False))
        if agg_df is not None:
            logger.debug(agg_df.head())
            logger.debug(f'volume_gex {agg_df.volume_gex.sum()} state_gex {agg_df.state_gex.sum()}')

    except KeyboardInterrupt:
        sys.exit(1)
    except:
        traceback.print_exc()
        sys.exit(1)

async def compute_gex(ticker,et_tstamp,from_scratch=None,persist_to_postgres=True):
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        async with apool.connection() as aconn:
            return await _compute_gex(aconn,ticker,et_tstamp,persist_to_postgres=persist_to_postgres)

async def _compute_gex(aconn,ticker,et_tstamp,persist_to_postgres=True):
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

    """
        NOTE:
        in postgres tables `order_imbalance` we have:
        sum(ask_volume)-sum(bid_volume) as order_imbalance
        and then we sum `order_imbalance` up in table `order_imbalance_1day`
        ^^^^ note: order_imbalance is customer, thus we flip for market maker.
    """

    query_args = (ticker_alt,expiration,expiration,ticker_alt,expiration,expiration,ticker,expiration)
    fetched = await cpostgres_execute(aconn,ORDER_IMBALANCE_GEX_QUERY,query_args)
    df = pd.DataFrame([dict(x) for x in fetched])
    df['mm_order_imbalance'] = -1 * df.order_imbalance
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
        await cpostgres_copy(aconn,query_dict)

    time_e = time.time()
    logger.info(f'postgres_execute_many {time_e-time_c}')


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

python -m utils.compute_intraday SPX 2025-07-11

python -m utils.compute_intraday SPX 2025-07-02-10-00-00

"""