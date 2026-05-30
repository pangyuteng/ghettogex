
import logging
logger = logging.getLogger(__file__)
import warnings
import os
import signal
import re
import sys
import uuid
import ast
import time
import math
import traceback
import datetime
import pytz
import json
import pathlib

import pandas as pd
import numpy as np

import uuid
from anyio import sleep
from httpx_ws import HTTPXWSException
import aiofiles
import aiofiles.os
import asyncio
from dataclasses import dataclass

import pandas_market_calendars as mcal
import tastytrade
from tastytrade import DXLinkStreamer
from tastytrade.instruments import get_option_chain, get_future_option_chain
from tastytrade.dxfeed import (
    Candle, Greeks, Profile, Quote, Summary, TheoPrice, TimeAndSale, Trade, Underlying, 
)
from tastytrade.instruments import Equity, Option, Future, FutureOption, OptionType, InstrumentType
from tastytrade.session import Session
from tastytrade.utils import today_in_new_york
from tastytrade.market_data import get_market_data

from .misc import (
    now_in_new_york,
    is_market_open,
    timedelta_from_market_open,
)
from .postgres_utils import (
    cpostgres_execute,cpostgres_copy,
    psycopg,psycopg_pool,postgres_uri,
    postgres_execute,
)


def time_to_datetime(epoch_time):
    return datetime.datetime.fromtimestamp(epoch_time//1e3)

def is_test_func():
    return False if os.environ.get('IS_TEST') == 'FALSE' else True

def get_session():
    is_test = is_test_func()
    client_secret = os.environ.get('TASTYTRADE_CLIENT_SECRET')
    refresh_token = os.environ.get('TASTYTRADE_REFRESH_TOKEN')
    session = Session(client_secret,refresh_token,is_test=is_test)
    return session

async def get_equity_data(ticker):
    session = get_session()
    return await get_market_data(session,ticker,InstrumentType.EQUITY)

async def get_equity_data_session_reuse(ticker):
    session = get_session_reuse()
    return await get_market_data(session,ticker,InstrumentType.EQUITY)

def get_session_reuse(refresh_serialized=False):
    is_test = is_test_func()
    client_secret = os.environ.get('TASTYTRADE_CLIENT_SECRET')
    refresh_token = os.environ.get('TASTYTRADE_REFRESH_TOKEN')

    daystamp = now_in_new_york().strftime("%Y-%m-%d")

    if refresh_serialized is False:
        fetched = postgres_execute("select * from session where session_id = 1",(),is_commit=False)
    else:
        fetched = []

    if len(fetched) == 0:
        serialized_session = None
        logger.debug("no existing session found, will create new session")
    else:
        serialized_session = fetched[0]['serialized_session']

    if serialized_session:
        session = Session.deserialize(serialized_session)
    else:
        session = Session(client_secret,refresh_token,is_test=is_test)
        serialized_session = session.serialize()
        
        query_str = """
        INSERT INTO session (session_id,serialized_session) VALUES (%s,%s) ON CONFLICT (session_id) DO UPDATE SET serialized_session = %s;
        """
        query_args = (1,serialized_session,serialized_session)
        postgres_execute(query_str,query_args,is_commit=True)
        logger.debug("persisting new session to postgres")

    return session

from decimal import Decimal
def postgres_friendly(value):
    if type(value) == Decimal:
        return float(value)
    else:
        return value

async def persist_to_postgres(aconn,ticker,streamer_symbol,event_type,event):
    warnings.warn("deprecated")
    event_dict = dict(event)

    if streamer_symbol.startswith("."):
        ticker,expiration,contract_type,strike = parse_symbol(streamer_symbol)
        event_dict['ticker']=ticker
        event_dict['expiration']=expiration
        event_dict['contract_type']=contract_type
        event_dict['strike']=strike

    if "{=" in event_dict["event_symbol"]: # eventSymbol
        event_dict['event_symbol'] = streamer_symbol

    cols = list(event_dict.keys())
    vals = [postgres_friendly(event_dict[x]) for x in cols]
    vals_str_list = ["%s"] * len(vals)
    vals_str = ", ".join(vals_str_list)
    query_str = "INSERT INTO {event_type} ({cols}) VALUES ({vals_str})".format(event_type=event_type,cols = ','.join(cols), vals_str = vals_str)
    query_args = vals
    await cpostgres_execute(aconn,query_str,query_args,is_commit=True)

async def cpostgres_execute_list(aconn,insert_list):
    warnings.warn("deprecated")
    response = None
    try:
        async with aconn.cursor() as curs:
            for query_str,query_args in insert_list:
                await curs.execute(query_str,query_args)
        await aconn.commit()
    except:
        traceback.print_exc()
    return response

def print_copy_statements():
    event_class_dict = {
        "candle":Candle,
        "quote":Quote,
        "greeks":Greeks,
        "summary":Summary,
        "timeandsale":TimeAndSale,
        "profile":Profile,
        "thoeprice":TheoPrice,
        "underlying":Underlying,
        "trade":Trade,
    }
    extra_cols = ',ticker,expiration,contract_type,strike'
    for k,v in event_class_dict.items():
        cols_str=','.join([x for x in v.model_fields])+extra_cols
        copy_statement = f'COPY {k} ({cols_str}) FROM STDIN'
        print(copy_statement)

COPY_STATEMENT_DICT = dict(
    candle_underlying="""COPY candle (event_symbol,event_time,event_flags,index,time,sequence,count,volume,vwap,bid_volume,ask_volume,imp_volatility,open_interest,open,high,low,close) FROM STDIN""",
    quote_underlying="""COPY quote (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size) FROM STDIN""",
    candle="""COPY candle (event_symbol,event_time,event_flags,index,time,sequence,count,volume,vwap,bid_volume,ask_volume,imp_volatility,open_interest,open,high,low,close,ticker,expiration,contract_type,strike) FROM STDIN""",
    quote="""COPY quote (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike) FROM STDIN""",
    greeks="""COPY greeks (event_symbol,event_time,event_flags,index,time,sequence,price,volatility,delta,gamma,theta,rho,vega,ticker,expiration,contract_type,strike) FROM STDIN""",
    summary="""COPY summary (event_symbol,event_time,day_id,day_close_price_type,prev_day_id,prev_day_close_price_type,open_interest,day_open_price,day_high_price,day_low_price,day_close_price,prev_day_close_price,prev_day_volume,ticker,expiration,contract_type,strike) FROM STDIN""",
    timeandsale="""COPY timeandsale (event_symbol,event_time,event_flags,index,time,time_nano_part,sequence,exchange_code,price,size,bid_price,ask_price,exchange_sale_conditions,trade_through_exempt,aggressor_side,spread_leg,extended_trading_hours,valid_tick,type,buyer,seller,ticker,expiration,contract_type,strike) FROM STDIN""",
    profile="""COPY profile (event_symbol,event_time,description,short_sale_restriction,trading_status,halt_start_time,halt_end_time,ex_dividend_day_id,status_reason,high_52_week_price,low_52_week_price,beta,shares,high_limit_price,low_limit_price,earnings_per_share,ex_dividend_amount,dividend_frequency,free_float,ticker,expiration,contract_type,strike) FROM STDIN""",
    thoeprice="""COPY thoeprice (event_symbol,event_time,event_flags,index,time,sequence,price,underlying_price,delta,gamma,dividend,interest,ticker,expiration,contract_type,strike) FROM STDIN""",
    underlying="""COPY underlying (event_symbol,event_time,event_flags,index,time,sequence,volatility,front_volatility,back_volatility,call_volume,put_volume,option_volume,put_call_ratio,ticker,expiration,contract_type,strike) FROM STDIN""",
    trade="""COPY trade (event_symbol,event_time,time,time_nano_part,sequence,exchange_code,day_id,tick_direction,extended_trading_hours,price,change,size,day_volume,day_turnover,ticker,expiration,contract_type,strike) FROM STDIN""",
)

@dataclass
class PgInsertQueue:
    queue_dict: dict
    flush_event_dict: dict
    max_queue_size: int
    interval: float
    @classmethod
    async def create(cls):
        queue_dict = dict(
            candle_underlying=asyncio.Queue(),
            quote_underlying=asyncio.Queue(),
            candle=asyncio.Queue(),
            quote=asyncio.Queue(),
            greeks=asyncio.Queue(),
            summary=asyncio.Queue(),
            timeandsale=asyncio.Queue(),
        )
        
        flush_event_dict = dict(
            candle_underlying=asyncio.Event(),
            quote_underlying=asyncio.Event(),
            candle=asyncio.Event(),
            quote=asyncio.Event(),
            greeks=asyncio.Event(),
            summary=asyncio.Event(),
            timeandsale=asyncio.Event(),
        )

        max_queue_size = 500
        interval = 0.1

        self = cls(queue_dict,flush_event_dict,max_queue_size,interval)
        return self

    async def push_event(self,ticker,streamer_symbol,event_type,event):
        event_dict = dict(event)
        if streamer_symbol.startswith("."):
            ticker,expiration,contract_type,strike = parse_symbol(streamer_symbol)
            event_dict['ticker']=ticker
            event_dict['expiration']=expiration
            event_dict['contract_type']=contract_type
            event_dict['strike']=strike
            flusher_key = event_type
        else:
            flusher_key = f"{event_type}_underlying"

        if "{=" in event_dict["event_symbol"]: # eventSymbol
            event_dict['event_symbol'] = streamer_symbol

        # NOTE: ordering needs to match COPY_STATEMENT_DICT
        cols = list(event_dict.keys())
        vals = [postgres_friendly(event_dict[x]) for x in cols]
        await self.queue_dict[flusher_key].put(vals)
        if self.queue_dict[flusher_key].qsize() >= self.max_queue_size:
            self.flush_event_dict[flusher_key].set()

# 
# NOTE: 
# copy_rows upside: probably fastest way to populate rows.
# copy_rows downside: you need to seperate connection pools per table. can't handle two concurrent calls.
#
# +if you want to increase tickers, while not blow up db connections
# ideally you want to share flusher among tickers (li) LivePrices instances.
# with VIX,SPX you have 2*7 flushers which yields 14*4 connections.
#
# + another alternative is to go back to using insert, 
#   but construct full sql statement with multiple rows and values inside the sql statement (frawned upon but we dont care about sql injections).
#

async def flusher(myqueue,flusher_key):
    max_lifetime = 25200
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False,max_lifetime=max_lifetime) as apool:
        await apool.check()
        async with apool.connection() as aconn:
            while True:
                task1 = asyncio.create_task(myqueue.flush_event_dict[flusher_key].wait())
                task2 = asyncio.create_task(asyncio.sleep(myqueue.interval))
                done, pending = await asyncio.wait(
                    [task1, task2],
                    return_when=asyncio.FIRST_COMPLETED
                )

                insert_list = []
                while True:
                    try:
                        item = myqueue.queue_dict[flusher_key].get_nowait()
                        insert_list.append(item)
                    except asyncio.QueueEmpty:
                        break

                if len(insert_list) > 0:
                    copy_statement = COPY_STATEMENT_DICT[flusher_key]
                    query_dict = {
                        copy_statement:insert_list
                    }
                    try:
                        await cpostgres_copy(aconn,query_dict)
                    except:
                        logger.error(f"{query_dict}")
                        traceback.print_exc()

                # clear flush event if it was set
                if myqueue.flush_event_dict[flusher_key].is_set():
                    myqueue.flush_event_dict[flusher_key].clear()

# sample event_symbol ".TSLA240927C105"
PATTERN = r"\.([A-Z]+)(\d{6})([CP])(\d+)"

def parse_symbol(event_symbol):
    matched = re.match(PATTERN,event_symbol)
    ticker = matched.group(1)
    expiration = datetime.datetime.strptime(matched.group(2),'%y%m%d').date()
    contract_type = matched.group(3)
    strike = float(matched.group(4))
    return ticker,expiration,contract_type,strike

#
# below are copy pastas authored by Graeme22
# amazing stuff!!!
# https://tastyworks-api.readthedocs.io/en/latest/data-streamer.html#advanced-usage
# commit https://github.com/tastyware/tastytrade/blob/97e1bc6632cfd4a15721da816085eb906a02bcb0/docs/data-streamer.rst#L76
# # interval '5s' '15s', '5m', '1h', '3d',
CANDLE_TYPE = 's'
async def _subscribe(streamer, streamer_symbols, is_option):
    # subscribe to quotes and greeks for all options on that date
    start_time = now_in_new_york() # start from now

    await streamer.subscribe_candle(streamer_symbols, CANDLE_TYPE, start_time,refresh_interval=1.0)
    await streamer.subscribe(Quote,streamer_symbols,refresh_interval=1.0)

    if is_option:
        await streamer.subscribe(Greeks, streamer_symbols)
        await streamer.subscribe(Summary, streamer_symbols)
        
    if False:
        await streamer.subscribe(TimeAndSale, streamer_symbols)
        await streamer.subscribe(Trade, streamer_symbols)
        await streamer.subscribe(Profile, streamer_symbols)
        await streamer.subscribe(TheoPrice, streamer_symbols)
        await streamer.subscribe(Underlying, streamer_symbols)

    logger.debug(f"_subscribe {streamer_symbols[0]}")
    if False:
        # TODO: delete later after confirming reconnection working.
        #########
        tstamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        logfile = os.path.join("/mnt/sm-data",f"{streamer_symbols[0]}-{tstamp}.txt")
        with open(logfile,'w') as f:
            f.write(str(streamer_symbols))
        #########

@dataclass
class LivePrices:
    candle: dict[str, Candle]
    greeks: dict[str, Greeks]
    profile: dict[str, Profile]
    quote: dict[str, Quote]
    summary: dict[str, Summary]
    thoeprice: dict[str, TheoPrice]
    timeandsale: dict[str, TimeAndSale]
    trade: dict[str, Trade]
    underlying: dict[str, Underlying]
    streamer: DXLinkStreamer
    streamer_symbols: list[str]
    task_list: list[str]
    ticker: str
    is_option: bool
    save_to_postres: bool=False
    @classmethod
    async def create(
        cls,
        myqueue: PgInsertQueue,
        streamer: DXLinkStreamer,
        ticker: str,
        streamer_symbols: list,
        is_option: bool,
        save_to_postres: bool = False,
        ):

        await _subscribe(streamer,streamer_symbols,is_option)

        self = cls({}, {}, {}, {}, {}, {}, {}, {}, {},
                   streamer, streamer_symbols,[],ticker,is_option,save_to_postres=save_to_postres)

        t_listen_candles = asyncio.create_task(self._update_candle(myqueue))
        t_listen_quote = asyncio.create_task(self._update_event(Quote,"quote",myqueue))

        if self.is_option:
            t_listen_greeks = asyncio.create_task(self._update_event(Greeks,"greeks",myqueue))
            t_listen_summary = asyncio.create_task(self._update_event(Summary,"summary",myqueue))

        if False:
            t_listen_time_and_sale = asyncio.create_task(self._update_event(TimeAndSale,"timeandsale",myqueue))
            t_listen_profile = asyncio.create_task(self._update_event(Profile,"profile",myqueue))
            t_listen_theo_price = asyncio.create_task(self._update_event(TheoPrice,"thoeprice",myqueue))
            t_listen_underlying = asyncio.create_task(self._update_event(Underlying,"underlying",myqueue))
            t_listen_trade = asyncio.create_task(self._update_event(Trade,"trade",myqueue))

        self.task_list = [
            t_listen_candles,
            t_listen_quote,
        ]
        if self.is_option:
            self.task_list.extend([
                t_listen_greeks,
                t_listen_summary,
            ])

        if False:
            self.task_list.extend([
                t_listen_profile,
                t_listen_theo_price,
                t_listen_underlying,
                t_listen_trade,
                t_listen_time_and_sale,
            ])

        asyncio.gather(*self.task_list)

        # wait we have quotes and greeks for each option
        while len(self.candle) < 1:
            await asyncio.sleep(0.1)
        if self.is_option:
            while len(self.quote) < 1 or len(self.greeks) < 1 or len(self.summary) < 1:
                await asyncio.sleep(0.1)
        return self

    async def shutdown(self):
        logger.info(f"streamer.unsubscribe...{self.ticker}")

        await self.streamer.unsubscribe_candle(self.streamer_symbols,CANDLE_TYPE)
        await self.streamer.unsubscribe(Quote,self.streamer_symbols)

        if self.is_option:
            await self.streamer.unsubscribe(Greeks, self.streamer_symbols)
            await self.streamer.unsubscribe(Summary, self.streamer_symbols)

        if False:
            await self.streamer.unsubscribe(TimeAndSale, self.streamer_symbols)
            await self.streamer.unsubscribe(Trade, self.streamer_symbols)
            await self.streamer.unsubscribe(Profile, self.streamer_symbols)
            await self.streamer.unsubscribe(TheoPrice, self.streamer_symbols)
            await self.streamer.unsubscribe(Underlying, self.streamer_symbols)

        logger.info(f"streamer closed...{self.streamer_symbols}")

        logger.info(f"cancel tasks...{self.ticker}")
        for task in self.task_list:
            logger.info(f"cancel tasks...{task}")
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"cancel done.{task}")

    async def _update_candle(self,myqueue):
        async for e in self.streamer.listen(Candle):
            streamer_symbol = e.event_symbol.replace("{="+CANDLE_TYPE+",tho=true}","")
            self.candle[streamer_symbol] = e
            if self.save_to_postres:
                await myqueue.push_event(self.ticker,streamer_symbol,'candle',e)

    async def _update_event(self,event_type,attribue_name,myqueue):
        async for e in self.streamer.listen(event_type):
            myparam = getattr(self,attribue_name)
            myparam[e.event_symbol] = e
            if self.save_to_postres:
                await myqueue.push_event(self.ticker,e.event_symbol,attribue_name,e)

class MarketCloseException(Exception):
    pass

async def background_subscribe(ticker,streamer_symbols,is_option,save_to_postres=True):
    try:

        session = get_session_reuse()

        myqueue = await PgInsertQueue.create()
        event_type_list = ['candle_underlying','quote_underlying','candle','quote','greeks','summary','timeandsale']
        flusher_task_list = [asyncio.create_task(flusher(myqueue,event_type)) for event_type in event_type_list]
        asyncio.gather(*flusher_task_list)

        tries, max_tries = 0, 3000
        while (tries := tries + 1) <= max_tries:
            try:
                async with DXLinkStreamer(session) as streamer:

                    live_prices = await LivePrices.create(myqueue,streamer,ticker,streamer_symbols,is_option,save_to_postres=save_to_postres)

                    while True:
                        et_tstamp = now_in_new_york()
                        try:
                            marketopendelta, _ = timedelta_from_market_open(et_tstamp)
                        except:
                            traceback.print_exc()
                            warnings.warn('market likely not open today')
                            marketopendelta = datetime.timedelta(minutes=1)

                        if not is_market_open() and marketopendelta.total_seconds() > 0:
                            logger.info("market closing -------------------------------")
                            await asyncio.sleep(10)
                            logger.info("shutdown...")
                            await live_prices.shutdown()

                            # clean up
                            for flusher_task in flusher_task_list:
                                flusher_task.cancel()
                                try:
                                    await flusher_task
                                except asyncio.CancelledError:
                                    logger.info(f"cancel done.{flusher_task}")

                            logger.info("pool close...")
                            raise MarketCloseException("market closed!")
                        else:
                            logger.info("market open -------------------------------")

                            # print candle info
                            tmp_candle = list(live_prices.candle.values())[0]
                            logger.info(f"Current candle: {tmp_candle}")

                            await asyncio.sleep(5)
            except* HTTPXWSException:
                logger.error(f"streamer disconnected {tries}")
                await sleep(1)

    except MarketCloseException:
        logger.error("MarketCloseException...")
    except KeyboardInterrupt:
        logger.error("Stopping live price streaming...")
    finally:
        logger.info("finally...")

    logger.info("attempt to exit!!")
    # unable to exit gracefully, just use kill lol.
    os.kill(os.getpid(), signal.SIGKILL)

if __name__ == "__main__":
    log_level = logging.INFO #  logging.DEBUG # 
    tastytrade.logger.setLevel(log_level)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    ticker = sys.argv[1]
    streamer_symbols = sys.argv[2]
    is_option = ast.literal(sys.argv[3])
    output = asyncio.run(background_subscribe(ticker,streamer_symbols,is_option,save_to_postres=True))

"""

python -m utils.data_tasty NDX None,2025-09-04,2025-09-05

"""
