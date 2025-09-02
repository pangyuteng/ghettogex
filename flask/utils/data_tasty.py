
import logging
logger = logging.getLogger(__file__)
import warnings
import os
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
from tastytrade.instruments import Equity, Option, Future, FutureOption, OptionType
from tastytrade.session import Session
from tastytrade.streamer import EventType
from tastytrade.utils import today_in_new_york

from .misc import (
    now_in_new_york,
    is_market_open,
    timedelta_from_market_open,
    CACHE_FOLDER,
    CACHE_TASTY_FOLDER,
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
    username = os.environ.get('TASTYTRADE_USERNAME')
    password = os.environ.get('TASTYTRADE_PASSWORD')
    session = Session(username,password,is_test=is_test)
    return session

def get_session_reuse():
    remember_me = True
    is_test = is_test_func()
    username = os.environ.get('TASTYTRADE_USERNAME')
    password = os.environ.get('TASTYTRADE_PASSWORD')
    daystamp = now_in_new_york().strftime("%Y-%m-%d")

    fetched = postgres_execute("select * from session where session_id = 1",(),is_commit=False)
    if len(fetched) == 0:
        serialized_session = None
        logger.debug("no existing session found, will create new session")
    else:
        serialized_session = fetched[0]['serialized_session']
        streamer_expiration = json.loads(serialized_session)['streamer_expiration']
        expiration_tstamp = datetime.datetime.strptime(streamer_expiration,'%Y-%m-%d %H:%M:%S%z')
        if datetime.datetime.utcnow() > expiration_tstamp.replace(tzinfo=None):
            logger.debug("session expired, will create new session")
            serialized_session = None
        else:
            logger.debug("session found will use existing session")

    if serialized_session:
        # ** reuse of remember_token can only be used once, second time locks the account** so we use serialize!
        session = Session.deserialize(serialized_session)
    else:
        session = Session(username,password,remember_me=remember_me,is_test=is_test)
        serialized_session = session.serialize()
        
        query_str = """
        INSERT INTO session (session_id,serialized_session) VALUES (%s,%s) ON CONFLICT (session_id) DO UPDATE SET serialized_session = %s;
        """
        query_args = (1,serialized_session,serialized_session)
        postgres_execute(query_str,query_args,is_commit=True)
        logger.debug("persisting new session to postgres")

    return session

async def save_data_to_json(ticker,streamer_symbols,event_type,event):
    tstamp = now_in_new_york().strftime("%Y-%m-%d-%H-%M-%S.%f")
    daystamp = now_in_new_york().strftime("%Y-%m-%d")
    workdir = os.path.join(CACHE_TASTY_FOLDER,ticker,daystamp,streamer_symbols,event_type)
    await aiofiles.os.makedirs(workdir,exist_ok=True)
    uid = uuid.uuid4().hex
    json_file = os.path.join(workdir,f'{tstamp}-uid-{uid}.json')
    async with aiofiles.open(json_file,'w') as f:
        event_dict = dict(event)
        await f.write(json.dumps(event_dict,indent=4,sort_keys=True,default=str))

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
                done, pending = await asyncio.wait(
                    [myqueue.flush_event_dict[flusher_key].wait(), asyncio.sleep(myqueue.interval)],
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
                    await cpostgres_copy(aconn,query_dict)

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
    expiration: datetime.date
    save_to_json: bool=True
    save_to_postres: bool=False
    @classmethod
    async def create(
        cls,
        myqueue: PgInsertQueue,
        session: Session,
        ticker: str,
        streamer_symbols: list,
        expiration: None,
        save_to_json: bool = True,
        save_to_postres: bool = False,
        ):

        streamer = await DXLinkStreamer(session)
        # subscribe to quotes and greeks for all options on that date
        start_time = now_in_new_york() # start from now
        await streamer.subscribe_candle(streamer_symbols, CANDLE_TYPE, start_time)
        await streamer.subscribe(Quote,streamer_symbols,refresh_interval=0.1)

        if expiration is not None:
            await streamer.subscribe(Greeks, streamer_symbols)
            await streamer.subscribe(Summary, streamer_symbols)
            await streamer.subscribe(TimeAndSale, streamer_symbols)

        if False:
            await streamer.subscribe(Trade, streamer_symbols)
            await streamer.subscribe(Profile, streamer_symbols)
            await streamer.subscribe(TheoPrice, streamer_symbols)
            await streamer.subscribe(Underlying, streamer_symbols)


        self = cls({}, {}, {}, {}, {}, {}, {}, {}, {},
                   streamer, streamer_symbols,[],ticker,expiration,
                   save_to_json=save_to_json,save_to_postres=save_to_postres)

        t_listen_candles = asyncio.create_task(self._update_candle(myqueue))
        t_listen_quote = asyncio.create_task(self._update_event(Quote,"quote",myqueue))

        if expiration is not None:
            t_listen_greeks = asyncio.create_task(self._update_event(Greeks,"greeks",myqueue))
            t_listen_summary = asyncio.create_task(self._update_event(Summary,"summary",myqueue))
            t_listen_time_and_sale = asyncio.create_task(self._update_event(TimeAndSale,"timeandsale",myqueue))

        if False:
            t_listen_profile = asyncio.create_task(self._update_event(Profile,"profile",myqueue))
            t_listen_theo_price = asyncio.create_task(self._update_event(TheoPrice,"thoeprice",myqueue))
            t_listen_underlying = asyncio.create_task(self._update_event(Underlying,"underlying",myqueue))
            t_listen_trade = asyncio.create_task(self._update_event(Trade,"trade",myqueue))

        self.task_list = [
            t_listen_candles,
            t_listen_quote,
        ]
        if expiration is not None:
            self.task_list.extend([
                t_listen_greeks,
                t_listen_summary,
                t_listen_time_and_sale,
            ])

        if False:
            self.task_list.extend([
                t_listen_profile,
                t_listen_theo_price,
                t_listen_underlying,
                t_listen_trade,
            ])

        asyncio.gather(*self.task_list)

        # wait we have quotes and greeks for each option
        while len(self.candle) < 1:
            await asyncio.sleep(0.1)
        if expiration is not None:
            while len(self.quote) < 1 or len(self.greeks) < 1 or len(self.summary) < 1:
                await asyncio.sleep(0.1)
        return self

    async def shutdown(self):
        logger.info(f"streamer.unsubscribe...{self.ticker}")

        await self.streamer.unsubscribe_candle(self.streamer_symbols,CANDLE_TYPE)
        await self.streamer.unsubscribe(Quote,self.streamer_symbols)

        if self.expiration is not None:
            await self.streamer.unsubscribe(Greeks, self.streamer_symbols)
            await self.streamer.unsubscribe(Summary, self.streamer_symbols)
            await self.streamer.unsubscribe(TimeAndSale, self.streamer_symbols)

        if False:
            await self.streamer.unsubscribe(Trade, self.streamer_symbols)
            await self.streamer.unsubscribe(Profile, self.streamer_symbols)
            await self.streamer.unsubscribe(TheoPrice, self.streamer_symbols)
            await self.streamer.unsubscribe(Underlying, self.streamer_symbols)

        await self.streamer.close()

        logger.info(f"cancel tasks...{self.ticker}")
        for task in self.task_list:
            logger.info(f"cancel tasks...{task}")
            task.cancel()

        logger.debug(f"sreamer closed...{self.streamer_symbols}")

    async def _update_candle(self,myqueue):
        async for e in self.streamer.listen(Candle):
            streamer_symbol = e.event_symbol.replace("{="+CANDLE_TYPE+",tho=true}","")
            self.candle[streamer_symbol] = e
            if self.save_to_json:
                await save_data_to_json(self.ticker,streamer_symbol,'candle',e)
            if self.save_to_postres:
                await myqueue.push_event(self.ticker,streamer_symbol,'candle',e)

    async def _update_event(self,event_type,attribue_name,myqueue):
        async for e in self.streamer.listen(event_type):
            myparam = getattr(self,attribue_name)
            myparam[e.event_symbol] = e
            if self.save_to_json:
                await save_data_to_json(self.ticker,e.event_symbol,attribue_name,e)
            if self.save_to_postres:
                await myqueue.push_event(self.ticker,e.event_symbol,attribue_name,e)

class MarketCloseException(Exception):
    pass

async def background_subscribe(ticker,expirations_str,save_to_postres=True,save_to_json=True):
    try:
        expiration_list = expirations_str.split(",")

        # why wait?
        # when below was disabled, time_till_expire was erroring out.
        # maybe disabling wait is the culprit, you need market to open then sub to get all the evetns???
        # 
        # while True:
        #     if not is_market_open():
        #         logger.info(f"market is closed! {ticker}")
        #         await asyncio.sleep(0.1)
        #     else:
        #         break
        
        session = get_session_reuse()
        
        if "/" in ticker:
            warnings.warn("futures not tested")
            chain = get_future_option_chain(session, ticker)
            underlying_symbol = list(chain.values())[0][0].underlying_symbol
            equity = await Future.a_get(session, underlying_symbol)
        else:
            equity = await Equity.a_get(session, ticker)
            chain = get_option_chain(session, ticker)

        expirations = sorted(list(chain.keys()))
        live_prices_list = []
        myqueue = await PgInsertQueue.create()
        event_type_list = ['candle_underlying','quote_underlying','candle','quote','greeks','summary','timeandsale']
        flusher_task_list = [asyncio.create_task(flusher(myqueue,event_type)) for event_type in event_type_list]
        # underlying
        if "None" in expiration_list:
            underlying_streamer_symbols = [equity.streamer_symbol]
            live_prices = await LivePrices.create(myqueue,session,ticker,underlying_streamer_symbols,expiration=None,save_to_postres=save_to_postres,save_to_json=save_to_json)
            live_prices_list.append(live_prices)

        for expiration in expirations:
            if ticker == 'VIX': # ignore options for VIX
                continue 
            if expiration.strftime("%Y-%m-%d") not in expiration_list:
                continue
            options_list = [o for o in chain[expiration]]
            streamer_symbols = [o.streamer_symbol for o in options_list]
            print(streamer_symbols)
            live_prices = await LivePrices.create(myqueue,session,ticker,streamer_symbols,expiration=expiration,save_to_postres=save_to_postres,save_to_json=save_to_json)
            live_prices_list.append(live_prices)

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
                for lp in live_prices_list:
                    logger.info("shutdown...")
                    await lp.shutdown()

                # clean up
                for flusher_task in flusher_task_list:
                    flusher_task.cancel()
                    try:
                        await flusher_task
                    except asyncio.CancelledError:
                        pass

                logger.info("pool close...")
                raise MarketCloseException("market closed!")
            else:
                logger.info("market open -------------------------------")

                # print quotes
                if len(live_prices_list)>0:
                    tmp_candle = list(live_prices_list[0].candle.values())[0]
                    logger.info(f"Current candle: {tmp_candle}")

                await asyncio.sleep(5)

    except MarketCloseException:
        logger.error("MarketCloseException...")
    except KeyboardInterrupt:
        logger.error("Stopping live price streaming...")
    finally:
        logger.info("finally...")

    logger.info("attempt to exit!!")
    sys.exit(0)

if __name__ == "__main__":
    log_level = logging.INFO #  logging.DEBUG # 
    tastytrade.logger.setLevel(log_level)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    ticker = sys.argv[1]
    expirations_str = sys.argv[2]
    output = asyncio.run(background_subscribe(ticker,expirations_str,save_to_postres=True))

"""

python -m utils.data_tasty NDX None,2025-09-04,2025-09-05

"""
