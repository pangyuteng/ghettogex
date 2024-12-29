
import logging
logger = logging.getLogger(__file__)

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

from tastytrade import DXLinkStreamer
from tastytrade.instruments import get_option_chain
from tastytrade.dxfeed import (
    Candle, Greeks, Profile, Quote, Summary, TheoPrice, TimeAndSale, Trade, Underlying, 
)
from tastytrade.instruments import Equity, Option, OptionType
from tastytrade.session import Session
from tastytrade.streamer import EventType
from tastytrade.utils import today_in_new_york

from .misc import now_in_new_york, is_market_open, CACHE_FOLDER, CACHE_TASTY_FOLDER

def time_to_datetime(tstamp):
    return datetime.datetime.fromtimestamp(float(tstamp) / 1e3)

def is_test_func():
    return False if os.environ.get('IS_TEST') == 'FALSE' else True

def get_session(remember_me=True):

    is_test = is_test_func()
    username = os.environ.get('TASTYTRADE_USERNAME')
    
    daystamp = now_in_new_york().strftime("%Y-%m-%d")
    token_file = f'/tmp/.tastytoken-{daystamp}.json'
    logger.debug(token_file)
    if not os.path.exists(token_file):
        password = os.environ.get('TASTYTRADE_PASSWORD')
        logger.debug(username)
        session = Session(username,password,remember_me=remember_me,is_test=is_test)
        # #use of remember_token locks the account!
        # TODO: need to read tasty api
        # with open(token_file,'w') as f:
        #    f.write(json.dumps({"remember_token":session.remember_token}))
        return session
    else:
        logger.debug('loading token file...')
        with open(token_file,'r') as f:
            content = json.loads(f.read())
            remember_token = content["remember_token"]
            logger.debug(f"remember_token {remember_token}")
            return Session(username,remember_token=remember_token,is_test=is_test)

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

async def persist_to_postgres(ticker,streamer_symbol,event_type,event):
    event_dict = dict(event)
    if streamer_symbol.startswith("."):
        ticker,expiration,contractType,strike = parse_symbol(streamer_symbol)
        event_dict['ticker']=ticker
        event_dict['expiration']=expiration
        event_dict['contractType']=contractType
        event_dict['strike']=strike

    if "{=" in event_dict["eventSymbol"]:
        event_dict['eventSymbol'] = streamer_symbol

    cols = list(event_dict.keys())
    vals = [postgres_friendly(event_dict[x]) for x in cols]
    vals_str_list = ["%s"] * len(vals)
    vals_str = ", ".join(vals_str_list)
    query_str = "INSERT INTO {event_type} ({cols}) VALUES ({vals_str})".format(event_type=event_type,cols = ','.join(cols), vals_str = vals_str)
    query_args = vals
    await apostgres_execute(query_str,query_args,is_commit=True)


# sample eventSymbol ".TSLA240927C105"
PATTERN = r"\.([A-Z]+)(\d{6})([CP])(\d+)"

def parse_symbol(eventSymbol):
    matched = re.match(PATTERN,eventSymbol)
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
# # interval '15s', '5m', '1h', '3d',
CANDLE_TYPE = '5s'
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
    equity: list[Equity]
    puts: list[Option]
    calls: list[Option]
    streamer_symbols: list[str]
    ticker: str
    save_to_postres: bool=False
    save_to_json: bool=True
    @classmethod
    async def create(
        cls,
        session: Session,
        ticker: str = 'SPY',
        expiration: datetime.date = today_in_new_york()
        ):

        equity = Equity.get_equity(session, ticker)
        chain = get_option_chain(session, ticker)
        expiration = sorted(list(chain.keys()))[0]
        print(expiration,'!!')
        options = [o for o in chain[expiration]]
        # the `streamer_symbol` property is the symbol used by the streamer
        streamer_symbols = [o.streamer_symbol for o in options]
        print("??oooooooooooooooooo?")
        streamer = await DXLinkStreamer(session)
        print("??????????")
        # subscribe to quotes and greeks for all options on that date
        start_time = now_in_new_york()
        start_time = datetime.datetime(start_time.year,start_time.month,start_time.day,9,30,0)
        await streamer.subscribe_candle([ticker] + streamer_symbols, CANDLE_TYPE, start_time)
        await streamer.subscribe(Greeks, streamer_symbols)
        await streamer.subscribe(Profile, streamer_symbols)
        await streamer.subscribe(Quote, [ticker] + streamer_symbols)
        await streamer.subscribe(Summary, streamer_symbols)
        #await streamer.subscribe(EventType.THEO_PRICE, streamer_symbols)
        #await streamer.subscribe(EventType.TIME_AND_SALE, streamer_symbols)
        #await streamer.subscribe(EventType.TRADE, streamer_symbols)
        #await streamer.subscribe(EventType.UNDERLYING, [ticker])

        puts = [o for o in options if o.option_type == OptionType.PUT]
        calls = [o for o in options if o.option_type == OptionType.CALL]

        self = cls({}, {}, {}, {}, {}, {}, {}, {}, {},
                   streamer, equity, puts, calls, streamer_symbols,ticker)

        t_listen_candles = asyncio.create_task(self._update_candle())
        t_listen_greeks = asyncio.create_task(self._update_event(Greeks,"greeks"))
        t_listen_profile = asyncio.create_task(self._update_event(Profile,"profile"))
        t_listen_quote = asyncio.create_task(self._update_event(Quote,"quote"))
        t_listen_summary = asyncio.create_task(self._update_event(Summary,"summary"))
        #t_listen_theo_price = asyncio.create_task(self._update_event(EventType.THEO_PRICE,"thoeprice"))
        #t_listen_time_and_sale = asyncio.create_task(self._update_event(EventType.TIME_AND_SALE,"timeandsale"))
        #t_listen_trade = asyncio.create_task(self._update_event(EventType.TRADE,"trade"))
        #t_listen_underlying = asyncio.create_task(self._update_event(EventType.UNDERLYING,"underlying"))

        asyncio.gather(t_listen_candles,
                       t_listen_greeks,
                       t_listen_profile,
                       t_listen_quote,
                       t_listen_summary)
                       #t_listen_underlying,
                       #t_listen_trade,
                       #t_listen_theo_price,
                       #t_listen_time_and_sale,

        # wait we have quotes and greeks for each option
        while len(self.quote) < 1 or len(self.candle) < 1 or len(self.greeks) < 1 or len(self.summary) < 1 or len(self.trade) < 1:
            await asyncio.sleep(0.1)

        return self

    async def shutdown(self):
        logger.debug(f"sreamer.unsubscribe...{self.streamer_symbols}")
        await self.streamer.unsubscribe_candle([self.ticker] +self.streamer_symbols,CANDLE_TYPE)
        await self.streamer.unsubscribe(Greeks, self.streamer_symbols)
        await self.streamer.unsubscribe(Profile, self.streamer_symbols)
        await self.streamer.unsubscribe(Quote, [self.ticker]+self.streamer_symbols)
        await self.streamer.unsubscribe(Summary, self.streamer_symbols)
        #await self.streamer.unsubscribe(EventType.THEO_PRICE, self.streamer_symbols)
        #await self.streamer.unsubscribe(EventType.TIME_AND_SALE, self.streamer_symbols)
        #await self.streamer.unsubscribe(EventType.TRADE, self.streamer_symbols)
        #await self.streamer.unsubscribe(EventType.Underlying, [self.ticker])
        await self.streamer.close()
        logger.debug(f"sreamer closed...{self.streamer_symbols}")

    async def _update_candle(self):
        async for e in self.streamer.listen(Candle):
            print("_update_candle",e.event_symbol)
            streamer_symbol = e.event_symbol.replace("{="+CANDLE_TYPE+",tho=true}","")
            self.candle[streamer_symbol] = e
            if self.save_to_json:
                await save_data_to_json(self.ticker,streamer_symbol,Candle,e)
            if self.save_to_postres:
                await persist_to_postgres(self.ticker,streamer_symbol,Candle,e)
    async def _update_event(self,event_type,attribue_name):
        async for e in self.streamer.listen(event_type):
            myparam = getattr(self,attribue_name)
            print("_update_event",e.eventSymbol)
            print(attribue_name)
            myparam[e.eventSymbol] = e
            if self.save_to_json:
                await save_data_to_json(self.ticker,e.eventSymbol,event_type,e)
            if self.save_to_postres:
                await persist_to_postgres(self.ticker,e.eventSymbol,event_type,e)

def get_cancel_file(ticker):
    return os.path.join(CACHE_TASTY_FOLDER,f"cancel-{ticker}.txt")

def get_running_file(ticker):
    return os.path.join(CACHE_TASTY_FOLDER,f"running-{ticker}.txt")

async def background_subscribe(ticker,session):
    try:

        running_file = get_running_file(ticker)
        cancel_file = get_cancel_file(ticker)
        if not os.path.exists(running_file):
            pathlib.Path(running_file).touch()

        live_prices = await LivePrices.create(session,ticker)

        while True:
            if not is_market_open():
                print("market not open -------------------------------")
                await live_prices.shutdown()
                logger.info(f"canceling!")
                logger.info("market is closed!")
                break
            else:
                print("market open -------------------------------")
            # Print or process the quotes in real time
            if live_prices.ticker in live_prices.quote.keys():
                logger.info(f"Current quote: {live_prices.quote[live_prices.ticker]}")
            if live_prices.ticker in live_prices.candle.keys():
                logger.info(f"Current candle: {live_prices.candle[live_prices.ticker]}")
            if live_prices.ticker in live_prices.summary.keys():
                logger.info(f"Current summary: {live_prices.summary[live_prices.ticker]}")

            pathlib.Path(running_file).touch()
            await asyncio.sleep(5)
            if os.path.exists(cancel_file):
                logger.info(f"canceljob receieved...")
                os.remove(cancel_file)
                logger.info(f"canceling!")
                await live_prices.shutdown()
                if os.path.exists(running_file):
                    os.remove(running_file)
                raise ValueError("canceljob")
    except KeyboardInterrupt:
        logger.error("Stopping live price streaming...")
    finally:
        if os.path.exists(running_file):
            os.remove(running_file)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,#level=logging.DEBUG,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    ticker = sys.argv[1]
    action = sys.argv[2]
    
    if action == "background_subscribe":
        session = get_session()
        output = asyncio.run(background_subscribe(ticker,session))

"""

python -m utils.data_tasty SPY background_subscribe

"""
