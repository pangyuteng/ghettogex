
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

from .misc import now_in_new_york, is_market_open, CACHE_FOLDER, CACHE_TASTY_FOLDER
from .postgres_utils import (
    apostgres_execute,
    psycopg_pool,postgres_uri,
)

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
        # TOFO: need to read tasty api
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

async def persist_to_postgres(apool,ticker,streamer_symbol,event_type,event):
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
    await apostgres_execute(apool,query_str,query_args,is_commit=True)

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
    equity: list[Equity]
    puts: list[Option]
    calls: list[Option]
    streamer_symbols: list[str]
    task_list: list[str]
    ticker: str
    save_to_json: bool=True
    save_to_postres: bool=False
    @classmethod
    async def create(
        cls,
        apool: psycopg_pool.AsyncConnectionPool,
        session: Session,
        ticker: str = 'SPY',
        expiration: datetime.date = today_in_new_york(),
        save_to_json: bool = True,
        save_to_postres: bool = False,
        ):
        print("ticker",ticker)
        if "/" in ticker:
            equity = Future.get_future(session, ticker)
            chain = get_future_option_chain(session, ticker)
        else:
            equity = Equity.get_equity(session, ticker)
            chain = get_option_chain(session, ticker)

        expirations = [expiration]
        options = []
        for e in expirations:
            options.extend([o for o in chain[e]])
        # the `streamer_symbol` property is the symbol used by the streamer
        streamer_symbols = [o.streamer_symbol for o in options]
        print(len(streamer_symbols))
        streamer = await DXLinkStreamer(session)
        # subscribe to quotes and greeks for all options on that date
        start_time = now_in_new_york() # start from now
        await streamer.subscribe_candle([ticker] + streamer_symbols, CANDLE_TYPE, start_time)
        await streamer.subscribe(Greeks, streamer_symbols)
        await streamer.subscribe(Profile, streamer_symbols)
        await streamer.subscribe(Quote, [ticker] + streamer_symbols)
        await streamer.subscribe(Summary, streamer_symbols)
        await streamer.subscribe(TimeAndSale, streamer_symbols)
        await streamer.subscribe(Trade, streamer_symbols)
        if False:
            await streamer.subscribe(TheoPrice, streamer_symbols)
            await streamer.subscribe(Underlying, [ticker])

        puts = [o for o in options if o.option_type == OptionType.PUT]
        calls = [o for o in options if o.option_type == OptionType.CALL]

        self = cls({}, {}, {}, {}, {}, {}, {}, {}, {},
                   streamer, equity, puts, calls, streamer_symbols,[],ticker,
                   save_to_json=save_to_json,save_to_postres=save_to_postres)

        t_listen_candles = asyncio.create_task(self._update_candle(apool))
        t_listen_greeks = asyncio.create_task(self._update_event(Greeks,"greeks",apool))
        t_listen_profile = asyncio.create_task(self._update_event(Profile,"profile",apool))
        t_listen_quote = asyncio.create_task(self._update_event(Quote,"quote",apool))
        t_listen_summary = asyncio.create_task(self._update_event(Summary,"summary",apool))
        t_listen_time_and_sale = asyncio.create_task(self._update_event(TimeAndSale,"timeandsale",apool))
        t_listen_trade = asyncio.create_task(self._update_event(Trade,"trade",apool))
        if False:
            t_listen_theo_price = asyncio.create_task(self._update_event(TheoPrice,"thoeprice",apool))
            t_listen_underlying = asyncio.create_task(self._update_event(Underlying,"underlying",apool))

        self.task_list = [
            t_listen_candles,
            t_listen_greeks,
            t_listen_profile,
            t_listen_quote,
            t_listen_summary,
            t_listen_time_and_sale,
            t_listen_trade,
        ]

        asyncio.gather(*self.task_list)


        # wait we have quotes and greeks for each option
        while len(self.quote) < 1 or len(self.candle) < 1 or len(self.greeks) < 1 or len(self.summary) < 1 or len(self.trade) < 1:
            await asyncio.sleep(0.1)

        return self

    async def shutdown(self):
        logger.debug(f"cancel tasks...{self.ticker}")
        for task in self.task_list:
            task.cancel()
        logger.debug(f"sreamer.unsubscribe...{self.ticker}")
        await self.streamer.unsubscribe_candle([self.ticker] +self.streamer_symbols,CANDLE_TYPE)
        await self.streamer.unsubscribe(Greeks, self.streamer_symbols)
        await self.streamer.unsubscribe(Profile, self.streamer_symbols)
        await self.streamer.unsubscribe(Quote, [self.ticker]+self.streamer_symbols)
        await self.streamer.unsubscribe(Summary, self.streamer_symbols)
        await self.streamer.unsubscribe(TimeAndSale, self.streamer_symbols)
        await self.streamer.unsubscribe(Trade, self.streamer_symbols)
        if False:
            await self.streamer.unsubscribe(TheoPrice, self.streamer_symbols)
            await self.streamer.unsubscribe(Underlying, [self.ticker])
        await self.streamer.close()
        logger.debug(f"sreamer closed...{self.streamer_symbols}")

    async def _update_candle(self,apool):
        async for e in self.streamer.listen(Candle):
            streamer_symbol = e.event_symbol.replace("{="+CANDLE_TYPE+",tho=true}","")
            self.candle[streamer_symbol] = e
            if self.save_to_json:
                await save_data_to_json(self.ticker,streamer_symbol,'candle',e)
            if self.save_to_postres:
                await persist_to_postgres(apool,self.ticker,streamer_symbol,'candle',e)

    async def _update_event(self,event_type,attribue_name,apool):
        async for e in self.streamer.listen(event_type):
            myparam = getattr(self,attribue_name)
            myparam[e.event_symbol] = e
            if self.save_to_json:
                await save_data_to_json(self.ticker,e.event_symbol,attribue_name,e)
            if self.save_to_postres:
                await persist_to_postgres(apool,self.ticker,e.event_symbol,attribue_name,e)

def get_cancel_file(ticker):
    ticker = ticker.replace("/","^")
    return os.path.join(CACHE_TASTY_FOLDER,f"cancel-{ticker}.txt")

def get_running_file(ticker):
    ticker = ticker.replace("/","^")
    return os.path.join(CACHE_TASTY_FOLDER,f"running-{ticker}.txt")

async def background_subscribe(ticker,save_to_postres=False,save_to_json=True):
    try:

        running_file = get_running_file(ticker)
        cancel_file = get_cancel_file(ticker)
        if not os.path.exists(running_file):
            pathlib.Path(running_file).touch()

        while True:
            if not is_market_open():
                logger.info("market is closed!")
                await asyncio.sleep(1)
            else:
                break
        
        session = get_session()
        
        chain = get_option_chain(session, ticker)
        expirations = sorted(list(chain.keys()))
        # get 2 expirations
        live_prices_list = []
        EXPIRATION_LIM = 30
        async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=30,open=False,reconnect_timeout=2) as apool:
            for expiration in expirations:
                live_prices = await LivePrices.create(apool,session,ticker,expiration=expiration,save_to_postres=save_to_postres,save_to_json=save_to_json)
                live_prices_list.append(live_prices)
                if len(live_prices_list)>=EXPIRATION_LIM:
                    break

            while True:
                if not is_market_open():
                    await asyncio.sleep(10)
                    print("market not open -------------------------------")
                    for lp in live_prices_list:
                        await lp.shutdown()
                    logger.info(f"canceling!")
                    logger.info("market is closed, exiting...")
                    sys.exit(1)
                else:
                    print("market open -------------------------------")
                # Print or process the quotes in real time
                for lp in live_prices_list:
                    if lp.ticker in lp.quote.keys():
                        logger.info(f"Current quote: {lp.quote[lp.ticker]}")
                    if lp.ticker in lp.candle.keys():
                        logger.info(f"Current candle: {lp.candle[lp.ticker]}")
                    if lp.ticker in lp.summary.keys():
                        logger.info(f"Current summary: {lp.summary[lp.ticker]}")

                pathlib.Path(running_file).touch()
                await asyncio.sleep(5)
                if os.path.exists(cancel_file):
                    logger.info(f"canceljob receieved...")
                    os.remove(cancel_file)
                    logger.info(f"canceling!")
                    for lp in live_prices_list:
                        await lp.shutdown()
                    if os.path.exists(running_file):
                        os.remove(running_file)
                    raise ValueError("canceljob")
    except KeyboardInterrupt:
        logger.error("Stopping live price streaming...")
    finally:
        if os.path.exists(running_file):
            os.remove(running_file)

if __name__ == "__main__":
    log_level = logging.INFO # logging.DEBUG
    tastytrade.logger.setLevel(log_level)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    ticker = sys.argv[1]
    action = sys.argv[2]
    
    if action == "background_subscribe":
        output = asyncio.run(background_subscribe(ticker))

"""

docker run -it --env-file=.env \
    -w $PWD -v /mnt:/mnt \
    fi-flask:latest bash


python -m utils.data_tasty "/ES" background_subscribe
python -m utils.data_tasty SPX background_subscribe

"""
