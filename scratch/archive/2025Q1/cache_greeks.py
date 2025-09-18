
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
from tastytrade.utils import today_in_new_york, now_in_new_york

nyse = mcal.get_calendar('NYSE')
def is_market_open(tstamp=None):
    if tstamp is None:
        tstamp = now_in_new_york()
    today = tstamp.strftime("%Y-%m-%d")
    early = nyse.schedule(start_date=today, end_date=today)
    if len(early) == 0:
        return False
    hour_list = [
        list(early.to_dict()['market_open'].values())[0],
        list(early.to_dict()['market_close'].values())[0]
    ]
    eastern = pytz.timezone('US/Eastern')
    logger.debug(f'{tstamp},{hour_list[0].astimezone(eastern)},{hour_list[1].astimezone(eastern)}')
    if tstamp > min(hour_list) and tstamp < max(hour_list):
        return True
    else:
        return False

CACHE_TASTY_FOLDER = "tmp"
os.makedirs(CACHE_TASTY_FOLDER,exist_ok=True)

def time_to_datetime(tstamp):
    return datetime.datetime.fromtimestamp(float(tstamp) / 1e3)

def get_session():

    is_test = False if os.environ.get('IS_TEST') == 'FALSE' else True
    username = os.environ.get('TASTYTRADE_CLIENT_SECRET')
    password = os.environ.get('TASTYTRADE_REFRESH_TOKEN')
    logger.debug(username)
    session = Session(username,password,is_test=is_test)
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

async def persist_to_postgres(ticker,streamer_symbol,event_type,event):
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
    raise NotImplementedError()
    await apostgres_execute(query_str,query_args,is_commit=True)

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
    greek_only: bool
    save_to_postres: bool=False
    save_to_json: bool=True
    @classmethod
    async def create(
        cls,
        session: Session,
        ticker: str = 'SPY',
        expiration: datetime.date = today_in_new_york(),
        chain: list = [],
        greek_only: bool = True,
        ):

        equity = Equity.get_equity(session, ticker)

        options = []
        options.extend([o for o in chain[expiration]])
        streamer_symbols = [o.streamer_symbol for o in options]
        streamer = await DXLinkStreamer(session)
        # subscribe to quotes and greeks for all options on that date
        start_time = now_in_new_york()
        start_time = datetime.datetime(start_time.year,start_time.month,start_time.day,9,30,0)
        if greek_only:
            await streamer.subscribe(Greeks, streamer_symbols)
        else:
            await streamer.subscribe_candle([ticker] + streamer_symbols, CANDLE_TYPE, start_time)
            await streamer.subscribe(Greeks, streamer_symbols)
            await streamer.subscribe(Profile, streamer_symbols)
            await streamer.subscribe(Quote, [ticker] + streamer_symbols)
            await streamer.subscribe(Summary, streamer_symbols)
            await streamer.subscribe(TimeAndSale, streamer_symbols)
            await streamer.subscribe(Trade, streamer_symbols)
            #await streamer.subscribe(TheoPrice, streamer_symbols)
            #await streamer.subscribe(Underlying, [ticker])

        puts = [o for o in options if o.option_type == OptionType.PUT]
        calls = [o for o in options if o.option_type == OptionType.CALL]

        self = cls({}, {}, {}, {}, {}, {}, {}, {}, {},
                   streamer, equity, puts, calls, streamer_symbols,ticker,greek_only)

        if greek_only:

            t_listen_greeks = asyncio.create_task(self._update_event(Greeks,"greeks"))
            asyncio.gather(t_listen_greeks)

            while len(self.greeks) < 1:
                await asyncio.sleep(0.1)
        else:
            t_listen_candles = asyncio.create_task(self._update_candle())
            t_listen_greeks = asyncio.create_task(self._update_event(Greeks,"greeks"))
            t_listen_profile = asyncio.create_task(self._update_event(Profile,"profile"))
            t_listen_quote = asyncio.create_task(self._update_event(Quote,"quote"))
            t_listen_summary = asyncio.create_task(self._update_event(Summary,"summary"))
            t_listen_time_and_sale = asyncio.create_task(self._update_event(TimeAndSale,"timeandsale"))
            t_listen_trade = asyncio.create_task(self._update_event(Trade,"trade"))
            #t_listen_theo_price = asyncio.create_task(self._update_event(TheoPrice,"thoeprice"))
            #t_listen_underlying = asyncio.create_task(self._update_event(Underlying,"underlying"))

            asyncio.gather(t_listen_candles,
                        t_listen_greeks,
                        t_listen_profile,
                        t_listen_quote,
                        t_listen_summary,
                        t_listen_time_and_sale,
                        t_listen_trade,
                        )
                        #t_listen_theo_price,
                        #t_listen_underlying,

            # wait we have quotes and greeks for each option
            while len(self.quote) < 1 or len(self.candle) < 1 or len(self.greeks) < 1 or len(self.summary) < 1 or len(self.trade) < 1:
                await asyncio.sleep(0.1)

        return self

    async def shutdown(self):
        logger.debug(f"sreamer.unsubscribe...{self.streamer_symbols}")
        if self.greek_only:
            await self.streamer.unsubscribe(Greeks, self.streamer_symbols)
        else:

            await self.streamer.unsubscribe_candle([self.ticker] +self.streamer_symbols,CANDLE_TYPE)
            await self.streamer.unsubscribe(Greeks, self.streamer_symbols)
            await self.streamer.unsubscribe(Profile, self.streamer_symbols)
            await self.streamer.unsubscribe(Quote, [self.ticker]+self.streamer_symbols)
            await self.streamer.unsubscribe(Summary, self.streamer_symbols)
            await self.streamer.unsubscribe(TimeAndSale, self.streamer_symbols)
            await self.streamer.unsubscribe(Trade, self.streamer_symbols)
            #await self.streamer.unsubscribe(TheoPrice, self.streamer_symbols)
            #await self.streamer.unsubscribe(Underlying, [self.ticker])
            await self.streamer.close()
            logger.debug(f"sreamer closed...{self.streamer_symbols}")

    async def _update_candle(self):
        async for e in self.streamer.listen(Candle):
            streamer_symbol = e.event_symbol.replace("{="+CANDLE_TYPE+",tho=true}","")
            self.candle[streamer_symbol] = e
            if self.save_to_json:
                await save_data_to_json(self.ticker,streamer_symbol,'candle',e)
            if self.save_to_postres:
                await persist_to_postgres(self.ticker,streamer_symbol,'candle',e)

    async def _update_event(self,event_type,attribue_name):
        async for e in self.streamer.listen(event_type):
            myparam = getattr(self,attribue_name)
            myparam[e.event_symbol] = e
            if self.save_to_json:
                await save_data_to_json(self.ticker,e.event_symbol,attribue_name,e)
            if self.save_to_postres:
                await persist_to_postgres(self.ticker,e.event_symbol,attribue_name,e)

async def main(ticker):        
    greek_only = True
    session = get_session()
    chain = get_option_chain(session, ticker)
    expiration_list = sorted(list(chain.keys()))
    sub_list = []
    for expiration in expiration_list:
        live_prices = await LivePrices.create(session,ticker,expiration,chain,greek_only)
        print(live_prices.greeks.keys())
        sub_list.append(live_prices)
    
    # sleep for 10 minutes to gather data.
    await asyncio.sleep(60*30)

    for live_prices in sub_list:
        await live_prices.shutdown()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,#level=logging.DEBUG,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    ticker = sys.argv[1]
    output = asyncio.run(main(ticker))

"""

python cache_greeks.py SPX

"""
