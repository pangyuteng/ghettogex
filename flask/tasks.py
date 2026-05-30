import logging
logger = logging.getLogger(__file__)

import os
import re
import sys
import ast
import time
import math
import traceback
import pytz
import datetime
import json
import asyncio
import threading
import luigi
from celery import Celery

from tastytrade.instruments import get_option_chain, get_future_option_chain
from tastytrade.instruments import Equity, Future

from utils.postgres_utils import postgres_execute, vaccum_full_analyze
from utils.data_tasty import background_subscribe, get_session_reuse
from utils.misc import is_market_open, now_in_new_york, timedelta_from_market_open
from utils.compute_intraday import compute_gex
from utils.mytelegram import telegram_bot

import tastytrade

celery_app = Celery('tasks')
import celeryconfig
celery_app.config_from_object(celeryconfig)

IGNORE_OPTIONS_TICKER_LIST = ["VIX","ES","UVXY","VIX1D","VIX9D"] # ignore options for VIX and futures.
NON_TICKER_LIST = ["VIX1D","VIX9D"] # just use ticker as streamer_symbol


class AlwaysRunTarget(luigi.Target):
    def __init__(self,):
        super().__init__()
    def exists(self):
        return False

class Subscription(luigi.Task):
    ticker = luigi.parameter.StrParameter()
    streamer_symbols_str = luigi.parameter.StrParameter()
    is_option = luigi.parameter.BoolParameter()

    def output(self): # an output that never exists
        return AlwaysRunTarget()

    def run(self):
        et_tstamp = now_in_new_york()

        try:
            marketopendelta, _ = timedelta_from_market_open(et_tstamp)
        except:
            logger.warning('market likely not open today')
            marketopendelta = None

        if marketopendelta is None:
            logger.info(f"market closed today, no need to trigger background_subscribe")
            return

        if marketopendelta.total_seconds() < -30 and is_market_open() is False:
            logger.info(f"market not yet open, no need to trigger background_subscribe")
            return

        if marketopendelta.total_seconds() > 0 and is_market_open() is False:
            logger.info(f"market closed")
            return

        tastytrade.logger.setLevel(logging.INFO)
        asyncio.run(background_subscribe(self.ticker,self.streamer_symbols_str.split(","),self.is_option,save_to_postres=True))
        logger.info("background_subscribe exit success!")


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# if ticker == 'SPX': # ?
#     ticker_alt = 'SPXW'
# elif ticker == 'NDX':
#     ticker_alt = 'NDXP'
# elif ticker == 'VIX':
#     ticker_alt = 'VIXW'
# else:
#     ticker_alt = ticker

async def a_manage_subscriptions():
    query_str = "select * from watchlist"
    query_args = ()
    mydict = {}

    if is_market_open() is False:
        return

    # refresh serialized session
    session = get_session_reuse(refresh_serialized=True)

    while True:
        fetched = postgres_execute(query_str,query_args,is_commit=False)
        if fetched is None:
            return
        et_tstamp = now_in_new_york()
        fetched = [dict(x) for x in fetched]
        for row in fetched:
            ticker = row['ticker']
            expiration_count = row['expiration_count']
            logger.info(f"trigger subscriptions apply_async {ticker}")

            if ticker in ["ES"]: # futures with options
                future_list = await Future.get(session,product_codes=ticker)
                future_list = sorted(future_list,key=lambda x: x.expires_at,reverse=False)
                equity = await Future.get(session, future_list[0].symbol)
                
            elif ticker in NON_TICKER_LIST:
                equity= None
                chain = {}
            else: # equity with options
                equity = await Equity.get(session, ticker)

            if True:

                if ticker in NON_TICKER_LIST:
                    streamer_symbols_str = ticker
                else:
                    streamer_symbols_str = equity.streamer_symbol

                is_option = False
                trigger_subscription.apply_async(args=[ticker,streamer_symbols_str,is_option],queue="stream")

            if ticker in IGNORE_OPTIONS_TICKER_LIST:
                continue

            if ticker not in mydict.keys():
                session = get_session_reuse()
                if ticker in ["ES"]:
                    chain = await get_future_option_chain(session, ticker)
                else:
                    chain = await get_option_chain(session, ticker)

                options_list = []
                exp_counter = 0
                for k,v in chain.items():
                    # there were a few incidents where i saw expired contracts
                    if k >= et_tstamp.date():
                        options_list.extend(v)
                    exp_counter+=1
                    if exp_counter == expiration_count:
                        break
                streamer_symbols = [o.streamer_symbol for o in options_list]
                mydict[ticker] = streamer_symbols

            streamer_symbols = mydict[ticker]
            for chunked in chunks(streamer_symbols,20):
                is_option = True
                streamer_symbols_str = ",".join(chunked)
                trigger_subscription.apply_async(args=(ticker,streamer_symbols_str,is_option),queue="stream")            

        if is_market_open():
            time.sleep(60)
        else:
            break

class ManageSubscriptions(luigi.Task):
    def output(self):
        return AlwaysRunTarget()
    def run(self):
        asyncio.run(a_manage_subscriptions())

class TelegramBot(luigi.Task):
    def output(self): # an output that never exists
        return AlwaysRunTarget()
    def run(self):
        asyncio.run(telegram_bot())

@celery_app.task
def trigger_telegram_bot(*args,**kwargs):
    logger.info(f"trigger_telegram_bot")
    task = TelegramBot()
    ret_code = luigi.build([task])

@celery_app.task
def trigger_subscription(ticker,streamer_symbols_str,is_option):
    logger.info(f"trigger_subscription! {ticker}")
    task = Subscription(ticker=ticker,streamer_symbols_str=streamer_symbols_str,is_option=is_option)
    ret_code = luigi.build([task])

# for fast jobs don't bother with luigi
@celery_app.task
def task_foo(*args,**kwargs):
    print(args)

@celery_app.task
def manage_subscriptions(*args,**kwargs):
    task = ManageSubscriptions()
    ret_code = luigi.build([task])

@celery_app.task
def trigger_vaccum_full(*args,**kwargs):
    vaccum_full_analyze()

@celery_app.task
def trigger_shutdown(*args,**kwargs):
    celery_app.control.shutdown()

@celery_app.task
def trigger_gex_cache(*args,**kwargs):
    query_str = "select * from watchlist,settings"
    query_args = ()
    utc_tstamp = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    eastern = pytz.timezone('US/Eastern')
    et_tstamp = utc_tstamp.astimezone(tz=eastern)
    if is_market_open() is False:
        pass
    else:
        fetched = postgres_execute(query_str,query_args,is_commit=False)

        REFRESH_CANDLE_EXPIRATION = """REFRESH MATERIALIZED VIEW candle_expiration"""
        postgres_execute(REFRESH_CANDLE_EXPIRATION,(),is_commit=True)

        if fetched is None:
            return
        fetched = [dict(x) for x in fetched]

        async def waitforjobs(fetched):

            jobs_list = []
            for row in fetched:
                ticker = row['ticker']
                is_compute_gex = row['compute_gex']
                if not is_compute_gex:
                    continue

                logger.info(f"trigger_gex_cache {ticker}")
                jobs_list.append(compute_gex(ticker,et_tstamp,persist_to_postgres=True))

            await asyncio.gather(*jobs_list)

        asyncio.run(waitforjobs(fetched))


if __name__ == "__main__":
    ticker = sys.argv[1]
    streamer_symbols_str = sys.argv[2] # None,2025-09-03,2025-09-04
    is_option = ast.literal_eval(sys.argv[3])
    trigger_subscription(ticker,streamer_symbols_str,is_option)

""" 

python -m luigi --module tasks Subscription --ticker SPX --streamer-symbols-str .SPXW260529P7600,.SPXW260529C7600 --is-option --local-scheduler
python -m luigi --module tasks Subscription --ticker SPX --streamer-symbols-str SPX --local-scheduler

celery_app.control.broadcast('shutdown') ??

"""
