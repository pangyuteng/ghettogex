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

from tastytrade.instruments import get_option_chain

from utils.postgres_utils import postgres_execute, vaccum_full_analyze
from utils.data_tasty import background_subscribe, get_session_reuse
from utils.misc import is_market_open, now_in_new_york, timedelta_from_market_open
from utils.compute_intraday import compute_gex

import tastytrade

celery_app = Celery('tasks')
import celeryconfig
celery_app.config_from_object(celeryconfig)


class AlwaysRunTarget(luigi.Target):
    def __init__(self,):
        super().__init__()
    def exists(self):
        return False

class Subscription(luigi.Task):
    ticker = luigi.parameter.Parameter()
    expirations_str = luigi.parameter.Parameter()
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
        asyncio.run(background_subscribe(self.ticker,self.expirations_str,save_to_postres=True))
        logger.info("background_subscribe exit success!")

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

class ManageSubscription(luigi.Task):
    def output(self):
        return AlwaysRunTarget()
    def run(self):
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

            fetched = [dict(x) for x in fetched]
            for row in fetched:
                ticker = row['ticker']
                logger.info(f"trigger subscriptions apply_async {ticker}")
                if ticker in ["VIX","VIX1D","ES","UVXY"]:

                    expirations_str = "None"
                    trigger_subscription.apply_async(args=[ticker,expirations_str],queue="stream")

                else:
                    if ticker not in mydict.keys():
                        session = get_session_reuse()
                        chain = get_option_chain(session, ticker)
                        expiration_list = ["None"]
                        expiration_list.extend([k.strftime("%Y-%m-%d") for k,v in chain.items()])
                    else:
                        expiration_list = mydict[ticker]

                    #chunk_list = [','.join(x) for x in chunks(expiration_list, 2)]
                    for n,expirations_str in enumerate(expiration_list):
                        trigger_subscription.apply_async(args=[ticker,expirations_str],queue="stream")

                        if n == 3:
                            break

            if is_market_open():
                time.sleep(60)
            else:
                break

@celery_app.task
def trigger_subscription(*args,**kwargs):
    ticker = args[0]
    expirations_str = args[1]
    logger.info(f"trigger_subscription! {ticker}")
    task = Subscription(ticker=ticker,expirations_str=expirations_str)
    ret_code = luigi.build([task])

# for fast jobs don't bother with luigi
@celery_app.task
def task_foo(*args,**kwargs):
    print(args)

@celery_app.task
def manage_subscriptions(*args,**kwargs):
    task = ManageSubscription()
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
        if fetched is None:
            return
        fetched = [dict(x) for x in fetched]
        for row in fetched:
            ticker = row['ticker']
            is_compute_gex = row['compute_gex']
            if not is_compute_gex:
                continue

            logger.info(f"trigger_gex_cache {ticker}")
            asyncio.run(compute_gex(ticker,et_tstamp,persist_to_postgres=True))


if __name__ == "__main__":
    ticker = sys.argv[1]
    expirations_str = sys.argv[2] # None,2025-09-03,2025-09-04
    trigger_subscription(ticker,expirations_str)

""" 

python -m luigi --module tasks Subscription --ticker SPX --local-scheduler

celery_app.control.broadcast('shutdown') ??

"""