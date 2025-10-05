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
from utils.data_cache import cache_cboe

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
                if ticker in ["VIX","VIX1D","ES"]:

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

                    chunk_list = [','.join(x) for x in chunks(expiration_list, 3)]
                    for n,expirations_str in enumerate(chunk_list):
                        trigger_subscription.apply_async(args=[ticker,expirations_str],queue="stream")
                        if n > 3:
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
def trigger_cache_cboe(*args,**kwargs):
    cache_cboe()

@celery_app.task
def trigger_shutdown(*args,**kwargs):
    celery_app.control.shutdown()

class GexTarget(luigi.Target):
    ticker = luigi.parameter.Parameter()
    et_tstamp = luigi.parameter.DateSecondParameter()
    def __init__(self,ticker,et_tstamp):
        super().__init__()
        self.ticker = ticker
        self.et_tstamp = et_tstamp
    def exists(self):
        utc_tstamp = self.et_tstamp.astimezone(tz=pytz.timezone('UTC'))
        query_str = "select * from gex_net where ticker = %s and tstamp = %s"
        query_args = (self.ticker,utc_tstamp)
        fetched = postgres_execute(query_str,query_args,is_commit=False)
        if fetched is None:
            return
        fetched = [dict(x) for x in fetched]
        if len(fetched)>0:
            return True
        else:
            return False

class ComputeSpotGex(luigi.Task):
    ticker = luigi.parameter.Parameter()
    et_tstamp = luigi.parameter.DateSecondParameter()
    def output(self):
        return GexTarget(self.ticker,self.et_tstamp)
    def requires(self):
        if self.et_tstamp.hour == 9 and self.et_tstamp.minute == 30:
            return None
        prior_tstamp = self.et_tstamp-datetime.timedelta(seconds=1)
        return ComputeSpotGex(self.ticker,prior_tstamp)
    def run(self):
        et_tstamp = self.et_tstamp.astimezone(tz=pytz.timezone('US/Eastern'))
        asyncio.run(compute_gex(self.ticker,et_tstamp,from_scratch=None,persist_to_postgres=True))

@celery_app.task
def trigger_gex_cache(*args,**kwargs):
    query_str = "select * from watchlist,settings"
    query_args = ()
    utc_tstamp = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    eastern = pytz.timezone('US/Eastern')
    et_tstamp = utc_tstamp.astimezone(tz=eastern)-datetime.timedelta(seconds=1)
    if is_market_open() is False:
        pass
    else:
        fetched = postgres_execute(query_str,query_args,is_commit=False)
        if fetched is None:
            return
        fetched = [dict(x) for x in fetched]
        for row in fetched:
            ticker = row['ticker']
            from_scratch = row['from_scratch']
            is_compute_gex = row['compute_gex']
            if not is_compute_gex:
                continue
            logger.info(f"trigger_gex_cache {ticker}")
            asyncio.run(compute_gex(ticker,et_tstamp,from_scratch=from_scratch,persist_to_postgres=True))
            
            # TODO: testbelow?
            # task = ComputeSpotGex(ticker=ticker,et_tstamp=et_tstamp)
            # ret_code = luigi.build([task])

if __name__ == "__main__":
    ticker = sys.argv[1]
    expirations_str = sys.argv[2] # None,2025-09-03,2025-09-04
    trigger_subscription(ticker,expirations_str)

""" 

python -m luigi --module tasks Subscription --ticker SPX --local-scheduler

python -m luigi --module tasks ComputeSpotGex --ticker SPX --et-tstamp 2025-08-21T093000 --local-scheduler

celery_app.control.broadcast('shutdown') ??

"""