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

from utils.postgres_utils import postgres_execute, vaccum_full_analyze
from utils.data_tasty import background_subscribe, is_market_open, now_in_new_york
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
    def output(self): # an output that never exists
        return AlwaysRunTarget()
    def run(self):
        if not is_market_open():
            logger.info(f"market closed no need to trigger background_subscribe")
            return
        tastytrade.logger.setLevel(logging.INFO)
        output = asyncio.run(background_subscribe(self.ticker,save_to_postres=True,save_to_json=False))

@celery_app.task
def trigger_subscription(*args,**kwargs):
    ticker = args[0]
    logger.info(f"trigger_subscription! {ticker}")
    task = Subscription(ticker=ticker)
    ret_code = luigi.build([task])

# for fast jobs don't bother with luigi
@celery_app.task
def task_foo(*args,**kwargs):
    print(args)

@celery_app.task
def manage_subscriptions(*args,**kwargs):
    query_str = "select * from watchlist"
    query_args = ()

    if is_market_open() is False:
        pass
    else:
        fetched = postgres_execute(query_str,query_args,is_commit=False)
        if fetched is None:
            return
        fetched = [dict(x) for x in fetched]
        for row in fetched:
            ticker = row['ticker']
            logger.info(f"trigger subscriptions apply_async {ticker}")
            trigger_subscription.apply_async(args=[ticker],queue="stream")

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
            logger.info(f"trigger_gex_cache {ticker}")
            output = asyncio.run(compute_gex(ticker,et_tstamp,from_scratch=from_scratch,persist_to_postgres=True))

@celery_app.task
def trigger_vaccum_full(*args,**kwargs):
    vaccum_full_analyze()

@celery_app.task
def trigger_cache_cboe(*args,**kwargs):
    cache_cboe()

@celery_app.task
def trigger_shutdown(*args,**kwargs):
    celery_app.control.shutdown()

# TODO: you can implement backfill via luigi
class GexTarget(luigi.Target):
    ticker = luigi.parameter.Parameter()
    tstamp = luigi.parameter.DateSecondParameter()
    def __init__(self,ticker,tstamp):
        super().__init__()
        self.ticker = ticker
        self.tstamp = tstamp
    def exists(self):
        # TODO
        raise NotImplementedError()
        query_str = "select * from gex_net where ticker = %s and tstamp = %s"
        query_args = (self,ticker,self.tstamp)
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
    tstamp = luigi.parameter.DateSecondParameter()
    def output(self):
        return GexTarget(self.tstamp,self.tstamp)
    def requires():
        # TODO
        raise NotImplementedError()
        prior_tstamp = self.tstamp-datetime.timedelta(second=1)
        return ComputeSpotGex(self.ticker,prior_tstamp)
    def run(self):
        # TODO
        raise NotImplementedError()
        compute_gex(self.ticker,tstamp,persist_to_postgres=True,from_scratch=False)



if __name__ == "__main__":
    ticker = sys.argv[1]
    trigger_subscription(ticker)

""" 

python -m luigi --module tasks Subscription --ticker SPX --local-scheduler

python -m luigi --module tasks ComputeSpotGex --ticker SPX --tstamp 2024-10-18-13-30

celery_app.control.broadcast('shutdown') ??

"""