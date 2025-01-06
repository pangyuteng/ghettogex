import logging
logger = logging.getLogger(__file__)

import os
import re
import sys
import ast
import time
import math
import traceback
import datetime
import json
import asyncio
import threading
import luigi
from celery import Celery

# from gex_utils import persist_spot_gex

from utils.postgres_utils import postgres_execute
from utils.data_tasty import background_subscribe, is_market_open
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
        output = asyncio.run(background_subscribe(self.ticker,save_to_postres=True,save_to_json=True))

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
            trigger_subscription.apply_async(args=[ticker])

@celery_app.task
def trigger_gex_cache(*args,**kwargs):
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
            logger.info(f"trigger_gex_cache {ticker}")

"""
@celery_app.task
def trigger_gex_cache(*args,**kwargs):
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
            min_tstamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
            # persist some data, then finally full data.
            for countdown in [1,5,10,60]:
                compute_spot_gex.apply_async(args=[ticker,min_tstamp],countdown=countdown)


class SpotGexTarget(luigi.Target):
    def __init__(self,ticker,tstamp):
        super().__init__()
        self.ticker = ticker
        self.tstamp = tstamp
    def exists(self):
        return False

class ComputeSpotGex(luigi.Task):
    ticker = luigi.parameter.Parameter()
    tstamp = luigi.parameter.Parameter() # min is smallest unit - decided by me
    def output(self):
        return AlwaysRunTarget()
    def run(self):
        min_tstamp = datetime.datetime.strptime(self.tstamp,'%Y-%m-%d-%H-%M')
        persist_spot_gex(self.ticker,min_tstamp)

@celery_app.task
def compute_spot_gex(*args,**kwargs):
    ticker = args[0]
    tstamp = args[1]
    task = ComputeSpotGex(ticker=ticker,tstamp=tstamp)
    ret_code = luigi.build([task])

if __name__ == "__main__":
    ticker = sys.argv[1]
    trigger_subscription(ticker)

"""

""" 

python -m luigi --module tasks Subscription --ticker SPX --local-scheduler

python -m luigi --module tasks ComputeSpotGex --ticker SPX --tstamp 2024-10-18-13-30

celery_app.control.broadcast('shutdown') ??

"""