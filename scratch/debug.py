# kubectl port-forward --address 0.0.0.0 svc/postgres -n gg 5432:5432
postgres_uri = "postgres://postgres:postgres@192.168.68.143:5432/postgres"

import asyncio

import warnings
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import numpy as np
import datetime
import pytz
import time


import os
import sys
#sys.path.append("/opt/fi/flask")
import traceback
import psycopg
from psycopg.rows import dict_row

def postgres_execute(query_str,query_args,is_commit=False):
    response = None
    try:
        with psycopg.connect(postgres_uri,autocommit=True,row_factory=dict_row) as conn:
            with conn.cursor() as curs:
                curs.execute(query_str,query_args)
                if is_commit is False:
                    response = curs.fetchall()
    except:
        traceback.print_exc()
    return response

sys.path.append("/mnt/hd1/code/aigonewrong.com/ghettogex/flask")
from utils.compute_intraday import get_events_df,compute_gex_core

ticker = "SPX"
apool = None
# 14:28:19.550395
utc_tstamp = datetime.datetime(2025,5,23,14,28,19,0)
max_utc_tstamp = utc_tstamp+datetime.timedelta(seconds=1)
future_tstamp = utc_tstamp+datetime.timedelta(seconds=60)
prior_minute_utc_tstamp = utc_tstamp-datetime.timedelta(seconds=60)

event_df = asyncio.run(get_events_df(apool,ticker,utc_tstamp,max_utc_tstamp,future_tstamp,prior_minute_utc_tstamp))

print(event_df)
print(event_df.event_type.value_counts())

from_scratch = True

agg_df, qc_pass = compute_gex_core(event_df.copy(deep=True),from_scratch)


"""

export CACHE_FOLDER="/mnt/hd1/data/fi"
export CACHE_TASTY_FOLDER="/mnt/hd1/data/tastyfi"
export POSTGRES_URI="postgres://postgres:postgres@192.168.68.143:5432/postgres"
"""