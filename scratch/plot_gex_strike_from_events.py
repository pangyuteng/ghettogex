import os
import sys
import time
import pytz
import json
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pandas_market_calendars as mcal
sys.path.append("/opt/fi/flask")
from utils.postgres_utils import postgres_execute


def cache_data(ticker,day_stamp):

    stime = time.time()
    postgres_query = "select * from candle where event_symbol = %s and tstamp::date = %s order by tstamp"
    postgres_args = (ticker,day_stamp)
    fetched = postgres_execute(postgres_query,postgres_args)
    underlying_candle_df = pd.DataFrame(fetched)

    postgres_query = "select * from candle where event_symbol like '.'||%s||'%%' and tstamp::date = %s order by tstamp"
    postgres_args = (ticker,day_stamp)
    fetched = postgres_execute(postgres_query,postgres_args)
    candle_df = pd.DataFrame(fetched)

    postgres_query = "select * from greeks where event_symbol like '.'||%s||'%%' and tstamp::date = %s order by tstamp"
    postgres_args = (ticker,day_stamp)
    fetched = postgres_execute(postgres_query,postgres_args)
    greeks_df = pd.DataFrame(fetched)

    postgres_query = "select * from summary where event_symbol like '.'||%s||'%%' and tstamp::date = %s order by tstamp"
    postgres_args = (ticker,day_stamp)
    fetched = postgres_execute(postgres_query,postgres_args)
    summary_df = pd.DataFrame(fetched)

    postgres_query = "select * from timeandsale where event_symbol like '.'||%s||'%%' and tstamp::date = %s order by tstamp"
    postgres_args = (ticker,day_stamp)
    fetched = postgres_execute(postgres_query,postgres_args)
    timeandsale_df = pd.DataFrame(fetched)

    etime = time.time()
    print(etime-stime)
    print(underlying_candle_df.shape)
    print(candle_df.shape)
    print(greeks_df.shape)
    print(summary_df.shape)
    print(timeandsale_df.shape)

    #x.replace(microsecond=0,tzinfo=utc).astimezone(tz=eastern))

    nyse = mcal.get_calendar('NYSE')
    early = nyse.schedule(start_date=day_stamp, end_date=day_stamp)

    start_time = early.market_open.to_list()[0]
    end_time = early.market_close.to_list()[0]
    print(start_time,end_time)

    eastern = pytz.timezone('US/Eastern')
    utc = pytz.timezone('UTC')
    reference_tstamp_list = pd.date_range(start=start_time,end=end_time,freq='s')
    print(len(reference_tstamp_list))


    stime = time.time()
    # get underlying price at 1sec freq

    spot_df = pd.DataFrame([])
    spot_df['tstamp_sec']=reference_tstamp_list
    print(len(spot_df))
    uc = underlying_candle_df[underlying_candle_df.close > 0]
    uc = uc[['tstamp_sec','close']].groupby(['tstamp_sec']).last().reset_index()
    uc = uc.rename({'close':'spot_price'},axis=1)
    spot_df = spot_df.merge(uc,how='left',on=['tstamp_sec'])
    spot_df_null = spot_df.copy(deep=True)
    nullcount_init = np.sum(spot_df.spot_price.isnull())
    spot_df.spot_price = spot_df.spot_price.ffill()
    nullcount_after_ffill = np.sum(spot_df.spot_price.isnull())
    print(nullcount_init,nullcount_after_ffill,'!!')
    prct_ffilled = 100*(nullcount_init-nullcount_after_ffill) / len(spot_df)
    print('prct_ffilled',prct_ffilled)


    print(spot_df.shape)
    print(candle_df.shape)
    print(greeks_df.shape)
    print(summary_df.shape)
    print(timeandsale_df.shape)

    gby_summary_df = summary_df[['event_symbol','ticker','strike','contract_type','expiration','open_interest']]
    gby_summary_df = gby_summary_df.groupby(['event_symbol','ticker','strike','contract_type','expiration']).last().reset_index()
    gby_summary_df['contract_type_int'] = gby_summary_df.contract_type.apply(lambda x: -1 if x == 'P' else 1)

    gby_greeks_df = greeks_df[['event_symbol','price','volatility','delta','gamma','theta','rho','vega','tstamp_sec']]
    gby_greeks_df = gby_greeks_df.groupby(['event_symbol','tstamp_sec']).last().reset_index()

    timeandsale_df['size_signed'] = timeandsale_df['size'].where(timeandsale_df.aggressor_side == 'BUY', other=-1*timeandsale_df['size'])
    gby_timeandsale_df = timeandsale_df[['event_symbol','size_signed','tstamp_sec']]
    gby_timeandsale_df = gby_timeandsale_df.groupby(['event_symbol','tstamp_sec']).sum().reset_index()

    gby_candle_df = candle_df[['event_symbol','open','high','low','close','volume','bid_volume','ask_volume','tstamp_sec']]
    gby_candle_df = gby_candle_df.groupby(['event_symbol','tstamp_sec']).agg(
        open=pd.NamedAgg(column="open", aggfunc="last"),
        high=pd.NamedAgg(column="high", aggfunc="last"),
        low=pd.NamedAgg(column="low", aggfunc="last"),
        close=pd.NamedAgg(column="close", aggfunc="last"),
        volume=pd.NamedAgg(column="volume", aggfunc="sum"),
        bid_volume=pd.NamedAgg(column="bid_volume", aggfunc="sum"),
        ask_volume=pd.NamedAgg(column="ask_volume", aggfunc="sum"),
    ).reset_index()

    # each tstamp,event_symbol, strike*gmma*open_interest*spot_price*contract_type_int
    pd.set_option('future.no_silent_downcasting', True)
    mylist = []
    for event_symbol in list(gby_summary_df.event_symbol.unique()):
        su = gby_summary_df[gby_summary_df.event_symbol==event_symbol]
        gk = gby_greeks_df[gby_greeks_df.event_symbol==event_symbol]
        ts = gby_timeandsale_df[gby_timeandsale_df.event_symbol==event_symbol]
        cd = gby_candle_df[gby_candle_df.event_symbol==event_symbol]
        
        ok = pd.merge(spot_df, su, how='cross')
        ok = ok.merge(gk,how='left',on=['event_symbol','tstamp_sec'])
        ok = ok.merge(ts,how='left',on=['event_symbol','tstamp_sec'])
        ok = ok.merge(cd,how='left',on=['event_symbol','tstamp_sec'])
        
        # ffill gamma
        ok.gamma = ok.gamma.ffill()
        ok.close = ok.close.ffill()
        
        # fill 0s for timeandsale and candle volumes
        ok.size_signed = ok.size_signed.fillna(value=0)
        ok.bid_volume = ok.bid_volume.fillna(value=0)
        ok.ask_volume = ok.ask_volume.fillna(value=0)
        ok['oi_timeandsale'] = ok.size_signed
        ok['oi_volume'] = ok.ask_volume-ok.bid_volume
        try:
            init_oi = float(ok.open_interest.to_list()[-1])
        except:
            init_oi = 0
        ok.oi_timeandsale = ok.oi_timeandsale.cumsum().astype(float)+init_oi
        ok.oi_volume = ok.oi_volume.cumsum().astype(float)+init_oi
        mylist.append(ok.copy(deep=True))
    foodf = pd.concat(mylist)

    etime = time.time()
    print(etime-stime)
    return foodf

if __name__ == "__main__":

    ticker = 'SPX'
    day_stamp = '2025-01-08'
    pq_file = "tmp/pg.parquet.gzip"
    if not os.path.exists(pq_file):
        foodf = cache_data(ticker,day_stamp)
        foodf.to_parquet(pq_file,compression='gzip',index=False)
    else:
        foodf = pd.read_parquet(pq_file)

"""

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-6b6b89f7c6-ftwg4 5432:5432

docker run -it --env-file=.env  -w $PWD -v /mnt:/mnt -p 8888:8888 fi-flask:latest bash

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres

pip install jupyter notebook

jupyter notebook --allow-root --ip=*

python plot_gex_strike_from_events.py

"""