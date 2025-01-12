import os
import sys
import time
import pytz
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pandas_market_calendars as mcal
sys.path.append("/opt/fi/flask")
from utils.postgres_utils import postgres_execute


def main():
    ticker = 'SPX'
    day_stamp = '2025-01-07'
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

    mydict = {
        'uc': underlying_candle_df,
        'c': candle_df,
        'g': greeks_df,
        's': summary_df,
        't': timeandsale_df,
    }

    for k,v in mydict.items():
        print(len(v))
        v['tstamp_sec']=v.tstamp.apply(lambda x: x.replace(microsecond=0,tzinfo=utc))

    # get underlying price at 1sec freq

    mergedf = pd.DataFrame([])
    mergedf['tstamp_sec']=reference_tstamp_list
    print(len(mergedf))
    uc = underlying_candle_df[underlying_candle_df.close > 0]
    uc = uc[['tstamp_sec','close']].groupby(['tstamp_sec']).last().reset_index()
    uc = uc.rename({'close':'spot_price'},axis=1)
    mergedf = mergedf.merge(uc,how='left',on=['tstamp_sec'])
    mergedf_null = mergedf.copy(deep=True)
    nullcount_init = np.sum(mergedf.spot_price.isnull())
    mergedf.spot_price = mergedf.spot_price.ffill()
    nullcount_after_ffill = np.sum(mergedf.spot_price.isnull())
    print(nullcount_init,nullcount_after_ffill,'!!')
    prct_ffilled = 100*(nullcount_init-nullcount_after_ffill) / len(mergedf)
    print('prct_ffilled',prct_ffilled)


    summary_df = summary_df[['event_symbol','ticker','strike','contract_type','expiration','tstamp_sec','open_interest']]
    summary_df = summary_df.groupby(['event_symbol','ticker','strike','contract_type','expiration','tstamp_sec']).last().reset_index()

    greeks_df = greeks_df[['event_symbol','tstamp_sec','price','volatility','delta','gamma','theta','rho','vega']]
    greeks_df = greeks_df.groupby(['event_symbol','tstamp_sec']).last().reset_index()

    timeandsale_df['size_signed'] = timeandsale_df['size'].where(timeandsale_df.aggressor_side == 'BUY', other=-1*timeandsale_df['size'])
    timeandsale_df = timeandsale_df[['event_symbol','size_signed','tstamp_sec']]
    timeandsale_df = timeandsale_df.groupby(['event_symbol','tstamp_sec']).sum().reset_index()
    
    candle_df = candle_df[['event_symbol','tstamp_sec','open','high','low','close','volume','bid_volume','ask_volume']]
    candle_df = candle_df.groupby(['event_symbol','tstamp_sec']).agg(
        open=pd.NamedAgg(column="open", aggfunc="last"),
        high=pd.NamedAgg(column="high", aggfunc="last"),
        low=pd.NamedAgg(column="low", aggfunc="last"),
        close=pd.NamedAgg(column="close", aggfunc="last"),
        volume=pd.NamedAgg(column="volume", aggfunc="sum"),
        bid_volume=pd.NamedAgg(column="bid_volume", aggfunc="sum"),
        ask_volume=pd.NamedAgg(column="ask_volume", aggfunc="sum"),
    ).reset_index()
    print(candle_df.columns)
    # summary OI is only at initial few seconds.... given we want to intraday OI....
    # naive merge will not work.

    mergedf = mergedf.merge(summary_df,how='left',on=['tstamp_sec'])
    mergedf = mergedf.merge(greeks_df,how='left',on=['event_symbol','tstamp_sec'])
    mergedf = mergedf.merge(timeandsale_df,how='left',on=['event_symbol','tstamp_sec'])
    mergedf = mergedf.merge(candle_df,how='left',on=['event_symbol','tstamp_sec'])
    mergedf['contract_type_int'] = mergedf.contract_type.apply(lambda x: -1 if x == 'P' else 1)
    print(mergedf.columns)
    print(mergedf.shape)

if __name__ == "__main__":
    main()



"""

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-6b6b89f7c6-ftwg4 5432:5432

docker run -it --env-file=.env  -w $PWD -v /mnt:/mnt -p 8888:8888 fi-flask:latest bash

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres

pip install jupyter notebook

jupyter notebook --allow-root --ip=*

plot_gex_strike_from_events.py

"""