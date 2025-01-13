import os
import sys
import time
import pytz
import json
import pathlib
import shutil
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pandas_market_calendars as mcal
sys.path.append("/opt/fi/flask")
from utils.postgres_utils import postgres_execute
from tqdm import tqdm
from moviepy import ImageClip, concatenate_videoclips, VideoFileClip

work_dir = 'tmp'

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
        ok['gex_timeandsale'] = ok.gamma * ok.oi_timeandsale * 100 * ok.spot_price * ok.spot_price * 0.01 * ok.contract_type_int
        ok['gex_volume'] = ok.gamma * ok.oi_volume * 100 * ok.spot_price * ok.spot_price * 0.01 * ok.contract_type_int
        mylist.append(ok.copy(deep=True))
    foodf = pd.concat(mylist)

    etime = time.time()
    print(etime-stime)
    return foodf

def gex_to_ani(df,mp4_file):

    png_file_list = sorted([str(x) for x in pathlib.Path("tmp/pngs").rglob("*.png")])

    if len(png_file_list) == 0:

        tstamp_list = sorted(list(df.tstamp_sec.unique()))[::60]
        
        df.gex_volume = df.gex_volume/1e9

        gex_lim = np.max(np.abs(df.gex_volume))
        spot_min,spot_max = np.min(df.spot_price)*.98,np.max(df.spot_price)*1.02
        print(gex_lim)
        print(spot_min,spot_max)

        for tstamp in tqdm(tstamp_list):

            tmp = df[df.tstamp_sec==tstamp].reset_index()

            try:
                spot_price = tmp.spot_price.to_list()[-1]
            except:
                spot_price = np.nan
            print(tstamp,spot_price)

            png_file = os.path.join(work_dir,"pngs",f"gex-{ticker}-{tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.png")
            # Plot 3D surface
            fig = plt.figure()
            strike_list = [[x,x] for x in tmp.strike.to_numpy()]
            naive_gex_list = [[0,x] for x in tmp.gex_volume.to_numpy()]
            for x,y in zip(naive_gex_list,strike_list):
                if x[-1] > 0:
                    color = 'green'
                else:
                    color = 'red'
                plt.plot(x,y,color=color)
            plt.axhline(spot_price,color='blue')
            plt.grid(True)
            plt.ylabel("strike")
            plt.xlabel("net naive gex ($Bn/%Move)")
            plt.title(f"ticker: {ticker} price {spot_price:1.2f}\n{tstamp}")
            plt.ylim(spot_min,spot_max)
            plt.xlim(-gex_lim,gex_lim)
            plt.show()
            plt.savefig(png_file)
            plt.close()
            png_file_list.append(png_file)


    print(len(png_file_list))

    #gif_file = os.path.join(work_dir,f'ani.gif')
    fps = 5
    clips = [ImageClip(m).with_duration(0.1) for m in png_file_list]
    concat_clip = concatenate_videoclips(clips, method="compose")
    concat_clip.write_videofile(mp4_file, fps=fps)
    print(os.path.exists(mp4_file))
    clip=VideoFileClip(mp4_file)
    #clip.write_gif(gif_file)


if __name__ == "__main__":

    ticker = 'SPX'
    day_stamp = '2025-01-08'
    day_stamp = '2025-01-10'
    day_stamp = '2025-01-07'
    pq_file = os.path.join(work_dir,f"pg-{day_stamp}.parquet.gzip")
    mp4_file = os.path.join(work_dir,f"pg-{day_stamp}.mp4")
    png_folder =os.path.join(work_dir,"pngs")
    if not os.path.exists(pq_file):
        foodf = cache_data(ticker,day_stamp)
        foodf.to_parquet(pq_file,compression='gzip',index=False)
        shutil.rmtree(png_folder)
    else:
        os.makedirs(png_folder,exist_ok=True)
        foodf = pd.read_parquet(pq_file)
    if not os.path.exists(mp4_file):
        gex_to_ani(foodf,mp4_file)
    print('done')
"""

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-6b6b89f7c6-ftwg4 5432:5432

docker run -it --env-file=.env  -w $PWD -v /mnt:/mnt -p 8888:8888 fi-flask:latest bash

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres

pip install jupyter notebook

jupyter notebook --allow-root --ip=*

python plot_gex_strike_from_events.py

"""