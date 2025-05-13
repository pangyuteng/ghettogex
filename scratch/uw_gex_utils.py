import warnings
import traceback
import os
import sys
import datetime
import time
import pathlib
import tempfile
import shutil
import zipfile
from tqdm import tqdm
import numpy as np
import pandas as pd
import py_vollib.black_scholes.greeks.numerical
import py_vollib_vectorized

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from moviepy import ImageClip, concatenate_videoclips, VideoFileClip

import pandas_market_calendars as mcal

def get_market_open_close(day_stamp,no_tzinfo=True):
    nyse = mcal.get_calendar('NYSE')
    early = nyse.schedule(start_date=day_stamp, end_date=day_stamp)
    market_open = list(early.to_dict()['market_open'].values())[0]
    market_close = list(early.to_dict()['market_close'].values())[0]
    if no_tzinfo:
        return market_open.replace(tzinfo=None),market_close.replace(tzinfo=None)
    else:
        return market_open,market_close

TOTAL_SECONDS_ONE_YEAR = 365*24*60*60 # total seconds

def get_expiry_tstamp(expiry):
    if not isinstance(expiry,str):
        return np.nan
    expiry = datetime.datetime.strptime(expiry,"%Y-%m-%d")
    _,expiry_tstamp = get_market_open_close(expiry)
    return expiry_tstamp.replace(tzinfo=None)

def get_annualized_time_to_expiration(row,expiry_mapper):
    expiry_tstamp = expiry_mapper[row.expiry]
    tstamp_sec = row.tstamp_sec
    sec_to_expiration = (expiry_tstamp-tstamp_sec).total_seconds()
    atte = sec_to_expiration/TOTAL_SECONDS_ONE_YEAR
    return atte

BOT_EOD_ROOT = "/mnt/hd2/data/finance/bot-eod-zip"
CACHE_FOLDER = "/mnt/hd1/data/uw-options-cache"

def format_stamp(x):
    if '.' in x:
        return datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S.%f+00')
    else:
        return datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S+00')

class GexService(object):
    def __init__(self,ticker,output_folder,day_stamp_str,expiration_count):
        self.ticker = ticker
        self.zip_file_list = None
        self.pq_file_list = None

        self.input_day_pq_file = None
        self.input_day_df = None
        self.time_sec_list = None
        self.symbol_list = None
        self.oi_df = None
        self.price_df = None
        self.sg_df = None # sum gex

        self.day_stamp_str = day_stamp_str
        self.day_stamp = datetime.datetime.strptime(self.day_stamp_str,'%Y-%m-%d').date()
        print("day_stamp",self.day_stamp)

        self.expiration_count = expiration_count
        self.expiration_list = None
        
        self.output_folder = output_folder
        self.mp4_file = os.path.join(self.output_folder,f'{self.ticker}-{self.day_stamp_str}.mp4')
        self.price_pq_file = os.path.join(self.output_folder,f'{self.ticker}-{self.day_stamp_str}-price.parquet.gzip')
        self.oi_pq_file = os.path.join(self.output_folder,f'{self.ticker}-{self.day_stamp_str}-oi.parquet.gzip')
        self.sg_pq_file = os.path.join(self.output_folder,f'{self.ticker}-{self.day_stamp_str}-sg.parquet.gzip')

    def _cache_ticker_flow(self):
        # save option flow parquet file.
        self.zip_file_list = sorted(str(x) for x in pathlib.Path(BOT_EOD_ROOT).rglob("*.zip"))
        with tempfile.TemporaryDirectory() as tmpdir:
            # replace `tmpdir`` with `CACHE_FOLDER`
            for zip_file in tqdm(self.zip_file_list):
                tstamp_from_file = os.path.basename(zip_file).replace("bot-eod-report-","").replace(".zip","")
                pq_file = os.path.join(CACHE_FOLDER,self.ticker,f"{tstamp_from_file}.parquet.gzip")
                os.makedirs(os.path.dirname(pq_file),exist_ok=True)
                if not os.path.exists(pq_file):
                    print(pq_file)
                    print('reading',zip_file)
                    archive = zipfile.ZipFile(zip_file, 'r')
                    csv_file = os.path.basename(zip_file).replace(".zip",".csv")
                    with archive.open(csv_file) as f:
                        df = pd.read_csv(f,low_memory=False)
                        if self.ticker == 'SPX':
                            df = df[(df.underlying_symbol==self.ticker)|(df.underlying_symbol=="SPXW")]
                        elif self.ticker == 'NDX':
                            df = df[(df.underlying_symbol==self.ticker)|(df.underlying_symbol=="NDXP")]
                        elif self.ticker == 'VIX':
                            df = df[(df.underlying_symbol==self.ticker)|(df.underlying_symbol=="VIXW")]
                        else:
                            df = df[df.underlying_symbol==self.ticker]
                        df['tstamp'] = df.executed_at.apply(lambda x: format_stamp(x))
                        df['tstamp_sec'] = df.tstamp.apply(lambda x: x.replace(microsecond=0))
                        df.to_parquet(pq_file,compression='gzip')
        
        self.pq_file_list = sorted(str(x) for x in pathlib.Path(os.path.join(CACHE_FOLDER,self.ticker)).rglob("*.gzip"))

    def get_gex_detailed(self,):

        self._cache_ticker_flow()

        # find zip file.
        day_pq_file_basename = f'{self.day_stamp_str}.parquet.gzip'
        try:
            basename_pq_file_list = [os.path.basename(x) for x in self.pq_file_list]
            pq_file_index = basename_pq_file_list.index(day_pq_file_basename)
        except:
            raise LookupError(f"pq_file not found given day_stamp_str {day_pq_file_basename}")

        if pq_file_index < 30:
            raise ValueError(f"Not enough data. pq_file_index {pq_file_index}!")

        self.input_day_pq_file = self.pq_file_list[pq_file_index]
        self.pq_file_list = self.pq_file_list[:pq_file_index+1]
        print(self.input_day_pq_file)
        print(self.pq_file_list[-1])
        assert(self.input_day_pq_file==self.pq_file_list[-1])
        # get trading time seconds
        self.input_day_df = pd.read_parquet(self.input_day_pq_file)

        market_open, market_close = self.input_day_df.tstamp_sec.min(),self.input_day_df.tstamp_sec.max()

        # TODO: you'll get nan for expiry_mapper using below market_open
        self.true_market_open, self.true_market_close = get_market_open_close(self.day_stamp)
        self.time_sec_list = pd.date_range(start=market_open,end=self.true_market_close,freq='s')
        
        print(self.time_sec_list[0],self.time_sec_list[-1])

        print(self.input_day_df.columns)
        expiration_list = sorted(self.input_day_df.expiry.unique())
        self.expiration_list = expiration_list[:self.expiration_count]
        print("expiration_list",self.expiration_list)
        if self.day_stamp_str not in self.expiration_list:
            warnings.warn(f"day_stamp_str {self.day_stamp_str} not in expiration_list")

        # TODO: maybe above can be split to a seperate func.

        price_df = self.input_day_df.copy()
        price_df = price_df[['underlying_price','tstamp_sec']]
        missing_underlying_price = np.sum(price_df.underlying_price.isna()) == len(price_df.underlying_price)
        if missing_underlying_price:
            raise ValueError("missing_underlying_price!")

        price_df = price_df.groupby(['tstamp_sec']).agg(
            underlying_price=pd.NamedAgg(column="underlying_price", aggfunc="last"),
        ).resample('1s').last().reset_index()

        # get order flow history
        print('gathering orders...')
        zero_count = 0
        mylist = []
        for pq_file in tqdm(self.pq_file_list[::-1]):
            df = pd.read_parquet(pq_file)
            unq_price = df.underlying_price.unique()
            # NOTE: use unq_price to ensure underlying_price is estimates.
            df = df[df.expiry.apply(lambda x: x in self.expiration_list)]
            print(df.shape,pq_file)
            mylist.append(df)

            if len(df) == 0:
                zero_count+=1
            if zero_count > 10:
                break
        df = pd.concat(mylist)
        df = df.sort_values(['option_chain_id','tstamp'],ignore_index=True)
        df = df.reset_index()
        print(df.shape,'!!!!!!!!!!!!!!!!!!!')

        """
        index,executed_at,underlying_symbol,option_chain_id,side,strike,option_type,expiry,
        underlying_price,nbbo_bid,nbbo_ask,ewma_nbbo_bid,ewma_nbbo_ask,price,size,premium,
        volume,open_interest,implied_volatility,delta,theta,gamma,vega,rho,theo,
        sector,exchange,report_flags,canceled,upstream_condition_detail,equity_type,
        tstamp,tstamp_sec,size_signed,oi
        """

        """
        # naive gex, dealer short put, long call
        # https://perfiliev.com/blog/how-to-calculate-gamma-exposure-and-zero-gamma-level
        # A crude approximation is that the dealers are long the calls and short the puts,
        df['contract_type_int'] = df.option_type.apply(lambda x: -1 if x == 'put' else 1)

        The number of option contracts that are held by option dealers, and the direction in which those
        contracts are held. When dealers are short the option, the DDOI is negative; when dealers are long the
        option, the DDOI is positive. DDOI is created by assessing trade direction of all option volume
        throughout the day, then comparing that volume to subsequent change in open interest.        

        https://www.gexbot.com/static/media/hau.0fcbcd78dd6272834a38.pdf
        Notice how the new best ask is lifted from 2.20 to 2.21, while the best bid remains the same. 
        Furthermore, the contracts offered at 2.21 drops from 120 to 79, indicating less liquidity being
        sold from market makers (liquidity was likely ‘eaten up’ by a buy order

        tldr
        if ask price increase, likely was buy order.

        """
        def get_side_mod(row,arg_df):
            try:
                mod_side = None
                if row.side == 'ask': # near ask, client bought, dealer short
                    mod_side = 'ask'
                elif row.side == 'bid': # near bid, client sold, dealer long
                    mod_side = 'bid'
                else:
                    idx = row['index']
                    if arg_df.at[idx+1,"nbbo_ask"] > arg_df.at[idx,"nbbo_ask"]:
                        mod_side = 'likely_ask'
                    elif arg_df.at[idx+1,"nbbo_bid"] > arg_df.at[idx,"nbbo_bid"]:
                        mod_side = 'likely_ask'
                    else:
                        mod_side = 'likely_bid' #???
                return mod_side
            except:
                return "exception"

        def get_size_signed(row):
            if row.size_mod in ['ask','likely_ask']: # near ask, client bought, dealer short
                return -1*row['size'] 
            elif row.size_mod in ['bid','likely_bid']: # near bid, client sold, dealer long
                return row['size']
            else:
                return 0 # SET TO ZERO NOT GOOD. TODO: FIX THIS USING HUA!

        df['size_mod'] = df.apply(lambda x: get_side_mod(x,df),axis=1)
        df['size_signed'] = df.apply(lambda x: get_size_signed(x),axis=1)

        # if you are net long call, you gotta short, if you are net long put, you gotta long
        # so we will need to flip based on contract type?!!
        df['contract_type_int'] = df.option_type.apply(lambda x: -1 if x == 'put' else 1)
        self._raw_df = df

        print("df.side.value_counts()")
        print(df.side.value_counts())
        print("df.size_mod.value_counts()")
        print(df.size_mod.value_counts())
        print("df.canceled.value_counts()")
        print(df.canceled.value_counts())

        self.symbol_list = df.option_chain_id.unique()
        print(self.symbol_list)
        print('compute ddoi...')
        oi_list = []
        for option_chain_id in tqdm(self.symbol_list):
            # assume this is sorted?
            tmp_oi = df[df.option_chain_id==option_chain_id].copy()
            tmp_oi['oi'] = tmp_oi.size_signed
            tmp_oi.oi = tmp_oi.oi.cumsum().astype(float)
            oi_list.append(tmp_oi)

        oi_df = pd.concat(oi_list)
        oi_df = oi_df.sort_values(['option_chain_id','tstamp'])
        oi_df = oi_df.reset_index()

        print('preparing gamma and gex compute...')

        # keep today's data
        # gex doesn't matter anyways.
        min_stamp = self.input_day_df.tstamp_sec.min()
        oi_df = oi_df[oi_df.tstamp_sec >= min_stamp]
        oi_df = oi_df.drop(['level_0'], axis=1) #??
        oi_df = oi_df.reset_index()

        # setup "structured grid" for tstamp_sec,option_chain_id
        xv, yv = np.meshgrid(self.time_sec_list,self.symbol_list)
        sdf = pd.DataFrame({
            "tstamp_sec":xv.flatten(),
            "option_chain_id":yv.flatten(),
        })
        sdf = sdf.merge(price_df,how='left',on='tstamp_sec')
        sdf = sdf.sort_values(['tstamp_sec','option_chain_id'],ignore_index=True)
        print(sdf.shape)

        # assuming greeks are computed at this time, this is the wrong assumption.
        cols =  [
            'tstamp','tstamp_sec','option_chain_id',
            'strike', 'option_type', 'expiry',
            'side','size_mod','size','size_signed', 'contract_type_int', 'oi',
            'price','nbbo_bid','nbbo_ask','ewma_nbbo_bid','ewma_nbbo_ask','canceled',
            'implied_volatility','delta', 'theta', 'gamma', 'vega', 'rho', 'theo',
            #'underlying_price', 
        ] # once merged, underlying_price, price, greeks all likely outdated!

        oi_df = oi_df[cols]
        oi_df = oi_df.sort_values(['tstamp_sec','option_chain_id'],ignore_index=True)
        print(oi_df.shape)

        # NOTE: greeks and gex should be recomputed here.
        warnings.warn("TODO: greeks and gex should be recomputed here")
        gdf = pd.merge_asof(sdf,oi_df,on='tstamp_sec',direction='backward',by='option_chain_id')
        print(gdf.shape)

        gdf['gex'] = \
            gdf.gamma * gdf.oi * 100 \
            * gdf.underlying_price * gdf.underlying_price * 0.01 * gdf.contract_type_int

        # 
        # TODO: get implied_volatility and underlying and recompute gamma and gex
        #

        if False: # self.ticker == "SPX":
            #disable for now, below is flawed as 
            # IV is surface, you can't just 
            # so ideally you want, at each timepoint
            # you need bid/ask prices for option chain
            # to get IV curve, then you derive gamma.
            print(gdf.expiry.unique())
            expiry_mapper = {x:get_expiry_tstamp(x) for x in list(gdf.expiry.unique())}

            gdf['annualized_time_to_expiration'] = mdf.apply(
                lambda x: get_annualized_time_to_expiration(x,expiry_mapper),axis=1)

            interest_rate = 0.0
            flag = gdf.option_type.apply(lambda x: 'c' if x == 'call' else 'p')
            S = gdf.underlying_price
            K = gdf.strike
            t = gdf.annualized_time_to_expiration
            r = interest_rate
            #sigma = gdf.vix*0.01
            # TODO: 
            # every 15 or x sec, get price of available contracts.
            # use above to compute IV and estimate IV surface
            # then get volatility at each strike at time x.
            sigma = 0.2 
            gamma = py_vollib.black_scholes.greeks.numerical.gamma(flag, S, K, t, r, sigma, return_as='series')
            gdf['updated_gamma'] = gamma

            gdf['updated_gex'] = \
                oi_df.updated_gamma * oi_df.oi * 100 \
                * oi_df.underlying_price * oi_df.underlying_price * 0.01 * oi_df.contract_type_int

        #sg_df = gdf[['tstamp_sec','strike','gex','underlying_price','updated_gex']].copy()
        sg_df = gdf[['tstamp_sec','strike','gex','underlying_price']].copy()
        sg_df = sg_df.groupby(['tstamp_sec','strike']).agg(
            gex=pd.NamedAgg(column="gex", aggfunc="sum"),
            underlying_price=pd.NamedAgg(column="underlying_price", aggfunc="last"),
        ).reset_index()
        sg_df['gex'] = sg_df['gex']/ 10**9
        print(sg_df.shape)

        
        os.makedirs(self.output_folder,exist_ok=True)

        self.price_df = price_df
        self.price_df.to_parquet(self.price_pq_file,compression='gzip')
        self.oi_df = oi_df
        self.oi_df.to_parquet(self.oi_pq_file,compression='gzip')
        self.sg_df = sg_df
        self.sg_df.to_parquet(self.sg_pq_file,compression='gzip')


    def gen_mp4(self):
        png_folder = os.path.join(self.output_folder,f'pngs-{self.ticker}-{self.day_stamp_str}')
        if os.path.exists(png_folder):
            shutil.rmtree(png_folder)
        os.makedirs(png_folder,exist_ok=True)

        #tstamp_lim = [self.price_df.tstamp_sec.min(),self.true_market_close]
        tstamp_lim = [self.true_market_open,self.true_market_close]
        price_lim = [self.price_df.underlying_price.min()*0.98,self.price_df.underlying_price.max()*1.02]
        gex_lim = [self.sg_df.gex.min(),self.sg_df.gex.max()]
        gex_lim = self.sg_df.gex.quantile([0.01,0.99]).to_list()
        print(price_lim)

        major_df = self.sg_df.groupby(['tstamp_sec']).agg(
                major_pos_gex_idx=pd.NamedAgg(column="gex", aggfunc="idxmax"),
                major_neg_gex_idx=pd.NamedAgg(column="gex", aggfunc="idxmin"),
                underlying_price=pd.NamedAgg(column="underlying_price", aggfunc="last"),
        ).reset_index()

        def get_strike(row,df):
            return df.at[row.major_pos_gex_idx,'strike'],df.at[row.major_neg_gex_idx,'strike']
        major_df['major_pos_gex_strike'], major_df['major_neg_gex_strike']= \
            zip(*major_df.apply(lambda row:get_strike(row,self.sg_df),axis=1))
        # weed out noise
        major_df = major_df[(major_df.major_pos_gex_strike>major_df.underlying_price/2)&(major_df.major_neg_gex_strike>major_df.underlying_price/2)]

        total_gex_df = self.sg_df.groupby(['tstamp_sec']).agg(
                total_gex=pd.NamedAgg(column="gex", aggfunc="sum"),
            ).reset_index()

        png_file_list = []
        for time_sec in tqdm(self.time_sec_list[::30]):
            png_file = os.path.join(png_folder,
                f'{time_sec.strftime("%Y-%m-%d-%H-%M-%S")}.png')
            plot_func(self.ticker,time_sec,png_file,self.sg_df,self.price_df,major_df,total_gex_df,tstamp_lim,gex_lim,price_lim)
            if os.path.exists(png_file):
                png_file_list.append(png_file)

        fps = 24
        clips = [ImageClip(m).with_duration(0.1) for m in png_file_list]
        concat_clip = concatenate_videoclips(clips, method="compose")
        concat_clip.write_videofile(self.mp4_file, fps=fps)
        print(os.path.exists(self.mp4_file))
        if os.path.exists(png_folder):
            shutil.rmtree(png_folder)

def plot_func(ticker,time_sec,png_file,sg_df,price_df,major_df,total_gex_df,tstamp_lim,gex_lim,price_lim):
    tmpdf = sg_df[sg_df.tstamp_sec==time_sec].reset_index()
    tmpmajor_df = major_df[major_df.tstamp_sec<=time_sec].reset_index()
    tmptotal_gex_df = total_gex_df[total_gex_df.tstamp_sec<=time_sec].reset_index()
    if len(tmpdf) == 0:
        return
    if False:
        #DISABLE TOTAL_GEX PLOT
        fig, (ax1, ax2) = plt.subplots(2,1)
    else:
        fig, ax1 = plt.subplots()

    try:
        underlying_price = tmpdf.at[0,'underlying_price']
    except:
        print(tmpdf.underlying_price)
        underlying_price = np.nan
        traceback.print_exc()

    ax1.set_title(f"{str(time_sec)} {ticker} {underlying_price}")

    color_label = 'tab:red'
    ax1.set_xlabel('GEX ($ bn/1% move)', color=color_label)
    ax1.set_ylabel('Strike')

    for n,row in tmpdf.iterrows():
        color = 'green' if row.gex > 0 else 'red'
        x = [0,row.gex]
        #ucolor = 'olive' if row.updated_gex > 0 else 'orange'
        #ux = [0,row.updated_gex]
        y = [row.strike,row.strike]
        ax1.plot(x,y,color=color)
        #ax1.plot(ux,y,color=ucolor,linestyle='--',alpha=0.5)
        if n == 0:
            ax1.axhline(row.underlying_price,color='gray',linestyle='--')
    ax1.tick_params(axis='x', labelcolor=color_label)
    ax1_twin = ax1.twiny()
    color_label = 'tab:blue'
    ax1_twin.set_xlabel('time (utc)', color=color_label)
    tmp_price = price_df[price_df.tstamp_sec <= time_sec]
    # plot price, major pos/neg gex (**different from gexbot**)
    ax1_twin.plot(tmpmajor_df.tstamp_sec,tmpmajor_df.major_pos_gex_strike,color='lightgreen',alpha=1)
    ax1_twin.plot(tmpmajor_df.tstamp_sec,tmpmajor_df.major_neg_gex_strike,color='lightpink',alpha=1)
    ax1_twin.plot(tmp_price.tstamp_sec, tmp_price.underlying_price, color='black',linewidth=1)
    ax1_twin.xaxis.set_major_formatter(mdates.DateFormatter('%H-%M-%S'))
    ax1_twin.tick_params(axis='x', rotation=30)
    ax1_twin.tick_params(axis='y', labelcolor=color_label)

    if tstamp_lim:
        ax1_twin.set_xlim(tstamp_lim)
    if price_lim:
        ax1.set_ylim(price_lim)
    gex_lim = [tmpdf.gex.min(),tmpdf.gex.max()]
    if gex_lim and gex_lim[0] != gex_lim[1]:
        ax1.set_xlim(gex_lim)

    ax1.grid(True)

    # plot total gex
    if False:
        ax2.scatter(tmptotal_gex_df.tstamp_sec,tmptotal_gex_df.total_gex,color='black',s=1)
        ax2.axhline(0)
        if tstamp_lim:
            ax2.set_xlim(tstamp_lim)
        ax2.grid(True)

    fig.tight_layout()
    plt.show()
    plt.savefig(png_file)
    plt.close()

if __name__ == "__main__":
    ticker = sys.argv[1]
    day_stamp_str = sys.argv[2]
    expiration_count = 1
    gs = GexService(ticker,"tmp",day_stamp_str,expiration_count)
    if not os.path.exists(gs.mp4_file):
        gs.get_gex_detailed()
        gs.gen_mp4()


"""

util to get gex from UW option flow data zip file.

docker run -it -u $(id -u):$(id -g) -w $PWD -v /mnt:/mnt -p 8888:8888 fi-notebook:latest bash

docker run -it -w $PWD -v /mnt:/mnt -v $PWD/tmp:/.local -p 8888:8888 fi-notebook:latest bash

python uw_gex_utils.py SPY 2025-05-08
python uw_gex_utils.py SPX 2025-05-09

"""