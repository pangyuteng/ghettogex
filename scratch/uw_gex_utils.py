import warnings
import traceback
import os
import sys
import datetime
import time
import pathlib
import tempfile
import zipfile
from tqdm import tqdm
import numpy as np
import pandas as pd
import py_vollib.black_scholes.greeks.numerical
import py_vollib_vectorized

import matplotlib.pyplot as plt
from moviepy import ImageClip, concatenate_videoclips, VideoFileClip



BOT_EOD_ROOT = "/mnt/hd2/data/finance/bot-eod-zip"
CACHE_FOLDER = "/mnt/hd1/data/uw-options-cache"

def format_stamp(x):
    if '.' in x:
        return datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S.%f+00')
    else:
        return datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S+00')

class GexService(object):
    def __init__(self,ticker):
        self.ticker = ticker
        self.zip_file_list = None
        self.pq_file_list = None

        self.debug = False
        self.input_day_pq_file = None
        self.input_day_df = None
        self.time_sec_list = None
        self.symbol_list = None
        self.oi_df = None
        self.price_df = None

        self.sg_df = None # sum gex
        
        self.price_pq_file = 'price.parquet.gzip'
        self.oi_pq_file = 'oi.parquet.gzip'
        self.sg_pq_file = 'sg.parquet.gzip'

    def _prepare(self):
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


    def get_gex_detailed(self,day_stamp_str,lookfoward_days):

        self._prepare()
        """
        what you want ultimately want - higher to lower level
            + net gex at each strike per second
            + gex at each strike by put/call per second
            + if ticker is SPX, then you need to get SPY price...
            + ddoi, gamma per second, underlying price

            # first get ddoi for each contract from all prior days.

        """
        
        day_stamp = datetime.datetime.strptime(day_stamp_str,'%Y-%m-%d').date()
        expiration_list = []
        for x in range(lookfoward_days):
            expiration = day_stamp + datetime.timedelta(days=x)
            expiration = expiration.strftime("%Y-%m-%d")
            expiration_list.append(expiration)

        print("day_stamp",day_stamp)
        print("expiration_list",expiration_list)

        # find zip file.
        day_pq_file_basename = f'{day_stamp_str}.parquet.gzip'
        try:
            basename_pq_file_list = [os.path.basename(x) for x in self.pq_file_list]
            pq_file_index = basename_pq_file_list.index(day_pq_file_basename)
        except:
            raise LookupError(f"pq_file not found given day_stamp_str {day_pq_file_basename}")

        if pq_file_index < 30:
            raise ValueError(f"Not enough data. pq_file_index {pq_file_index}!")

        # get order flow history
        print('gathering orders...')
        zero_count = 0
        mylist = []
        for pq_file in tqdm(self.pq_file_list[::-1]):
            df = pd.read_parquet(pq_file)
            unq_price = df.underlying_price.unique()
            df = df[df.expiry.apply(lambda x: x in expiration_list)]
            mylist.append(df)
            if len(df) == 0:
                zero_count+=1
            if zero_count > 10:
                break
        df = pd.concat(mylist)
        df = df.sort_values(['option_chain_id','tstamp'])
        df = df.reset_index()

        if self.debug:
            print(df.shape)
            print(df.columns)

        """
        index,executed_at,underlying_symbol,option_chain_id,side,strike,option_type,expiry,
        underlying_price,nbbo_bid,nbbo_ask,ewma_nbbo_bid,ewma_nbbo_ask,price,size,premium,
        volume,open_interest,implied_volatility,delta,theta,gamma,vega,rho,theo,
        sector,exchange,report_flags,canceled,upstream_condition_detail,equity_type,
        tstamp,tstamp_sec,size_signed,oi
        """

        """
        the DDOI is negative; when dealers are long the
        option, the DDOI is positive. DDOI is created by assessing trade direction of all option volume
        throughout the day, then comparing that volume to subsequent change in open interest
        
        """

        # ask implies it is bought, thus dealer is short -1
        # 
        def get_size_signed(row,okdf):
            if row.side == 'ask':
                return row['size']
            elif row.side == 'bid':
                return -1*row['size']
            else:
                return 0
        
        def get_ddoi_size_signed(row,okdf):
            if row.side == 'ask':
                return row['size']
            elif row.side == 'bid':
                return -1*row['size']
            else:
                return 0

        if False:
            df['size_signed'] = df.apply(lambda x: get_size_signed(x,df),axis=1)
            df['contract_type_int'] = df.option_type.apply(lambda x: -1 if x == 'put' else 1)
        if True:
            df['size_signed'] = df.apply(lambda x: get_ddoi_size_signed(x,df),axis=1)
            df['contract_type_int'] = df.option_type.apply(lambda x: -1 if x == 'put' else 1)

        self.symbol_list = df.option_chain_id.unique()
        print('compute oi...')
        oi_list = []
        for option_chain_id in tqdm(self.symbol_list):
            tmp_oi = df[df.option_chain_id==option_chain_id].copy()
            init_oi = 0
            tmp_oi['oi'] = tmp_oi.size_signed
            tmp_oi.oi = tmp_oi.oi.cumsum().astype(float)+init_oi
            oi_list.append(tmp_oi)
        oi_df = pd.concat(oi_list)

        if self.debug:
            print(oi_df.shape)

        self.input_day_pq_file = self.pq_file_list[pq_file_index]
        # get trading time seconds
        self.input_day_df = pd.read_parquet(self.input_day_pq_file)
        self.time_sec_list = pd.date_range(start=self.input_day_df.tstamp_sec.min(),end=self.input_day_df.tstamp_sec.max(),freq='s')
        print(self.time_sec_list[0],self.time_sec_list[-1])

        price_df = self.input_day_df.copy()
        price_df = price_df[['underlying_price','tstamp_sec']]
        price_df = price_df.groupby(['tstamp_sec']).agg(
            underlying_price=pd.NamedAgg(column="underlying_price", aggfunc="last"),
        ).resample('1s').last().reset_index()

        print('preparing gamma and gex compute...')
        oi_df = oi_df.sort_values(['option_chain_id','tstamp'])
        oi_df = oi_df.reset_index()
        #oi_df = oi_df.drop(['level_0'], axis=1) #??

        # leave only today's data
        # gex doesn't matter anyways.
        min_stamp = self.input_day_df.tstamp_sec.min()
        oi_df = oi_df[oi_df.tstamp_sec >= min_stamp]

        # setup "structured grid" for tstamp_sec,option_chain_id
        xv, yv = np.meshgrid(self.time_sec_list,self.symbol_list)
        sdf = pd.DataFrame({
            "tstamp_sec":xv.flatten(),
            "option_chain_id":yv.flatten(),
        })
        sdf = sdf.sort_values(['tstamp_sec','option_chain_id'])
        print(sdf.shape)

        # assuming greeks are computed at this time
        cols =  [
            'tstamp_sec','option_chain_id',
            'strike', 'option_type', 'expiry',
            'size_signed', 'contract_type_int', 'oi',
            'implied_volatility','delta', 'theta', 'gamma', 'vega', 'rho', 'theo','gex']
            #'gex'] #?you can't compute gex like this.
        oi_df = oi_df[cols]
        oi_df = oi_df.sort_values(['tstamp_sec','option_chain_id'])
        print(oi_df.shape)

        # greeks and gex should be recomputed here.
        warnings.warn("TODO: greeks and gex should be recomputed here")
        gdf = pd.merge_asof(sdf,oi_df,on='tstamp_sec',direction='backward',by='option_chain_id')
        print(gdf.shape)

        # 
        # TODO: get implied_volatility and underlying and recompute gamma and gex
        #
        """
        if self.ticker == "SPX":
            flag = ['c', 'p']
            S = 95
            K = [100, 90]
            t = .2
            r = .2
            sigma = .2
        
        gdf['gamma']=py_vollib.black_scholes.greeks.numerical.gamma(flag, S, K, t, r, sigma, return_as='series')
        gdf['gex'] = \
            oi_df.gamma * oi_df.oi * 100 \
            * oi_df.underlying_price * oi_df.underlying_price * 0.01 * oi_df.contract_type_int
        """
        sg_df = gdf[['tstamp_sec','strike','gex']].copy()
        sg_df = sg_df.groupby(['tstamp_sec','strike']).agg(
            gex=pd.NamedAgg(column="gex", aggfunc="sum"),
        ).reset_index()
        sg_df = sg_df.merge(price_df,how='left',on='tstamp_sec')
        print(sg_df.shape)
        
        self.price_df = price_df
        self.price_df.to_parquet(self.price_pq_file,compression='gzip')        
        self.oi_df = oi_df
        self.oi_df.to_parquet(self.oi_pq_file,compression='gzip')
        self.sg_df = sg_df
        self.sg_df.to_parquet(self.sg_pq_file,compression='gzip')

    def todos(self):
        if self.ticker == 'SPX':
            # SPX SPXW
            pass
        elif self.ticker == 'NDX':
            # NDX NDXP
            pass
        else:
            pass

    def gen_mp4(self,tmp_folder):
        png_folder = os.path.join(tmp_folder,'pngs')
        os.makedirs(png_folder,exist_ok=True)
        mp4_file = os.path.join(tmp_folder,'ok.mp4')

        png_file_list = []
        for time_sec in tqdm(self.time_sec_list[::60]):
            png_file = os.path.join(png_folder,
                f'{time_sec.strftime("%Y-%m-%d-%H-%M-%S")}.png')
            plot_func(time_sec,png_file,self.sg_df,self.price_df)
            if os.path.exists(png_file):
                png_file_list.append(png_file)

        fps = 24
        clips = [ImageClip(m).with_duration(0.1) for m in png_file_list]
        concat_clip = concatenate_videoclips(clips, method="compose")
        concat_clip.write_videofile(mp4_file, fps=fps)
        print(os.path.exists(mp4_file))

def plot_func(time_sec,png_file,sg_df,price_df):
    tmpdf = sg_df[sg_df.tstamp_sec==time_sec].reset_index()
    
    plt.figure()
    for n,row in tmpdf.iterrows():
        color = 'green' if row.gex > 0 else 'red'
        x = [0,row.gex]
        y = [row.strike,row.strike]
        plt.plot(x,y,color=color)
        if n ==0:
            plt.axhline(row.underlying_price,color='black',linestyle='--')
    plt.title(f"{str(time_sec)}")
    _ = plt.ylim(price_df.underlying_price.min()*0.90,price_df.underlying_price.max()*1.10)
    plt.grid(True)
    plt.show()
    plt.savefig(png_file)
    plt.close()

if __name__ == "__main__":
    ticker = sys.argv[1]
    day_stamp_str = sys.argv[2] # "2025-04-25"
    gs = GexService(ticker)
    lookfoward_days = 5 # +90 days
    gs.get_gex_detailed(day_stamp_str,lookfoward_days)
    #gs.gen_mp4('tmp')


"""

util to get gex from UW option flow data zip file.

docker run -it -u $(id -u):$(id -g) -w $PWD -v /mnt:/mnt -p 8888:8888 fi-notebook:latest bash
python uw_gex_utils.py SPY 2025-05-02
python uw_gex_utils.py SPX 2025-05-05

"""