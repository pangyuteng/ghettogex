
import os
import sys
import datetime
import pathlib
import tempfile
import zipfile
from tqdm import tqdm
import numpy as np
import pandas as pd
import time
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
        self.gex_df = None

        self.oi_pq_file = 'oi.parquet.gzip'
        self.gex_pq_file = 'gex.parquet.gzip'

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
                        else:
                            df = df[df.underlying_symbol==self.ticker]
                        df['tstamp'] = df.executed_at.apply(lambda x: format_stamp(x))
                        df['tstamp_sec'] = df.tstamp.apply(lambda x: x.replace(microsecond=0))
                        df.to_parquet(pq_file,compression='gzip')
        
        
        self.pq_file_list = sorted(str(x) for x in pathlib.Path(os.path.join(CACHE_FOLDER,self.ticker)).rglob("*.gzip"))

        #SPX,SPXW
        #NDX,NDXP

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
                return -1*row['size']
            elif row.side == 'bid':
                return row['size']
            else:
                return 0

        if False:
            df['size_signed'] = df.apply(lambda x: get_size_signed(x,df),axis=1)
            df['contract_type_int'] = df.option_type.apply(lambda x: -1 if x == 'put' else 1)
        if True:
            df['size_signed'] = df.apply(lambda x: get_ddoi_size_signed(x,df),axis=1)
            df['contract_type_int'] = 1.0

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

        print('computing gex...')
        oi_df = oi_df.sort_values(['option_chain_id','tstamp'])
        oi_df = oi_df.drop(['level_0'], axis=1) #??
        oi_df = oi_df.reset_index()

        oi_df['gex'] = \
            oi_df.gamma * oi_df.oi * 100 \
            * oi_df.underlying_price * oi_df.underlying_price * 0.01 * oi_df.contract_type_int

        self.oi_df = oi_df
        self.oi_df.to_parquet(self.oi_pq_file,compression='gzip')
        print('getting gex...')

        self.input_day_pq_file = self.pq_file_list[pq_file_index]
        # get trading time seconds
        self.input_day_df = pd.read_parquet(self.input_day_pq_file)
        self.time_sec_list = pd.date_range(start=self.input_day_df.tstamp_sec.min(),end=self.input_day_df.tstamp_sec.max(),freq='s')
        print(self.time_sec_list[0],self.time_sec_list[-1])
        
        return


        # get gex at each sec per contract.
        mylist = []
        
        #??? TODO replace below with ?? df.groupby('a').resample('3min', include_groups=False).sum()

        for time_sec in tqdm(self.time_sec_list):
            for symbol in self.symbol_list:
                tmp = self.oi_df[(self.oi_df.tstamp_sec<=time_sec)&(self.oi_df.option_chain_id==symbol)]
                if len(tmp)>0:
                    mydict = dict(tmp.iloc[-1,:])
                    myrow = dict(
                        strike=mydict['strike'],
                        underlying_price=mydict['underlying_price'],
                        option_type=mydict['option_type'],
                        tstamp_sec=time_sec,
                        gex=mydict['gex'],
                    )
                    mylist.append(myrow)

        gex_df = pd.DataFrame(mylist)
        gex_df = gexdf.groupby(['tstamp_sec','strike']).agg(
            gex=pd.NamedAgg(column="gex", aggfunc="sum"),
        ).reset_index()
        self.gex_df = gex_df
        self.gex_df.to_parquet(self.gex_pq_file,compression='gzip')
        

    """

    def compute_ddoi(self):

        moidf['spot_price'] = spot_price
        moidf['contract_type_int'] = moidf.option_type.apply(lambda x: -1 if x == 'P' else 1)
        moidf['naive_gex'] = \
            moidf.gamma * moidf.open_interest * 100 \
            * moidf.spot_price * moidf.spot_price * 0.01 * moidf.contract_type_int

        moidf['ddoi_gex'] = \
            moidf.gamma * moidf.mod_oi * 100 \
            * moidf.spot_price * moidf.spot_price * 0.01 * moidf.contract_type_int

        moidf.naive_gex = moidf.naive_gex/1e9
        moidf.ddoi_gex = moidf.ddoi_gex/1e9

        assert(len(moidf.expiration.unique())==1)
        assert(len(moidf.strike.unique())*2==len(moidf))
        cols = ['expiration','strike','naive_gex','ref_ddoi_gex','ddoi_gex']
        gexdf = moidf[cols]
        gexdf = gexdf.groupby(['expiration','strike']).agg(
            naive_gex=pd.NamedAgg(column="naive_gex", aggfunc="sum"),
            ddoi_gex=pd.NamedAgg(column="ddoi_gex", aggfunc="sum"),
        ).reset_index()
    """
    def todos(self):
        sys.exit(1)
        # TODO: update estimate price
        if self.ticker == "SPX":
            # determine if underlying_price is legit
            underlying_price_requires_recompute = False
            if underlying_price_requires_recompute is False:
                pass
        elif self.ticker == "NDX":
            raise NotImplementedError()
        else:
            pass
        
        # determine ranges for time, strikes

        # ??? index = pd.MultiIndex.from_tuples(tuples, names=["first", "second"])
        # time,expriation,strike,contract_type,underyling_price,gamma,

        #df = df.resample('7s').first()
        #df = df.resample(rule='1s')
        
        # group by contract id.
        # cumsum? drop_duplicates()
        # then resample 

        strike_df = None
        ddoi_gex_df = None



        #sys.exit(1)
        #     
        #     self.alt_inst = GexService("SPY")
        #     self.alt_inst._prepare()

        #tstamp_sec
        #time_sec_list
        #df = pd.DataFrame(dict(A=[1, 1, 3], B=[5, None, 6], C=[1, 2, 3]))
        #df.groupby("A").last()

        
        # get list of contract relevant contract.
        # compute ddoi - dealer directional open interest.
        # for each second
        #  get estimated underlying price
        #  (from uw, use percentage change from SPY, alternatively compute from theoretical)
        #  get oi per contract.
        #  get gamm (from uw, alternatively compute from theoretical)
        #  gex per strike with gamma from 
        # 

        # day_stamp: is the day we will compute gex per strike per second
        # using expiration betwen day_stamp to day_stamp+lookfoward_days


        # get the contract of interest
        # based on strike,expiry,..

        # typically trading hr
        # ET: 9:30 to 16:00
        # UTC: 13:30 to 20:00
        # totals to 23400 seconds for full trading day 6.5*60*60 

        print(len(df.tstamp_sec.unique()),df.tstamp_sec.min(),df.tstamp_sec.max())
        # assert(24000)
        print('--')
        sys.exit(1)
        # day_stamp
        # df = pd.read_parquet(PQ_FILE)
        if self.ticker == 'SPX':
            # SPX SPXW
            pass
        elif self.ticker == 'NDX':
            # NDX NDXP
            pass
        else:
            pass

    def get_gex_total(self,tstamp):
        df = get_gex_detailed(tstamp,5)
        return df.gex.sum()

if __name__ == "__main__":
    ticker = sys.argv[1]
    day_stamp_str = sys.argv[2] # "2025-04-25"
    gs = GexService(ticker)
    lookfoward_days = 5 # +90 days
    gs.get_gex_detailed(day_stamp_str,lookfoward_days)


"""

util to get gex from UW option flow data zip file.

docker run -it -u $(id -u):$(id -g) -w $PWD -v /mnt:/mnt -p 8888:8888 fi-notebook:latest bash
python uw_gex_utils.py SPY 2025-05-02
python uw_gex_utils.py SPX

"""