
import os
import sys
import datetime
import pathlib
import tempfile
import zipfile
from tqdm import tqdm
import pandas as pd

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

    def _prepare(self):
        # save option flow parquet file.
        self.zip_file_list = sorted(str(x) for x in pathlib.Path(BOT_EOD_ROOT).rglob("*.zip"))
        with tempfile.TemporaryDirectory() as tmpdir:
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

        for pq_file in self.pq_file_list:
            df = pd.read_parquet(pq_file)
            # typically trading hr
            # ET: 9:30 to 16:00
            # UTC: 13:30 to 20:00
            # totals to 23400 seconds for full trading day 6.5*60*60 
            print(pq_file)
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
        df = get_gex_detailed(tstamp)
        return df.gex.sum()

if __name__ == "__main__":
    ticker = sys.argv[1]
    gs = GexService(ticker)
    day_stamp_str = "2025-04-10"
    lookfoward_days = 90 # +90 days
    gs.get_gex_detailed(day_stamp_str,lookfoward_days)


"""

util to get gex from UW option flow data zip file.

docker run -it -u $(id -u):$(id -g) -w $PWD -v /mnt:/mnt -p 8888:8888 fi-notebook:latest bash
python uw_gex_utils.py SPY
python uw_gex_utils.py SPX

"""