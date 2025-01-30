import logging
logger = logging.getLogger(__file__)
import traceback
import os
import sys
import json
import time
import pytz
import datetime
import pathlib
import pandas as pd

from .data_coin import scrape_btcusd
from .data_cboe import scrape_options_data,scrape_underlying_data
from .misc import now_in_new_york, is_market_open, CACHE_FOLDER

SPX = "^SPX"
CBOEX_TICKER_LIST = ['^SPX','^NDX','^VIX']
BTC_TICKER = "BTC-USD"
INDEX_TICKER_LIST = ['SPY','QQQ','^SPX','^NDX','^VIX']
# https://etfdb.com/themes/bitcoin-etfs/#complete-list__overview&sort_name=assets_under_management&sort_order=desc&page=1
BTC_TICKER_LIST = ['IBIT','GBTC','FBTC','ARKB','BTC','BITO','BITX','BITU','^CBTX','^MBTX']
OTHER_TICKER_LIST = ['MSTR','COIN','TSLA','NVDA','AAPL','MSFT','AMZN','META','GOOGL','GOOG','AVGO','COST']
BTC_MSTR_TICKER_LIST = list(BTC_TICKER_LIST)
BTC_MSTR_TICKER_LIST.append("MSTR")
USMARKET_TICKER = "USMARKET"
USMARKET_TICKER_LIST = ['SPY','QQQ','^SPX','^NDX','TSLA','NVDA','AAPL','MSFT','AMZN','META','GOOGL','GOOG','AVGO','COST']

def cache_cboe():
    now_et = now_in_new_york()
    logger.info(str(now_et))
    year_stamp = datetime.datetime.strftime(now_et,'%Y')
    date_stamp = datetime.datetime.strftime(now_et,'%Y-%m-%d')
    time_stamp = datetime.datetime.strftime(now_et,'%Y-%m-%d-%H-%M-%Z') # '%Y-%m-%d-%H-%M-%S-%Z'
    ticker_list = [BTC_TICKER]
    ticker_list.extend(INDEX_TICKER_LIST)
    ticker_list.extend(BTC_TICKER_LIST)
    ticker_list.extend(OTHER_TICKER_LIST)
    for ticker in ticker_list:
        logger.info(f'{ticker} underlying')
        cache_folder = os.path.join(CACHE_FOLDER,ticker,year_stamp,date_stamp)
        os.makedirs(cache_folder,exist_ok=True)
        json_file = os.path.join(cache_folder,f"underlying-{ticker}-{date_stamp}.json")
        if not os.path.exists(json_file):
            try:
                if ticker == BTC_TICKER:
                    info_dict = scrape_btcusd()
                else:
                    info_dict = scrape_underlying_data(ticker)

                assert('last_price' in info_dict.keys())

                with open(json_file,'w') as f:
                    f.write(json.dumps(info_dict))

            except:
                traceback.print_exc()

        else:
            logger.info('underlying found')

    ticker_list = []
    ticker_list.extend(INDEX_TICKER_LIST)
    ticker_list.extend(BTC_TICKER_LIST)
    ticker_list.extend(OTHER_TICKER_LIST)
    for ticker in ticker_list:
        logger.info(f'{ticker} options')
        cache_folder = os.path.join(CACHE_FOLDER,ticker,year_stamp,date_stamp)
        os.makedirs(cache_folder,exist_ok=True)
        # cboe updates OI everyday, thus cache just with date_stamp and not time_stamp
        csv_file = os.path.join(cache_folder,f"options-{ticker}-{date_stamp}.csv")
        if not os.path.exists(csv_file):
            spot_price, df = scrape_options_data(ticker)
            df.to_csv(csv_file,index=False)
        else:
            logger.info('options found')

def get_cache_latest(ticker,tstamp=None):

    if tstamp is None:
        years_folder = os.path.join(CACHE_FOLDER,ticker)
        year_str = sorted(os.listdir(years_folder))[-1]
        dates_folder = os.path.join(CACHE_FOLDER,ticker,year_str)
        date_str = sorted(os.listdir(dates_folder))[-1]
        cache_folder = os.path.join(CACHE_FOLDER,ticker,year_str,date_str)
        json_file_list = sorted([os.path.abspath(str(x)) for x in pathlib.Path(cache_folder).rglob(f"underlying-{ticker}-*.json")])
        csv_file_list = sorted([os.path.abspath(str(x)) for x in pathlib.Path(cache_folder).rglob(f"options-{ticker}-*.csv")])
        if ticker != BTC_TICKER:
            if len(csv_file_list) == 0 or len(json_file_list) == 0:
                raise LookupError()
            last_json_file = json_file_list[-1]
            last_csv_file = csv_file_list[-1]
        else:
            if len(json_file_list) == 0:
                raise LookupError()
            last_json_file = json_file_list[-1]
            last_csv_file = None
    else:
        # assume tstamp is str YYY-mm-dd
        date_str = tstamp.strftime("%Y-%m-%d")
        year_str = tstamp.strftime("%Y")
        cache_folder = os.path.join(CACHE_FOLDER,ticker,year_str,date_str)
        json_file_list = sorted([os.path.abspath(str(x)) for x in pathlib.Path(cache_folder).rglob(f"underlying-{ticker}-*.json")])
        csv_file_list = sorted([os.path.abspath(str(x)) for x in pathlib.Path(cache_folder).rglob(f"options-{ticker}-*.csv")])
        if ticker != BTC_TICKER:
            if len(csv_file_list) == 0 or len(json_file_list) == 0:
                raise LookupError()
            last_json_file = json_file_list[-1]
            last_csv_file = csv_file_list[-1]
        else:
            if len(json_file_list) == 0:
                raise LookupError()
            last_json_file = json_file_list[-1]
            last_csv_file = None
    if ticker != BTC_TICKER:
        options_df = pd.read_csv(last_csv_file)
    else:
        options_df = None

    with open(last_json_file,'r') as f:
        underlying_dict = json.loads(f.read())

    return underlying_dict,options_df,last_json_file,last_csv_file

if __name__== "__main__":
    # os.getpid()
    logger.setLevel(logging.INFO)
    #fh = logging.FileHandler(os.path.join(CACHE_FOLDER,'log.txt'))
    #fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    #logger.addHandler(fh)
    logger.addHandler(ch)
    while True:
        now_et = now_in_new_york()
        if is_market_open():
            logger.info('market open, will update when market closed...')
        elif now_et.hour > 19: # cache after market close.
            cache_cboe()
        else:
            pass
        # TODO: add celery
        logger.info('sleeping...')
        time.sleep(30)


"""

python -m utils.data_cache

"""