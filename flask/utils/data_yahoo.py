import logging
logger = logging.getLogger(__file__)
import os
import sys
import json
import time
import pytz
import datetime
import pathlib
import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf
from requests_ratelimiter import LimiterSession
from .data_cboe import scrape_data
from .misc import now_in_new_york
CACHE_FOLDER = os.environ.get("CACHE_FOLDER","utils/tmp")


nyse = mcal.get_calendar('NYSE')
def market_is_open(tstamp=None):
    if tstamp is None:
        tstamp = now_in_new_york()
    today = tstamp.strftime("%Y-%m-%d")
    early = nyse.schedule(start_date=today, end_date=today)
    if len(early) == 0:
        return False
    hour_list = [
        list(early.to_dict()['market_open'].values())[0],
        list(early.to_dict()['market_close'].values())[0]
    ]
    eastern = pytz.timezone('US/Eastern')
    logger.debug(f'{tstamp},{hour_list[0].astimezone(eastern)},{hour_list[1].astimezone(eastern)}')
    if tstamp > min(hour_list) and tstamp < max(hour_list):
        return True
    else:
        return False

def get_option_chain(ticker,ticker_obj):
    mylist = []
    for expiration in ticker_obj.options:
        call_df = ticker_obj.option_chain(expiration).calls
        call_df['ticker']=ticker
        call_df['option_type']='call'
        call_df['expiration']=expiration
        mylist.append(call_df)

        put_df = ticker_obj.option_chain(expiration).puts
        put_df['ticker']=ticker
        put_df['option_type']='put'
        put_df['expiration']=expiration
        mylist.append(put_df)
    if len(mylist) == 0:
        return pd.DataFrame([])
    df = pd.concat(mylist)
    return df

BTC_TICKER = "BTC-USD"
INDEX_TICKER_LIST = ['SPY','QQQ','^SPX','^NDX']
BTC_TICKER_LIST = ['^CBTX','^MBTX','ARKB','GBTC','IBIT','MSTR']
def cache_main():
    now_et = now_in_new_york()
    logger.info(str(now_et))
    year_stamp = datetime.datetime.strftime(now_et,'%Y')
    date_stamp = datetime.datetime.strftime(now_et,'%Y-%m-%d')
    time_stamp = datetime.datetime.strftime(now_et,'%Y-%m-%d-%H-%M-%Z') # '%Y-%m-%d-%H-%M-%S-%Z'
    ticker_list = [BTC_TICKER]
    ticker_list.extend(INDEX_TICKER_LIST)
    ticker_list.extend(BTC_TICKER_LIST)
    for ticker in ticker_list:
        logger.info(f'{ticker} underlying')
        cache_folder = os.path.join(CACHE_FOLDER,ticker,year_stamp,date_stamp)
        os.makedirs(cache_folder,exist_ok=True)
        json_file = os.path.join(cache_folder,f"underlying-{ticker}-{time_stamp}.json")
        if not os.path.exists(json_file):
            ticker_obj = yf.Ticker(ticker,session=LimiterSession(per_second=5))
            info_dict = ticker_obj.info
            with open(json_file,'w') as f:
                f.write(json.dumps(info_dict))
        else:
            logger.info('underlying found')

    ticker_list = []
    ticker_list.extend(INDEX_TICKER_LIST)
    ticker_list.extend(BTC_TICKER_LIST)
    tickers = yf.Tickers(ticker_list,session=LimiterSession(per_second=5))
    for ticker in ticker_list:
        logger.info(f'{ticker} options')
        cache_folder = os.path.join(CACHE_FOLDER,ticker,year_stamp,date_stamp)
        os.makedirs(cache_folder,exist_ok=True)
        csv_file = os.path.join(cache_folder,f"options-{ticker}-{time_stamp}.csv")
        ticker_obj = tickers.tickers[ticker]
        if not os.path.exists(csv_file):
            #df = get_option_chain(ticker,ticker_obj)
            spot_price, df = scrape_data(ticker)
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
        if len(csv_file_list) == 0 or len(json_file_list) == 0:
            raise LookupError()
        last_json_file = json_file_list[-1]
        last_csv_file = csv_file_list[-1]
    else:
        raise NotImplementedError()

    with open(last_json_file,'r') as f:
        underlying_dict = json.loads(f.read())
    options_df = pd.read_csv(last_csv_file)
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
        if market_is_open():
            cache_main()
        else:
            logger.info('market closed...')
        logger.info('sleeping...')
        time.sleep(5)


"""

python -m utils.data_yahoo

"""