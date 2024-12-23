# from requests_ratelimiter import LimiterSession
# prices = yf.download(tickers, period='5d', interval='1d', session=LimiterSession(per_second=3))
import os
import sys
import json
import time
import pytz
import datetime
import pandas as pd
import yfinance as yf
from requests_ratelimiter import LimiterSession


def get_option_chain(ticker,ticker_obj):
    mylist = []
    for expiry in ticker_obj.options:
        call_df = ticker_obj.option_chain(expiry).calls
        call_df['ticker']=ticker
        call_df['option_type']='call'
        call_df['expiry']=expiry
        mylist.append(call_df)

        put_df = ticker_obj.option_chain(expiry).puts
        put_df['ticker']=ticker
        put_df['option_type']='put'
        put_df['expiry']=expiry
        mylist.append(put_df)
    if len(mylist) == 0:
        return pd.DataFrame([])
    df = pd.concat(mylist)
    return df

BTC_TICKER = "BTC-USD"
INDEX_TICKER_LIST = ['SPY','QQQ','^SPX','^NDX']
BTC_TICKER_LIST = ['^CBTX','^MBTX','ARKB','GBTC','IBIT','MSTR']
# format
# ticker/date_stamp/options_time_stamp.json
# ticker/date_stamp/time_stamp/underlying.json
MYFOLDER = os.environ.get("MYFOLDER","tmp")
def main():
    now_utc = datetime.datetime.now(pytz.utc)
    eastern = pytz.timezone('US/Eastern')
    now_et = now_utc.astimezone(eastern)
    print(now_et)
    date_stamp = datetime.datetime.strftime(now_et,'%Y-%m-%d')
    time_stamp = datetime.datetime.strftime(now_et,'%Y-%m-%d-%H-%M-%S-%Z')
    ticker_list = [BTC_TICKER]
    ticker_list.extend(INDEX_TICKER_LIST)
    ticker_list.extend(BTC_TICKER_LIST)
    for ticker in ticker_list:
        cache_folder = os.path.join(MYFOLDER,ticker,date_stamp)
        os.makedirs(cache_folder,exist_ok=True)
        json_file = os.path.join(cache_folder,f"underlying-{ticker}-{time_stamp}.json")
        if not os.path.exists(json_file):
            ticker_obj = yf.Ticker(ticker,session=LimiterSession(per_second=3))
            info_dict = ticker_obj.info
            with open(json_file,'w') as f:
                f.write(json.dumps(info_dict))
            #price_df = ticker_obj.history(period='5d')
            #last_price = price_df['Close'][-1]
            #print(ticker,last_price)
        print("----")
    print("----")
    ticker_list = []
    ticker_list.extend(INDEX_TICKER_LIST)
    ticker_list.extend(BTC_TICKER_LIST)
    tickers = yf.Tickers(ticker_list,session=LimiterSession(per_second=3))
    for ticker in ticker_list:
        print(ticker)
        cache_folder = os.path.join(MYFOLDER,ticker,date_stamp)
        os.makedirs(cache_folder,exist_ok=True)
        ticker_obj = tickers.tickers[ticker]
        print('----')
        csv_file = os.path.join(cache_folder,f"options-{ticker}-{time_stamp}.json")
        if not os.path.exists(csv_file):
            df = get_option_chain(ticker,ticker_obj)
            df.to_csv(csv_file,index=False)

if __name__== "__main__":
    while True:
        main()
        print(datetime.datetime.now())
        print('sleeping...')
        time.sleep(60)