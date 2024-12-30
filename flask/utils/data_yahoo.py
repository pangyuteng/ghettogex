import sys
import json
import pandas as pd
import yfinance as yf
from requests_ratelimiter import LimiterSession


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

def test_yahoo_scrape(ticker):
    ticker_obj = yf.Ticker(ticker,session=LimiterSession(per_second=5))
    info_dict = ticker_obj.get_info()
    df = get_option_chain(ticker,ticker_obj)
    return info_dict,df 

if __name__== "__main__":
    ticker = sys.argv[1]
    info_dict, df  = test_yahoo_scrape(ticker)
    with open(f"ok-{ticker}.json","w") as f:
        f.write(json.dumps(info_dict))
    df.to_csv(f"ok-{ticker}.csv",index=False)

"""

python3 -m utils.data_yahoo SPY

"""