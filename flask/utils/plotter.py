
import pathlib
import datetime
from .data_yahoo import (
    CACHE_FOLDER,
    BTC_TICKER,
    INDEX_TICKER_LIST,
    BTC_TICKER_LIST,
    get_cache_latest
)

def get_last_trading_dateime():
    return datetime.datetime(2024,12,23)


tstamp = get_last_trading_dateime()
ticker = 'SPY'
underlying_dict,options_df,last_json_file,csv_json_file = get_cache_latest(ticker,tstamp)
print(underlying_dict,options_df)
print(last_json_file,csv_json_file)

"""

python -m utils.plotter

"""