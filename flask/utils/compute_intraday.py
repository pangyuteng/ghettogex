import logging
logger = logging.getLogger(__file__)
import os
import sys
import traceback
import datetime
import pandas as pd
from .postgres_utils import postgres_execute
from .data_tasty import background_subscribe, is_market_open, now_in_new_york

def compute_gex(ticker,tstamp,persist_to_postgres=True):
    
    mydict = {}
    table_list = [('candle','event_symbol'),('candle','ticker'),('summary','ticker'),('greeks','ticker'),('timeandsale','ticker')]
    for table_name, col_name in table_list:
        if col_name == 'ticker' and ticker in ['SPX']:
            tmpticker = ticker+"W"
        else:
            tmpticker = ticker
        fetched = postgres_execute("select * from "+table_name+" where "+col_name+" = %s and tstamp >= %s and tstamp < %s + interval '1 second' ",(tmpticker,tstamp,tstamp))
        if fetched is None:
            fetched = []
        mydict[table_name]=len(fetched)

    print(ticker,tstamp,mydict)

def mainone(ticker,tstamp):
    tstamp = datetime.datetime.strptime(tstamp_str,"%Y-%m-%d-%H-%M-%S")
    compute_gex(ticker,tstamp)

def main(ticker):
    tstamp_list = pd.date_range(start="2025-01-06 16:45:03",end="2025-01-06 20:59:57",freq='s')
    for tstamp in tstamp_list:
        compute_gex(ticker,tstamp)

if __name__ == "__main__":
    ticker = sys.argv[1]
    tstamp_str = sys.argv[2]
    mainone(ticker,tstamp_str)
    main(ticker)

"""
select * from candle 
where event_symbol = 'SPX'
order by tstamp asc limit 10

select * from candle 
where event_symbol = 'SPX'
order by tstamp desc limit 10

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-554bc784bf-xrgkg 5432:5432

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.149:5432/postgres

python -m utils.compute_intraday SPX 2025-01-06-16-45-03
python -m utils.compute_intraday SPX 2025-01-06-20-59-57

"""