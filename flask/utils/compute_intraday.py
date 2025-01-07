import logging
logger = logging.getLogger(__file__)
import os
import sys
import traceback
import pytz
import datetime
import pandas as pd
from .postgres_utils import postgres_execute
from .data_tasty import background_subscribe, is_market_open, now_in_new_york
from .misc import timedelta_from_market_open


def compute_gex(ticker,et_tstamp,persist_to_postgres=True):
    utc_tstamp = et_tstamp.astimezone(tz="UTC")
    max_utc_tstamp = utc_tstamp+datetime.timedelta(seconds=1)

    delta, market_open_tstamp_et = timedelta_from_market_open(et_tstamp)
    if delta < datetime.timedelta(minutes=1):
        first_minute = True
    else:
        first_minute = False

    # the first minute, grab everything
    if first_minute:
        query_str = """
        (select 'underlying_candle' as event_type,event_symbol,close as spot_price,null::float as close,null::int as open_interest,null::float as gamma,null::int as size,null as aggressor_side,tstamp,null as ticker,null as expiration,null as contract_type,null as strike from candle
        where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
        ) union all (
        select 'candle' as event_type,event_symbol,null::float as spot_price,close,null::int as open_interest,null::float as gamma,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from candle
        where tstamp >= %s and tstamp < %s and event_symbol like '.'||%s
        ) union all (
        select 'summary' as event_type,event_symbol,null::float as spot_price,null::float as close,open_interest,null::float as gamma,null::int as size,null as aggressor_side,tstamp ,ticker,expiration,contract_type,strike from summary
        where tstamp >= %s and tstamp < %s and event_symbol like '.'||%s||'%%'
        ) union all (
        select 'greeks' as event_type,event_symbol,null::float as spot_price,null::float as close,null::int as open_interest, gamma,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from greeks
        where tstamp >= %s and tstamp < %s and event_symbol like '.'||%s||'%%'
        ) union all (
        select 'timeandsale' as event_type,event_symbol,null::float as spot_price,null::float as close,null::int as open_interest, null::float as gamma,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
        where tstamp >= %s and tstamp < %s and event_symbol like '.'||%s||'%%'
        )
        """

        query_args = (
            market_open_tstamp_et,max_utc_tstamp,ticker,
            market_open_tstamp_et,max_utc_tstamp,ticker,
            market_open_tstamp_et,max_utc_tstamp,ticker,
            market_open_tstamp_et,max_utc_tstamp,ticker,
            market_open_tstamp_et,max_utc_tstamp,ticker,
        )

        fetched = postgres_execute(query_str,query_args)
        columns = ['event_type','event_symbol','spot_price','open_interest','gamma','size','aggressor_side','ticker','expiration','contract_type','strike','tstamp']
        if fetched is None:
            df = pd.DataFrame([],columns=columns)
        else:
            df = pd.DataFrame(fetched,columns=columns)
            df = df.sort_values(['event_type','tstamp'])
            df.to_csv(f"tmp/fetched-{et_tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.csv",index=False)
        print(first_minute,len(df))
        if len(df) == 0:
            return None

        #spot_price = 

    else:
        query_str = """
        (select 'underlying_candle' as event_type,event_symbol,close as spot_price,null::float as close,null::int as open_interest,null::float as gamma,null::int as size,null as aggressor_side,tstamp,null as ticker,null as expiration,null as contract_type,null as strike from candle
        where tstamp >= %s and tstamp < %s and event_symbol = %s
        ) union all (
        select 'candle' as event_type,event_symbol,null::float as spot_price,close,null::int as open_interest,null::float as gamma,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from candle
        where tstamp >= %s and tstamp < %s and event_symbol like '.'||%s||'%%'
        ) union all (
        select 'summary' as event_type,event_symbol,null::float as spot_price,null::float as close,open_interest,null::float as gamma,null::int as size,null as aggressor_side,tstamp ,ticker,expiration,contract_type,strike from summary
        where tstamp >= %s and tstamp < %s and event_symbol like '.'||%s||'%%'
        ) union all (
        select 'greeks' as event_type,event_symbol,null::float as spot_price,null::float as close,null::int as open_interest, gamma,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from greeks
        where tstamp >= %s and tstamp < %s and event_symbol like '.'||%s||'%%'
        ) union all (
        select 'timeandsale' as event_type,event_symbol,null::float as spot_price,null::float as close,null::int as open_interest, null::float as gamma,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
        where tstamp >= %s and tstamp < %s and event_symbol like '.'||%s||'%%'
        )
        """

        query_args = (
            utc_tstamp,max_utc_tstamp,ticker,
            utc_tstamp,max_utc_tstamp,ticker,
            utc_tstamp,max_utc_tstamp,ticker,
            utc_tstamp,max_utc_tstamp,ticker,
            utc_tstamp,max_utc_tstamp,ticker,
        )

        fetched = postgres_execute(query_str,query_args)
        columns = ['event_type','event_symbol','spot_price','close','open_interest','gamma','size','aggressor_side','ticker','expiration','contract_type','strike','tstamp']
        if fetched is None:
            df = pd.DataFrame([],columns=columns)
        else:
            df = pd.DataFrame(fetched,columns=columns)
            df = df.sort_values(['event_type','tstamp'])
            df.to_csv(f"tmp/fetched-{et_tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.csv",index=False)
        print(first_minute,len(df))
        if len(df) == 0:
            return None

    # observations
    # + greeks needs to be updated if no greeks and options candle exists
    # + spot needs to be updated if candle, and you got underlying quotes.
    # + summary event seems to be only once a day?
    
def mainone(ticker,tstamp_str):
    eastern = pytz.timezone('US/Eastern')
    tstamp = datetime.datetime.strptime(tstamp_str,"%Y-%m-%d-%H-%M-%S")
    tstamp = tstamp.replace(tzinfo=eastern)
    compute_gex(ticker,tstamp)

def main(ticker):
    #tstamp_list = pd.date_range(start="2025-01-07 09:30:00",end="2025-01-07 09:31:30",freq='s',tz=pytz.timezone('US/Eastern'))
    tstamp_list = pd.date_range(start="2025-01-07 09:30:00",end="2025-01-07 14:50:30",freq='s',tz=pytz.timezone('US/Eastern'))
    for tstamp in tstamp_list:
        try:
            compute_gex(ticker,tstamp)
        except KeyboardInterrupt:
            sys.exit(1)
        except:
            traceback.print_exc()
            pass

if __name__ == "__main__":
    ticker = sys.argv[1]
    tstamp_str = sys.argv[2]
    #mainone(ticker,tstamp_str)
    main(ticker)

"""
select * from candle 
where event_symbol = 'SPX'
order by tstamp asc limit 10

select * from candle 
where event_symbol = 'SPX'
order by tstamp desc limit 10

SELECT * from candle
WHERE event_symbol = 'SPX'
and tstamp >= '2025-01-07 14:31:00' and tstamp < '2025-01-07 14:32:00'
-- 66 rows


kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-554bc784bf-xrgkg 5432:5432

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.149:5432/postgres

python -m utils.compute_intraday SPX 2025-01-06-16-45-03
python -m utils.compute_intraday SPX 2025-01-06-20-59-57

"""