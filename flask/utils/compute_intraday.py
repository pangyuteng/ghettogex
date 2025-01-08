import logging
logger = logging.getLogger(__file__)
import os
import sys
import traceback
import pytz
import datetime
import numpy as np
import pandas as pd
from .postgres_utils import postgres_execute
from .data_tasty import background_subscribe, is_market_open, now_in_new_york
from .misc import timedelta_from_market_open


def compute_gex(ticker,et_tstamp,persist_to_postgres=True):
    
    csv_file = f"tmp/fetched-{et_tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.csv"

    utc_tstamp = et_tstamp.astimezone(tz="UTC")
    max_utc_tstamp = utc_tstamp+datetime.timedelta(seconds=1)
    
    delta, market_open_tstamp_et = timedelta_from_market_open(et_tstamp)
    if delta < datetime.timedelta(minutes=1):
        first_minute = True
    else:
        first_minute = False

    # the first minute, grab everything
    columns = [
        'event_type','event_symbol',
        'close','spot_price',
        'open_interest','gamma',
        'size','aggressor_side','ticker','expiration','contract_type','strike','tstamp',
    ]
    if first_minute:
        query_str = """
        (select 'underlying_candle' as event_type,event_symbol,close as spot_price,null::float as close,null::int as open_interest,null::float as gamma,null::int as size,null as aggressor_side,tstamp,null as ticker,null as expiration,null as contract_type,null as strike from candle
        where tstamp >= %s and tstamp < %s and event_symbol = %s and ticker is null
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
            market_open_tstamp_et,max_utc_tstamp,ticker,
            market_open_tstamp_et,max_utc_tstamp,ticker,
            market_open_tstamp_et,max_utc_tstamp,ticker,
            market_open_tstamp_et,max_utc_tstamp,ticker,
            market_open_tstamp_et,max_utc_tstamp,ticker,
        )

        fetched = postgres_execute(query_str,query_args)
        if fetched is None:
            df = pd.DataFrame([],columns=columns)
        else:
            df = pd.DataFrame(fetched,columns=columns)
        print(first_minute,et_tstamp,len(df))
        if len(df) == 0:
            return None

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
        if fetched is None:
            df = pd.DataFrame([],columns=columns)
        else:
            df = pd.DataFrame(fetched,columns=columns)
        print(first_minute,et_tstamp,len(df))
        if len(df) == 0:
            return None

    #if first_minute:
    
    df = df.sort_values(['event_type','tstamp'])
    # 'ticker','expiration','contract_type','strike','tstamp'
    underlying_candle_df = df[df.event_type=='underlying_candle']
    print(underlying_candle_df.shape)
    
    if len(underlying_candle_df)>0:
        print(csv_file)

    df.to_csv(csv_file,index=False)
    if first_minute:
        hola(df=df)
    else:
        raise NotImplementedError()

    return df


    # df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    # df['bid_volume'] = pd.to_numeric(df['bid_volume'], errors='coerce')
    # df['ask_volume'] = pd.to_numeric(df['ask_volume'], errors='coerce')
    # df['volatility'] = pd.to_numeric(df['volatility'], errors='coerce')
    # df['delta'] = pd.to_numeric(df['delta'], errors='coerce')
    # df['theta'] = pd.to_numeric(df['theta'], errors='coerce')
    # df['rho'] = pd.to_numeric(df['rho'], errors='coerce')
    # df['vega'] = pd.to_numeric(df['vega'], errors='coerce')
    # 'volume','bid_volume','ask_volume',
    # 'price','volatility','delta',,'theta','rho','vega',


    # aggressor_side_int =
    # latest_open_interest = 
    #event_type,event_symbol,close,spot_price,open_interest,gamma,size,aggressor_side,ticker,expiration,contract_type,strike,tstamp
    # grab last close,spot_price, 
    # observations
    # + greeks needs to be updated if no greeks and options candle exists
    # + spot needs to be updated if candle, and you got underlying quotes.
    # + summary event seems to be only once a day?

def hola(df=None):
    if df is None:
        df = pd.read_csv("tmp/fetched-2025-01-07-09-30-32.csv")
        df['spot_price'] = pd.to_numeric(df['spot_price'], errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce')
        df['gamma'] = pd.to_numeric(df['gamma'], errors='coerce')
        df['size'] = pd.to_numeric(df['size'], errors='coerce')
        df['size_signed'] = df['size'].where(df.aggressor_side == 'BUY', other=-1*df['size'])
        df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
        df = df.sort_values(['event_type','tstamp'])

    df['size_signed'] = df['size'].where(df.aggressor_side == 'BUY', other=-1*df['size'])
    underlying_candle_df = df[df.event_type=='underlying_candle']
    print('underlying_candle',underlying_candle_df.shape)

    first_minute = True
    if first_minute:
        if len(underlying_candle_df)>0:
            underlying_candle_df = df[df.event_type=='underlying_candle']
            spot_price = underlying_candle_df.spot_price.to_list()[-1]
        else:
            spot_price = np.nan

        
        candle_df = df[df.event_type=='candle']
        print('candle',candle_df.shape)
        print(len(candle_df.event_symbol.unique()))
        
        summary_df = df[df.event_type=='summary']
        print('summary',summary_df.shape)
        print(len(summary_df.event_symbol.unique()))
        
        greeks_df = df[df.event_type=='greeks']
        print('greeks',greeks_df.shape)
        print(len(greeks_df.event_symbol.unique()))

        timeandsale_df = df[df.event_type=='timeandsale']
        print('timeandsale',timeandsale_df.shape)
        print(len(timeandsale_df.event_symbol.unique()))

        oi_df = summary_df[['event_symbol','ticker','strike','contract_type','expiration','open_interest']]
        oi_df = oi_df.groupby(['event_symbol','ticker','strike','contract_type','expiration']).last().reset_index()
        print(oi_df.columns)

        greeks_df = greeks_df[['event_symbol','gamma']]
        greeks_df = greeks_df.groupby(['event_symbol']).last().reset_index()
        print(greeks_df.columns)

        size_df = timeandsale_df[['event_symbol','size_signed']]
        size_df = size_df.groupby(['event_symbol']).sum().reset_index()
        print(size_df.columns)

        
        print('spot_price',spot_price)
        merged_df = oi_df.merge(greeks_df,how='left',on=['event_symbol'])
        merged_df = merged_df.merge(size_df,how='left',on=['event_symbol'])
        merged_df['contract_type_int'] = merged_df.contract_type.apply(lambda x: -1 if x == 'P' else 1)
        merged_df['spot_price']=spot_price
        merged_df['open_interest']=merged_df.open_interest+merged_df.size_signed

        merged_df['gex'] = merged_df.gamma * merged_df.open_interest * 100 * merged_df.spot_price * merged_df.spot_price * 0.01 * merged_df.contract_type_int / 1e9
        print(len(merged_df),merged_df.gex.sum())
    else:
        raise NotImplementedError()

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
    #hola()

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