

CANDLE_QC_QUERY = """
(
SELECT DISTINCT event_symbol as ticker, null::date as expiration ,max(tstamp) as tstamp FROM candle 
WHERE event_symbol = %s AND tstamp > %s - interval '10 minute'
GROUP BY event_symbol
) union all (
SELECT DISTINCT ticker, expiration, max(tstamp) as tstamp FROM candle 
WHERE ticker = %s AND tstamp > %s - interval '10 minute' 
GROUP BY ticker, expiration
) 
"""

QUOTE_1MIN_QUERY = """
SELECT DISTINCT event_symbol,strike,contract_type,
last(last_bid_price,tstamp) as last_bid_price,last(last_ask_price,tstamp) as last_ask_price 
FROM quote_1min
WHERE expiration = %s AND ticker = %s AND tstamp > %s - interval '10 minute'
GROUP BY event_symbol,strike,contract_type
ORDER BY contract_type,strike
"""

ORDER_IMBALANCE_QUERY = """
select * from order_imbalance where tstamp::date = %s and ticker = %s
"""

ORDER_IMBALANCE_5MIN_QUERY = """
select * from candle_1min where ticker = %s and expiration = %s AND tstamp > %s - interval '30 minute'
"""

CONVEXITY_QUERY = """
WITH o_1day AS (
select distinct ticker,strike,sum(order_imbalance) as order_imbalance
from order_imbalance_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
), g_1day AS (
select distinct ticker,strike,last(gamma,tstamp) as gamma
from greeks_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
)
SELECT ticker,strike,gamma,order_imbalance from o_1day
LEFT JOIN g_1day using (ticker,strike)
ORDER BY strike
"""

CONVEXITYDX_QUERY = """
WITH o_1day AS (
select distinct ticker,strike,sum(order_imbalance) as order_imbalance
from order_imbalance_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
), g_1day AS (
select distinct ticker,strike,last(gamma,tstamp) as gamma
from greeksdx_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
)
SELECT ticker,strike,gamma,order_imbalance from o_1day
LEFT JOIN g_1day using (ticker,strike)
ORDER BY strike
"""

CONVEXITY_1HOUR_QUERY = """
WITH o_1hr AS (
select distinct ticker,strike,sum(order_imbalance) as order_imbalance
from order_imbalance where ticker = %s and expiration = %s and tstamp::date = %s
and tstamp > %s - interval '1 hour' and tstamp < %s
group by ticker,strike
), g_1day AS (
select distinct ticker,strike,last(gamma,tstamp) as gamma
from greeks_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
)
SELECT ticker,strike,gamma,order_imbalance from o_1hr
LEFT JOIN g_1day using (ticker,strike)
ORDER BY strike
"""

GREEKS_QUERY = """
select distinct event_symbol,ticker,expiration,strike,contract_type,last(gamma,tstamp) as gamma,last(volatility,tstamp) as volatility
from greeks_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by event_symbol,ticker,expiration,strike,contract_type
ORDER BY contract_type,strike
"""

ORDER_IMBALANCE_GEX_QUERY = """
WITH oi AS (
SELECT DISTINCT event_symbol,ticker,expiration,contract_type,strike, 
last(order_imbalance,tstamp) as order_imbalance
FROM order_imbalance_1day WHERE ticker = %s and expiration = %s and tstamp::date = %s
group by event_symbol,ticker,expiration,contract_type,strike
), grk as (
SELECT DISTINCT event_symbol,ticker,expiration,contract_type,strike,
last(gamma,tstamp) as gamma
FROM greeks_1day WHERE ticker = %s and expiration = %s and tstamp::date = %s
group by event_symbol,ticker,expiration,contract_type,strike
), qt as (
SELECT DISTINCT event_symbol,ticker,expiration,contract_type,strike,
last(last_bid_price,tstamp) as bid_price,last(last_ask_price,tstamp) as ask_price
FROM quote_1day WHERE ticker = %s and expiration = %s and tstamp::date = %s
group by event_symbol,ticker,expiration,contract_type,strike
), candle AS (
SELECT DISTINCT last(close,tstamp) as spot_price
FROM candle_1min WHERE event_symbol = %s and tstamp::date = %s
), vix AS (
SELECT DISTINCT last(close,tstamp) as spot_volatility
FROM candle_1min WHERE event_symbol = %s and tstamp::date = %s
) 
SELECT * FROM oi 
left join grk using (event_symbol,ticker,expiration,contract_type,strike)
left join qt using (event_symbol,ticker,expiration,contract_type,strike)
FULL JOIN candle ON true
FULL JOIN vix ON true
"""

CANDLE_1MIN_QUERY = """
WITH spx_1min AS (select tstamp,close as spx_close from candle_1min where tstamp::date = %s and event_symbol = 'SPX' and close != 0),
ndx_1min AS (select tstamp,close as ndx_close from candle_1min where tstamp::date = %s and event_symbol like 'NDX' and close != 0),
es_1min AS (select tstamp,close as es_close from candle_1min where tstamp::date = %s and event_symbol like '/ES%%' and close != 0),
vix_1min AS (select tstamp,close as vix_close from candle_1min where tstamp::date = %s and event_symbol = 'VIX' and close != 0),
vix1d_1min AS (select tstamp,close as vix1d_close from candle_1min where tstamp::date = %s and event_symbol = 'VIX1D' and close != 0)
SELECT * FROM spx_1min
LEFT JOIN es_1min using (tstamp)
LEFT JOIN ndx_1min using (tstamp)
LEFT JOIN vix_1min using (tstamp)
LEFT JOIN vix1d_1min using (tstamp)
ORDER BY tstamp
"""


LATEST_GEX_STRIKE_QUERY = """
WITH last_tstamp AS (select tstamp from gex_net where tstamp <= %s and tstamp >= %s - interval '1 minute' and ticker = %s order by tstamp desc limit 1),
last_gex_strike AS (select * from gex_strike where tstamp <= %s and tstamp >= %s - interval '1 minute' and ticker = %s order by tstamp,strike)
SELECT * FROM last_tstamp
LEFT JOIN last_gex_strike using (tstamp)
"""

GEX_CONVEXITY_QUERY = """
WITH price_sec AS (select tstamp,close as spx_close from candle_1min where event_symbol = %s and close != 0 and tstamp > %s - interval '30 minute' and tstamp <= %s),
event_sec AS (select tstamp,gex,convexity from event_underlying_1min where ticker = %s and tstamp > %s - interval '30 minute' and tstamp <= %s)
SELECT * FROM price_sec
LEFT JOIN event_sec using (tstamp)
ORDER BY tstamp
"""
