

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
last(close_bid_price,tstamp) as last_bid_price,last(close_ask_price,tstamp) as last_ask_price
FROM quote_1min
WHERE expiration = %s AND ticker = %s AND tstamp > %s - interval '10 minute'
GROUP BY event_symbol,strike,contract_type
ORDER BY contract_type,strike
"""

ORDER_IMBALANCE_QUERY = """
select * from candle_5min where tstamp::date = %s and ticker = %s
"""

ORDER_IMBALANCE_LASTXMIN_QUERY = """
select tstamp,event_symbol,strike,contract_type,(ask_volume-bid_volume) as order_imbalance from candle 
where ticker = %s and expiration = %s AND tstamp > %s - interval '5 minute'
"""

# maybe this is wrong does not match with gexbot. end of day convexity is always extremely negative.
# while that could be fine for gex???? 
CONVEXITY_QUERY = """
WITH v_1day AS (
select distinct ticker,strike,sum(order_imbalance) as order_imbalance
from candle_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
), g_1day AS (
select distinct ticker,strike,last(gamma,tstamp) as gamma
from greeks_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
)
SELECT ticker,strike,gamma,order_imbalance from v_1day
LEFT JOIN g_1day using (ticker,strike)
ORDER BY strike
"""

# CONVEXITYDX_QUERY does not match gexbot. maybe they are doing rolling 1 hr?
CONVEXITYDX_QUERY = """
WITH v_1day AS (
select distinct ticker,strike,sum(order_imbalance) as order_imbalance
from candle_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
), g_1day AS (
select distinct ticker,strike,last(gamma,tstamp) as gamma
from greeksdx_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
)
SELECT ticker,strike,gamma,order_imbalance from v_1day
LEFT JOIN g_1day using (ticker,strike)
ORDER BY strike
"""

INTERVAL_CONVEXITY_QUERY = """
WITH o_interval AS (
select distinct ticker,strike,sum(order_imbalance) as order_imbalance
from candle_5min where ticker = %s and expiration = %s and tstamp::date = %s and tstamp >= %s - interval '15 minute'
group by ticker,strike
), g_1day AS (
select distinct ticker,strike,last(gamma,tstamp) as gamma
from greeksdx_1day where ticker = %s and expiration = %s and tstamp::date = %s
group by ticker,strike
)
SELECT ticker,strike,gamma,order_imbalance from o_interval
LEFT JOIN g_1day using (ticker,strike)
ORDER BY strike
"""

# not used, unsure if order imablance 1day or 1hr is preferred, maybe both?
CONVEXITY_1HOUR_QUERY = """
WITH o_1hr AS (
select distinct ticker,strike,sum(order_imbalance) as order_imbalance
from candle_5min where ticker = %s and expiration = %s and tstamp::date = %s
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
select distinct event_symbol,ticker,expiration,strike,contract_type,
last(greeks_1day.gamma,tstamp) as gamma,last(greeks_1day.volatility,tstamp) as volatility,
last(greeksdx_1day.gamma,tstamp) as dx_gamma,last(greeksdx_1day.volatility,tstamp) as dx_volatility
from greeks_1day 
left join greeksdx_1day using (event_symbol,ticker,expiration,strike,contract_type,tstamp)
where ticker = %s and expiration = %s and tstamp::date = %s
group by event_symbol,ticker,expiration,strike,contract_type
ORDER BY contract_type,strike
"""

ORDER_IMBALANCE_GEX_QUERY = """
WITH oi AS (
SELECT DISTINCT event_symbol,ticker,expiration,contract_type,strike, 
last(order_imbalance,tstamp) as order_imbalance
FROM candle_1day WHERE ticker = %s and expiration = %s and tstamp::date = %s
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
ORDER BY contract_type,strike
"""

CANDLE_1MIN_QUERY = """
WITH spx_1min AS (select tstamp,close as spx_close from candle_1min where tstamp::date = %s and event_symbol = 'SPX' and close != 0),
ndx_1min AS (select tstamp,close as ndx_close from candle_1min where tstamp::date = %s and event_symbol like 'NDX' and close != 0),
es_1min AS (select tstamp,close as es_close from candle_1min where tstamp::date = %s and event_symbol like '/ES%%' and close != 0),
vix_1min AS (select tstamp,close as vix_close from candle_1min where tstamp::date = %s and event_symbol = 'VIX' and close != 0),
vix1d_1min AS (select tstamp,close as vix1d_close from candle_1min where tstamp::date = %s and event_symbol = 'VIX1D' and close != 0),
vix9d_1min AS (select tstamp,close as vix9d_close from candle_1min where tstamp::date = %s and event_symbol = 'VIX9D' and close != 0),
spy_1min AS (select tstamp,close as spy_close from candle_1min where tstamp::date = %s and event_symbol = 'SPY' and close != 0),
qqq_1min AS (select tstamp,close as qqq_close from candle_1min where tstamp::date = %s and event_symbol = 'QQQ' and close != 0)
SELECT * FROM spx_1min
LEFT JOIN es_1min using (tstamp)
LEFT JOIN ndx_1min using (tstamp)
LEFT JOIN spy_1min using (tstamp)
LEFT JOIN qqq_1min using (tstamp)
LEFT JOIN vix_1min using (tstamp)
LEFT JOIN vix9d_1min using (tstamp)
LEFT JOIN vix1d_1min using (tstamp)
ORDER BY tstamp
"""

CANDLE_1MIN_SINGLE_QUERY = """
WITH ticker_1min AS (
    SELECT tstamp, close AS ticker_close
    FROM candle_1min
    WHERE tstamp::date = %s AND event_symbol = %s AND close != 0
),
companion_1min AS (
    SELECT tstamp, close AS companion_close
    FROM candle_1min
    WHERE tstamp::date = %s AND event_symbol = %s AND close != 0
)
SELECT * FROM ticker_1min
LEFT JOIN companion_1min USING (tstamp)
ORDER BY tstamp
"""

CANDLE_1MIN_PRICE_QUERY = """
WITH ticker_1min AS (
    SELECT tstamp, close AS ticker_close
    FROM candle_1min WHERE tstamp::date = %s AND event_symbol = %s AND close != 0
),
vix_1min AS (
    SELECT tstamp, close AS vix_close
    FROM candle_1min WHERE tstamp::date = %s AND event_symbol = 'VIX' AND close != 0
),
vix1d_1min AS (
    SELECT tstamp, close AS vix1d_close
    FROM candle_1min WHERE tstamp::date = %s AND event_symbol = 'VIX1D' AND close != 0
),
vix9d_1min AS (
    SELECT tstamp, close AS vix9d_close
    FROM candle_1min WHERE tstamp::date = %s AND event_symbol = 'VIX9D' AND close != 0
)
SELECT * FROM ticker_1min
LEFT JOIN vix_1min USING (tstamp)
LEFT JOIN vix1d_1min USING (tstamp)
LEFT JOIN vix9d_1min USING (tstamp)
ORDER BY tstamp
"""

LATEST_GEX_STRIKE_QUERY = """
WITH last_tstamp AS (select tstamp from event_underlying where tstamp <= %s and tstamp >= %s - interval '1 minute' and ticker = %s order by tstamp desc limit 1),
last_gex_strike AS (select * from event_strike where tstamp <= %s and tstamp >= %s - interval '1 minute' and ticker = %s order by tstamp,strike)
SELECT * FROM last_tstamp
LEFT JOIN last_gex_strike using (tstamp)
"""

GEX_CONVEXITY_LASTXMIN_QUERY = """
select tstamp,spot_price,gex,convexity,dex,call_dex,put_dex 
from event_underlying where ticker = %s and tstamp > %s - interval '10 minute' and tstamp <= %s
ORDER BY tstamp
"""

GEX_CONVEXITY_1DAY_QUERY = """
select tstamp,spot_price,gex,convexity,dex,call_dex,put_dex from event_underlying_1min 
where ticker = %s and tstamp::date = %s
ORDER BY tstamp
"""

"""
SELECT tstamp,close
FROM candle_1sec
WHERE tstamp >= '2026-04-17 19:59:00'::timestamp AND tstamp < '2026-04-17 20:00:00'::timestamp
AND event_symbol = 'SPX' 
ORDER BY tstamp


SELECT 
    time_bucket_gapfill('1 sec', tstamp, '2026-04-17 19:59:00', '2026-04-17 20:00:00') AS bucket,
    LOCF(avg(close)) AS close
FROM candle_1sec
WHERE tstamp >= '2026-04-17 19:59:00'::timestamp AND tstamp < '2026-04-17 20:00:00'::timestamp
AND event_symbol = 'SPX' 
GROUP BY bucket
ORDER BY bucket


WITH price AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, '2026-04-17 20:00:00'::timestamp - interval '15 minute' , '2026-04-17 20:00:00'::timestamp) AS bucket,
    LOCF(avg(close)) AS ticker_close
    FROM candle_1sec WHERE tstamp <= '2026-04-17 20:00:00'::timestamp AND tstamp >= '2026-04-17 20:00:00'::timestamp - interval '15 minute'
    AND event_symbol = 'SPX' AND close != 0
    GROUP BY bucket ORDER BY bucket
),
em AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, '2026-04-17 20:00:00'::timestamp - interval '15 minute' , '2026-04-17 20:00:00'::timestamp) AS bucket,
    LOCF(avg(expected_move)) AS expected_move
    FROM event_underlying_1sec WHERE tstamp <= '2026-04-17 20:00:00'::timestamp AND tstamp >= '2026-04-17 20:00:00'::timestamp- interval '15 minute' 
    AND ticker = 'SPX'
    GROUP BY bucket ORDER BY bucket
),
vix AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, '2026-04-17 20:00:00'::timestamp - interval '15 minute' , '2026-04-17 20:00:00'::timestamp) AS bucket,
    LOCF(avg(close)) AS vix_close
    FROM candle_1sec WHERE tstamp <= '2026-04-17 20:00:00'::timestamp AND tstamp >= '2026-04-17 20:00:00'::timestamp- interval '15 minute' 
    AND event_symbol = 'VIX' AND close != 0
    GROUP BY bucket ORDER BY bucket
),
vix1d AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, '2026-04-17 20:00:00'::timestamp - interval '15 minute' , '2026-04-17 20:00:00'::timestamp) AS bucket,
    LOCF(avg(close)) AS vix1d_close
    FROM candle_1sec WHERE tstamp <= '2026-04-17 20:00:00'::timestamp AND tstamp >= '2026-04-17 20:00:00'::timestamp - interval '15 minute' 
    AND event_symbol = 'VIX1D' AND close != 0
    GROUP BY bucket ORDER BY bucket
),
vix9d AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, '2026-04-17 20:00:00'::timestamp - interval '15 minute' , '2026-04-17 20:00:00'::timestamp) AS bucket,
    LOCF(avg(close)) AS vix9d_close
    FROM candle_1sec WHERE tstamp <= '2026-04-17 20:00:00'::timestamp AND tstamp >= '2026-04-17 20:00:00'::timestamp - interval '15 minute' 
    AND event_symbol = 'VIX9D' AND close != 0
    GROUP BY bucket ORDER BY bucket
)
SELECT bucket as tstamp, *
FROM price
LEFT JOIN vix USING (bucket)
LEFT JOIN vix1d USING (bucket)
LEFT JOIN vix9d USING (bucket)
LEFT JOIN em USING (bucket)
ORDER BY bucket

"""



PRICE_1SEC_QUERY = """
WITH price AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, %(starttime)s::timestamp, %(endtime)s::timestamp) AS bucket,
    LOCF(avg(close)) AS ticker_close
    FROM candle_1sec
    WHERE tstamp <= %(endtime)s::timestamp AND tstamp >= %(starttime)s::timestamp
    AND event_symbol = %(ticker)s AND close != 0
    GROUP BY bucket ORDER BY bucket
),
em AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, %(starttime)s::timestamp, %(endtime)s::timestamp) AS bucket,
    LOCF(avg(expected_move)) AS expected_move
    FROM event_underlying_1sec
    WHERE tstamp <= %(endtime)s::timestamp AND tstamp >= %(starttime)s::timestamp
    AND ticker = %(ticker)s
    GROUP BY bucket ORDER BY bucket
),
vix AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, %(starttime)s::timestamp, %(endtime)s::timestamp) AS bucket,
    LOCF(avg(close)) AS vix_close
    FROM candle_1sec
    WHERE tstamp <= %(endtime)s::timestamp AND tstamp >= %(starttime)s::timestamp
    AND event_symbol = 'VIX' AND close != 0
    GROUP BY bucket ORDER BY bucket
),
vix1d AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, %(starttime)s::timestamp, %(endtime)s::timestamp) AS bucket,
    LOCF(avg(close)) AS vix1d_close
    FROM candle_1sec
    WHERE tstamp <= %(endtime)s::timestamp AND tstamp >= %(starttime)s::timestamp
    AND event_symbol = 'VIX1D' AND close != 0
    GROUP BY bucket ORDER BY bucket
),
vix9d AS (
    SELECT time_bucket_gapfill('1 sec', tstamp, %(starttime)s::timestamp, %(endtime)s::timestamp) AS bucket,
    LOCF(avg(close)) AS vix9d_close
    FROM candle_1sec
    WHERE tstamp <= %(endtime)s::timestamp AND tstamp >= %(starttime)s::timestamp
    AND event_symbol = 'VIX9D' AND close != 0
    GROUP BY bucket ORDER BY bucket
)

SELECT bucket as tstamp, * FROM price
LEFT JOIN vix USING (bucket)
LEFT JOIN vix1d USING (bucket)
LEFT JOIN vix9d USING (bucket)
LEFT JOIN em USING (bucket)
ORDER BY bucket

"""

VOLUME_1SEC_QUERY = """
WITH volume AS (
SELECT time_bucket_gapfill('1 sec', tstamp, %(starttime)s::timestamp, %(endtime)s::timestamp) AS bucket,
strike,avg(volume) as volume FROM volume_1sec
WHERE tstamp <= %(endtime)s::timestamp AND tstamp >= %(starttime)s::timestamp
AND ticker = %(ticker)s
GROUP BY bucket, strike
ORDER BY bucket, strike
)
SELECT bucket as tstamp, * from volume
ORDER BY bucket
"""


PRICE_1MIN_QUERY = """
WITH price AS (
    SELECT tstamp, close AS ticker_close
    FROM candle_1min WHERE tstamp::date = %s AND event_symbol = %s AND close != 0
),
em AS (
    SELECT tstamp, expected_move
    FROM event_underlying_1min WHERE tstamp::date = %s AND ticker = %s 
),
vix AS (
    SELECT tstamp, close AS vix_close
    FROM candle_1min WHERE tstamp::date = %s AND event_symbol = 'VIX' AND close != 0
),
vix1d AS (
    SELECT tstamp, close AS vix1d_close
    FROM candle_1min WHERE tstamp::date = %s AND event_symbol = 'VIX1D' AND close != 0
),
vix9d AS (
    SELECT tstamp, close AS vix9d_close
    FROM candle_1min WHERE tstamp::date = %s AND event_symbol = 'VIX9D' AND close != 0
)
SELECT * FROM price
LEFT JOIN vix USING (tstamp)
LEFT JOIN vix1d USING (tstamp)
LEFT JOIN vix9d USING (tstamp)
LEFT JOIN em USING (tstamp)
ORDER BY tstamp
"""

VOLUME_1MIN_QUERY = """
SELECT * FROM volume_1min
WHERE tstamp::date = %s AND ticker = %s
ORDER BY tstamp, strike
"""

PRICE_5MIN_QUERY = """
WITH price AS (
    SELECT tstamp, close AS ticker_close
    FROM candle_5min WHERE tstamp::date = %s AND event_symbol = %s AND close != 0
),
em AS (
    SELECT tstamp, expected_move
    FROM event_underlying_5min WHERE tstamp::date = %s AND ticker = %s 
),
vix AS (
    SELECT tstamp, close AS vix_close
    FROM candle_5min WHERE tstamp::date = %s AND event_symbol = 'VIX' AND close != 0
),
vix1d AS (
    SELECT tstamp, close AS vix1d_close
    FROM candle_5min WHERE tstamp::date = %s AND event_symbol = 'VIX1D' AND close != 0
),
vix9d AS (
    SELECT tstamp, close AS vix9d_close
    FROM candle_5min WHERE tstamp::date = %s AND event_symbol = 'VIX9D' AND close != 0
)
SELECT * FROM price
LEFT JOIN vix USING (tstamp)
LEFT JOIN vix1d USING (tstamp)
LEFT JOIN vix9d USING (tstamp)
LEFT JOIN em USING (tstamp)
ORDER BY tstamp
"""

VOLUME_5MIN_QUERY = """
SELECT * FROM volume_5min
WHERE tstamp::date = %s AND ticker = %s
ORDER BY tstamp, strike
"""

CONTRACT_VOLUME_1MIN_QUERY = """
with foo as (
select tstamp,event_symbol,strike,contract_type,volume,open,high,low,close, 'SPX' as ticker
from candle_1min 
where (bid_volume+ask_volume) > 1000 
and tstamp::date = %s
and ticker = %s),
bar as (
select * from event_underlying_1min
where tstamp::date  = %s
and ticker = %s)
select tstamp,event_symbol,strike,contract_type,volume,open,high,low,close,spot_price,expected_move from foo
left join bar using (tstamp,ticker)
order by tstamp desc,event_symbol
"""