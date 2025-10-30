

EVENT_STATUS_QUERY = """
(select 'timeandsale' as event_type, count(timeandsale_id) as id_count, max(tstamp) as tstamp 
from timeandsale where tstamp > now() - interval '2 second' and expiration = now()::date
) union all (
select 'candle' as event_type, count(candle_id) as id_count, max(tstamp) as tstamp 
from candle where tstamp > now() - interval '3 second' and expiration = now()::date
) union all (
select 'quote' as event_type, count(quote_id) as id_count, max(tstamp) as tstamp 
from quote where tstamp > now() - interval '3 second' and expiration = now()::date
) union all (
select 'greeks' as event_type, count(greeks_id) as id_count, max(tstamp) as tstamp 
from greeks where tstamp > now() - interval '60 second' and expiration = now()::date
) union all (
select 'gex_net' as event_type, count(gex_net_id) as id_count, max(tstamp) as tstamp
from gex_net where tstamp > now() - interval '4 second'
) union all (
select 'gex_strike' as event_type, count(gex_strike_id) as id_count, max(tstamp) as tstamp
from gex_strike where tstamp > now() - interval '4 second'
)
"""

CANDLE_QC_QUERY = """
SELECT DISTINCT ticker, max(tstamp) as tstamp FROM candle 
WHERE expiration = %s AND ticker = %s AND tstamp > %s - interval '1 minute'
GROUP BY ticker
"""

QUOTE_1MIN_QUERY = """
SELECT DISTINCT event_symbol,strike,contract_type,
last(last_bid_price,tstamp) as last_bid_price,last(last_ask_price,tstamp) as last_ask_price 
FROM quote_1min
WHERE expiration = %s AND ticker = %s AND tstamp > %s - interval '1 minute'
GROUP BY event_symbol,strike,contract_type
ORDER BY contract_type,strike
"""

ORDER_IMBALANCE_QUERY = """
select * from order_imbalance where tstamp::date = %s and ticker = %s
"""

CANDLE_1MIN_QUERY = """
WITH spx_1min AS (select tstamp,close as spx_close from candle_1min where tstamp::date = %s and event_symbol = 'SPX' and close != 0),
es_1min AS (select tstamp,close as es_close from candle_1min where tstamp::date = %s and event_symbol like '/ES%%' and close != 0),
vix_1min AS (select tstamp,close as vix_close from candle_1min where tstamp::date = %s and event_symbol = 'VIX' and close != 0),
vix1d_1min AS (select tstamp,close as vix1d_close from candle_1min where tstamp::date = %s and event_symbol = 'VIX1D' and close != 0)
SELECT * FROM spx_1min
LEFT JOIN es_1min using (tstamp)
LEFT JOIN vix_1min using (tstamp)
LEFT JOIN vix1d_1min using (tstamp)
ORDER BY tstamp
"""


LATEST_GEX_STRIKE_QUERY = """
WITH last_tstamp AS (select tstamp from gex_net where tstamp <= %s and tstamp >= %s - interval '1 minute' and ticker = %s order by tstamp desc limit 1),
last_gex_strike AS (select * from gex_strike where tstamp <= %s and tstamp >= %s - interval '1 minute' and ticker = %s order by tstamp,strike),
last_price AS (select close from candle where tstamp <= %s and tstamp >= %s - interval '1 minute' and event_symbol = %s order by tstamp desc limit 1)
SELECT * FROM last_tstamp
LEFT JOIN last_gex_strike using (tstamp)
WHERE strike < (select close*1.02 from last_price)
AND strike > (select close*0.98 from last_price)
"""

LATEST_ONE_MIN_GEX_STRIKE_QUERY = """

WITH last_gex_strike AS (select * from gex_strike where tstamp <= %s and tstamp >= %s - interval '1 minute' and ticker = %s order by tstamp,strike),
last_price AS (select close from candle where tstamp <= %s and tstamp >= %s - interval '1 minute' and event_symbol = %s order by tstamp desc limit 1)
select * from last_gex_strike
WHERE strike < (select close*1.02 from last_price)
AND strike > (select close*0.98 from last_price)
"""

LATEST_DAY_GEX_NET_QUERY = """
WITH gex_net AS (select * from gex_net where tstamp::date = %s and tstamp >= %s - interval '15 minute' and ticker = %s order by tstamp),
vix_price AS (select tstamp::timestamp(0),close as vix_price from candle where tstamp::date = %s and tstamp >= %s - interval '15 minute' and event_symbol = 'VIX' and close != 0)
SELECT * FROM gex_net
LEFT JOIN vix_price using (tstamp)
ORDER BY tstamp
"""

GEX_NET_1MIN_QUERY = """
WITH gex_net_1min AS (select * from gex_net_1min where tstamp::date = %s and ticker = %s),
candle_1min AS (select tstamp, close as vix_price from candle_1min where tstamp::date = %s and event_symbol = 'VIX' and close != 0)
SELECT * FROM gex_net_1min
LEFT JOIN candle_1min using (tstamp)
ORDER BY tstamp
"""


"""
WITH last_tstamp AS (select tstamp from gex_net where tstamp <= timestamp '2025-06-20 19:59:58' and tstamp >=  timestamp '2025-06-20 19:59:58' - interval '1 minute' and ticker = 'SPX' order by tstamp desc limit 1),
last_gex_strike AS (select * from gex_strike where tstamp <= timestamp '2025-06-20 19:59:58' and tstamp >=  timestamp '2025-06-20 19:59:58' - interval '1 minute' and ticker = 'SPX' order by tstamp),
last_price AS (select close from candle where tstamp <= timestamp '2025-06-20 19:59:58' and tstamp >= timestamp '2025-06-20 19:59:58' - interval '1 minute' and event_symbol = 'SPX' order by tstamp desc limit 1)
SELECT * FROM last_tstamp
LEFT JOIN last_gex_strike using (tstamp)
WHERE strike < (select close*1.02 from last_price)
AND strike > (select close*0.98 from last_price)


explain analyze
WITH gex_net_1min AS (select * from gex_net where tstamp::date = '2025-06-20' and ticker = 'SPX'),
candle_1min AS (select tstamp, close as vix_price from candle_1min where tstamp::date = '2025-06-20' and event_symbol = 'VIX' and close != 0)
SELECT * FROM gex_net
LEFT JOIN candle_1min using (tstamp)
ORDER BY tstamp

"""

