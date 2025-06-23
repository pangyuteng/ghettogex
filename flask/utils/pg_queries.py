

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
vix_price AS (select tstamp::timestamp(0),close as vix_price from candle where tstamp::date = %s and tstamp >= %s - interval '15 minute' and event_symbol = 'VIX')
SELECT * FROM gex_net
LEFT JOIN vix_price using (tstamp)
ORDER BY tstamp
"""

GEX_NET_1MIN_QUERY = """
SELECT * FROM get_net_1min
where tstamp::date = %s and ticker = %s
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


WITH gex_net AS (select * from gex_net where tstamp::date = '2025-06-20' and ticker = 'SPX'),
vix_price AS (select tstamp::timestamp(0),close as vix_price from candle where tstamp::date = '2025-06-20' and event_symbol = 'VIX')
SELECT * FROM gex_net
LEFT JOIN vix_price using (tstamp)
ORDER BY tstamp
"""

"""
                SELECT DISTINCT ON (ticker,date_trunc('minute', tstamp),strike) 
                date_trunc('minute', tstamp) AS tstamp, ticker, strike,
                AVG(volume_gex) as volume_gex, AVG(state_gex) as state_gex,AVG(dex) as dex,
                AVG(convexity) as convexity, AVG(vex) as vex,AVG(cex) as cex
                FROM gex_strike 
                WHERE ticker = %s and tstamp::date = %s and tstamp > %s
                GROUP BY ticker,tstamp,strike
                ORDER BY tstamp, strike DESC
"""