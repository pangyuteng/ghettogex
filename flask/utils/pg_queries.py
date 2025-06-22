

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
last_gex_strike AS (select * from gex_strike where tstamp <= %s and tstamp >= %s - interval '1 minute' and ticker = %s order by tstamp)
SELECT * FROM last_tstamp
LEFT JOIN last_gex_strike using (tstamp)
ORDER BY strike
"""

"""
WITH last_tstamp AS (select tstamp from gex_net where tstamp <= '2025-06-20 19:59:58' and tstamp >=  timestamp '2025-06-20 19:59:58' - interval '1 minute' and ticker = 'SPX' order by tstamp desc limit 1),
last_gex_strike AS (select * from gex_strike where tstamp <= '2025-06-20 19:59:58' and tstamp >=  timestamp '2025-06-20 19:59:58' - interval '1 minute' and ticker = 'SPX' order by tstamp)
SELECT * FROM last_tstamp
LEFT JOIN last_gex_strike using (tstamp)
"""

LATEST_ONE_MIN_GEX_STRIKE_QUERY = """
select * from gex_strike where tstamp <= %s and tstamp >= %s - interval '1 minute'  and ticker = %s order by tstamp,strike
"""

LATEST_DAY_GEX_NET_QUERY = """

select * from gex_net where tstamp::date = %s and ticker = %s order by tstamp

"""
