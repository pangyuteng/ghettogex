


https://www.pgmustard.com/blog/max-parallel-workers-per-gather

https://stackoverflow.com/questions/63680444/why-less-than-has-much-better-performance-than-greater-than-in-a-query


```
EXPLAIN ANALYZE
(select 'underlying_candle' as event_type,event_symbol,close as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,null as ticker,null as expiration,null as contract_type,null as strike from candle
where tstamp >= '2025-06-06 17:00:00' and tstamp < '2025-06-06 17:01:00' and event_symbol = 'SPX'
) union all (
select 'candle' as event_type,event_symbol,null::float as spot_price,open,high,low,close,volume,ask_volume,bid_volume,null::int as open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from candle
where tstamp >= '2025-06-06 17:00:59' and tstamp < '2025-06-06 17:01:00' and ticker = 'SPXW'
) union all (
select 'summary' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,open_interest,null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,null::int as size,null as aggressor_side,tstamp ,ticker,expiration,contract_type,strike from event_agg
where tstamp >= '2025-06-06 17:00:00' and tstamp < '2025-06-06 17:01:00' and ticker = 'SPXW'
) union all (
select 'greeks' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, price,volatility,delta,gamma,theta,rho,vega,null::int as size,null as aggressor_side,tstamp,ticker,expiration,contract_type,strike from greeks
where tstamp >= '2025-06-06 17:00:00' and tstamp < '2025-06-06 17:01:00' and ticker = 'SPXW'
) union all (
select 'timeandsale' as event_type,event_symbol,null::float as spot_price,null::float as open,null::float as high,null::float as low,null::float as close,null::float as volume,null::float as ask_volume,null::float as bid_volume,null::int as open_interest, null::float as price,null::float as volatility,null::float as delta,null::float as gamma,null::float as theta,null::float as rho,null::float as vega,size,aggressor_side,tstamp,ticker,expiration,contract_type,strike from timeandsale
where tstamp >= '2025-06-06 17:00:59' and tstamp < '2025-06-06 17:01:00' and ticker = 'SPXW'
) union all (

)


EXPLAIN ANALYZE
select 'candle' as event_type,event_symbol,open,high,low,close,volume,ask_volume,bid_volume,time,tstamp,ticker,expiration,contract_type,strike from candle
where tstamp >= '2025-06-06 17:00:59' and tstamp < '2025-06-06 17:01:00' and ticker = 'SPXW' and expiration = '2025-06-06'

EXPLAIN ANALYZE
select 'summary' as event_type,event_symbol,true_oi,open_interest,tstamp,ticker,expiration,contract_type,strike from event_agg
where dstamp = '2025-06-06' and ticker = 'SPXW' and expiration = '2025-06-06'

EXPLAIN ANALYZE
select 'greeks' as event_type,event_symbol,price,volatility,delta,gamma,theta,rho,vega,time,tstamp,ticker,expiration,contract_type,strike from greeks
where tstamp >= '2025-06-06 17:00:59' and tstamp < '2025-06-06 17:01:00' and ticker = 'SPXW' and expiration = '2025-06-06'

EXPLAIN ANALYZE
select 'timeandsale' as event_type,event_symbol,size,price,bid_price,ask_price,aggressor_side,time,tstamp,ticker,expiration,contract_type,strike from timeandsale
where tstamp >= '2025-06-06 17:00:59' and tstamp < '2025-06-06 17:01:00' and ticker = 'SPXW' and expiration = '2025-06-06'

EXPLAIN ANALYZE
select 'quote' as event_type,event_symbol,bid_time,ask_time,bid_price,ask_price,bid_size,ask_size,tstamp,ticker,expiration,contract_type,strike from quote
where tstamp >= '2025-06-06 17:00:59' and tstamp < '2025-06-06 17:01:01' and ticker = 'SPXW' and expiration = '2025-06-06'



```


```

VACUUM FULL ANALYZE

https://dba.stackexchange.com/questions/246045/how-often-do-you-run-vacuum-full


```

```

https://www.reddit.com/r/algotrading/comments/11wctxl/what_storage_systems_do_you_use/

Postgres + Timescale. The compression ratio is phenomenal with TimeScaleDB.

https://github.com/timescale/timescaledb

https://docs.timescale.com/self-hosted/latest/install/installation-docker

timescale/timescaledb:latest-pg17

timescale/timescaledb-ha:pg17

https://hub.docker.com/r/timescale/timescaledb-ha/tags


+ [ ] edit sql, add hypertable and index 

+ [ ] archive partition py logic

https://docs.timescale.com/api/latest/hypertable/create_index/


```

```
https://www.reddit.com/r/algotrading/comments/1l2gywd/what_db_do_you_use/


This is my uniform prior. Without knowing what you do, Parquet is a good starting point.
A binary flat file in record-oriented layout (rather than column-oriented like Parquet) is also a very good starting point. It has mainly 3 advantages over Parquet:
If most of your tasks require all columns and most of the data, like backtesting, it strips away a lot of the benefit of a column-oriented layout.
It simplifies your architecture since it's easy to use this same format for real-time messaging and in-memory representation.
You'll usually find it easier to mux this with your logging format.
We store about 6 PB compressed in this manner with DBN encoding.
```