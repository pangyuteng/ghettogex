

CREATE MATERIALIZED VIEW gex_net_1min WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', tstamp) as tstamp, ticker, 
  last(spot_price,tstamp) as spot_price,
  avg(volume_gex) as volume_gex,
  avg(state_gex) as state_gex,
  avg(convexity) as convexity,
  avg(dex) as dex,
  avg(vex) as vex,
  avg(cex) as cex,
  avg(call_convexity) as call_convexity,
  avg(call_oi) as call_oi,
  avg(call_dex) as call_dex,
  avg(call_gex) as call_gex,
  avg(call_vex) as call_vex,
  avg(call_cex) as call_cex,
  avg(put_convexity) as put_convexity,
  avg(put_oi) as put_oi,
  avg(put_dex) as put_dex,
  avg(put_gex) as put_gex,
  avg(put_vex) as put_vex,
  avg(put_cex) as put_cex
FROM gex_net 
GROUP BY time_bucket('1m', tstamp), ticker;

SELECT add_continuous_aggregate_policy('gex_net_1min',
  start_offset => INTERVAL '1 month',
  end_offset => NULL,
  schedule_interval => INTERVAL '1 sec');

CALL refresh_continuous_aggregate('gex_net_1min', NULL, NULL);
ALTER MATERIALIZED VIEW gex_net_1min set (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW gex_net_1min set (timescaledb.enable_columnstore = true);

-- DROP MATERIALIZED VIEW gex_net_1min;
-- SELECT remove_continuous_aggregate_policy('gex_net_1min');

CREATE MATERIALIZED VIEW candle_1min WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', tstamp) as tstamp, event_symbol,ticker,expiration,contract_type,strike,
first(open,tstamp) as open,
last(close,tstamp) as close,
max(high) as high,
min(low) as low,
sum(ask_volume) as ask_volume,
sum(bid_volume) as bid_volume,
sum(ask_volume)-sum(bid_volume) as order_imbalance
FROM candle 
GROUP BY time_bucket('1m', tstamp), event_symbol,ticker,expiration,contract_type,strike;

SELECT add_continuous_aggregate_policy('candle_1min',
  start_offset => INTERVAL '1 month',
  end_offset => NULL,
  schedule_interval => INTERVAL '1 sec');

CALL refresh_continuous_aggregate('candle_1min', NULL, NULL);
ALTER MATERIALIZED VIEW candle_1min set (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW candle_1min set (timescaledb.enable_columnstore = true);


-- DROP MATERIALIZED VIEW candle_1min;
-- SELECT remove_continuous_aggregate_policy('candle_1min');
-- https://gist.github.com/mathisve/3cf9fd3f97ba75ec4d20c483fd5016d2


CREATE MATERIALIZED VIEW order_imbalance WITH (timescaledb.continuous) AS
SELECT time_bucket('5m', tstamp) as tstamp, event_symbol,ticker,expiration,contract_type,strike,
sum(order_imbalance) as order_imbalance
FROM candle_1min where (ticker in ('SPXW','NDXP') or event_symbol like '/ES%' )
GROUP BY time_bucket('5m', tstamp), event_symbol, ticker,expiration,contract_type,strike;

SELECT add_continuous_aggregate_policy('order_imbalance',
  start_offset => INTERVAL '1 month',
  end_offset => NULL,
  schedule_interval => INTERVAL '1 sec');

CALL refresh_continuous_aggregate('order_imbalance', NULL, NULL);
ALTER MATERIALIZED VIEW order_imbalance set (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW order_imbalance set (timescaledb.enable_columnstore = true);

-- DROP MATERIALIZED VIEW order_imbalance
-- SELECT remove_continuous_aggregate_policy('order_imbalance');

CREATE MATERIALIZED VIEW quote_1min WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', tstamp) as tstamp, event_symbol,ticker,expiration,contract_type,strike,
last(bid_price,tstamp) as last_bid_price,last(ask_price,tstamp) as last_ask_price
FROM quote where ticker in ('SPXW','NDXP') 
GROUP BY time_bucket('1m', tstamp), event_symbol, ticker,expiration,contract_type,strike;

SELECT add_continuous_aggregate_policy('quote_1min',
  start_offset => INTERVAL '1 month',
  end_offset => NULL,
  schedule_interval => INTERVAL '1 sec');

CALL refresh_continuous_aggregate('quote_1min', NULL, NULL);
ALTER MATERIALIZED VIEW quote_1min set (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW quote_1min set (timescaledb.enable_columnstore = true);

-- DROP MATERIALIZED VIEW quote_1min
-- SELECT remove_continuous_aggregate_policy('quote_1min');

-- NOTE: below *_1day are meant to simplify query, maybe this is a bad idea, as it hides potential missing data

--

CREATE MATERIALIZED VIEW quote_1day WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', tstamp) as tstamp, event_symbol,ticker,expiration,contract_type,strike,
last(bid_price,tstamp) as last_bid_price,last(ask_price,tstamp) as last_ask_price
FROM quote where ticker in ('SPXW','NDXP')
GROUP BY time_bucket('1 day', tstamp), event_symbol, ticker,expiration,contract_type,strike;

SELECT add_continuous_aggregate_policy('quote_1day',
  start_offset => INTERVAL '1 month',
  end_offset => NULL,
  schedule_interval => INTERVAL '1 sec');

CALL refresh_continuous_aggregate('quote_1day', NULL, NULL);
ALTER MATERIALIZED VIEW quote_1day set (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW quote_1day set (timescaledb.enable_columnstore = true);

-- DROP MATERIALIZED VIEW quote_1day
-- SELECT remove_continuous_aggregate_policy('quote_1day');


--

CREATE MATERIALIZED VIEW order_imbalance_1day WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', tstamp) as tstamp, event_symbol,ticker,expiration,contract_type,strike,
sum(order_imbalance) as order_imbalance
FROM order_imbalance where ticker in ('SPXW','NDXP')
GROUP BY time_bucket('1 day', tstamp), event_symbol, ticker,expiration,contract_type,strike;

SELECT add_continuous_aggregate_policy('order_imbalance_1day',
  start_offset => INTERVAL '1 month',
  end_offset => NULL,
  schedule_interval => INTERVAL '1 sec');

CALL refresh_continuous_aggregate('order_imbalance_1day', NULL, NULL);
ALTER MATERIALIZED VIEW order_imbalance_1day set (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW order_imbalance_1day set (timescaledb.enable_columnstore = true);

-- DROP MATERIALIZED VIEW order_imbalance_1day
-- SELECT remove_continuous_aggregate_policy('order_imbalance_1day');

-- 

CREATE MATERIALIZED VIEW greeksdx_1day WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', tstamp) as tstamp, event_symbol, ticker, expiration, contract_type, strike,
last(price,tstamp) as price,
last(volatility,tstamp) as volatility,	
last(delta,tstamp) as delta,
last(gamma,tstamp) as gamma
FROM greeks where ticker in ('SPXW','NDXP')
GROUP BY time_bucket('1 day', tstamp), event_symbol, ticker, expiration, contract_type, strike;

SELECT add_continuous_aggregate_policy('greeksdx_1day',
  start_offset => INTERVAL '1 month',
  end_offset => NULL,
  schedule_interval => INTERVAL '1 sec');

CALL refresh_continuous_aggregate('greeksdx_1day', NULL, NULL);
ALTER MATERIALIZED VIEW greeksdx_1day set (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW greeksdx_1day set (timescaledb.enable_columnstore = true);

-- DROP MATERIALIZED VIEW greeksdx_1day
-- SELECT remove_continuous_aggregate_policy('greeksdx_1day');

CREATE MATERIALIZED VIEW greeks_1day WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', tstamp) as tstamp, event_symbol, ticker, expiration, contract_type, strike,
last(ask_price,tstamp) as ask_price,
last(bid_price,tstamp) as bid_price,
last(volatility,tstamp) as volatility,	
last(delta,tstamp) as delta,
last(gamma,tstamp) as gamma
FROM event_contract where ticker in ('SPXW','NDXP')
GROUP BY time_bucket('1 day', tstamp), event_symbol, ticker, expiration, contract_type, strike;

SELECT add_continuous_aggregate_policy('greeks_1day',
  start_offset => INTERVAL '1 month',
  end_offset => NULL,
  schedule_interval => INTERVAL '1 sec');

CALL refresh_continuous_aggregate('greeks_1day', NULL, NULL);
ALTER MATERIALIZED VIEW greeks_1day set (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW greeks_1day set (timescaledb.enable_columnstore = true);

-- DROP MATERIALIZED VIEW greeks_1day
-- SELECT remove_continuous_aggregate_policy('greeks_1day');

--
