

/*
CREATE MATERIALIZED VIEW gex_net_1min WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', tstamp) as tstamp, ticker, 
  last(spot_price,tstamp) as spot_price,
  last(volume_gex,tstamp) as volume_gex,
  last(state_gex,tstamp) as state_gex,
  last(convexity,tstamp) as convexity,
  last(dex,tstamp) as dex,
  last(vex,tstamp) as vex,
  last(cex,tstamp) as cex,
  last(call_convexity,tstamp) as call_convexity,
  last(call_oi,tstamp) as call_oi,
  last(call_dex,tstamp) as call_dex,
  last(call_gex,tstamp) as call_gex,
  last(call_vex,tstamp) as call_vex,
  last(call_cex,tstamp) as call_cex,
  last(put_convexity,tstamp) as put_convexity,
  last(put_oi,tstamp) as put_oi,
  last(put_dex,tstamp) as put_dex,
  last(put_gex,tstamp) as put_gex,
  last(put_vex,tstamp) as put_vex,
  last(put_cex,tstamp) as put_cex
FROM gex_net 
GROUP BY time_bucket('1m', tstamp), ticker;
*/

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
  start_offset => NULL,
  end_offset => INTERVAL '1 min',
  schedule_interval => INTERVAL '1 min');

CALL refresh_continuous_aggregate('gex_net_1min', NULL, NULL);

CREATE MATERIALIZED VIEW candle_1min WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', tstamp) as tstamp, event_symbol,
last(close,tstamp) as close
FROM candle where (event_symbol in ('SPX','VIX','VIX1D') or event_symbol like '/ES%' )
GROUP BY time_bucket('1m', tstamp), event_symbol;

SELECT add_continuous_aggregate_policy('candle_1min',
  start_offset => NULL,
  end_offset => INTERVAL '1 min',
  schedule_interval => INTERVAL '1 min');

CALL refresh_continuous_aggregate('candle_1min', NULL, NULL);
ALTER MATERIALIZED VIEW candle_1min set (timescaledb.materialized_only = false);

-- DROP MATERIALIZED VIEW gex_net_1min;
-- SELECT remove_continuous_aggregate_policy('gex_net_1min');
-- DROP MATERIALIZED VIEW candle_1min;
-- SELECT remove_continuous_aggregate_policy('candle_1min');
-- https://gist.github.com/mathisve/3cf9fd3f97ba75ec4d20c483fd5016d2


CREATE MATERIALIZED VIEW order_imbalance WITH (timescaledb.continuous) AS
SELECT time_bucket('5m', tstamp) as tstamp, event_symbol,ticker,expiration,contract_type,strike,
sum(ask_volume)-sum(bid_volume) as order_imbalance
FROM candle where ticker in ('SPXW','NDXW')
GROUP BY time_bucket('5m', tstamp), event_symbol, ticker,expiration,contract_type,strike;

SELECT add_continuous_aggregate_policy('order_imbalance',
  start_offset => NULL,
  end_offset => INTERVAL '5 min',
  schedule_interval => INTERVAL '5 min');

CALL refresh_continuous_aggregate('order_imbalance', NULL, NULL);
ALTER MATERIALIZED VIEW order_imbalance set (timescaledb.materialized_only = false);

-- DROP MATERIALIZED VIEW quote_1min
-- SELECT remove_continuous_aggregate_policy('quote_1min');

CREATE MATERIALIZED VIEW quote_1min WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', tstamp) as tstamp, event_symbol,ticker,expiration,contract_type,strike,
last(bid_price,tstamp) as last_bid_price,last(ask_price,tstamp) as last_ask_price
FROM quote where ticker in ('SPXW','NDXW') and expiration = now()::date
GROUP BY time_bucket('1m', tstamp), event_symbol, ticker,expiration,contract_type,strike;

SELECT add_continuous_aggregate_policy('quote_1min',
  start_offset => NULL,
  end_offset => INTERVAL '1 min',
  schedule_interval => INTERVAL '1 min');

CALL refresh_continuous_aggregate('quote_1min', NULL, NULL);
ALTER MATERIALIZED VIEW quote_1min set (timescaledb.materialized_only = false);



--

CREATE MATERIALIZED VIEW order_imbalance_1day WITH (timescaledb.continuous) AS
SELECT time_bucket('1day', tstamp) as tstamp, event_symbol,ticker,expiration,contract_type,strike,
sum(order_imbalance) as order_imbalance
FROM order_imbalance where ticker in ('SPXW','NDXW') and expiration = now()::date
GROUP BY time_bucket('1day', tstamp), event_symbol, ticker,expiration,contract_type,strike;

SELECT add_continuous_aggregate_policy('order_imbalance_1day',
  start_offset => NULL,
  end_offset => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 day');

CALL refresh_continuous_aggregate('order_imbalance_1day', NULL, NULL);
ALTER MATERIALIZED VIEW order_imbalance_1day set (timescaledb.materialized_only = false);

-- 

CREATE MATERIALIZED VIEW greeks_1day WITH (timescaledb.continuous) AS
SELECT time_bucket('1day', tstamp) as tstamp, event_symbol, ticker, expiration, contract_type, strike,
last(price,tstamp) as price,
last(volatility,tstamp) as volatility,	
last(delta,tstamp) as delta,
last(gamma,tstamp) as gamma,
last(theta,tstamp) as theta,
last(rho,tstamp) as rho,
last(vega,tstamp) as vega
FROM greeks where ticker in ('SPXW','NDXW') and expiration = now()::date
GROUP BY time_bucket('1day', tstamp), event_symbol, ticker, expiration, contract_type, strike;

SELECT add_continuous_aggregate_policy('greeks_1day',
  start_offset => NULL,
  end_offset => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 day');

CALL refresh_continuous_aggregate('greeks_1day', NULL, NULL);
ALTER MATERIALIZED VIEW greeks_1day set (timescaledb.materialized_only = false);

--