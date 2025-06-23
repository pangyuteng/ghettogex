


CREATE MATERIALIZED VIEW get_net_1min WITH (timescaledb.continuous) AS
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

SELECT add_continuous_aggregate_policy('get_net_1min',
  start_offset => NULL,
  end_offset => INTERVAL '1 min',
  schedule_interval => INTERVAL '1 min');

CALL refresh_continuous_aggregate('get_net_1min', NULL, NULL);

CREATE MATERIALIZED VIEW candle_1min WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', tstamp) as tstamp, event_symbol,
last(close,tstamp) as close
FROM candle where event_symbol in ('SPX','VIX')
GROUP BY time_bucket('1m', tstamp), event_symbol;

SELECT add_continuous_aggregate_policy('candle_1min',
  start_offset => NULL,
  end_offset => INTERVAL '1 min',
  schedule_interval => INTERVAL '1 min');

CALL refresh_continuous_aggregate('candle_1min', NULL, NULL);


-- DROP MATERIALIZED VIEW get_net_1min;
-- SELECT remove_continuous_aggregate_policy('get_net_1min');
-- DROP MATERIALIZED VIEW candle_1min;
-- SELECT remove_continuous_aggregate_policy('candle_1min');
-- https://gist.github.com/mathisve/3cf9fd3f97ba75ec4d20c483fd5016d2
