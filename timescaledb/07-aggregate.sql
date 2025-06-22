


CREATE MATERIALIZED VIEW get_net_1min WITH (timescaledb.continuous) AS
SELECT time_bucket('1m', tstamp) as tstamp, ticker, 
    last(volume_gex,tstamp) as volume_gex, last(state_gex,tstamp) as state_gex, last(spot_price,tstamp) as spot_price
FROM gex_net 
GROUP BY time_bucket('1m', tstamp), ticker;

SELECT add_continuous_aggregate_policy('get_net_1min',
  start_offset => NULL,
  end_offset => INTERVAL '1 min',
  schedule_interval => INTERVAL '1 min');

CALL refresh_continuous_aggregate('get_net_1min', NULL, NULL);


-- DROP MATERIALIZED VIEW get_net_1min;
-- SELECT remove_continuous_aggregate_policy('get_net_1min');
-- https://gist.github.com/mathisve/3cf9fd3f97ba75ec4d20c483fd5016d2
