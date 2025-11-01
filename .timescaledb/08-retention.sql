
SELECT add_retention_policy('candle', INTERVAL '100 days');
SELECT add_retention_policy('greeks', INTERVAL '100 days');
SELECT add_retention_policy('summary', INTERVAL '100 days');
SELECT add_retention_policy('timeandsale', INTERVAL '100 days');
SELECT add_retention_policy('quote', INTERVAL '100 days');

SELECT add_retention_policy('gex_strike', INTERVAL '100 days');
SELECT add_retention_policy('gex_net', INTERVAL '100 days');
SELECT add_retention_policy('event_agg', INTERVAL '100 days');

SELECT add_retention_policy('candle_1min', INTERVAL '100 days');
SELECT add_retention_policy('order_imbalance', INTERVAL '100 days');
SELECT add_retention_policy('quote_1min', INTERVAL '100 days');
