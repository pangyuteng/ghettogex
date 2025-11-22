
SELECT remove_retention_policy('candle');
SELECT remove_retention_policy('greeks');
SELECT remove_retention_policy('summary');
SELECT remove_retention_policy('timeandsale');
SELECT remove_retention_policy('quote');

SELECT remove_retention_policy('gex_strike');
SELECT remove_retention_policy('gex_net');

SELECT remove_retention_policy('event_contract');
SELECT remove_retention_policy('event_strike');
SELECT remove_retention_policy('event_underlying');

SELECT remove_retention_policy('event_underlying_1min');
SELECT remove_retention_policy('candle_1min');
SELECT remove_retention_policy('order_imbalance');
SELECT remove_retention_policy('quote_1min');
SELECT remove_retention_policy('quote_1day');
SELECT remove_retention_policy('order_imbalance_1day');
SELECT remove_retention_policy('greeks_1day');
SELECT remove_retention_policy('greeksdx_1day');

SELECT add_retention_policy('candle', INTERVAL '100 days');
SELECT add_retention_policy('greeks', INTERVAL '100 days');
SELECT add_retention_policy('summary', INTERVAL '100 days');
SELECT add_retention_policy('timeandsale', INTERVAL '100 days');
SELECT add_retention_policy('quote', INTERVAL '100 days');

SELECT add_retention_policy('gex_strike', INTERVAL '100 days');
SELECT add_retention_policy('gex_net', INTERVAL '100 days');

SELECT add_retention_policy('event_contract', INTERVAL '100 days');
SELECT add_retention_policy('event_strike', INTERVAL '100 days');
SELECT add_retention_policy('event_underlying', INTERVAL '100 days');

SELECT add_retention_policy('event_underlying_1min', INTERVAL '100 days');
SELECT add_retention_policy('candle_1min', INTERVAL '100 days');
SELECT add_retention_policy('order_imbalance', INTERVAL '100 days');
SELECT add_retention_policy('quote_1min', INTERVAL '100 days');
SELECT add_retention_policy('quote_1day', INTERVAL '100 days');
SELECT add_retention_policy('order_imbalance_1day', INTERVAL '100 days');
SELECT add_retention_policy('greeks_1day', INTERVAL '100 days');
SELECT add_retention_policy('greeksdx_1day', INTERVAL '100 days');

