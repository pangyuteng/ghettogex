
-- SELECT remove_retention_policy('candle');
-- SELECT remove_retention_policy('greeks');
-- SELECT remove_retention_policy('summary');
-- SELECT remove_retention_policy('timeandsale');
-- SELECT remove_retention_policy('quote');

-- SELECT remove_retention_policy('event_contract');
-- SELECT remove_retention_policy('event_strike');
-- SELECT remove_retention_policy('event_underlying');

-- SELECT remove_retention_policy('event_underlying_1min');
-- SELECT remove_retention_policy('event_strike_1min');
-- SELECT remove_retention_policy('candle_1min');
-- SELECT remove_retention_policy('candle_5min');
-- SELECT remove_retention_policy('quote_1min');
-- SELECT remove_retention_policy('quote_1day');
-- SELECT remove_retention_policy('candle_1day');
-- SELECT remove_retention_policy('greeks_1day');
-- SELECT remove_retention_policy('greeksdx_1day');

SELECT add_retention_policy('candle', INTERVAL '20 days');
SELECT add_retention_policy('greeks', INTERVAL '20 days');
SELECT add_retention_policy('summary', INTERVAL '20 days');
SELECT add_retention_policy('timeandsale', INTERVAL '20 days');
SELECT add_retention_policy('quote', INTERVAL '20 days');

SELECT add_retention_policy('event_contract', INTERVAL '20 days');
SELECT add_retention_policy('event_strike', INTERVAL '20 days');
SELECT add_retention_policy('event_underlying', INTERVAL '20 days');

SELECT add_retention_policy('event_underlying_1min', INTERVAL '20 days');
SELECT add_retention_policy('event_strike_1min', INTERVAL '20 days');
SELECT add_retention_policy('candle_1min', INTERVAL '20 days');
SELECT add_retention_policy('candle_5min', INTERVAL '20 days');
SELECT add_retention_policy('quote_1min', INTERVAL '20 days');
SELECT add_retention_policy('quote_1day', INTERVAL '20 days');
SELECT add_retention_policy('candle_1day', INTERVAL '20 days');
SELECT add_retention_policy('greeks_1day', INTERVAL '20 days');
SELECT add_retention_policy('greeksdx_1day', INTERVAL '20 days');
