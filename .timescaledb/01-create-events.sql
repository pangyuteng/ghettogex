
-- candle_id SERIAL PRIMARY KEY,

CREATE TABLE IF NOT EXISTS candle (
    
    candle_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    event_flags numeric,
    index numeric,
    time numeric,
    sequence numeric,
    count numeric,
    volume double precision,
    vwap double precision,
    bid_volume double precision,
    ask_volume double precision,
    imp_volatility double precision,
    open_interest numeric,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('candle');
CALL add_columnstore_policy('candle', after => INTERVAL '1d');

CREATE TABLE IF NOT EXISTS event (
    
    event_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('event');
CALL add_columnstore_policy('event', after => INTERVAL '1d');


CREATE TABLE IF NOT EXISTS greeks (
    
    greeks_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    event_flags numeric,
    index numeric,
    time numeric,
    sequence numeric,
    price double precision,
    volatility double precision,
    delta double precision,
    gamma double precision,
    theta double precision,
    rho double precision,
    vega double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('greeks');
CALL add_columnstore_policy('greeks', after => INTERVAL '1d');


CREATE TABLE IF NOT EXISTS profile (
    
    profile_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    description text,
    short_sale_restriction text,
    trading_status text,
    halt_start_time numeric,
    halt_end_time numeric,
    ex_dividend_day_id numeric,
    status_reason text,
    high_52_week_price double precision,
    low_52_week_price double precision,
    beta double precision,
    shares double precision,
    high_limit_price double precision,
    low_limit_price double precision,
    earnings_per_share double precision,
    ex_dividend_amount double precision,
    dividend_frequency double precision,
    free_float double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('profile');
CALL add_columnstore_policy('profile', after => INTERVAL '1d');

CREATE TABLE IF NOT EXISTS quote (
    
    quote_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    sequence numeric,
    time_nano_part numeric,
    bid_time numeric,
    bid_exchange_code text,
    ask_time numeric,
    ask_exchange_code text,
    bid_price double precision,
    ask_price double precision,
    bid_size double precision,
    ask_size double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('quote');
CALL add_columnstore_policy('quote', after => INTERVAL '2 hours', schedule_interval => INTERVAL '1 hour');
-- SELECT remove_retention_policy('quote');
-- SELECT add_retention_policy('quote', INTERVAL '7 days');

CREATE TABLE IF NOT EXISTS summary (
    
    summary_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    day_id numeric,
    day_close_price_type text,
    prev_day_id numeric,
    prev_day_close_price_type text,
    open_interest numeric,
    day_open_price double precision,
    day_high_price double precision,
    day_low_price double precision,
    day_close_price double precision,
    prev_day_close_price double precision,
    prev_day_volume double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('summary');
CALL add_columnstore_policy('summary', after => INTERVAL '1d');

CREATE TABLE IF NOT EXISTS theoprice (
    
    theoprice_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    event_flags numeric,
    index numeric,
    time numeric,
    sequence numeric,
    price double precision,
    underlying_price double precision,
    delta double precision,
    gamma double precision,
    dividend double precision,
    interest double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('theoprice');
CALL add_columnstore_policy('theoprice', after => INTERVAL '1d');


CREATE TABLE IF NOT EXISTS timeandsale (
    
    timeandsale_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    event_flags numeric,
    index numeric,
    time numeric,
    time_nano_part numeric,
    sequence numeric,
    exchange_code text,
    price double precision,
    size numeric,
    bid_price double precision,
    ask_price double precision,
    exchange_sale_conditions text,
    trade_through_exempt text,
    aggressor_side text,
    spread_leg boolean,
    extended_trading_hours boolean,
    valid_tick boolean,
    type text,
    buyer text,
    seller text,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('timeandsale');
CALL add_columnstore_policy('timeandsale', after => INTERVAL '1d');


CREATE TABLE IF NOT EXISTS trade (
    
    trade_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    time numeric,
    time_nano_part numeric,
    sequence numeric,
    exchange_code text,
    day_id numeric,
    tick_direction text,
    extended_trading_hours boolean,
    price double precision,
    change double precision,
    size numeric,
    day_volume numeric,
    day_turnover double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('trade');
CALL add_columnstore_policy('trade', after => INTERVAL '1d');

CREATE TABLE IF NOT EXISTS underlying (
    
    underlying_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    event_flags numeric,
    index numeric,
    time numeric,
    sequence numeric,
    volatility double precision,
    front_volatility double precision,
    back_volatility double precision,
    call_volume numeric,
    put_volume numeric,
    option_volume numeric,
    put_call_ratio double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('underlying');
CALL add_columnstore_policy('underlying', after => INTERVAL '1d');


