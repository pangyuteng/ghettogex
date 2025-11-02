
CREATE TABLE IF NOT EXISTS gex_strike (
    gex_strike_id SERIAL,
    ticker text,
    tstamp TIMESTAMP,
    strike double precision,
    volume_gex double precision,
    state_gex double precision,
    dex double precision,
    convexity double precision,
    vex double precision,
    cex double precision,
    call_convexity double precision,
    call_oi double precision,
    call_dex double precision,
    call_gex double precision,
    call_vex double precision,
    call_cex double precision,
    put_convexity double precision,
    put_oi double precision,
    put_dex double precision,
    put_gex double precision,
    put_vex double precision,
    put_cex double precision,
    UNIQUE (ticker, tstamp, strike)
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('gex_strike');
CALL add_columnstore_policy('gex_strike', after => INTERVAL '1d');

CREATE TABLE IF NOT EXISTS gex_net (
    gex_net_id SERIAL,
    ticker text,
    tstamp TIMESTAMP,
    spot_price double precision,
    volume_gex double precision,
    state_gex double precision,
    dex double precision,
    convexity double precision,
    vex double precision,
    cex double precision,
    call_convexity double precision,
    call_oi double precision,
    call_dex double precision,
    call_gex double precision,
    call_vex double precision,
    call_cex double precision,
    put_convexity double precision,
    put_oi double precision,
    put_dex double precision,
    put_gex double precision,
    put_vex double precision,
    put_cex double precision,
    UNIQUE (ticker, tstamp)
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('gex_net');
CALL add_columnstore_policy('gex_net', after => INTERVAL '1d');

CREATE TABLE IF NOT EXISTS event_agg (
    event_agg_id SERIAL,
    event_symbol text NOT NULL,
    tstamp TIMESTAMP,
    spot_price double precision,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    ask_volume double precision,
    bid_volume double precision,
    open_interest double precision,
    true_oi double precision,
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
    UNIQUE (event_symbol, tstamp)
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('event_agg');
CALL add_columnstore_policy('event_agg', after => INTERVAL '1d');

CREATE TABLE IF NOT EXISTS settings (
    settings_id bool PRIMARY KEY DEFAULT true
    , from_scratch bool
    , CONSTRAINT settings_uni CHECK (settings_id)
);
