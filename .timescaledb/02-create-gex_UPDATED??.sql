
??? 

CREATE TABLE IF NOT EXISTS gex_contract (
    event_symbol text NOT NULL,
    tstamp TIMESTAMP,
    mid_price double precision,
    mm_order_imbalance double precision,
    gex double precision,
    dex double precision,
    vex double precision,
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

CREATE TABLE IF NOT EXISTS gex_strike (
    ticker text,
    tstamp TIMESTAMP,
    strike double precision,
    gex double precision,
    convexity double precision,
    dex double precision,
    vex double precision,
    cex double precision,
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
    ticker text,
    tstamp TIMESTAMP,
    spot_price double precision,
    gex double precision,
    dex double precision,
    vex double precision,
    cex double precision,
    UNIQUE (ticker, tstamp)
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('gex_net');
CALL add_columnstore_policy('gex_net', after => INTERVAL '1d');
