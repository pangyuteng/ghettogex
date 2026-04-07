
CREATE TABLE IF NOT EXISTS event_contract (
    event_symbol text NOT NULL,
    tstamp TIMESTAMP,
    ask_price double precision,
    bid_price double precision,
    mm_order_imbalance double precision,
    volatility double precision,
    delta double precision,
    gamma double precision,
    theta double precision,
    rho double precision,
    vega double precision,
    gex double precision,
    dex double precision,
    vex double precision,
    cex double precision,
    convexity double precision,
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

CALL remove_columnstore_policy('event_contract');
CALL add_columnstore_policy('event_contract', after => INTERVAL '1d');


CREATE TABLE IF NOT EXISTS event_strike (
    ticker text,
    tstamp TIMESTAMP,
    strike double precision,
    gex double precision,
    dex double precision,
    vex double precision,
    cex double precision,
    convexity double precision,
    UNIQUE (ticker, tstamp, strike)
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('event_strike');
CALL add_columnstore_policy('event_strike', after => INTERVAL '1d');

CREATE TABLE IF NOT EXISTS event_underlying (
    ticker text,
    tstamp TIMESTAMP,
    spot_price double precision,
    gex double precision,
    dex double precision,
    vex double precision,
    cex double precision,
    convexity double precision,
    call_dex double precision,
    put_dex double precision,
    expected_move double precision,
    UNIQUE (ticker, tstamp)
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL remove_columnstore_policy('event_underlying');
CALL add_columnstore_policy('event_underlying', after => INTERVAL '1d');
