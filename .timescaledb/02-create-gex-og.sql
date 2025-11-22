
CREATE TABLE IF NOT EXISTS gex_strike (
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
