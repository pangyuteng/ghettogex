
CREATE TABLE IF NOT EXISTS gex_strike (
    gex_strike_id SERIAL,
    ticker text,
    tstamp TIMESTAMP,
    strike double precision,
    naive_gex double precision,
    true_gex double precision,
    UNIQUE (ticker, tstamp, strike)
) PARTITION BY RANGE (tstamp);

CREATE TABLE IF NOT EXISTS gex_net (
    gex_net_id SERIAL,
    ticker text,
    tstamp TIMESTAMP,
    spot_price double precision,
    naive_gex double precision,
    true_gex double precision,
    UNIQUE (ticker, tstamp)
) PARTITION BY RANGE (tstamp);

CREATE TABLE IF NOT EXISTS event_agg (
    event_agg_id SERIAL,
    event_symbol text NOT NULL,
    dstamp TIMESTAMP,
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
    naive_gex double precision,
    true_gex double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP,
    UNIQUE (event_symbol, dstamp)
) PARTITION BY RANGE (dstamp);

CREATE TABLE IF NOT EXISTS settings (
    settings_id bool PRIMARY KEY DEFAULT true
    , from_scratch bool
    , CONSTRAINT settings_uni CHECK (settings_id)
);
