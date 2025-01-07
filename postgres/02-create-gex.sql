
CREATE TABLE IF NOT EXISTS gex_strike (
    gex_strike_id SERIAL PRIMARY KEY, 
    ticker text,
    tstamp TIMESTAMP,
    strike double precision,
    naive_gex double precision,
    volume_gex double precision,
    UNIQUE (ticker, tstamp, strike)
);

CREATE TABLE IF NOT EXISTS gex_net (
    gex_net_id SERIAL PRIMARY KEY, 
    ticker text,
    tstamp TIMESTAMP,
    spot_price numeric,
    naive_gex double precision,
    volume_gex double precision,
    UNIQUE (ticker, tstamp)
);

CREATE TABLE IF NOT EXISTS event_agg (
    event_agg_id SERIAL PRIMARY KEY,
    event_symbol text NOT NULL,
    tstamp_et TIMESTAMP,
    spot_price numeric,
    open_interest double precision,
    ask_price numeric,
    bid_price numeric,
    imp_volatility numeric,
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
);
