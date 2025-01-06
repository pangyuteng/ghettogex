
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
    price_open double precision,
    price_high double precision,
    price_low double precision,
    price_close double precision,
    naive_gex double precision,
    volume_gex double precision,
    UNIQUE (ticker, tstamp)
);
