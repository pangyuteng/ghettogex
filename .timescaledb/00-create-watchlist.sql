
CREATE TABLE IF NOT EXISTS watchlist (
    watchlist_id SERIAL PRIMARY KEY,
    ticker text UNIQUE,
    expiration_count integer, 
    compute_gex bool,
    candle_sub bool
);

INSERT INTO watchlist(ticker,compute_gex,candle_sub,expiration_count) VALUES('SPX',true,true,1);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('VIX',false,true);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('VXV',false,true);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('VIX1D',false,true);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('VIX9D',false,true);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('UVXY',false,true);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('ES',false,true);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('NQ',false,true);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('VX',false,true);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('COR1M',false,true);
INSERT INTO watchlist(ticker,compute_gex,candle_sub) VALUES('DSPX',false,true);

CREATE TABLE IF NOT EXISTS session (
    session_id SERIAL PRIMARY KEY,
    serialized_session text
);

CREATE TABLE IF NOT EXISTS settings (
    settings_id bool PRIMARY KEY DEFAULT true
    , from_scratch bool
    , CONSTRAINT settings_uni CHECK (settings_id)
);

CREATE TABLE IF NOT EXISTS external_apps (
    external_apps_id SERIAL PRIMARY KEY,
    app_name text,
    chat_id text,
    alert_type text
);
