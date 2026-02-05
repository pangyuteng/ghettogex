
CREATE TABLE IF NOT EXISTS watchlist (
    watchlist_id SERIAL PRIMARY KEY,
    ticker text UNIQUE,
    compute_gex bool
);

INSERT INTO watchlist(ticker,compute_gex) VALUES('SPX',true);
INSERT INTO watchlist(ticker,compute_gex) VALUES('NDX',true);
INSERT INTO watchlist(ticker,compute_gex) VALUES('SPY',true);
INSERT INTO watchlist(ticker,compute_gex) VALUES('QQQ',true);
INSERT INTO watchlist(ticker,compute_gex) VALUES('VIX',false);
INSERT INTO watchlist(ticker,compute_gex) VALUES('UVXY',false);
INSERT INTO watchlist(ticker,compute_gex) VALUES('ES',false);

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
