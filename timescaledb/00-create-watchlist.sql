
    CREATE TABLE IF NOT EXISTS watchlist (
        watchlist_id SERIAL PRIMARY KEY, 
        ticker text UNIQUE,
        compute_gex bool
    );
    INSERT INTO watchlist(ticker,compute_gex) VALUES('SPX',true);
    INSERT INTO watchlist(ticker,compute_gex) VALUES('VIX',false);
    