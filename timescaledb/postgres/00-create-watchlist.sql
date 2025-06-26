
    CREATE TABLE IF NOT EXISTS watchlist (
        watchlist_id SERIAL PRIMARY KEY, 
        ticker text UNIQUE
    );
    INSERT INTO watchlist(ticker) VALUES('SPX');
    INSERT INTO watchlist(ticker) VALUES('VIX');

    