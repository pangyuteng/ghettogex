FROM postgres

COPY create-events.sql /docker-entrypoint-initdb.d/
COPY create-gex.sql /docker-entrypoint-initdb.d/
COPY create-watchlist.sql /docker-entrypoint-initdb.d/
