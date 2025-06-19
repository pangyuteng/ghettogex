
import logging
logger = logging.getLogger(__file__)

import datetime
import os
import sys
import traceback
import psycopg
import psycopg_pool
from psycopg.rows import dict_row

postgres_uri = os.environ.get("POSTGRES_URI")

def postgres_execute(query_str,query_args,is_commit=False):
    response = None
    try:
        with psycopg.connect(postgres_uri,autocommit=True,row_factory=dict_row) as conn:
            with conn.cursor() as curs:
                curs.execute(query_str,query_args)
                if is_commit is False:
                    response = curs.fetchall()
    except:
        traceback.print_exc()
    return response


create_query =  """

CREATE TABLE IF NOT EXISTS gex_strike (
    gex_strike_id SERIAL,
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

CALL add_columnstore_policy('gex_strike', after => INTERVAL '1d');

"""

index_query = """
create index gex_strike_tstamp_ticker_index on gex_strike using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);

"""

if __name__ == "__main__":
    print("ok")
    postgres_execute(create_query,(),True)
    postgres_execute(index_query,(),True)
    print("done")

"""

docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:latest-pg17
export POSTGRES_URI=postgres://postgres:password@192.168.68.143:5432/postgres

"""
