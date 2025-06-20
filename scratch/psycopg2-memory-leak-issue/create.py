
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

CREATE TABLE IF NOT EXISTS foobar (
    foobar_id SERIAL,
    foofoo text,
    tstamp TIMESTAMP,
    value0 double precision,
    value1 double precision,
    value2 double precision,
    value3 double precision,
    value4 double precision,
    value5 double precision,
    value6 double precision,
    value7 double precision,
    value8 double precision,
    value9 double precision,
    value10 double precision,
    value11 double precision,
    value12 double precision,
    value13 double precision,
    value14 double precision,
    value15 double precision,
    value16 double precision,
    value17 double precision,
    value18 double precision,
    value19 double precision,
    value20 double precision,
    value21 double precision,
    value22 double precision,
    value23 double precision,
    value24 double precision,
    value25 double precision,
    value26 double precision,
    value27 double precision,
    value28 double precision,
    value29 double precision,
    value30 double precision,
    value31 double precision,
    value32 double precision,
    value33 double precision,
    value34 double precision,
    value35 double precision,
    value36 double precision,
    value37 double precision,
    value38 double precision,
    value39 double precision,
    value40 double precision,
    value41 double precision,
    value42 double precision,
    value43 double precision,
    value44 double precision,
    value45 double precision,
    value46 double precision,
    value47 double precision,
    value48 double precision,
    value49 double precision,
    value50 double precision,
    value51 double precision,
    value52 double precision,
    value53 double precision,
    value54 double precision,
    value55 double precision,
    value56 double precision,
    value57 double precision,
    value58 double precision,
    value59 double precision,
    value60 double precision,
    value61 double precision,
    value62 double precision,
    value63 double precision,
    value64 double precision,
    value65 double precision,
    value66 double precision,
    value67 double precision,
    value68 double precision,
    value69 double precision,
    value70 double precision,
    value71 double precision,
    value72 double precision,
    value73 double precision,
    value74 double precision,
    value75 double precision,
    value76 double precision,
    value77 double precision,
    value78 double precision,
    value79 double precision,
    value80 double precision,
    value81 double precision,
    value82 double precision,
    value83 double precision,
    value84 double precision,
    value85 double precision,
    value86 double precision,
    value87 double precision,
    value88 double precision,
    value89 double precision,
    value90 double precision,
    value91 double precision,
    value92 double precision,
    value93 double precision,
    value94 double precision,
    value95 double precision,
    value96 double precision,
    value97 double precision,
    value98 double precision,
    value99 double precision
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='foofoo',
  tsdb.orderby='tstamp DESC'
);

CALL add_columnstore_policy('foobar', after => INTERVAL '1d');

"""

index_query = """
create index foobar_index on foobar using brin (tstamp,foofoo) WITH (timescaledb.transaction_per_chunk);

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
