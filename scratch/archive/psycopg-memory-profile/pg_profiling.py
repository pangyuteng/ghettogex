


"""

CREATE TABLE IF NOT EXISTS hola (
    
    hola_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    sequence numeric,
    time_nano_part numeric,
    bid_time numeric,
    bid_exchange_code text,
    ask_time numeric,
    ask_exchange_code text,
    bid_price double precision,
    ask_price double precision,
    bid_size double precision,
    ask_size double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
)


CREATE TABLE IF NOT EXISTS hola (
    
    hola_id SERIAL,
    event_symbol text NOT NULL,
    event_time numeric,
    sequence numeric,
    time_nano_part numeric,
    bid_time numeric,
    bid_exchange_code text,
    ask_time numeric,
    ask_exchange_code text,
    bid_price double precision,
    ask_price double precision,
    bid_size double precision,
    ask_size double precision,
    ticker text,
    expiration TIMESTAMP,
    contract_type text,
    strike double precision,
    tstamp TIMESTAMP default (now() at time zone 'utc')
) WITH (
  tsdb.hypertable=true,
  tsdb.partition_column='tstamp',
  tsdb.segmentby='ticker',
  tsdb.orderby='tstamp DESC'
);

CALL add_columnstore_policy('hola', after => INTERVAL '1d');
create index hola_tstamp_event_symbol_index on hola using brin (tstamp,event_symbol) WITH (timescaledb.transaction_per_chunk);
create index hola_tstamp_ticker_index on hola using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);



insert into hola (
event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp ) VALUES 
('.SPX250620C5400',0,0,0,0,'C',0,'C',578.5,590.8,21,21,'SPX','2025-06-20','C',5400,'2025-06-18 20:00:28.66071' );



echo ".SPX250620C5400,0,0,0,0,C,0,C,578.5,590.8,21,21,SPX,2025-06-20,C,5400,2025-06-18 20:00:28.66071" >> ok.txt

psql -U postgres postgres -c  "COPY hola(event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp )  FROM '/tmp/ok.txt' DELIMITER ',';"


"""



import time
import datetime
import os
import sys
import traceback
import asyncio
import psycopg
import psycopg_pool
from psycopg.rows import dict_row
postgres_uri = os.environ.get("POSTGRES_URI")


async def apostgres_copy(apool,query_str,stdin):
    try:
        if apool is None:
            async with await psycopg.AsyncConnection.connect(postgres_uri) as aconn:
                async with aconn.cursor() as curs:
                    async with curs.copy(query_str) as copy:
                        await copy.write(stdin)
        else:
            # await apool.check() SLOW!!!
            async with apool.connection() as aconn:
                async with aconn.cursor() as curs:
                    async with curs.copy(query_str) as copy:
                        await copy.write(stdin)
    except:
        traceback.print_exc()
    return 1

async def apostgres_execute(apool,query_str,query_args,is_commit=False):
    response = None
    try:
        if apool is None:
            # ,row_factory=dict_row
            async with await psycopg.AsyncConnection.connect(postgres_uri,autocommit=True) as aconn:
                #row_factory=dict_row
                async with aconn.cursor() as curs:
                    await curs.execute(query_str,query_args)
                    if is_commit is False:
                        response = await curs.fetchall()
        else:
            #await apool.check() # not good! SLOW
            async with apool.connection() as aconn:
                # row_factory=dict_row
                async with aconn.cursor() as curs:
                    await curs.execute(query_str,query_args)
                    if is_commit is False:
                        response = await curs.fetchall()
    except:
        traceback.print_exc()

    return response


async def myfunnotworking():
    print("not working...")
    timea = time.time()
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        query_str = "COPY hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) FROM STDIN DELIMITER ','"
        stdin = ".SPX250620C5400,0,0,0,0,C,0,C,578.5,590.8,21,21,SPX,2025-06-20,C,5400,2025-06-18 20:00:28.66071"
        mylist = [stdin for x in range(1000)]
        async with apool.connection() as aconn:
            async with aconn.cursor() as curs:
                async with curs.copy(query_str) as copy:
                    for row in mylist:
                        await copy.write_row(row)
    timeb = time.time()
    print(timeb-timea)



async def myfunca():
    print("COPY")
    timea = time.time()
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        for x in range(1000):
            query_str = "COPY hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) FROM STDIN DELIMITER ','"
            stdin = ".SPX250620C5400,0,0,0,0,C,0,C,578.5,590.8,21,21,SPX,2025-06-20,C,5400,2025-06-18 20:00:28.66071"
            ok = await apostgres_copy(apool,query_str,stdin)
        timeb = time.time()
    print(timeb-timea)


async def myfuncaa():
    print("COPY2")
    timea = time.time()
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        async with apool.connection() as aconn:
            async with aconn.cursor() as curs:
                query_str = "COPY hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) FROM STDIN DELIMITER ','"
                stdin = ".SPX250620C5400,0,0,0,0,C,0,C,578.5,590.8,21,21,SPX,2025-06-20,C,5400,2025-06-18 20:00:28.66071"
                for x in range(1000):
                    async with curs.copy(query_str) as copy:
                        await copy.write(stdin)
    timeb = time.time()
    print(timeb-timea)



async def myfuncb():
    print("INSERT")
    timea = time.time()
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        for x in range(1000):
            cols = "event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp".split(",")
            colstr = ','.join(["%s"]*len(cols))
            query_str = f"""INSERT INTO hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) VALUES ({colstr})"""
            query_args = ('.SPX250620C5400',0,0,0,0,'C',0,'C',578.5,590.8,21,21,'SPX','2025-06-20','C',5400,'2025-06-18 20:00:28.66071' )
            await apostgres_execute(apool,query_str,query_args,is_commit=True)
    timeb = time.time()
    print(timeb-timea)


async def myfuncbb():
    print("INSERT2")
    timea = time.time()
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        
        cols = "event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp".split(",")
        colstr = ','.join(["%s"]*len(cols))
        query_str = f"""INSERT INTO hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) VALUES ({colstr})"""
        query_args = ('.SPX250620C5400',0,0,0,0,'C',0,'C',578.5,590.8,21,21,'SPX','2025-06-20','C',5400,'2025-06-18 20:00:28.66071' )
        async with apool.connection() as aconn:
            async with aconn.cursor() as curs:
                for x in range(1000):
                    await curs.execute(query_str,query_args)
    timeb = time.time()
    print(timeb-timea)

async def mycreate():
    print("CREATE")
    timea = time.time()
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:

        query_str = """
            CREATE TABLE IF NOT EXISTS hola (
                
                hola_id SERIAL,
                event_symbol text NOT NULL,
                event_time numeric,
                sequence numeric,
                time_nano_part numeric,
                bid_time numeric,
                bid_exchange_code text,
                ask_time numeric,
                ask_exchange_code text,
                bid_price double precision,
                ask_price double precision,
                bid_size double precision,
                ask_size double precision,
                ticker text,
                expiration TIMESTAMP,
                contract_type text,
                strike double precision,
                tstamp TIMESTAMP default (now() at time zone 'utc')
            )
        """
        query_args = ()
        await apostgres_execute(apool,query_str,query_args,is_commit=True)
    timeb = time.time()
    print(timeb-timea)

async def myfuncpipeline():
    print("PIPELINE")
    timea = time.time()

    cols = "event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp".split(",")
    colstr = ','.join(["%s"]*len(cols))
    query_str = f"""INSERT INTO hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) VALUES ({colstr})"""
    query_args = ('.SPX250620C5400',0,0,0,0,'C',0,'C',578.5,590.8,21,21,'SPX','2025-06-20','C',5400,'2025-06-18 20:00:28.66071' )

    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        async with apool.connection() as aconn:
            async with aconn.pipeline() as apipeline:
                for x in range(1000):
                    await aconn.execute(query_str, query_args, prepare=True)

    timeb = time.time()
    print(timeb-timea)

async def main():
    #await mycreate()
    #await myfuncb()
    #await myfunca()
    #await myfuncbb()
    #await myfuncaa()
    #await myfuncc()
    await myfuncpipeline()
    # print("both are slow!!!!!")
    

if __name__ == "__main__":
    asyncio.run(main())


"""

ssh hawktuah 
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17.2-bullseye

export POSTGRES_URI="postgres://postgres:postgres@192.168.68.156:5432/postgres"

2.8,2.5 seconds

docker stop postgres && docker rm postgres

docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=postgres timescale/timescaledb:latest-pg17

2.5,2.9 seconds


https://stackoverflow.com/questions/52548446/increase-data-insert-speed-of-postgresql

iostat -x 1


docker stop timescaledb && docker rm timescaledb

"""