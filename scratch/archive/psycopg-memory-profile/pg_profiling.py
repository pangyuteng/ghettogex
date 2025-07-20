


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



import os
import sys
import traceback
import time
import datetime
import numpy as np
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

async def mycreatetimescale():
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
        ) WITH (
        tsdb.hypertable=true,
        tsdb.partition_column='tstamp',
        tsdb.segmentby='ticker',
        tsdb.orderby='tstamp DESC'
        );
        CALL add_columnstore_policy('hola', after => INTERVAL '1d');
        """
        query_args = ()
        await apostgres_execute(apool,query_str,query_args,is_commit=True)
        # query_str = """
        # create index hola_tstamp_event_symbol_index on hola using brin (tstamp,event_symbol) WITH (timescaledb.transaction_per_chunk);
        # create index hola_tstamp_ticker_index on hola using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
        # """
        # query_args = ()
        # await apostgres_execute(None,query_str,query_args,is_commit=True)
    timeb = time.time()
    print(timeb-timea)

async def mycreatepostgres():
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
    max_lifetime = 25200
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False,max_lifetime=max_lifetime) as apool:
        async with apool.connection() as aconn:
            async with aconn.pipeline() as apipeline:
                for x in range(2000):
                    async with aconn.cursor(row_factory=dict_row) as curs:
                        async with aconn.transaction() as tx:
                            await curs.execute(query_str,query_args)

    timeb = time.time()
    print(timeb-timea)


async def myfuncpipeline2():
    print("PIPELINE")

    cols = "event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp".split(",")
    colstr = ','.join(["%s"]*len(cols))
    query_str = f"""INSERT INTO hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) VALUES ({colstr})"""
    query_args = ('.SPX250620C5400',0,0,0,0,'C',0,'C',578.5,590.8,21,21,'SPX','2025-06-20','C',5400,'2025-06-18 20:00:28.66071' )
    max_lifetime = 25200
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False,max_lifetime=max_lifetime) as apool:
        async with apool.connection() as aconn:
            async with aconn.pipeline() as apipeline:
                while True:
                    timea = time.time()
                    async with aconn.cursor() as curs:
                        for x in range(1000):
                            await curs.execute(query_str,query_args)

                    timeb = time.time()
                    print(timeb-timea)

async def myfuncpipelineSIMULATE():
    print("PIPELINE")
    cols = "event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp".split(",")
    colstr = ','.join(["%s"]*len(cols))
    query_str = f"""INSERT INTO hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) VALUES ({colstr})"""
    query_args = ('.SPX250620C5400',0,0,0,0,'C',0,'C',578.5,590.8,21,21,'SPX','2025-06-20','C',5400,'2025-06-18 20:00:28.66071' )
    max_lifetime = 25200
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False,max_lifetime=max_lifetime) as apool:
        async with apool.connection() as aconn:
            async with aconn.pipeline() as apipeline:
                while True:
                    bolus_size = np.random.randint(10,1000)
                    #print(bolus_size)
                    timea = time.time()
                    for x in range(bolus_size):
                        async with aconn.cursor(row_factory=dict_row) as curs:
                            async with aconn.transaction() as tx:
                                await curs.execute(query_str,query_args)
                    timeb = time.time()
                    duration = timeb-timea
                    print(f"done. row_count:{bolus_size} duration(sec):{duration} rows/sec:{bolus_size/duration}",)
                    await asyncio.sleep(np.random.rand())



async def myfuncpipeline3():
    print("PIPELINE")
    cols = "event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp".split(",")
    colstr = ','.join(["%s"]*len(cols))
    query_str = f"""INSERT INTO hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) VALUES ({colstr})"""
    query_args = ('.SPX250620C5400',0,0,0,0,'C',0,'C',578.5,590.8,21,21,'SPX','2025-06-20','C',5400,'2025-06-18 20:00:28.66071' )
    # print(query_str)
    # print(query_args)
    # sys.exit(1)
    query_list= [query_args for x in range(2000)]
    max_lifetime = 25200
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False,max_lifetime=max_lifetime) as apool:
        async with apool.connection() as aconn:
            async with aconn.pipeline() as apipeline:
                for x in range(10):
                    timea = time.time()
                    async with aconn.cursor() as curs:
                        await curs.executemany(query_str,query_list,returning=False)
                    await aconn.commit()
                    timeb = time.time()
                    print(timeb-timea)

async def myfuncpipeline4():
    print("PIPELINE")
    cols = "event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp".split(",")
    colstr = ','.join(["%s"]*len(cols))
    
    query_args = ('.SPX250620C5400',0,0,0,0,'C',0,'C',578.5,590.8,21,21,'SPX','2025-06-20','C',5400,'2025-06-18 20:00:28.66071' )
    query_list = ','.join([str(query_args) for x in range(2000)])
    query_str = f"""INSERT INTO hola (event_symbol,event_time,sequence,time_nano_part,bid_time,bid_exchange_code,ask_time,ask_exchange_code,bid_price,ask_price,bid_size,ask_size,ticker,expiration,contract_type,strike,tstamp) VALUES {query_list}"""

    max_lifetime = 25200
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False,max_lifetime=max_lifetime) as apool:
        async with apool.connection() as aconn:
            async with aconn.pipeline() as apipeline:
                for x in range(1000):
                    timea = time.time()
                    async with aconn.cursor() as curs:
                        await curs.execute(query_str,())
                    await aconn.commit()
                    timeb = time.time()
                    print(timeb-timea)


async def mymonitor():
    await mycreatetimescale()
    print("MONITOR")
    max_lifetime = 25200
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False,max_lifetime=max_lifetime) as apool:
        async with apool.connection() as aconn:
            async with aconn.pipeline() as apipeline:
                while True:
                    async with aconn.cursor(row_factory=dict_row) as curs:
                        async with aconn.transaction() as tx:
                            query_str = "select count(1) from hola"
                            await curs.execute(query_str,())
                            response = await curs.fetchall()
                            fetched = [dict(x) for x in response]
                            print(fetched)
                    await asyncio.sleep(1)
    



async def main():
    #await mycreate()
    #await myfuncb()
    #await myfunca()
    # print("both are slow!!!!!")
    #await myfuncbb()
    #await myfuncaa()
    #await myfuncc()
    #await myfuncpipeline()
    #await myfuncpipeline2()
    #await myfuncpipeline3()
    await myfuncpipeline4()

async def mainsim():
    coros = [myfuncpipelineSIMULATE() for x in range(5)]
    await asyncio.gather(*coros)

if __name__ == "__main__":
    if sys.argv[1] == "monitor":
        asyncio.run(mymonitor())
    elif sys.argv[1] == "sim":
        asyncio.run(mainsim())
    elif sys.argv[1] == "ok":
        asyncio.run(main())
    else:
        raise NotImplementedError()

"""

ssh hawktuah 
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17.2-bullseye

export POSTGRES_URI="postgres://postgres:postgres@192.168.68.139:5432/postgres"


2.8,2.5 seconds

docker stop postgres && docker rm postgres

docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e TS_TUNE_MAX_CONNS=2000 timescale/timescaledb:latest-pg17

2.5,2.9 seconds


https://stackoverflow.com/questions/52548446/increase-data-insert-speed-of-postgresql

iostat -x 1

docker stop timescaledb && docker rm timescaledb



"""