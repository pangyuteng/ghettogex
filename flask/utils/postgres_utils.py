import logging
logger = logging.getLogger(__file__)

import datetime
import os
import sys
import traceback
import asyncio
import psycopg
import psycopg_pool
from psycopg.rows import dict_row
postgres_uri = os.environ.get("POSTGRES_URI")

async def cpostgres_execute(aconn,query_str,query_args,is_commit=False):
    response = None
    try:
        async with aconn.cursor(row_factory=dict_row) as curs:
            async with aconn.transaction() as tx:
                await curs.execute(query_str,query_args)
                if is_commit is False:
                    response = await curs.fetchall()
    except:
        traceback.print_exc()
    return response

async def cpostgres_execute_many(aconn,query_dict):
    response = None
    try:
        async with aconn.cursor() as curs:
            async with aconn.transaction() as tx:
                coros = [curs.executemany(query_str,query_list) for query_str,query_list in query_dict.items()]
                await asyncio.gather(*coros)
    except:
        traceback.print_exc()
    return response

async def apostgres_execute(apool,query_str,query_args,is_commit=False):
    response = None
    try:
        if apool is None:
            async with await psycopg.AsyncConnection.connect(postgres_uri,autocommit=True,row_factory=dict_row) as aconn:
                    async with aconn.cursor(row_factory=dict_row) as curs:
                        await curs.execute(query_str,query_args)
                        if is_commit is False:
                            response = await curs.fetchall()
        else:
            async with apool.connection() as aconn:
                async with aconn.cursor(row_factory=dict_row) as curs:
                    await curs.execute(query_str,query_args)
                    if is_commit is False:
                        response = await curs.fetchall()
                logger.debug(f"apostgres_execute...")
    except:
        traceback.print_exc()

    return response

async def apostgres_execute_many(apool,query_dict):
    response = None
    try:
        async with apool.connection() as aconn:
            async with aconn.cursor(row_factory=dict_row) as curs:
                coros = [curs.executemany(query_str,query_list) for query_str,query_list in query_dict.items()]
                await asyncio.gather(*coros)
    except:
        traceback.print_exc()
    return response

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

def postgres_execute_many(query_dict):
    response = None
    try:
        with psycopg.connect(postgres_uri,row_factory=dict_row) as conn:
            with conn.cursor() as curs:
                for query_str,query_list in query_dict.items():
                    curs.executemany(query_str,query_list)
                conn.commit()
    except:
        traceback.print_exc()
    return response

def vaccum_full_analyze():
    query = """vacuum full analyze"""
    postgres_execute(query,(),is_commit=True)


"""
('INSERT INTO Quote (eventSymbol,eventTime,sequence,timeNanoPart,bidTime,bidExchangeCode,askTime,askExchangeCode,bidPrice,askPrice,bidSize,askSize) VALUES 
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',)                                                                                                        
['SPX', 0, 0, 0, 0, '', 0, '', 5655.39, 5756.98, None, None]

"""