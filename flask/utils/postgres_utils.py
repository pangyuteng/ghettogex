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
                for query_str,query_list in query_dict.items():
                    await curs.executemany(query_str,query_list)
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

def manage_table_partition(utc_tstamp):

    table_list = 'candle,event,greeks,profile,quote,summary,theoprice,timeandsale,trade,underlying,gex_strike,gex_net,event_agg'.split(",")

    month_stamp = f"{utc_tstamp.strftime('%Y')}-{utc_tstamp.strftime('%m')}-01"
    next_month_utc_tstamp = utc_tstamp+datetime.timedelta(weeks=5)
    next_month_stamp = f"{next_month_utc_tstamp.strftime('%Y')}-{next_month_utc_tstamp.strftime('%m')}-01"

    for table_name in table_list:
        new_part_table_name = f"{table_name}_{utc_tstamp.strftime('%Y')}_{utc_tstamp.strftime('%m')}"

        try:
            print(f"creating {new_part_table_name}")
            create_query = f"""
                CREATE TABLE {new_part_table_name} PARTITION OF {table_name}
                FOR VALUES FROM ('{month_stamp}') TO ('{next_month_stamp}');
            """
            postgres_execute(create_query,(),is_commit=True)
            print("done")
        except:
            traceback.print_exc()

    for table_name in table_list:
        old_part_table_name = f"{table_name}_{utc_tstamp.year-1}_{utc_tstamp.strftime('%m')}"
        try:
            print(f"dropping {old_part_table_name}")
            drop_query = f"""
            ALTER TABLE {table_name} DETACH PARTITION {old_part_table_name};
            DROP TABLE {old_part_table_name};
            """
            postgres_execute(drop_query,(),is_commit=True)
            print("done")
        except:
            traceback.print_exc()


"""
('INSERT INTO Quote (eventSymbol,eventTime,sequence,timeNanoPart,bidTime,bidExchangeCode,askTime,askExchangeCode,bidPrice,askPrice,bidSize,askSize) VALUES 
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',)                                                                                                        
['SPX', 0, 0, 0, 0, '', 0, '', 5655.39, 5756.98, None, None]

"""