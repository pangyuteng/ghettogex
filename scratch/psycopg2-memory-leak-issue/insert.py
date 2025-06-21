import os
import asyncio
import traceback
import datetime
import pandas as pd
import numpy as np

import psycopg
import psycopg_pool
from psycopg.rows import dict_row


async def apostgres_execute(apool,query_str,query_args,is_commit=False):
    response = None
    try:
        await apool.check()
        async with apool.connection() as aconn:
            async with aconn.cursor(row_factory=dict_row) as curs:
                await curs.execute(query_str,query_args)
                if is_commit is False:
                    response = await curs.fetchall()
    except:
        traceback.print_exc()

    return response

async def apostgres_execute_many(apool,query_dict):
    response = None
    try:
        await apool.check()
        async with apool.connection() as aconn:
            async with aconn.cursor(row_factory=dict_row) as curs:
                for query_str,query_list in query_dict.items():
                    await curs.executemany(query_str,query_list)
    except:
        traceback.print_exc()
    return response

import gc
import os, psutil

postgres_uri = os.environ.get("POSTGRES_URI")
async def insert_job():

    count = 0
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        while True:
            query_dict = {}
            tstamp = datetime.datetime.now()

            cols = 'foofoo,tstamp,'+','.join([f'value{x}' for x in range(100)])
            slist = ','.join(['%s']*102)
            query_str = """INSERT INTO foobar ("""+cols+""") VALUES("""+slist+""")"""
            cols = cols.split(",")
            df = pd.DataFrame([],columns=cols)
            row_count = 10000
            df['foofoo']=np.array(['baz']*row_count)
            df['tstamp']=tstamp
            for field_name in cols:
                if field_name in ['foofoo','tstamp']:
                    continue
                df[field_name]=np.random.rand(row_count)

            async def get_args(row):
                query_args = [row[x] for x in cols]
                return query_args

            query_dict[query_str] = await asyncio.gather(*(get_args(row) for n,row in df.iterrows()))
            if False:
                await apostgres_execute_many(apool,query_dict)
            if True:
                mylist = []
                for v in query_dict.values():
                    for query_args in v:
                        f = apostgres_execute(apool,query_str,query_args,is_commit=True)
                        mylist.append(f)

                await asyncio.gather(*mylist)
        
            if count % 500 == 0:
                print(count,"done",datetime.datetime.now())
                print('After work: ', psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2, 'MB')
            count+=1
            if count > 100000:
                break

if __name__ == "__main__":
    process = psutil.Process(os.getpid())
    print('Before any work: ', process.memory_info().rss / 1024 ** 2, 'MB')
    asyncio.run(insert_job())
    gc.collect()
    print('After work: ', psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2, 'MB')


"""

psycopg[binary,pool]==3.2.9 have memory leak ???

[x]
3.18G/61.3G 2025-06-18 21:11:53
3.28G/61.3G 2025-06-18 21:16:52
3.49G/61.3G 2025-06-18 21:23:55
3.59G/61.3G 2025-06-18 21:27:50
4.01G/61.3G 2025-06-18 21:41:10

psycopg[binary,pool]==3.2.3
2.69G/61.3G 2025-06-18 21:52:25
2.99G/61.3G 2025-06-18 21:58:17
3.37G/61.3G 2025-06-18 22:12:53
3.46G/61.3G 2025-06-18 22:15:49
3.94G/61.3G 2025-06-18 22:32:43

moved timescaldb to seperate machine 'runner'

1.76G/61.3GB 2025-06-18 22:54:37
kept running overnight
memory still at 1.7GB at 2025-06-19 07:30:00

https://stackoverflow.com/questions/70214871/is-there-a-memory-leak-or-do-i-not-understand-garbage-collection-and-memory-mana

"""
