import os
import asyncio
import traceback
import datetime
import pandas as pd
import numpy as np

import psycopg
import psycopg_pool
from psycopg.rows import dict_row

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

postgres_uri = os.environ.get("POSTGRES_URI")
async def background_subscribe():
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
        while True:
            query_dict = {}
            tstamp = datetime.datetime.now()
            gex_strike_query_str = """
                INSERT INTO gex_strike (ticker,strike,tstamp,volume_gex,state_gex,dex,convexity,vex,cex,
                call_convexity,call_oi,call_dex,call_gex,call_vex,call_cex,
                put_convexity,put_oi,put_dex,put_gex,put_vex,put_cex
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s)
                on conflict (ticker,strike,tstamp) do update set 
                volume_gex = %s,state_gex = %s,dex = %s,convexity = %s,vex = %s,cex = %s,
                call_convexity = %s,call_oi = %s,call_dex = %s,call_gex = %s,call_vex = %s,call_cex = %s,
                put_convexity = %s,put_oi = %s,put_dex = %s,put_gex = %s,put_vex = %s,put_cex = %s
            """
            
            cols = 'ticker,strike,tstamp,volume_gex,state_gex,dex,convexity,vex,cex,call_convexity,call_oi,call_dex,call_gex,call_vex,call_cex,put_convexity,put_oi,put_dex,put_gex,put_vex,put_cex'.split(",")
            df = pd.DataFrame([],columns=cols)
            row_count = 100000
            df['ticker']=np.array(['SPX']*row_count)
            df['strike']=np.arange(0,row_count)
            df['tstamp']=tstamp
            for field_name in cols:
                if field_name in ['ticker','strike','tstamp']:
                    continue
                df[field_name]=np.random.rand(row_count)

            async def insert_gex_strike(row):
                query_args = [row.ticker,row.strike,row.tstamp,row.volume_gex,row.state_gex,row.dex,row.convexity,row.vex,row.cex,
                row.call_convexity,row.call_oi,row.call_dex,row.call_gex,row.call_vex,row.call_cex,
                row.put_convexity,row.put_oi,row.put_dex,row.put_gex,row.put_vex,row.put_cex,
                row.volume_gex,row.state_gex,row.dex,row.convexity,row.vex,row.cex,
                row.call_convexity,row.call_oi,row.call_dex,row.call_gex,row.call_vex,row.call_cex,
                row.put_convexity,row.put_oi,row.put_dex,row.put_gex,row.put_vex,row.put_cex]
                return query_args

            query_dict[gex_strike_query_str] = await asyncio.gather(*(insert_gex_strike(row) for n,row in df.iterrows()))
            #print(len(query_dict[gex_strike_query_str]))
            await apostgres_execute_many(apool,query_dict)
            for query_args in query_dict.values():
                await apostgres_execute(apool,gex_strike_query_str,query_args,is_commit=True)


if __name__ == "__main__":
    asyncio.run(background_subscribe())


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

"""
