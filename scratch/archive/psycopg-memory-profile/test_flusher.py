
import numpy as np
import json
import asyncio
from utils.postgres_utils import (
    psycopg,psycopg_pool,postgres_uri,
)
from utils.data_tasty import PgInsertQueue,flusher

json_file = '/mnt/hd1/data/tastyfi/NDX/2025-07-06/NDX/candle/2025-07-06-13-58-41.412458-uid-5e5d385a2ead43478c0a29bf950305b6.json'
async def main():
    total_event_count = 10000
    max_lifetime = 25200
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False,max_lifetime=max_lifetime) as apool:
        async with apool.connection() as aconn:
            async with aconn.pipeline() as apipeline:
                myqueue = PgInsertQueue()
                flusher_task = asyncio.create_task(flusher(myqueue, aconn))
                for x in range(total_event_count):
                    ticker = 'NDX'
                    with open(json_file,'r') as f:
                        content = json.loads(f.read())
                    streamer_symbol = 'NDX'
                    await myqueue.push(ticker,streamer_symbol,'candle',content)
                    num = np.random.randint(1,4) if np.random.rand() < 0.01 else 0.001
                    await asyncio.sleep(num)

    # Wait a bit to ensure all transactions are processed
    await asyncio.sleep(5)
    
    # clean up
    flusher_task.cancel()
    try:
        await flusher_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())

