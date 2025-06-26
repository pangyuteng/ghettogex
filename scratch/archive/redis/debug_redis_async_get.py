
import os
import datetime
import time
import numpy as np
import json
import pandas as pd

import asyncio
import redis.asyncio as redis
from redis.commands.json.path import Path

uri = os.environ.get("REDIS2_URI")
print(uri)

async def main():
    pool = redis.ConnectionPool.from_url(uri,decode_responses=True)
    client = redis.Redis.from_pool(pool)

    await client.ping()
    ticker = 'NDXP'
    expiration = '2025-06-25'
    pattern = f'quote:{ticker}:{expiration}:*'
    key_list = await client.keys(pattern)

    timea = time.time()
    res = await client.json().mget(key_list,Path.root_path())
    timeb = time.time()
    print(timeb-timea)

    await client.aclose()
    await pool.aclose()

    df = pd.DataFrame(res)
    print(df.head())
    print(df.columns)
    print(df.shape)


if __name__ == "__main__":
    output = asyncio.run(main())
