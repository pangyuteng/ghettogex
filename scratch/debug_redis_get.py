
import os
import datetime
import time
import numpy as np
import json
import redis
from redis.commands.json.path import Path

uri = os.environ.get("REDIS2_URI")
print(uri)

r = redis.from_url(uri,decode_responses=True)
r.ping()

ticker = 'NDX'
key_list = r.keys("quote:*")

timea = time.time()
res = r.json().mget(key_list,Path.root_path())
for x in res:
    print(x)
print(len(res))
timeb = time.time()
print(timeb-timea)


"""

keys quote:NDXP:*

json.get quote:NDXP:2025-06-25:C:21180.0

"""