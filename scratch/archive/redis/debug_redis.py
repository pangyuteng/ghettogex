
import os
import datetime
import time
import numpy as np
import json
import redis
from redis.commands.json.path import Path

uri = os.environ.get("REDIS_URI")
print(uri)

r = redis.from_url(uri,decode_responses=True)
r.ping()

ticker = 'SPX'
if False:
    key_list = []
    for contract_type in ['call','put']:
        for strike in np.arange(1000,5000,5):
            key = f'quote:{ticker}:{contract_type}:{strike}'
            key_list.append(key)
    for x in key_list:
        r.delete(x)

if False:
    timea = time.time()
    for contract_type in ['call','put']:
        for strike in np.arange(1000,5000,5):
            tstamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            key = f'quote:{ticker}:{contract_type}:{strike}'
            value = {
                'bid_price': float(np.random.rand()),
                'ask_price': float(np.random.rand()),
                'event_symbol': f'{ticker}:{contract_type}:{strike}',
                'tstamp': tstamp
            }
            #res = r.hset(key, mapping=value)
            r.json().set(key, Path.root_path(), value, decode_keys=True)
    timeb = time.time()
    print(timeb-timea)

key_list = []
for contract_type in ['call','put']:
    for strike in np.arange(1000,5000,5):
        key = f'quote:{ticker}:{contract_type}:{strike}'
        key_list.append(key)

if True:
    timea = time.time()
    contract_type = 'call'
    strike = 1000
    firstkey = f'quote:{ticker}:{contract_type}:{strike}'
    res = r.json().get(firstkey)
    print(res)
    timeb = time.time()
    print(timeb-timea)

if True:
    timea = time.time()
    res = r.json().mget(key_list,Path.root_path())
    #print(res)
    print(len(res))
    timeb = time.time()
    print(timeb-timea)


# r.json().mget

"""

kubectl port-forward --address 0.0.0.0 svc/postgres -n gg 5432:5432
kubectl port-forward --address 0.0.0.0 svc/redis -n gg 6379:6379

docker run -it \
-e CACHE_FOLDER="/mnt/hd1/data/fi" \
-e CACHE_TASTY_FOLDER="/mnt/hd1/data/tastyfi" \
-e POSTGRES_URI="postgres://postgres:postgres@192.168.68.143:5432/postgres" \
-e REDIS_URI="redis://192.168.68.143:6379/1" \
-w $PWD -v /mnt:/mnt \
-p 80:80 -p 8888:8888 \
fi-notebook:latest bash

"""