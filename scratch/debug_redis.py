import os
import redis
import redis
from redis.commands.json.path import Path

uri = os.environ.get("REDIS_URI")
print(uri)

r = redis.from_url(uri,decode_responses=True)
r.ping()
if True:
    myvalue = {'a':1,'b':'c'}
    mykey = 'mykey:10-10-01'
    r.json().set(mykey, Path.root_path(), myvalue)
    myvalue = {'a':2,'b':'z'}
    mykey = 'mykey:10-10-02'
    r.json().set(mykey, Path.root_path(), myvalue)
    res = r.delete("user:2")
    print(res)
res = r.hscan("mykey:",cursor=0,match="*")
print(res)
# HSCAN myhash 0 MATCH order_*