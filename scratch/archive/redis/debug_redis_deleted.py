
import redis.asyncio as redis
from redis.client import Redis
from redis.commands.json.path import Path as redisPath

redis2_uri = os.environ.get("REDIS2_URI")

async def ok():
    redispool = redis.ConnectionPool.from_url(redis2_uri,decode_responses=True)
    redisclient = redis.Redis.from_pool(redispool)

    try:
        if redisclient is not None:
            if event_type == 'quote' and expiration is not None:
                rediskey = f'quote:{ticker}:{expiration}:{contract_type}:{strike}'
                redisvalue = {k:postgres_friendly(v) for k,v in event_dict.items()}
                redisvalue = json.loads(json.dumps(redisvalue,default=str))
                await redisclient.json().set(rediskey, redisPath.root_path(), redisvalue, decode_keys=True)
    except:
        logger.error(traceback.format_exc())


    await redisclient.ping()
    await redisclient.aclose()
    await redispool.aclose()