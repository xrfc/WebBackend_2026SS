from fastapi import HTTPException
from common.redis_client import redis_client

RATE_LIMIT_LUA = '''
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('EXPIRE', KEYS[1], ARGV[2]) end
return count
'''


async def rate_limit(key: str, limit: int, seconds: int):
    count = await redis_client.eval(RATE_LIMIT_LUA, 1, 'limit:' + key, limit, seconds)
    if count > limit:
        raise HTTPException(429, '请求过于频繁，请稍后重试')
