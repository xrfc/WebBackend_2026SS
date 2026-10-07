import os
import time
import uuid
import asyncio
import pytest
from redis.asyncio import Redis
from common.seckill_store import INIT_LUA, ACTIVATE_LUA, SUBMIT_LUA, activity_key, active_key, users_key, request_key, OUTBOX

pytestmark = [pytest.mark.realredis, pytest.mark.skipif(not os.getenv('TEST_REDIS_URL'), reason='dedicated real Redis not configured')]


async def test_real_redis_atomic_stock_dedup_outbox():
    # Use only a dedicated local test instance. Never FLUSH a shared deployment.
    url = os.environ['TEST_REDIS_URL']
    assert url == 'redis://127.0.0.1:6397/15'
    redis = Redis.from_url(url, decode_responses=True)
    await redis.flushdb()
    aid = str(uuid.uuid4())
    now = int(time.time())
    await redis.eval(INIT_LUA, 1, activity_key(aid), aid, 1, 10, 1, now - 1, now + 100, now + 1000)
    await redis.eval(ACTIVATE_LUA, 2, activity_key(aid), active_key(1), aid, now + 1000)
    async def submit(uid):
        rid = str(uuid.uuid4())
        return await redis.eval(SUBMIT_LUA, 4, activity_key(aid), users_key(aid), request_key(rid), OUTBOX, uid, rid, 100, 1000, aid)
    results = await asyncio.gather(*[submit(uid) for uid in range(1, 101)])
    assert sum(result[0] == 1 for result in results) == 10
    assert await redis.hget(activity_key(aid), 'stock') == '0'
    assert await redis.xlen(OUTBOX) == 10
    duplicates = await asyncio.gather(*[submit(uid) for uid, result in enumerate(results, 1) if result[0] == 1])
    assert all(result[0] == 2 for result in duplicates)
    assert await redis.xlen(OUTBOX) == 10
    await redis.flushdb()
    await redis.aclose()
