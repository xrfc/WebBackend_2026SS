"""Redis admission + durable outbox. All keys share one hash tag; single Redis is supported."""
import json
from common.config import settings
from common.redis_client import redis_client

PREFIX = 'seckill:{seckill}:'
OUTBOX = PREFIX + 'outbox'
OUTBOX_GROUP = 'rabbit-relay'


def activity_key(activity_id):
    return PREFIX + 'activity:' + str(activity_id)


def users_key(activity_id):
    return PREFIX + 'users:' + str(activity_id)


def request_key(request_id):
    return PREFIX + 'request:' + str(request_id)


def active_key(product_id):
    return PREFIX + 'active:' + str(product_id)


INIT_LUA = '''
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'id', ARGV[1], 'product_id', ARGV[2], 'stock', ARGV[3],
    'price_cents', ARGV[4], 'starts_at', ARGV[5], 'ends_at', ARGV[6], 'state', 'initializing')
redis.call('EXPIREAT', KEYS[1], ARGV[7])
return 1
'''
ACTIVATE_LUA = '''
if redis.call('HGET', KEYS[1], 'state') ~= 'initializing' then return 0 end
redis.call('HSET', KEYS[1], 'state', 'active')
redis.call('SET', KEYS[2], ARGV[1])
redis.call('EXPIREAT', KEYS[2], ARGV[2])
return 1
'''
FREEZE_LUA = '''
local state = redis.call('HGET', KEYS[1], 'state')
local raw = redis.call('HGET', KEYS[1], 'stock')
local stock = tonumber(raw)
if not state or not stock or stock < 0 or stock > 100000 or tostring(stock) ~= raw then return -1 end
local active_type = redis.call('TYPE', KEYS[2])['ok']
if active_type ~= 'none' and active_type ~= 'string' then return -1 end
redis.call('HSET', KEYS[1], 'state', 'closed')
if redis.call('GET', KEYS[2]) == ARGV[1] then redis.call('DEL', KEYS[2]) end
return tonumber(redis.call('HGET', KEYS[1], 'stock'))
'''
# Check every type before mutation: a Lua runtime error does not roll back preceding writes.
SUBMIT_LUA = '''
local function typ(key)
    local result = redis.call('TYPE', key)
    return result['ok']
end
if typ(KEYS[1]) ~= 'hash' then return {-1, ''} end
local t2, t3, t4 = typ(KEYS[2]), typ(KEYS[3]), typ(KEYS[4])
if (t2 ~= 'none' and t2 ~= 'hash') or (t3 ~= 'none') or (t4 ~= 'none' and t4 ~= 'stream') then
    return {-5, ''}
end
local previous = redis.call('HGET', KEYS[2], ARGV[1])
if previous then return {2, previous} end
local now = tonumber(redis.call('TIME')[1])
local raw_stock = redis.call('HGET', KEYS[1], 'stock')
local stock = tonumber(raw_stock)
local price = tonumber(redis.call('HGET', KEYS[1], 'price_cents'))
local starts = tonumber(redis.call('HGET', KEYS[1], 'starts_at'))
local ends = tonumber(redis.call('HGET', KEYS[1], 'ends_at'))
local product_id = tonumber(redis.call('HGET', KEYS[1], 'product_id'))
if not stock or stock < 0 or stock > 100000 or tostring(stock) ~= raw_stock
    or not price or price <= 0 or price > 9999999999 or price ~= math.floor(price)
    or not starts or not ends or starts < 1 or ends > 2147483647 or ends <= starts
    or starts ~= math.floor(starts) or ends ~= math.floor(ends) or ends - starts > 86400
    or not product_id or product_id < 1 or product_id > 2147483647 or product_id ~= math.floor(product_id)
    or redis.call('HGET', KEYS[1], 'id') ~= ARGV[5] then return {-5, ''} end
if redis.call('HGET', KEYS[1], 'state') ~= 'active' or now >= ends then return {-3, ''} end
if now < starts then return {-4, ''} end
if stock <= 0 then return {0, ''} end
if t4 == 'stream' and redis.call('XLEN', KEYS[4]) >= tonumber(ARGV[3]) then return {-6, ''} end
local data = cjson.encode({version=1, kind='order.create', request_id=ARGV[2],
    activity_id=redis.call('HGET', KEYS[1], 'id'),
    product_id=tonumber(redis.call('HGET', KEYS[1], 'product_id')),
    user_id=tonumber(ARGV[1]), price_cents=price})
redis.call('XADD', KEYS[4], '*', 'data', data)
redis.call('HINCRBY', KEYS[1], 'stock', -1)
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], 'data', data, 'state', 'queued')
local retention_until = ends + tonumber(ARGV[4])
redis.call('EXPIREAT', KEYS[2], retention_until)
redis.call('EXPIREAT', KEYS[3], retention_until)
return {1, ARGV[2]}
'''


async def initialize_activity(row):
    return await redis_client.eval(INIT_LUA, 1, activity_key(row.id), row.id, row.product_id,
        row.stock, int(row.price * 100), row.starts_at, row.ends_at,
        row.ends_at + settings.REQUEST_RETENTION_SECONDS)


async def activate_activity(row):
    return await redis_client.eval(ACTIVATE_LUA, 2, activity_key(row.id), active_key(row.product_id),
        row.id, row.ends_at + settings.REQUEST_RETENTION_SECONDS)


async def reserve(activity_id, user_id, request_id):
    return await redis_client.eval(SUBMIT_LUA, 4, activity_key(activity_id), users_key(activity_id),
        request_key(request_id), OUTBOX, user_id, request_id,
        settings.MAX_OUTBOX_LENGTH, settings.REQUEST_RETENTION_SECONDS, str(activity_id))


async def read_request(request_id):
    value = await redis_client.hgetall(request_key(request_id))
    if not value:
        return None
    return {**json.loads(value['data']), 'state': value['state']}
