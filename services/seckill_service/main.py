"""秒杀服务 - Redis原子扣减 + MQ异步削峰"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import json
import uuid
import asyncio
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field

from common.config import settings
from common.auth import get_current_user
from common.response import ApiResponse

# ==================== 测试模式检测 ====================
TEST_MODE = os.environ.get("TEST_MODE", "").lower() == "true"

if TEST_MODE:
    import fakeredis.aioredis as aioredis
else:
    import redis.asyncio as aioredis
    import aio_pika

# ==================== Redis 连接 ====================

if TEST_MODE:
    redis_client = None
else:
    redis_client: "aioredis.Redis" = None

# Redis中秒杀库存key前缀
SECKILL_STOCK_PREFIX = "seckill:stock:"
# 已购买用户集合key前缀
SECKILL_USERS_PREFIX = "seckill:users:"

# Lua脚本：原子扣减库存
LUA_DECREMENT_STOCK = """
local stock_key = KEYS[1]
local stock = redis.call('get', stock_key)
if not stock then
    return -1  -- 秒杀不存在
end
stock = tonumber(stock)
if stock <= 0 then
    return 0  -- 库存不足
end
redis.call('decr', stock_key)
return 1  -- 扣减成功
"""

# Lua脚本：原子扣减库存 + 防重复购买
LUA_DECREMENT_WITH_DEDUP = """
local stock_key = KEYS[1]
local users_key = KEYS[2]
local user_id = ARGV[1]

-- 检查是否已购买
local already = redis.call('sismember', users_key, user_id)
if already == 1 then
    return -2  -- 已购买过
end

-- 检查并扣减库存
local stock = redis.call('get', stock_key)
if not stock then
    return -1  -- 秒杀不存在
end
stock = tonumber(stock)
if stock <= 0 then
    return 0  -- 库存不足
end

redis.call('decr', stock_key)
redis.call('sadd', users_key, user_id)
return 1  -- 扣减成功
"""


# ==================== MQ连接 ====================


class MQPublisher:
    """MQ消息发布器 - 生产模式用RabbitMQ, 测试模式用内存队列"""

    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        if TEST_MODE:
            return
        self.connection = await aio_pika.connect_robust(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            login=settings.RABBITMQ_USER,
            password=settings.RABBITMQ_PASS,
        )
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            "seckill_exchange", aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue = await self.channel.declare_queue("order_create_queue", durable=True)
        await queue.bind(self.exchange, routing_key="order.create")

    async def publish_order(self, order_data: dict):
        """发布订单创建消息"""
        if TEST_MODE:
            from common.test_mq import get_queue
            await get_queue().put(order_data)
            return
        message = aio_pika.Message(
            body=json.dumps(order_data).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(uuid.uuid4()),
            content_type="application/json",
        )
        await self.exchange.publish(message, routing_key="order.create")

    async def close(self):
        if TEST_MODE:
            return
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()


mq_publisher = MQPublisher()


# ==================== 请求模型 ====================

class SeckillStartRequest(BaseModel):
    product_id: int = Field(..., gt=0)
    stock: int = Field(..., ge=1, le=100000)
    seckill_price: float = Field(..., gt=0)


class SeckillSubmitRequest(BaseModel):
    product_id: int = Field(..., gt=0)


# ==================== FastAPI 应用 ====================

app = FastAPI(title="Seckill Service", version="1.0.0",
              description="秒杀服务 - Redis原子扣减 + MQ削峰")


@app.on_event("startup")
async def startup():
    global redis_client
    if TEST_MODE:
        redis_client = aioredis.FakeRedis(decode_responses=True)
    else:
        redis_client = aioredis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
        )
        await redis_client.ping()
    await mq_publisher.connect()
    app.state.lua_decrement = redis_client.register_script(LUA_DECREMENT_WITH_DEDUP)


@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.close()
    await mq_publisher.close()


# ==================== API接口 ====================

@app.post("/seckill/start", summary="开启秒杀活动（管理员）")
async def start_seckill(
    req: SeckillStartRequest,
    user: dict = Depends(get_current_user)
):
    """管理员开启秒杀活动，将库存预热到Redis"""
    if user.get("role") != "admin":
        return ApiResponse.fail(403, "仅管理员可操作")

    stock_key = f"{SECKILL_STOCK_PREFIX}{req.product_id}"
    # 设置秒杀库存
    await redis_client.set(stock_key, req.stock)
    # 同时缓存秒杀价格
    await redis_client.set(f"seckill:price:{req.product_id}", req.seckill_price, ex=3600)
    # 清除之前的购买记录
    await redis_client.delete(f"{SECKILL_USERS_PREFIX}{req.product_id}")
    # 设置过期时间1小时
    await redis_client.expire(stock_key, 3600)

    return ApiResponse.ok({
        "product_id": req.product_id,
        "seckill_stock": req.stock,
        "seckill_price": req.seckill_price,
    }, "秒杀活动已开启")


@app.post("/seckill/submit", summary="提交秒杀请求")
async def submit_seckill(
    req: SeckillSubmitRequest,
    user: dict = Depends(get_current_user)
):
    """用户提交秒杀请求 - Redis原子扣减 + MQ异步下单"""
    user_id = user["sub"]
    product_id = req.product_id
    stock_key = f"{SECKILL_STOCK_PREFIX}{product_id}"
    users_key = f"{SECKILL_USERS_PREFIX}{product_id}"

    # 执行Lua脚本：原子扣减库存 + 防重复
    lua = redis_client.register_script(LUA_DECREMENT_WITH_DEDUP)
    result = await lua(keys=[stock_key, users_key], args=[user_id])

    if result == -1:
        return ApiResponse.fail(400, "秒杀活动不存在")
    elif result == -2:
        return ApiResponse.fail(400, "您已经参与过本次秒杀")
    elif result == 0:
        return ApiResponse.fail(400, "库存已抢完")

    # 扣减成功 → 投递MQ消息
    seckill_price = await redis_client.get(f"seckill:price:{product_id}")
    order_data = {
        "user_id": int(user_id),
        "product_id": product_id,
        "quantity": 1,
        "price": float(seckill_price) if seckill_price else 0,
        "order_type": "seckill",
        "create_time": datetime.now(timezone.utc).isoformat(),
    }
    await mq_publisher.publish_order(order_data)

    return ApiResponse.ok(
        {"status": "queuing", "product_id": product_id},
        "抢购请求已接收，正在排队处理中..."
    )


@app.get("/seckill/stock/{product_id}", summary="查询秒杀库存")
async def get_seckill_stock(product_id: int):
    """查询Redis中剩余秒杀库存"""
    stock = await redis_client.get(f"{SECKILL_STOCK_PREFIX}{product_id}")
    if stock is None:
        return ApiResponse.fail(404, "秒杀活动不存在")
    return ApiResponse.ok({"product_id": product_id, "remaining_stock": int(stock)})


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
