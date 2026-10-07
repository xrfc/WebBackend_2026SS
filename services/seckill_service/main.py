import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from uuid import UUID
from fastapi import Depends, HTTPException, Path, Query, Response
from pydantic import BaseModel, ConfigDict, Field, StrictInt
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from redis.exceptions import ResponseError
from common.app import create_app
from common.auth import get_current_user, require_role
from common.config import settings
from common.database import async_session_factory, engine, init_db
from common.lifecycle import check_dependencies
from common.models import Product, ProductStatus, SeckillActivity
from common.redis_client import redis_client
from common.response import ApiResponse
from common.validation import ID, Money
from common.limits import rate_limit
from common.mq import connect_mq, persistent_message
from common.order_lookup import fetch_order_by_request
from common.seckill_store import (
    OUTBOX, OUTBOX_GROUP, activity_key, active_key, initialize_activity, activate_activity,
    reserve, read_request, FREEZE_LUA,
)

logger = logging.getLogger('seckill')


class SeckillStartRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    product_id: ID
    stock: StrictInt = Field(ge=1, le=100000)
    seckill_price: Money
    starts_at: StrictInt | None = Field(None, ge=1, le=2147483647)
    duration_seconds: StrictInt = Field(3600, ge=1, le=86400)


class SeckillSubmitRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    product_id: ID
    activity_id: UUID | None = None


async def prepare_activity(activity_id):
    async with async_session_factory() as db:
        candidate = await db.get(SeckillActivity, activity_id)
        if candidate is None:
            return
        await db.execute(select(Product).where(Product.id == candidate.product_id).with_for_update())
        row = (await db.execute(select(SeckillActivity).where(SeckillActivity.id == activity_id)
            .with_for_update().execution_options(populate_existing=True))).scalar_one()
        if row.state == 'closed':
            return
        if row.state == 'initializing':
            await initialize_activity(row)  # never overwrites an existing Redis reservation
            row.state = 'active'
            await db.commit()
        # Active SQL state + missing Redis data is unsafe to rebuild: refuse admission.
        if not await redis_client.exists(activity_key(row.id)):
            raise RuntimeError('Active activity Redis state missing; requires reconciliation')
        await activate_activity(row)


async def close_activity(activity_id):
    async with async_session_factory() as db:
        candidate = await db.get(SeckillActivity, activity_id)
        if candidate is None:
            raise HTTPException(404, '活动不存在')
        product = (await db.execute(select(Product).where(Product.id == candidate.product_id)
            .with_for_update())).scalar_one()
        row = (await db.execute(select(SeckillActivity).where(SeckillActivity.id == activity_id)
            .with_for_update().execution_options(populate_existing=True))).scalar_one()
        if row.state == 'closed':
            return {'activity_id': row.id, 'released_stock': row.released_stock, 'state': 'closed'}
        if row.state == 'initializing' and not await redis_client.exists(activity_key(row.id)):
            # No admission was possible before activation.
            remaining = row.stock
        else:
            remaining = await redis_client.eval(FREEZE_LUA, 2, activity_key(row.id), active_key(row.product_id), row.id)
            if remaining < 0:
                raise HTTPException(503, '活动状态缺失，暂停库存回收，需人工核对')
        product.stock += remaining
        row.released_stock = remaining
        row.active_product_id = None
        row.state = 'closed'
        await db.commit()
        return {'activity_id': row.id, 'released_stock': remaining, 'state': 'closed'}


async def maintain_activities():
    while True:
        try:
            async with async_session_factory() as db:
                rows = (await db.execute(select(SeckillActivity).where(SeckillActivity.state != 'closed'))).scalars().all()
            for row in rows:
                if row.ends_at <= time.time():
                    await close_activity(row.id)
                else:
                    await prepare_activity(row.id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning('activity maintenance deferred: %s', type(exc).__name__)
        await asyncio.sleep(5)


class OutboxRelay:
    def __init__(self):
        self.consumer = 'relay-' + str(uuid.uuid4())
        self.connection = None
        self.channel = None
        self.exchange = None
        self.connected = False
        self.claim_cursor = '0-0'

    async def close(self):
        self.connected = False
        if self.connection:
            await self.connection.close()
        self.connection = self.channel = self.exchange = None

    async def publish_entries(self, entries):
        for stream_id, fields in entries:
            data = json.loads(fields['data'])
            await asyncio.wait_for(self.exchange.publish(persistent_message(
                fields['data'].encode(), data['request_id']), routing_key='order.create', mandatory=True), 5)
            # Crash after confirmed publish but before this transaction => duplicate delivery, handled in SQL.
            async with redis_client.pipeline(transaction=True) as pipe:
                pipe.xack(OUTBOX, OUTBOX_GROUP, stream_id)
                pipe.xdel(OUTBOX, stream_id)
                await pipe.execute()

    async def run(self):
        while True:
            try:
                try:
                    await redis_client.xgroup_create(OUTBOX, OUTBOX_GROUP, id='0', mkstream=True)
                except ResponseError as exc:
                    if 'BUSYGROUP' not in str(exc):
                        raise
                if self.connection is None or self.connection.is_closed:
                    self.connection, self.channel, self.exchange, _ = await connect_mq()
                self.connected = True
                claimed = await redis_client.xautoclaim(OUTBOX, OUTBOX_GROUP, self.consumer,
                    min_idle_time=30000, start_id=self.claim_cursor, count=20)
                self.claim_cursor = claimed[0]
                if claimed[1]:
                    await self.publish_entries(claimed[1])
                streams = await redis_client.xreadgroup(OUTBOX_GROUP, self.consumer,
                    streams={OUTBOX: '>'}, count=20, block=1000)
                for _, entries in streams:
                    await self.publish_entries(entries)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                logger.warning('outbox retained for retry: %s', type(exc).__name__)
                await self.close()
                await asyncio.sleep(1)


relay = OutboxRelay()


@asynccontextmanager
async def lifespan(app):
    await init_db()
    await redis_client.ping()
    tasks = [asyncio.create_task(relay.run()), asyncio.create_task(maintain_activities())]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await relay.close()
    await redis_client.aclose()
    await engine.dispose()


app = create_app('Seckill Service', lifespan=lifespan)


@app.post('/seckill/start')
async def start_seckill(req: SeckillStartRequest, user=Depends(require_role('admin'))):
    now = int(time.time())
    starts = req.starts_at if req.starts_at is not None else now
    if starts < now - 5 or starts > now + 86400 or starts + req.duration_seconds > 2147483647:
        raise HTTPException(422, '开始时间应在现在至未来 24 小时内')
    async with async_session_factory() as db:
        product = (await db.execute(select(Product).where(Product.id == req.product_id).with_for_update())).scalar_one_or_none()
        if not product:
            raise HTTPException(404, '商品不存在')
        if product.status != ProductStatus.ON_SALE:
            raise HTTPException(409, '商品已下架')
        exists = (await db.execute(select(SeckillActivity.id).where(SeckillActivity.active_product_id == product.id))).scalar_one_or_none()
        if exists:
            return ApiResponse.fail(409, '该商品已有活动，不能重置库存', {'activity_id': exists})
        if product.stock < req.stock:
            raise HTTPException(409, '商品可用库存不足')
        row = SeckillActivity(id=str(uuid.uuid4()), product_id=product.id, active_product_id=product.id,
            stock=req.stock, price=req.seckill_price, product_name=product.name,
            starts_at=starts, ends_at=starts + req.duration_seconds, state='initializing')
        product.stock -= req.stock
        db.add(row)
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            raise HTTPException(409, '该商品已有活动')
    try:
        await prepare_activity(row.id)
    except Exception as exc:
        logger.warning('activity initialization pending activity=%s type=%s', row.id, type(exc).__name__)
        return ApiResponse.fail(503, '库存已预留，活动初始化待重试；请勿重新创建', {'activity_id': row.id})
    return ApiResponse.ok({'activity_id': row.id, 'product_id': row.product_id, 'seckill_stock': row.stock,
        'seckill_price': str(row.price), 'starts_at': row.starts_at, 'ends_at': row.ends_at}, '秒杀活动已创建')


@app.post('/seckill/activities/{activity_id}/close')
async def close(activity_id: UUID, user=Depends(require_role('admin'))):
    return ApiResponse.ok(await close_activity(str(activity_id)), '活动已关闭，未售库存已释放')


async def request_result(request_id, user_id):
    def completed(order):
        if order.user_id != user_id:
            raise HTTPException(404, '抢购记录不存在')
        return {'request_id': request_id, 'status': 'created', 'order_id': order.id,
            'activity_id': order.activity_id, 'product_id': order.product_id}
    order = await fetch_order_by_request(request_id)
    if order:
        return completed(order)
    record = await read_request(request_id)
    if not record or record['user_id'] != user_id:
        raise HTTPException(404, '抢购记录不存在或已过保留期')
    if record['state'] == 'created':
        # The consumer may commit between the first SQL read and the Redis read.
        # A fresh transaction sees that commit under MySQL REPEATABLE READ.
        order = await fetch_order_by_request(request_id)
        if order:
            return completed(order)
        raise HTTPException(503, '订单状态不一致，需核查；请勿重复回收库存')
    return {'request_id': request_id, 'status': record['state'], 'activity_id': record['activity_id'],
        'product_id': record['product_id'], 'order_id': None}


@app.post('/seckill/submit')
async def submit_seckill(req: SeckillSubmitRequest, user=Depends(get_current_user)):
    await rate_limit('seckill:' + user['sub'], settings.SECKILL_RATE_LIMIT, 1)
    activity_id = str(req.activity_id) if req.activity_id else await redis_client.get(active_key(req.product_id))
    if not activity_id:
        raise HTTPException(404, '秒杀活动不存在')
    product_id = await redis_client.hget(activity_key(activity_id), 'product_id')
    if product_id is None:
        raise HTTPException(503, '活动状态不可用，请联系管理员核对')
    if int(product_id) != req.product_id:
        raise HTTPException(422, '活动与商品不匹配')
    request_id = str(uuid.uuid4())
    code, accepted_id = await reserve(activity_id, int(user['sub']), request_id)
    errors = {0: (409, '库存已抢完'), -1: (503, '活动状态不可用'), -3: (409, '活动已结束'),
              -4: (409, '活动尚未开始'), -5: (503, '活动数据异常，已暂停受理'), -6: (503, '订单积压过多，请稍后重试')}
    if code in errors:
        raise HTTPException(*errors[code])
    if code not in (1, 2):
        raise HTTPException(503, '抢购结果异常')
    result = await request_result(accepted_id, int(user['sub']))
    result['duplicate'] = code == 2
    # HTTP 202 denotes accepted work, not an already-created order.
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=202 if result['status'] == 'queued' else 200,
        content=ApiResponse.ok(result, '请求已记录，可查询处理结果'))


@app.get('/seckill/requests/{request_id}')
async def get_result(request_id: UUID, user=Depends(get_current_user)):
    return ApiResponse.ok(await request_result(str(request_id), int(user['sub'])))


@app.get('/seckill/stock/{product_id}')
async def stock(product_id: int = Path(ge=1, le=2147483647)):
    activity_id = await redis_client.get(active_key(product_id))
    if not activity_id:
        raise HTTPException(404, '秒杀活动不存在')
    row = await redis_client.hgetall(activity_key(activity_id))
    if not row or 'stock' not in row:
        raise HTTPException(503, '活动状态不可用')
    return ApiResponse.ok({'activity_id': activity_id, 'product_id': product_id,
        'remaining_stock': int(row['stock']), 'state': row['state'], 'ends_at': int(row['ends_at'])})


@app.get('/ready')
async def ready():
    await check_dependencies()
    return {'status': 'ready', 'mq_connected': relay.connected, 'outbox_length': await redis_client.xlen(OUTBOX)}


@app.get('/seckill/observe/overview')
async def observe_overview(response: Response, limit: int = Query(25, ge=1, le=50), user=Depends(require_role('admin'))):
    from common.observation import overview
    await rate_limit('observe:' + user['sub'], 36, 60)
    data = await overview(limit)
    data['relay_connected'] = relay.connected
    response.headers['Cache-Control'] = 'no-store'
    return ApiResponse.ok(data)


@app.get('/seckill/observe/requests/{request_id}')
async def observe_request(request_id: UUID, response: Response, user=Depends(require_role('admin'))):
    from common.observation import inspect_request
    await rate_limit('observe-request:' + user['sub'], 30, 60)
    response.headers['Cache-Control'] = 'no-store'
    return ApiResponse.ok(await inspect_request(str(request_id)))
