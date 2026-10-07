"""Bounded, read-only observations. Samples across stores are not an atomic snapshot."""
import asyncio
from datetime import datetime, timezone
import httpx
from fastapi import HTTPException
from pydantic import ValidationError
from redis.exceptions import RedisError, ResponseError
from sqlalchemy import select, func
from common.config import settings
from common.database import async_session_factory
from common.events import OrderEvent
from common.models import SeckillActivity, Product, Order
from common.mq import ORDER_QUEUE, RETRY_QUEUE, DEAD_QUEUE
from common.redis_client import redis_client
from common.seckill_store import OUTBOX, OUTBOX_GROUP, activity_key, users_key, request_key
from common.order_lookup import fetch_order_by_request


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def order_view(row):
    return {'id': row.id, 'request_id': row.request_id, 'activity_id': row.activity_id,
        'user_id': row.user_id, 'product_id': row.product_id, 'product_name': row.product_name,
        'price': str(row.price), 'quantity': row.quantity, 'status': row.status.value if row.status else 'unknown',
        'created_at': row.created_at.isoformat() + 'Z' if row.created_at else None}


async def queue_observations():
    # Management API is internal only. Never read/consume/requeue messages to count them.
    async with httpx.AsyncClient(timeout=httpx.Timeout(2, connect=1),
        auth=(settings.RABBITMQ_USER, settings.RABBITMQ_PASS), trust_env=False) as client:
        async def sample(name):
            try:
                response = await client.get(f'http://{settings.RABBITMQ_HOST}:15672/api/queues/%2F/{name}')
                if response.status_code != 200:
                    return {'name': name, 'available': False, 'reason': 'not_found' if response.status_code == 404 else 'unavailable'}
                data = response.json()
                if not isinstance(data, dict):
                    raise ValueError('invalid statistics')
                counts = [data.get(key) for key in ('messages_ready', 'messages_unacknowledged', 'messages')]
                if any(type(value) is not int or value < 0 for value in counts):
                    return {'name': name, 'available': False, 'reason': 'statistics_pending'}
                return {'name': name, 'available': True, 'ready': counts[0], 'unacknowledged': counts[1], 'total': counts[2]}
            except (httpx.HTTPError, ValueError, TypeError):
                return {'name': name, 'available': False, 'reason': 'unavailable'}
        return await asyncio.gather(*[sample(name) for name in (ORDER_QUEUE, RETRY_QUEUE, DEAD_QUEUE)])


async def redis_activity(row):
    result = {'available': False, 'remaining_stock': None, 'accepted_users': None, 'state': None}
    try:
        async with redis_client.pipeline(transaction=False) as pipe:
            pipe.hmget(activity_key(row.id), 'stock', 'state')
            pipe.hlen(users_key(row.id))
            stock_state, count = await pipe.execute()
        raw, state = stock_state
        stock = int(raw) if raw is not None else None
        if stock is None or str(stock) != raw or not 0 <= stock <= row.stock or state not in ('initializing', 'active', 'closed'):
            result['reason'] = 'missing_or_invalid'
        else:
            result.update(available=True, remaining_stock=stock, accepted_users=count, state=state)
    except (RedisError, ValueError, TypeError):
        result['reason'] = 'missing_or_invalid'
    return result


async def stream_observation():
    result = {'available': False, 'length': None, 'pending': None, 'limit': settings.MAX_OUTBOX_LENGTH, 'preview': []}
    try:
        result['length'] = await redis_client.xlen(OUTBOX)
        for stream_id, fields in await redis_client.xrange(OUTBOX, count=10):
            try:
                event = OrderEvent.model_validate_json(fields.get('data', ''))
                result['preview'].append({'stream_id': stream_id, 'valid': True,
                    'request_id': str(event.request_id), 'activity_id': str(event.activity_id),
                    'product_id': event.product_id, 'price_cents': event.price_cents})
            except (ValidationError, ValueError, TypeError):
                result['preview'].append({'stream_id': stream_id, 'valid': False})
        try:
            result['pending'] = (await redis_client.xpending(OUTBOX, OUTBOX_GROUP))['pending']
        except ResponseError as exc:
            if 'NOGROUP' not in str(exc):
                raise
            result['pending'] = 0
        result['available'] = True
    except RedisError:
        result['reason'] = 'unavailable_or_invalid'
    return result


async def overview(limit=25):
    # Bound SQL work to the displayed activities; no Redis KEYS/SCAN of business data.
    async with async_session_factory() as db:
        rows = (await db.execute(select(SeckillActivity, Product.stock)
            .outerjoin(Product, Product.id == SeckillActivity.product_id)
            .order_by(SeckillActivity.created_at.desc(), SeckillActivity.id.desc()).limit(limit))).all()
        ids = [row.id for row, _ in rows]
        counts = dict((await db.execute(select(Order.activity_id, func.count(Order.id))
            .where(Order.activity_id.in_(ids)).group_by(Order.activity_id))).all()) if ids else {}
        orders = (await db.execute(select(Order).where(Order.request_id.is_not(None))
            .order_by(Order.id.desc()).limit(20))).scalars().all()
    activity_samples = await asyncio.gather(*[redis_activity(row) for row, _ in rows])
    activities = []
    for (row, product_stock), redis in zip(rows, activity_samples):
        activities.append({'id': row.id, 'product_id': row.product_id, 'product_name': row.product_name,
            'initial_stock': row.stock, 'price': str(row.price), 'state': row.state,
            'released_stock': row.released_stock, 'product_available_stock': product_stock,
            'starts_at': row.starts_at, 'ends_at': row.ends_at, 'orders_created': counts.get(row.id, 0), 'redis': redis})
    stream, queues = await asyncio.gather(stream_observation(), queue_observations())
    return {'sampled_at': timestamp(), 'atomic_snapshot': False, 'activity_limit': limit,
        'activities': activities, 'recent_orders': [order_view(row) for row in orders],
        'outbox': stream, 'queues': queues}


async def inspect_request(request_id):
    order = await fetch_order_by_request(request_id)
    record = None
    reason = None
    try:
        fields = await redis_client.hgetall(request_key(request_id))
        if fields:
            event = OrderEvent.model_validate_json(fields.get('data', ''))
            if str(event.request_id) != request_id or fields.get('state') not in ('queued', 'created', 'manual_review'):
                raise ValueError('invalid record')
            record = {**event.model_dump(mode='json'), 'state': fields['state']}
    except (RedisError, ValidationError, ValueError, TypeError):
        reason = 'unavailable_or_invalid'
    if not order and not record:
        raise HTTPException(503 if reason else 404, '记录不可用，需核查' if reason else '请求不存在或 Redis 记录已过保留期')
    if not order and record['state'] == 'created':
        order = await fetch_order_by_request(request_id)
    status = 'created' if order else ('inconsistent' if record['state'] == 'created' else record['state'])
    return {'sampled_at': timestamp(), 'request_id': request_id,
        'status': status, 'order': order_view(order) if order else None,
        'redis_record': record, 'redis_reason': reason,
        'broker_location': 'not_tracked', 'accepted_evidence': bool(record or order)}
