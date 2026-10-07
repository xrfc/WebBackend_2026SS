import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from fastapi import Depends, HTTPException, WebSocket, WebSocketDisconnect, Query, Path
from pydantic import ValidationError
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from common.app import create_app
from common.config import settings
from common.auth import get_current_user, authenticate_token
from common.database import async_session_factory, engine, init_db
from common.lifecycle import check_dependencies
from common.models import Order, OrderStatus, OrderType, SeckillActivity, User
from common.redis_client import redis_client
from common.response import ApiResponse
from common.events import OrderEvent
from common.mq import connect_mq, persistent_message, RETRY_QUEUE, DEAD_QUEUE
from common.seckill_store import read_request, request_key
from common.limits import rate_limit

logger = logging.getLogger('order')
NOTIFICATION_CHANNEL = 'orders:notifications'


class PermanentOrderError(Exception):
    """Malformed/conflicting work requires inspection, not automatic retries."""


def serialize_order(row):
    return {'id': row.id, 'request_id': row.request_id, 'activity_id': row.activity_id,
        'product_id': row.product_id, 'product_name': row.product_name,
        'quantity': row.quantity, 'price': str(row.price), 'total_amount': str(row.total_amount),
        'status': row.status.value, 'order_type': row.order_type.value, 'created_at': row.created_at.isoformat()}


async def find_existing(db, event):
    existing = (await db.execute(select(Order).where(Order.request_id == str(event.request_id)))).scalar_one_or_none()
    if existing and (existing.user_id != event.user_id or existing.product_id != event.product_id
                     or existing.activity_id != str(event.activity_id)):
        raise PermanentOrderError('request identity conflict')
    return existing


async def create_order(event: OrderEvent):
    async with async_session_factory() as db:
        existing = await find_existing(db, event)
        if existing:
            return serialize_order(existing), False
        activity = await db.get(SeckillActivity, str(event.activity_id))
        if not activity or activity.product_id != event.product_id or int(activity.price * 100) != event.price_cents:
            raise PermanentOrderError('activity or price mismatch')
        record = await read_request(str(event.request_id))
        if not record:
            raise PermanentOrderError('reservation absent; reconciliation required')
        for field in ('request_id', 'activity_id', 'user_id', 'product_id', 'price_cents'):
            if str(record[field]) != str(getattr(event, field)):
                raise PermanentOrderError('reservation mismatch')
        if await db.get(User, event.user_id) is None:
            raise PermanentOrderError('user absent')
        amount = Decimal(event.price_cents) / 100
        row = Order(request_id=str(event.request_id), activity_id=str(event.activity_id),
            user_id=event.user_id, product_id=event.product_id, product_name=activity.product_name,
            quantity=1, price=amount, total_amount=amount, status=OrderStatus.CREATED, order_type=OrderType.SECKILL)
        db.add(row)
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            # Another consumer may have committed the same event concurrently.
            existing = await find_existing(db, event)
            if existing:
                return serialize_order(existing), False
            raise PermanentOrderError('unique activity/user conflict')
        return serialize_order(row), True


async def mark_request(request_id, state):
    # Never recreate a partially expired record with no canonical data.
    await redis_client.eval("if redis.call('EXISTS', KEYS[1]) == 1 then return redis.call('HSET', KEYS[1], 'state', ARGV[1]) end return 0",
        1, request_key(request_id), state)


class MQConsumer:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.connected = False

    async def close(self):
        self.connected = False
        if self.connection:
            await self.connection.close()
        self.connection = self.channel = None

    async def handle_message(self, message):
        event = None
        try:
            event = OrderEvent.model_validate_json(message.body)
            order, created = await create_order(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            permanent = isinstance(exc, (ValidationError, PermanentOrderError, UnicodeError, ValueError))
            try:
                attempts = int((message.headers or {}).get('attempts', 0))
                if not 0 <= attempts <= settings.MQ_MAX_RETRIES:
                    permanent = True
            except (TypeError, ValueError):
                attempts, permanent = 0, True
            dead = permanent or attempts >= settings.MQ_MAX_RETRIES
            target = DEAD_QUEUE if dead else RETRY_QUEUE
            headers = {'attempts': attempts + 1, 'failure_type': type(exc).__name__}
            try:
                forwarded = persistent_message(message.body, message.message_id or 'invalid-event', headers)
                await asyncio.wait_for(self.channel.default_exchange.publish(forwarded, routing_key=target, mandatory=True), 5)
                if dead and event:
                    try:
                        await mark_request(str(event.request_id), 'manual_review')
                    except Exception:
                        logger.warning('manual-review status update unavailable request=%s', event.request_id)
                await message.ack()  # only after confirmed forwarding; no silent loss
                logger.warning('order moved queue=%s request=%s failure=%s', target,
                    event.request_id if event else 'invalid', type(exc).__name__)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(1)
                await message.nack(requeue=True)
            return
        # SQL commit is the success boundary. Notification failure must not undo/recreate an order.
        try:
            await mark_request(str(event.request_id), 'created')
            if created:
                await redis_client.publish(NOTIFICATION_CHANNEL, json.dumps({'user_id': event.user_id,
                    'message': {'type': 'order_created', 'data': order}}))
        except Exception:
            logger.warning('order committed; notification/status unavailable request=%s', event.request_id)
        await message.ack()

    async def run(self):
        while True:
            try:
                self.connection, self.channel, _, queue = await connect_mq()
                await queue.consume(self.handle_message)
                while not self.connection.is_closed:
                    self.connected = self.connection.connected.is_set()
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning('consumer connection retry: %s', type(exc).__name__)
                await asyncio.sleep(1)
            finally:
                await self.close()


class ConnectionManager:
    def __init__(self):
        self.connections = {}

    async def connect(self, user_id, ws):
        if len(self.connections.get(user_id, {})) >= 5 or sum(map(len, self.connections.values())) >= 200:
            await ws.close(code=1013)
            return None
        queue = asyncio.Queue(maxsize=32)
        self.connections.setdefault(user_id, {})[ws] = queue
        return queue

    def disconnect(self, user_id, ws):
        group = self.connections.get(user_id)
        if group is not None:
            group.pop(ws, None)
            if not group:
                self.connections.pop(user_id, None)

    async def send_to_user(self, user_id, message):
        for ws, queue in list(self.connections.get(user_id, {}).items()):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                self.disconnect(user_id, ws)
                try:
                    await asyncio.wait_for(ws.close(code=1013), 1)
                except Exception:
                    pass

    async def write_messages(self, ws, queue):
        try:
            while True:
                item = await queue.get()
                await asyncio.wait_for(ws.send_json(item), 2)
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                await ws.close(code=1013)
            except Exception:
                pass


consumer = MQConsumer()
ws_manager = ConnectionManager()
notification_ready = False


async def notification_listener():
    global notification_ready
    while True:
        try:
            async with redis_client.pubsub() as pubsub:
                await pubsub.subscribe(NOTIFICATION_CHANNEL)
                notification_ready = True
                async for item in pubsub.listen():
                    if item['type'] == 'message':
                        data = json.loads(item['data'])
                        await ws_manager.send_to_user(data['user_id'], data['message'])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning('notification listener retry: %s', type(exc).__name__)
            await asyncio.sleep(1)
        finally:
            notification_ready = False


@asynccontextmanager
async def lifespan(app):
    await init_db()
    await redis_client.ping()
    tasks = [asyncio.create_task(consumer.run()), asyncio.create_task(notification_listener())]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await consumer.close()
    await redis_client.aclose()
    await engine.dispose()


app = create_app('Order Service', lifespan=lifespan)


@app.websocket('/ws/{user_id}')
async def websocket_endpoint(ws: WebSocket, user_id: int):
    origin = ws.headers.get('origin')
    allowed = {item.strip() for item in settings.CORS_ORIGINS.split(',')}
    if origin and origin not in allowed:
        await ws.close(code=1008)
        return
    await ws.accept()
    writer = None
    try:
        auth = ws.headers.get('authorization', '')
        if auth.startswith('Bearer '):
            token = auth[7:]
        else:
            payload = await asyncio.wait_for(ws.receive_json(), 5)
            token = payload.get('token', '') if isinstance(payload, dict) else ''
        if not isinstance(token, str) or len(token) > 4096:
            raise HTTPException(401, 'Invalid token')
        user = await authenticate_token(token)
        if int(user['sub']) != user_id:
            raise HTTPException(403, 'Forbidden')
        await rate_limit('ws:' + user['sub'], 10, 60)
        queue = await ws_manager.connect(user_id, ws)
        if queue is None:
            return
        writer = asyncio.create_task(ws_manager.write_messages(ws, queue))
        queue.put_nowait({'type': 'connected'})
        while True:
            user = await authenticate_token(token)  # checks expiry/revocation at least every 20 seconds
            timeout = min(20, max(0.1, user['exp'] - time.time()))
            try:
                text = await asyncio.wait_for(ws.receive_text(), timeout)
                if len(text.encode()) > 1024:
                    await ws.close(code=1009)
                    break
            except asyncio.TimeoutError:
                await ws_manager.send_to_user(user_id, {'type': 'heartbeat'})
    except HTTPException as exc:
        await ws.close(code=1013 if exc.status_code == 503 else 1008)
    except (WebSocketDisconnect, asyncio.TimeoutError, ValueError, TypeError, KeyError):
        try:
            await ws.close(code=1008)
        except Exception:
            pass
    finally:
        ws_manager.disconnect(user_id, ws)
        if writer:
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)


@app.get('/orders')
async def list_orders(user=Depends(get_current_user), page: int = Query(1, ge=1, le=10000), size: int = Query(10, ge=1, le=50)):
    async with async_session_factory() as db:
        filters = [Order.user_id == int(user['sub'])]
        total = (await db.execute(select(func.count()).select_from(Order).where(*filters))).scalar()
        rows = (await db.execute(select(Order).where(*filters).order_by(Order.created_at.desc(), Order.id.desc())
            .offset((page - 1) * size).limit(size))).scalars().all()
        return ApiResponse.ok({'total': total, 'page': page, 'size': size, 'items': [serialize_order(row) for row in rows]})


@app.get('/orders/{order_id}')
async def get_order(order_id: int = Path(ge=1, le=2147483647), user=Depends(get_current_user)):
    async with async_session_factory() as db:
        row = (await db.execute(select(Order).where(Order.id == order_id, Order.user_id == int(user['sub'])))).scalar_one_or_none()
        if not row:
            raise HTTPException(404, '订单不存在')
        return ApiResponse.ok(serialize_order(row))


@app.get('/ready')
async def ready():
    await check_dependencies()
    if not consumer.connected or not notification_ready:
        raise HTTPException(503, '消息消费者或通知订阅尚未就绪')
    return {'status': 'ready'}
