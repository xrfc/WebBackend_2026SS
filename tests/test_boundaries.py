import asyncio
import time
import uuid
from decimal import Decimal
from unittest.mock import AsyncMock
import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import select, func
from common.database import async_session_factory
from common.models import Product, Order
from common.auth import authenticate_token
from common.seckill_store import activity_key, OUTBOX, reserve, read_request
from common.events import OrderEvent
from services.product_service import main as product
from services.seckill_service import main as seckill
from services.order_service import main as order
from services.user_service import main as user
from services.gateway import main as gateway
from services.ai_service import main as ai


def client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test')


def headers(token):
    return {'Authorization': 'Bearer ' + token}


async def activity(tokens, stock=5, product_stock=10, **kwargs):
    async with client(product.app) as api:
        response = await api.post('/products', headers=headers(tokens['admin']),
            json={'name': '测试商品', 'price': '99.99', 'stock': product_stock})
        assert response.status_code == 200, response.text
        pid = response.json()['data']['id']
    async with client(seckill.app) as api:
        response = await api.post('/seckill/start', headers=headers(tokens['admin']),
            json={'product_id': pid, 'stock': stock, 'seckill_price': '0.01', **kwargs})
        assert response.status_code == 200, response.text
    return pid, response.json()['data']['activity_id']


@pytest.mark.parametrize('field,value', [
    ('price', '0'), ('price', '-1'), ('price', '0.001'), ('price', 'NaN'), ('price', 'Infinity'),
    ('price', '100000000'), ('stock', -1), ('stock', 100001), ('stock', True), ('stock', '1'),
    ('name', ''), ('name', 'x' * 201), ('description', 'x' * 5001),
])
async def test_product_invalid_values(tokens, field, value):
    data = {'name': '商品', 'price': '1.00', 'stock': 5, field: value}
    async with client(product.app) as api:
        response = await api.post('/products', headers=headers(tokens['admin']), json=data)
        assert response.status_code == 422
    async with async_session_factory() as db:
        assert (await db.execute(select(func.count()).select_from(Product))).scalar() == 0


@pytest.mark.parametrize('price', ['0.01', '99999999.99', '1.0'])
async def test_money_boundaries_are_exact(tokens, price):
    async with client(product.app) as api:
        response = await api.post('/products', headers=headers(tokens['admin']),
            json={'name': '商品', 'price': price, 'stock': 0})
        assert response.status_code == 200
        assert Decimal(response.json()['data']['price']) == Decimal(price)


@pytest.mark.parametrize('field,value', [('stock', 0), ('stock', -1), ('stock', 100001),
    ('stock', True), ('product_id', 0), ('product_id', 2147483648), ('duration_seconds', 0),
    ('duration_seconds', 86401), ('starts_at', 0), ('seckill_price', 'NaN'), ('seckill_price', '0.001')])
async def test_activity_invalid_values(tokens, field, value):
    async with client(seckill.app) as api:
        response = await api.post('/seckill/start', headers=headers(tokens['admin']),
            json={'product_id': 1, 'stock': 1, 'seckill_price': '1.00', field: value})
        assert response.status_code == 422


async def test_reserve_stock_and_repeated_start_cannot_reset(tokens, isolated_state):
    pid, aid = await activity(tokens, stock=5)
    async with client(seckill.app) as api:
        response = await api.post('/seckill/start', headers=headers(tokens['admin']),
            json={'product_id': pid, 'stock': 5, 'seckill_price': '0.01'})
        assert response.status_code == 409
        assert response.json()['data']['activity_id'] == aid
    async with async_session_factory() as db:
        assert (await db.get(Product, pid)).stock == 5
    assert await isolated_state.hget(activity_key(aid), 'stock') == '5'


async def test_missing_delisted_or_insufficient_product(tokens):
    async with client(seckill.app) as api:
        response = await api.post('/seckill/start', headers=headers(tokens['admin']),
            json={'product_id': 999, 'stock': 1, 'seckill_price': '1.00'})
        assert response.status_code == 404
    async with client(product.app) as api:
        response = await api.post('/products', headers=headers(tokens['admin']), json={'name': '商品', 'stock': 1, 'price': '1.00'})
        pid = response.json()['data']['id']
    async with client(seckill.app) as api:
        response = await api.post('/seckill/start', headers=headers(tokens['admin']),
            json={'product_id': pid, 'stock': 2, 'seckill_price': '1.00'})
        assert response.status_code == 409
    async with client(product.app) as api:
        assert (await api.put(f'/products/{pid}/delist', headers=headers(tokens['admin']))).status_code == 200
    async with client(seckill.app) as api:
        response = await api.post('/seckill/start', headers=headers(tokens['admin']),
            json={'product_id': pid, 'stock': 1, 'seckill_price': '1.00'})
        assert response.status_code == 409


async def test_duplicate_returns_original_without_second_decrement(tokens, isolated_state):
    pid, aid = await activity(tokens)
    async with client(seckill.app) as api:
        responses = await asyncio.gather(*[api.post('/seckill/submit', headers=headers(tokens['customer']),
            json={'product_id': pid, 'activity_id': aid}) for _ in range(20)])
    assert all(response.status_code == 202 for response in responses)
    ids = {response.json()['data']['request_id'] for response in responses}
    assert len(ids) == 1
    assert await isolated_state.hget(activity_key(aid), 'stock') == '4'
    assert await isolated_state.xlen(OUTBOX) == 1
    assert sum(not response.json()['data']['duplicate'] for response in responses) == 1


async def test_concurrent_unique_users_cannot_oversell(tokens, isolated_state):
    _, aid = await activity(tokens, stock=3)
    requests = [str(uuid.uuid4()) for _ in range(50)]
    results = await asyncio.gather(*[reserve(aid, uid + 10, rid) for uid, rid in enumerate(requests)])
    assert sum(result[0] == 1 for result in results) == 3
    assert sum(result[0] == 0 for result in results) == 47
    assert await isolated_state.hget(activity_key(aid), 'stock') == '0'
    assert await isolated_state.xlen(OUTBOX) == 3


@pytest.mark.parametrize('change,error', [('future', 409), ('ended', 409), ('missing_price', 503), ('corrupt_stock', 503)])
async def test_state_boundaries_never_consume_stock(tokens, isolated_state, change, error):
    pid, aid = await activity(tokens)
    if change == 'future':
        await isolated_state.hset(activity_key(aid), 'starts_at', int(time.time()) + 100)
    elif change == 'ended':
        await isolated_state.hset(activity_key(aid), mapping={'starts_at': int(time.time()) - 100, 'ends_at': int(time.time()) - 1})
    elif change == 'missing_price':
        await isolated_state.hdel(activity_key(aid), 'price_cents')
    else:
        await isolated_state.hset(activity_key(aid), 'stock', 'NaN')
    before = await isolated_state.hget(activity_key(aid), 'stock')
    async with client(seckill.app) as api:
        response = await api.post('/seckill/submit', headers=headers(tokens['customer']), json={'product_id': pid, 'activity_id': aid})
        assert response.status_code == error
    assert await isolated_state.hget(activity_key(aid), 'stock') == before
    assert await isolated_state.xlen(OUTBOX) == 0


async def test_wrong_key_type_does_not_partially_decrement(tokens, isolated_state):
    _, aid = await activity(tokens)
    await isolated_state.set(OUTBOX, 'wrong-type')
    result = await reserve(aid, 2, str(uuid.uuid4()))
    assert result[0] == -5
    assert await isolated_state.hget(activity_key(aid), 'stock') == '5'


async def test_backlog_limit_rejects_without_stock_change(tokens, isolated_state, monkeypatch):
    monkeypatch.setattr(seckill.settings, 'MAX_OUTBOX_LENGTH', 1)
    pid, aid = await activity(tokens)
    assert (await reserve(aid, 2, str(uuid.uuid4())))[0] == 1
    async with client(seckill.app) as api:
        response = await api.post('/seckill/submit', headers=headers(tokens['other']), json={'product_id': pid})
        assert response.status_code == 503
    assert await isolated_state.hget(activity_key(aid), 'stock') == '4'


async def test_close_releases_only_unsold_stock_exactly_once(tokens, isolated_state):
    pid, aid = await activity(tokens, stock=8)
    await reserve(aid, 2, str(uuid.uuid4()))
    await reserve(aid, 3, str(uuid.uuid4()))
    async with client(seckill.app) as api:
        for _ in range(2):
            response = await api.post(f'/seckill/activities/{aid}/close', headers=headers(tokens['admin']))
            assert response.status_code == 200
            assert response.json()['data']['released_stock'] == 6
    async with async_session_factory() as db:
        assert (await db.get(Product, pid)).stock == 8
    assert (await reserve(aid, 4, str(uuid.uuid4())))[0] == -3


async def test_missing_active_redis_state_never_reinitializes_inventory(tokens, isolated_state):
    _, aid = await activity(tokens)
    await reserve(aid, 2, str(uuid.uuid4()))
    await isolated_state.delete(activity_key(aid))
    with pytest.raises(RuntimeError, match='reconciliation'):
        await seckill.prepare_activity(aid)
    assert not await isolated_state.exists(activity_key(aid))
    with pytest.raises(HTTPException) as err:
        await seckill.close_activity(aid)
    assert err.value.status_code == 503


async def test_order_redelivery_is_idempotent_and_cent_exact(tokens, isolated_state):
    _, aid = await activity(tokens)
    rid = str(uuid.uuid4())
    await reserve(aid, 2, rid)
    record = await read_request(rid)
    event = OrderEvent.model_validate({k: v for k, v in record.items() if k != 'state'})
    first, created = await order.create_order(event)
    assert created and first['price'] == '0.01'
    for _ in range(10):
        repeated, created = await order.create_order(event)
        assert not created and repeated['id'] == first['id']
    async with async_session_factory() as db:
        assert (await db.execute(select(func.count()).select_from(Order))).scalar() == 1
    assert (await seckill.request_result(rid, 2))['status'] == 'created'
    with pytest.raises(HTTPException) as err:
        await seckill.request_result(rid, 3)
    assert err.value.status_code == 404


async def test_unreserved_event_or_wrong_price_cannot_create_order(tokens):
    pid, aid = await activity(tokens)
    event = OrderEvent(version=1, kind='order.create', request_id=uuid.uuid4(), activity_id=aid,
        user_id=2, product_id=pid, price_cents=1)
    with pytest.raises(order.PermanentOrderError):
        await order.create_order(event)
    event.price_cents = 100
    with pytest.raises(order.PermanentOrderError):
        await order.create_order(event)
    async with async_session_factory() as db:
        assert (await db.execute(select(func.count()).select_from(Order))).scalar() == 0


async def test_logout_revokes_same_token_across_services(tokens):
    async with client(user.app) as api:
        response = await api.post('/logout', headers=headers(tokens['customer']))
        assert response.status_code == 200
    async with client(order.app) as api:
        response = await api.get('/orders', headers=headers(tokens['customer']))
        assert response.status_code == 401
    assert (await authenticate_token(tokens['other']))['sub'] == '3'


async def test_auth_redis_failure_fails_closed(tokens, monkeypatch):
    from common import auth
    from redis.exceptions import ConnectionError
    monkeypatch.setattr(auth.redis_client, 'exists', AsyncMock(side_effect=ConnectionError('down')))
    with pytest.raises(HTTPException) as err:
        await authenticate_token(tokens['customer'])
    assert err.value.status_code == 503


@pytest.mark.parametrize('path', ['/products', '/seckill/start'])
async def test_customer_cannot_administer(tokens, path):
    target = product.app if path == '/products' else seckill.app
    data = {'name': '商品', 'price': '1.00'} if path == '/products' else {'product_id': 1, 'stock': 1, 'seckill_price': '1.00'}
    async with client(target) as api:
        assert (await api.post(path, headers=headers(tokens['customer']), json=data)).status_code == 403


async def test_oversized_body_and_utf8_password(tokens):
    async with client(user.app) as api:
        assert (await api.post('/register', content=b'x' * 65537)).status_code == 413
        assert (await api.post('/register', json={'username': 'test', 'password': '汉' * 25})).status_code == 422


async def test_registration_duplicate_conflict():
    async with client(user.app) as api:
        data = {'username': 'same', 'password': 'strong-password'}
        assert (await api.post('/register', json=data)).status_code == 200
        assert (await api.post('/register', json=data)).status_code == 409


async def test_mutating_product_with_active_activity_is_rejected(tokens):
    pid, _ = await activity(tokens)
    async with client(product.app) as api:
        assert (await api.put(f'/products/{pid}/stock?stock=20', headers=headers(tokens['admin']))).status_code == 409
        assert (await api.put(f'/products/{pid}/delist', headers=headers(tokens['admin']))).status_code == 409


async def test_gateway_forwards_non_json_and_returns_404(monkeypatch):
    from fastapi import FastAPI
    from fastapi.responses import PlainTextResponse
    upstream = FastAPI()
    @upstream.get('/products')
    async def raw():
        return PlainTextResponse('plain result', status_code=201)
    async with client(upstream) as http:
        monkeypatch.setattr(gateway.app.state, 'http', http, raising=False)
        async with client(gateway.app) as api:
            response = await api.get('/products')
            assert response.status_code == 201 and response.text == 'plain result'
            assert (await api.get('/products-unknown')).status_code == 404


async def test_ai_degradation_is_explicit(tokens, monkeypatch):
    http = AsyncMock()
    http.get.return_value = httpx.Response(200, request=httpx.Request('GET', 'http://test'),
        json={'data': {'name': '商品', 'price': '1.00', 'stock': 2}})
    monkeypatch.setattr(ai.app.state, 'http', http, raising=False)
    monkeypatch.setattr(ai.app.state, 'llm', None, raising=False)
    async with client(ai.app) as api:
        response = await api.post('/ai/consult', headers=headers(tokens['customer']), json={'product_id': 1, 'question': '如何？'})
    assert response.status_code == 200
    assert response.json()['data']['degraded'] is True
    assert response.json()['data']['reason'] == 'not_configured'


async def test_rate_limit_preserves_inventory(tokens, isolated_state):
    pid, aid = await activity(tokens)
    async with client(seckill.app) as api:
        responses = [await api.post('/seckill/submit', headers=headers(tokens['customer']), json={'product_id': pid}) for _ in range(35)]
    assert responses[-1].status_code == 429
    assert await isolated_state.hget(activity_key(aid), 'stock') == '4'


@pytest.mark.parametrize('corruption', ['1.0', '001', 'inf', '100001'])
async def test_corrupt_stock_cannot_append_orphan_event(tokens, isolated_state, corruption):
    _, aid = await activity(tokens)
    await isolated_state.hset(activity_key(aid), 'stock', corruption)
    assert (await reserve(aid, 2, str(uuid.uuid4())))[0] == -5
    assert await isolated_state.xlen(OUTBOX) == 0
    assert await isolated_state.hget(activity_key(aid), 'stock') == corruption
