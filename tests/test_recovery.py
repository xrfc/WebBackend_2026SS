import asyncio
from unittest.mock import AsyncMock
import httpx
import pytest
from sqlalchemy import select, text, event, func
from sqlalchemy.orm import Session
from redis.exceptions import ConnectionError
from common.database import engine, async_session_factory
from common.models import Order, Product, SeckillActivity, User
from common.seckill_store import activity_key
from services.seckill_service import main as seckill
from services.ai_service import main as ai
from scripts import init_db
from tests.test_boundaries import activity, client, headers


async def test_failed_initialization_keeps_one_reservation_then_recovers(tokens, monkeypatch):
    async with client(__import__('services.product_service.main', fromlist=['app']).app) as api:
        row = await api.post('/products', headers=headers(tokens['admin']), json={'name': '恢复商品', 'price': '1.00', 'stock': 10})
        pid = row.json()['data']['id']
    original = seckill.initialize_activity
    monkeypatch.setattr(seckill, 'initialize_activity', AsyncMock(side_effect=ConnectionError('down')))
    async with client(seckill.app) as api:
        response = await api.post('/seckill/start', headers=headers(tokens['admin']),
            json={'product_id': pid, 'stock': 5, 'seckill_price': '0.01'})
        assert response.status_code == 503
    aid = response.json()['data']['activity_id']
    async with async_session_factory() as db:
        assert (await db.get(Product, pid)).stock == 5
        assert (await db.get(SeckillActivity, aid)).state == 'initializing'
    monkeypatch.setattr(seckill, 'initialize_activity', original)
    await seckill.prepare_activity(aid)
    await seckill.close_activity(aid)
    async with async_session_factory() as db:
        assert (await db.get(Product, pid)).stock == 10


async def test_sql_close_failure_keeps_frozen_inventory_recoverable(tokens, isolated_state):
    pid, aid = await activity(tokens)
    def fail_commit(session):
        raise RuntimeError('simulated commit failure')
    event.listen(Session, 'before_commit', fail_commit)
    try:
        with pytest.raises(RuntimeError, match='commit failure'):
            await seckill.close_activity(aid)
    finally:
        event.remove(Session, 'before_commit', fail_commit)
    assert await isolated_state.hget(activity_key(aid), 'state') == 'closed'
    async with async_session_factory() as db:
        assert (await db.get(Product, pid)).stock == 5
    await seckill.close_activity(aid)
    await seckill.close_activity(aid)
    async with async_session_factory() as db:
        assert (await db.get(Product, pid)).stock == 10


async def test_legacy_schema_upgrade_preserves_orders_and_existing_admin(tokens, monkeypatch):
    async with engine.begin() as connection:
        await connection.run_sync(Order.__table__.drop)
        await connection.execute(text('''CREATE TABLE orders (
            id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, product_id INTEGER NOT NULL,
            product_name VARCHAR(200), quantity INTEGER, price FLOAT, total_amount FLOAT,
            status VARCHAR(20), order_type VARCHAR(20), created_at DATETIME, updated_at DATETIME)'''))
        await connection.execute(text("INSERT INTO orders VALUES (1, 2, 1, 'legacy', 1, 1.23, 1.23, 'CREATED', 'SECKILL', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"))
    monkeypatch.setattr(init_db.settings, 'ADMIN_PASSWORD', 'valid-new-password')
    await init_db.initialize()
    # Rerunning the migration must not create duplicate indexes/users or erase rows.
    await init_db.initialize()
    async with async_session_factory() as db:
        old = await db.get(Order, 1)
        assert old.product_name == 'legacy' and old.request_id is None and old.activity_id is None
        assert (await db.get(User, 1)).password_hash == 'unused'
        assert (await db.execute(select(func.count()).select_from(User))).scalar() == 3


async def test_ai_overload_has_finite_wait_and_explicit_fallback(tokens, monkeypatch):
    http = AsyncMock()
    http.get.return_value = httpx.Response(200, request=httpx.Request('GET', 'http://test'),
        json={'data': {'name': '商品', 'price': '1.00', 'stock': 2}})
    monkeypatch.setattr(ai.app.state, 'http', http, raising=False)
    monkeypatch.setattr(ai.app.state, 'llm', object(), raising=False)
    monkeypatch.setattr(ai, 'slots', asyncio.Semaphore(0))
    async with client(ai.app) as api:
        response = await api.post('/ai/consult', headers=headers(tokens['customer']),
            json={'product_id': 1, 'question': '如何？'})
    assert response.status_code == 200
    assert response.json()['data']['degraded'] is True
    assert response.json()['data']['reason'] == 'overloaded'
