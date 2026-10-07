import os
import tempfile
import uuid
from pathlib import Path

LIVE = os.getenv('RUN_LIVE_TESTS') == '1'
if not LIVE:
    os.environ.setdefault('SECRET_KEY', 'unit-test-key-' + 'a' * 48)
    os.environ.setdefault('DATABASE_URL', 'sqlite+aiosqlite:///' + str(Path(tempfile.gettempdir()) / ('webbackend-' + uuid.uuid4().hex + '.db')))

import pytest_asyncio
import fakeredis.aioredis
from common.database import Base, engine, async_session_factory
from common.models import User, UserRole
from common.auth import create_access_token


@pytest_asyncio.fixture(autouse=True)
async def isolated_state(monkeypatch):
    if LIVE:
        yield None
        return
    from common import auth, redis_client, seckill_store, limits, lifecycle, observation
    from services.user_service import main as user
    from services.product_service import main as product
    from services.order_service import main as order
    from services.seckill_service import main as seckill
    from services.ai_service import main as ai
    from services.gateway import main as gateway
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    for module in (auth, redis_client, seckill_store, limits, lifecycle, observation, user, product, order, seckill, ai, gateway):
        if hasattr(module, 'redis_client'):
            monkeypatch.setattr(module, 'redis_client', fake)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    yield fake
    await fake.aclose()
    await engine.dispose()


@pytest_asyncio.fixture
async def tokens():
    async with async_session_factory() as db:
        db.add_all([User(id=1, username='admin', password_hash='unused', role=UserRole.ADMIN),
                    User(id=2, username='customer', password_hash='unused', role=UserRole.CUSTOMER),
                    User(id=3, username='other', password_hash='unused', role=UserRole.CUSTOMER)])
        await db.commit()
    return {role: create_access_token({'sub': str(uid), 'role': 'admin' if role == 'admin' else 'customer', 'username': role})
            for role, uid in [('admin', 1), ('customer', 2), ('other', 3)]}


@pytest_asyncio.fixture
async def api():
    import httpx
    async with httpx.AsyncClient(base_url=os.getenv('LIVE_BASE_URL', 'http://localhost:8000'), timeout=10) as client:
        yield client
