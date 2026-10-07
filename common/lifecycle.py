from contextlib import asynccontextmanager
from sqlalchemy import text
from common.database import engine, init_db
from common.redis_client import redis_client


@asynccontextmanager
async def lifespan(app):
    await init_db()
    await redis_client.ping()
    yield
    await redis_client.aclose()
    await engine.dispose()


async def check_dependencies():
    await redis_client.ping()
    async with engine.connect() as conn:
        await conn.execute(text('SELECT 1'))
    return {'status': 'ready'}
