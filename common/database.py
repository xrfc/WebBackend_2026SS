from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from common.config import settings


class Base(DeclarativeBase):
    pass


url = settings.DATABASE_URL or URL.create(
    'mysql+aiomysql', username=settings.MYSQL_USER, password=settings.MYSQL_PASSWORD,
    host=settings.MYSQL_HOST, port=settings.MYSQL_PORT, database=settings.MYSQL_DATABASE,
    query={'charset': 'utf8mb4'},
)
options = {'pool_pre_ping': True}
if not str(url).startswith('sqlite'):
    options.update(pool_size=10, max_overflow=5, pool_recycle=1800, connect_args={'connect_timeout': 3})
engine = create_async_engine(url, **options)
async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db():
    async with async_session_factory() as session:
        yield session


async def init_db():
    # Runtime startup does not create/alter tables. The one-shot db-init service does.
    async with engine.connect() as conn:
        await conn.execute(text('SELECT 1'))


async def create_schema():
    import common.models  # noqa: F401 - register every table in one place
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
