"""One-shot schema initialization / additive legacy upgrade, preserving existing rows."""
import asyncio
from sqlalchemy import inspect, text, select
from sqlalchemy.sql.sqltypes import Float
from common.database import engine, create_schema, async_session_factory
from common.models import User, UserRole
from common.auth import hash_password
from common.config import settings
from common.validation import check_password


async def initialize():
    # MySQL advisory lock serializes simultaneous deployments; runtime services wait on this job.
    async with engine.connect() as lock:
        mysql = engine.dialect.name == 'mysql'
        if mysql:
            acquired = (await lock.execute(text("SELECT GET_LOCK('webbackend_schema_v2', 60)"))).scalar()
            if acquired != 1:
                raise RuntimeError('Another deployment is changing the schema')
        try:
            await create_schema()
            async with engine.begin() as conn:
                columns = await conn.run_sync(lambda sync: inspect(sync).get_columns('orders'))
                names = {column['name'] for column in columns}
                for name in ('request_id', 'activity_id'):
                    if name not in names:
                        await conn.execute(text(f'ALTER TABLE orders ADD COLUMN {name} VARCHAR(36) NULL'))
                indexes = await conn.run_sync(lambda sync: inspect(sync).get_indexes('orders'))
                uniques = await conn.run_sync(lambda sync: inspect(sync).get_unique_constraints('orders'))
                known = {item['name'] for item in indexes + uniques}
                definitions = {
                    'uq_orders_request': 'UNIQUE INDEX uq_orders_request ON orders (request_id)',
                    'uq_orders_activity_user': 'UNIQUE INDEX uq_orders_activity_user ON orders (activity_id, user_id)',
                    'ix_orders_user_created': 'INDEX ix_orders_user_created ON orders (user_id, created_at)',
                }
                for name, definition in definitions.items():
                    if name not in known:
                        await conn.execute(text('CREATE ' + definition))
                if mysql:
                    for table, fields in {'products': ('price',), 'orders': ('price', 'total_amount')}.items():
                        infos = await conn.run_sync(lambda sync, table=table: inspect(sync).get_columns(table))
                        for field in fields:
                            info = next(c for c in infos if c['name'] == field)
                            if isinstance(info['type'], Float):
                                invalid = (await conn.execute(text(
                                    f'SELECT COUNT(*) FROM {table} WHERE {field} IS NULL OR {field} < 0 OR {field} > 9999999999.99'
                                ))).scalar()
                                if invalid:
                                    raise RuntimeError(f'Legacy {table}.{field} has invalid values; correct them before migration')
                                # MySQL rounds historical FLOAT currency to cents. Back up before upgrading.
                                await conn.execute(text(f'ALTER TABLE {table} MODIFY COLUMN {field} DECIMAL(12,2) NOT NULL'))
            if not settings.ADMIN_PASSWORD or len(settings.ADMIN_PASSWORD) < 12:
                raise RuntimeError('Set ADMIN_PASSWORD with at least 12 characters')
            check_password(settings.ADMIN_PASSWORD)
            async with async_session_factory() as session:
                admin = (await session.execute(select(User).where(User.username == settings.ADMIN_USERNAME))).scalar_one_or_none()
                if admin is None:
                    session.add(User(username=settings.ADMIN_USERNAME, password_hash=hash_password(settings.ADMIN_PASSWORD), role=UserRole.ADMIN))
                    await session.commit()
                elif admin.role != UserRole.ADMIN:
                    raise RuntimeError('ADMIN_USERNAME already belongs to a customer; choose another name')
            print('Schema and administrator ready. Existing administrator passwords are preserved.')
        finally:
            if mysql:
                await lock.execute(text("SELECT RELEASE_LOCK('webbackend_schema_v2')"))
    await engine.dispose()


if __name__ == '__main__':
    asyncio.run(initialize())
