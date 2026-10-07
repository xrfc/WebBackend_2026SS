"""Read a committed order in a fresh transaction, including after a Redis marker race."""
from sqlalchemy import select
from common.database import async_session_factory
from common.models import Order


async def fetch_order_by_request(request_id):
    async with async_session_factory() as db:
        return (await db.execute(select(Order).where(Order.request_id == request_id))).scalar_one_or_none()
