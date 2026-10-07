from redis.asyncio import Redis
from common.config import settings

redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True,
    socket_connect_timeout=2, socket_timeout=5, max_connections=100) if settings.REDIS_URL else Redis(
    host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB,
    password=settings.REDIS_PASSWORD or None, decode_responses=True,
    socket_connect_timeout=2, socket_timeout=5, max_connections=100,
)
