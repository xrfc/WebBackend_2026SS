import uuid
from datetime import datetime, timedelta, timezone
import bcrypt
from jose import JWTError, jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from redis.exceptions import RedisError
from common.config import settings
from common.redis_client import redis_client

security = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    encoded = password.encode('utf-8')
    if len(encoded) > 72:
        raise ValueError('密码超过 72 字节')
    return bcrypt.hashpw(encoded, bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    try:
        encoded = plain.encode('utf-8')
        return len(encoded) <= 72 and bcrypt.checkpw(encoded, hashed.encode())
    except (ValueError, TypeError):
        return False


def create_access_token(data: dict) -> str:
    now = datetime.now(timezone.utc)
    payload = {**data, 'iat': now, 'exp': now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
               'jti': str(uuid.uuid4())}
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM],
                             options={'require_exp': True, 'require_iat': True, 'require_sub': True, 'require_jti': True})
        if not 0 < int(payload['sub']) <= 2147483647 or payload.get('role') not in ('admin', 'customer'):
            raise ValueError('invalid claims')
        uuid.UUID(payload['jti'])
        return payload
    except (JWTError, ValueError, TypeError, KeyError):
        raise HTTPException(401, '无效或已过期的 Token')


async def authenticate_token(token: str) -> dict:
    payload = decode_token(token)
    try:
        revoked = await redis_client.exists(f'auth:revoked:{payload["jti"]}')
    except RedisError:
        raise HTTPException(503, '认证服务暂时不可用')
    if revoked:
        raise HTTPException(401, 'Token 已注销')
    return payload


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    if credentials is None or credentials.scheme.lower() != 'bearer':
        raise HTTPException(401, '请提供 Bearer Token')
    return await authenticate_token(credentials.credentials)


def require_role(role: str):
    async def checker(user=Depends(get_current_user)):
        if user['role'] != role:
            raise HTTPException(403, '权限不足')
        return user
    return checker
