import time
from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from common.app import create_app
from common.lifecycle import lifespan, check_dependencies
from common.models import User, UserRole
from common.database import get_db
from common.auth import hash_password, verify_password, create_access_token, get_current_user, require_role
from common.redis_client import redis_client
from common.limits import rate_limit
from common.validation import Password
from common.response import ApiResponse

app = create_app('User Service', lifespan=lifespan)


class RegisterRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    username: str = Field(min_length=2, max_length=50, pattern=r'^[A-Za-z0-9_\-]+$')
    password: Password


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    username: str = Field(min_length=2, max_length=50)
    password: str = Field(min_length=1, max_length=72)


async def create_user(req, role, db):
    user = User(username=req.username, password_hash=hash_password(req.password), role=role)
    db.add(user)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(409, '用户名已存在')
    return ApiResponse.ok({'id': user.id, 'username': user.username, 'role': user.role.value}, '注册成功')


@app.post('/register')
async def register(req: RegisterRequest, request: Request, db: AsyncSession = Depends(get_db)):
    await rate_limit('register-user:' + req.username, 3, 60)
    return await create_user(req, UserRole.CUSTOMER, db)


@app.post('/register/admin')
async def register_admin(req: RegisterRequest, user=Depends(require_role('admin')), db: AsyncSession = Depends(get_db)):
    return await create_user(req, UserRole.ADMIN, db)


@app.post('/login')
async def login(req: LoginRequest, request: Request, db: AsyncSession = Depends(get_db)):
    # In Compose the direct peer is the gateway; its rate limiter uses the actual incoming peer too.
    await rate_limit('login-user:' + req.username, 15, 60)
    user = (await db.execute(select(User).where(User.username == req.username))).scalar_one_or_none()
    if not user or not verify_password(req.password, user.password_hash):
        raise HTTPException(401, '用户名或密码错误')
    token = create_access_token({'sub': str(user.id), 'username': user.username, 'role': user.role.value})
    return ApiResponse.ok({'token': token, 'token_type': 'bearer',
        'user': {'id': user.id, 'username': user.username, 'role': user.role.value}})


@app.post('/logout')
async def logout(user=Depends(get_current_user)):
    ttl = max(1, int(user['exp'] - time.time()))
    await redis_client.set(f'auth:revoked:{user["jti"]}', '1', ex=ttl)
    return ApiResponse.ok(message='注销成功')


@app.get('/me')
async def me(user=Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    row = await db.get(User, int(user['sub']))
    if not row:
        raise HTTPException(404, '用户不存在')
    return ApiResponse.ok({'id': row.id, 'username': row.username, 'role': row.role.value})


app.get('/ready')(check_dependencies)
