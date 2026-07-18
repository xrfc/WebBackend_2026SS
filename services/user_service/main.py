import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from fastapi import FastAPI, Depends
from common.database import init_db, get_db, Base
from common.auth import hash_password, verify_password, create_access_token, get_current_user
from common.auth import TOKEN_BLACKLIST
from common.response import ApiResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, Column, Integer, String, DateTime, Enum as SAEnum
from datetime import datetime, timezone
import enum
from pydantic import BaseModel, Field


# ==================== 数据模型 ====================

class UserRole(str, enum.Enum):
    CUSTOMER = "customer"
    ADMIN = "admin"


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(SAEnum(UserRole), default=UserRole.CUSTOMER, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


# ==================== 请求/响应模型 ====================

class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=50)
    password: str = Field(..., min_length=6, max_length=100)

class LoginRequest(BaseModel):
    username: str
    password: str


# ==================== FastAPI应用 ====================

app = FastAPI(title="User Service", version="1.0.0",
              description="用户服务 - 注册/登录/JWT认证/鉴权")


@app.on_event("startup")
async def startup():
    await init_db()
    # 自动创建默认管理员账号
    from common.database import async_session_factory
    async with async_session_factory() as session:
        admin = (await session.execute(
            select(User).where(User.username == "admin")
        )).scalar_one_or_none()
        if not admin:
            admin_user = User(
                username="admin",
                password_hash=hash_password("admin123"),
                role=UserRole.ADMIN
            )
            session.add(admin_user)
            await session.commit()
            print("[UserService] 默认管理员已创建: admin / admin123")


@app.post("/register", summary="用户注册（客户角色）")
async def register(req: RegisterRequest, db: AsyncSession = Depends(get_db)):
    existing = (await db.execute(
        select(User).where(User.username == req.username)
    )).scalar_one_or_none()
    if existing:
        return ApiResponse.fail(400, "用户名已存在")
    user = User(
        username=req.username,
        password_hash=hash_password(req.password),
        role=UserRole.CUSTOMER
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return ApiResponse.ok(
        {"id": user.id, "username": user.username, "role": user.role.value},
        "注册成功"
    )


@app.post("/register/admin", summary="注册管理员（需管理员权限）")
async def register_admin(
    req: RegisterRequest,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    if user.get("role") != "admin":
        return ApiResponse.fail(403, "仅管理员可创建管理员账号")
    existing = (await db.execute(
        select(User).where(User.username == req.username)
    )).scalar_one_or_none()
    if existing:
        return ApiResponse.fail(400, "用户名已存在")
    admin_user = User(
        username=req.username,
        password_hash=hash_password(req.password),
        role=UserRole.ADMIN
    )
    db.add(admin_user)
    await db.commit()
    await db.refresh(admin_user)
    return ApiResponse.ok(
        {"id": admin_user.id, "username": admin_user.username, "role": admin_user.role.value},
        "管理员创建成功"
    )


@app.post("/login", summary="用户登录")
async def login(req: LoginRequest, db: AsyncSession = Depends(get_db)):
    user = (await db.execute(
        select(User).where(User.username == req.username)
    )).scalar_one_or_none()
    if not user or not verify_password(req.password, user.password_hash):
        return ApiResponse.fail(401, "用户名或密码错误")
    token = create_access_token({
        "sub": str(user.id),
        "username": user.username,
        "role": user.role.value
    })
    return ApiResponse.ok({
        "token": token,
        "token_type": "bearer",
        "user": {"id": user.id, "username": user.username, "role": user.role.value}
    })


@app.post("/logout", summary="注销登录")
async def logout(user: dict = Depends(get_current_user)):
    TOKEN_BLACKLIST.add(user.get("sub"))
    return ApiResponse.ok(message="注销成功")


@app.get("/me", summary="获取当前用户信息")
async def get_me(user: dict = Depends(get_current_user),
                 db: AsyncSession = Depends(get_db)):
    db_user = (await db.execute(
        select(User).where(User.id == int(user["sub"]))
    )).scalar_one_or_none()
    if not db_user:
        return ApiResponse.fail(404, "用户不存在")
    return ApiResponse.ok({
        "id": db_user.id,
        "username": db_user.username,
        "role": db_user.role.value
    })


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
