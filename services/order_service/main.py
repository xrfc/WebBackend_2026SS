"""订单服务 - MQ消费落库 + WebSocket实时推送"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import json
import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, Set

from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, Column, Integer, String, Float, DateTime, Enum as SAEnum, func

from common.database import init_db, get_db, Base
from common.auth import get_current_user
from common.config import settings
from common.response import ApiResponse

import enum

# ==================== 测试模式检测 ====================
TEST_MODE = os.environ.get("TEST_MODE", "").lower() == "true"
if not TEST_MODE:
    import aio_pika


# ==================== 数据模型 ====================

class OrderStatus(str, enum.Enum):
    PENDING = "pending"         # 待支付
    CREATED = "created"          # 已创建
    PAID = "paid"                # 已支付
    CANCELLED = "cancelled"      # 已取消


class OrderType(str, enum.Enum):
    NORMAL = "normal"            # 普通订单
    SECKILL = "seckill"          # 秒杀订单


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    product_id = Column(Integer, nullable=False, index=True)
    product_name = Column(String(200), default="")
    quantity = Column(Integer, default=1)
    price = Column(Float, nullable=False)
    total_amount = Column(Float, nullable=False)
    status = Column(SAEnum(OrderStatus), default=OrderStatus.CREATED)
    order_type = Column(SAEnum(OrderType), default=OrderType.NORMAL)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


# ==================== WebSocket连接管理器 ====================

class ConnectionManager:
    """WebSocket连接管理 - 维护用户连接映射"""

    def __init__(self):
        self._connections: Dict[int, Set[WebSocket]] = {}

    async def connect(self, user_id: int, ws: WebSocket):
        await ws.accept()
        if user_id not in self._connections:
            self._connections[user_id] = set()
        self._connections[user_id].add(ws)

    def disconnect(self, user_id: int, ws: WebSocket):
        if user_id in self._connections:
            self._connections[user_id].discard(ws)
            if not self._connections[user_id]:
                del self._connections[user_id]

    async def send_to_user(self, user_id: int, message: dict):
        """向指定用户推送消息"""
        if user_id in self._connections:
            dead_connections = set()
            for ws in self._connections[user_id]:
                try:
                    await ws.send_json(message)
                except Exception:
                    dead_connections.add(ws)
            self._connections[user_id] -= dead_connections


ws_manager = ConnectionManager()


# ==================== MQ消费者 ====================

class MQConsumer:
    """订单消息消费者 - 生产模式用RabbitMQ, 测试模式用内存队列"""

    def __init__(self):
        self.connection = None
        self.channel = None
        self._task = None

    async def connect(self):
        if TEST_MODE:
            # 测试模式：从共享的内存队列消费
            asyncio.create_task(self._test_consumer())
            return
        self.connection = await aio_pika.connect_robust(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            login=settings.RABBITMQ_USER,
            password=settings.RABBITMQ_PASS,
        )
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)
        queue = await self.channel.declare_queue("order_create_queue", durable=True)

        async def on_message(message: aio_pika.IncomingMessage):
            async with message.process():
                try:
                    body = json.loads(message.body.decode())
                    await self._handle_order(body)
                except Exception as e:
                    print(f"[OrderConsumer] 处理消息失败: {e}")

        await queue.consume(on_message)

    async def _test_consumer(self):
        """测试模式：从 common.test_mq 共享队列消费"""
        from common.test_mq import get_queue
        mq = get_queue()
        while True:
            try:
                msg = await asyncio.wait_for(mq.get(), timeout=1.0)
                await self._handle_order(msg)
                mq.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception:
                await asyncio.sleep(0.5)

    async def _handle_order(self, order_data: dict):
        """处理订单创建消息 → 落库 + 推送通知"""
        from common.database import async_session_factory
        async with async_session_factory() as db:
            # 获取商品名称
            from common.database import Base as DBBase
            from sqlalchemy import text
            result = await db.execute(
                text("SELECT name FROM products WHERE id = :pid"),
                {"pid": order_data["product_id"]}
            )
            row = result.fetchone()
            product_name = row[0] if row else "未知商品"

            order = Order(
                user_id=order_data["user_id"],
                product_id=order_data["product_id"],
                product_name=product_name,
                quantity=order_data.get("quantity", 1),
                price=order_data["price"],
                total_amount=order_data["price"] * order_data.get("quantity", 1),
                status=OrderStatus.CREATED,
                order_type=OrderType(order_data.get("order_type", "seckill")),
            )
            db.add(order)
            await db.commit()
            await db.refresh(order)

            # 通过WebSocket推送订单创建成功消息
            await ws_manager.send_to_user(order.user_id, {
                "type": "order_created",
                "message": "订单创建成功！",
                "data": {
                    "order_id": order.id,
                    "product_id": order.product_id,
                    "product_name": order.product_name,
                    "price": order.price,
                    "total_amount": order.total_amount,
                    "status": order.status.value,
                    "created_at": order.created_at.isoformat(),
                }
            })
            print(f"[OrderConsumer] 订单创建成功: order_id={order.id}, user_id={order.user_id}")

    async def close(self):
        if TEST_MODE:
            return
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()


mq_consumer = MQConsumer()


# ==================== FastAPI 应用 ====================

app = FastAPI(title="Order Service", version="1.0.0",
              description="订单服务 - MQ消费落库 + WebSocket实时推送")


@app.on_event("startup")
async def startup():
    await init_db()
    asyncio.create_task(mq_consumer.connect())


@app.on_event("shutdown")
async def shutdown():
    await mq_consumer.close()


# ==================== WebSocket端点 ====================

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(ws: WebSocket, user_id: int):
    """WebSocket连接端点 - 用于接收订单推送"""
    await ws_manager.connect(user_id, ws)
    try:
        while True:
            # 保持连接，接收客户端心跳
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(user_id, ws)
    except Exception:
        ws_manager.disconnect(user_id, ws)


# ==================== REST API ====================

@app.get("/orders", summary="查询个人订单列表")
async def list_orders(
    user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db)
):
    user_id = int(user["sub"])
    offset = (page - 1) * size

    total = (await db.execute(
        select(func.count()).select_from(Order).where(Order.user_id == user_id)
    )).scalar()

    orders = (await db.execute(
        select(Order).where(Order.user_id == user_id)
        .order_by(Order.created_at.desc())
        .offset(offset).limit(size)
    )).scalars().all()

    return ApiResponse.ok({
        "total": total,
        "page": page,
        "size": size,
        "items": [
            {
                "id": o.id,
                "product_id": o.product_id,
                "product_name": o.product_name,
                "quantity": o.quantity,
                "price": o.price,
                "total_amount": o.total_amount,
                "status": o.status.value,
                "order_type": o.order_type.value,
                "created_at": o.created_at.isoformat(),
            }
            for o in orders
        ]
    })


@app.get("/orders/{order_id}", summary="查询订单详情")
async def get_order(
    order_id: int,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    order = (await db.execute(
        select(Order).where(Order.id == order_id, Order.user_id == int(user["sub"]))
    )).scalar_one_or_none()
    if not order:
        return ApiResponse.fail(404, "订单不存在")
    return ApiResponse.ok({
        "id": order.id,
        "product_id": order.product_id,
        "product_name": order.product_name,
        "quantity": order.quantity,
        "price": order.price,
        "total_amount": order.total_amount,
        "status": order.status.value,
        "order_type": order.order_type.value,
        "created_at": order.created_at.isoformat(),
    })


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
