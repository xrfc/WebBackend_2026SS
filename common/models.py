import enum
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Numeric, Text, DateTime, Enum as SAEnum, UniqueConstraint, Index
from common.database import Base


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class UserRole(str, enum.Enum):
    CUSTOMER = 'customer'
    ADMIN = 'admin'


class ProductStatus(str, enum.Enum):
    ON_SALE = 'on_sale'
    OFF_SHELF = 'off_shelf'


class OrderStatus(str, enum.Enum):
    PENDING = 'pending'
    CREATED = 'created'
    PAID = 'paid'
    CANCELLED = 'cancelled'


class OrderType(str, enum.Enum):
    NORMAL = 'normal'
    SECKILL = 'seckill'


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(SAEnum(UserRole), default=UserRole.CUSTOMER, nullable=False)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False, index=True)
    description = Column(Text, default='')
    price = Column(Numeric(12, 2), nullable=False)
    image_url = Column(String(500), default='')
    stock = Column(Integer, default=0, nullable=False)
    status = Column(SAEnum(ProductStatus), default=ProductStatus.ON_SALE)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class SeckillActivity(Base):
    __tablename__ = 'seckill_activities'
    id = Column(String(36), primary_key=True)
    product_id = Column(Integer, nullable=False, index=True)
    # NULL after closing: MySQL/SQLite allow many NULLs in a unique index.
    active_product_id = Column(Integer, unique=True, nullable=True)
    stock = Column(Integer, nullable=False)
    price = Column(Numeric(12, 2), nullable=False)
    product_name = Column(String(200), nullable=False)
    starts_at = Column(Integer, nullable=False)
    ends_at = Column(Integer, nullable=False)
    state = Column(String(20), nullable=False, default='initializing')
    released_stock = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=utcnow)


class Order(Base):
    __tablename__ = 'orders'
    __table_args__ = (
        UniqueConstraint('request_id', name='uq_orders_request'),
        UniqueConstraint('activity_id', 'user_id', name='uq_orders_activity_user'),
        Index('ix_orders_user_created', 'user_id', 'created_at'),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    request_id = Column(String(36), nullable=True)  # NULL for legacy/normal orders
    activity_id = Column(String(36), nullable=True)
    user_id = Column(Integer, nullable=False, index=True)
    product_id = Column(Integer, nullable=False, index=True)
    product_name = Column(String(200), default='')
    quantity = Column(Integer, default=1)
    price = Column(Numeric(12, 2), nullable=False)
    total_amount = Column(Numeric(12, 2), nullable=False)
    status = Column(SAEnum(OrderStatus), default=OrderStatus.CREATED)
    order_type = Column(SAEnum(OrderType), default=OrderType.NORMAL)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)
