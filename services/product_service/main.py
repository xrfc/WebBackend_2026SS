import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from fastapi import FastAPI, Depends, Query
from common.database import init_db, get_db, Base
from common.auth import get_current_user, require_role
from common.response import ApiResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, Column, Integer, String, Float, Text, DateTime, Enum as SAEnum, func
from datetime import datetime, timezone
import enum
from pydantic import BaseModel, Field
from typing import Optional


# ==================== 数据模型 ====================

class ProductStatus(str, enum.Enum):
    ON_SALE = "on_sale"
    OFF_SHELF = "off_shelf"


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False, index=True)
    description = Column(Text, default="")
    price = Column(Float, nullable=False)
    image_url = Column(String(500), default="")
    stock = Column(Integer, default=0)
    status = Column(SAEnum(ProductStatus), default=ProductStatus.ON_SALE)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


# ==================== 请求/响应模型 ====================

class ProductCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str = ""
    price: float = Field(..., gt=0)
    image_url: str = ""
    stock: int = Field(default=0, ge=0)

class ProductUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    image_url: Optional[str] = None
    stock: Optional[int] = None


# ==================== FastAPI应用 ====================

app = FastAPI(title="Product Service", version="1.0.0",
              description="商品服务 - 商品CRUD/库存管理")


@app.on_event("startup")
async def startup():
    await init_db()


@app.post("/products", summary="添加商品（管理员）")
async def create_product(
    req: ProductCreate,
    user: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db)
):
    product = Product(**req.model_dump())
    db.add(product)
    await db.commit()
    await db.refresh(product)
    return ApiResponse.ok(
        {"id": product.id, "name": product.name, "stock": product.stock},
        "商品添加成功"
    )


@app.put("/products/{product_id}/delist", summary="下架商品（管理员）")
async def delist_product(
    product_id: int,
    user: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db)
):
    product = (await db.execute(
        select(Product).where(Product.id == product_id)
    )).scalar_one_or_none()
    if not product:
        return ApiResponse.fail(404, "商品不存在")
    product.status = ProductStatus.OFF_SHELF
    await db.commit()
    return ApiResponse.ok(message="商品已下架")


@app.get("/products", summary="商品列表（分页）")
async def list_products(
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
    keyword: str = Query("", max_length=100),
    db: AsyncSession = Depends(get_db)
):
    base_query = select(Product).where(Product.status == ProductStatus.ON_SALE)
    count_query = select(func.count()).select_from(Product).where(
        Product.status == ProductStatus.ON_SALE
    )

    if keyword:
        like_filter = Product.name.like(f"%{keyword}%")
        base_query = base_query.where(like_filter)
        count_query = count_query.where(like_filter)

    total = (await db.execute(count_query)).scalar()
    offset = (page - 1) * size
    products = (await db.execute(
        base_query.offset(offset).limit(size)
    )).scalars().all()

    return ApiResponse.ok({
        "total": total,
        "page": page,
        "size": size,
        "items": [
            {
                "id": p.id,
                "name": p.name,
                "description": p.description,
                "price": p.price,
                "image_url": p.image_url,
                "stock": p.stock
            }
            for p in products
        ]
    })


@app.get("/products/{product_id}", summary="商品详情")
async def get_product(product_id: int, db: AsyncSession = Depends(get_db)):
    product = (await db.execute(
        select(Product).where(Product.id == product_id)
    )).scalar_one_or_none()
    if not product:
        return ApiResponse.fail(404, "商品不存在")
    return ApiResponse.ok({
        "id": product.id,
        "name": product.name,
        "description": product.description,
        "price": product.price,
        "image_url": product.image_url,
        "stock": product.stock,
        "status": product.status.value
    })


@app.put("/products/{product_id}/stock", summary="设置库存（管理员）")
async def update_stock(
    product_id: int,
    stock: int = Query(..., ge=0),
    user: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db)
):
    product = (await db.execute(
        select(Product).where(Product.id == product_id)
    )).scalar_one_or_none()
    if not product:
        return ApiResponse.fail(404, "商品不存在")
    product.stock = stock
    await db.commit()
    return ApiResponse.ok({"id": product.id, "stock": product.stock}, "库存更新成功")


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
