from fastapi import Depends, HTTPException, Query, Path
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from common.app import create_app
from common.lifecycle import lifespan, check_dependencies
from common.database import get_db
from common.models import Product, ProductStatus, SeckillActivity
from common.auth import require_role
from common.validation import ID, Stock, Money
from common.response import ApiResponse

app = create_app('Product Service', lifespan=lifespan)


class ProductCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    name: str = Field(min_length=1, max_length=200)
    description: str = Field('', max_length=5000)
    price: Money
    image_url: str = Field('', max_length=500)
    stock: Stock = 0


def serialize_product(row):
    return {'id': row.id, 'name': row.name, 'description': row.description,
        'price': str(row.price), 'image_url': row.image_url, 'stock': row.stock, 'status': row.status.value}


@app.post('/products')
async def create_product(req: ProductCreate, user=Depends(require_role('admin')), db: AsyncSession = Depends(get_db)):
    row = Product(**req.model_dump())
    db.add(row)
    await db.commit()
    return ApiResponse.ok(serialize_product(row), '商品添加成功')


@app.get('/products')
async def list_products(page: int = Query(1, ge=1, le=10000), size: int = Query(10, ge=1, le=100),
    keyword: str = Query('', max_length=100), db: AsyncSession = Depends(get_db)):
    filters = [Product.status == ProductStatus.ON_SALE]
    if keyword:
        escaped = keyword.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
        filters.append(Product.name.like('%' + escaped + '%', escape='\\'))
    total = (await db.execute(select(func.count()).select_from(Product).where(*filters))).scalar()
    rows = (await db.execute(select(Product).where(*filters).order_by(Product.id)
        .offset((page - 1) * size).limit(size))).scalars().all()
    return ApiResponse.ok({'total': total, 'page': page, 'size': size, 'items': [serialize_product(r) for r in rows]})


@app.get('/products/{product_id}')
async def get_product(product_id: int = Path(ge=1, le=2147483647), db: AsyncSession = Depends(get_db)):
    row = await db.get(Product, product_id)
    if row is None:
        raise HTTPException(404, '商品不存在')
    return ApiResponse.ok(serialize_product(row))


async def locked_editable_product(db, product_id):
    # Same lock order as activity creation/close: product first, then activity.
    row = (await db.execute(select(Product).where(Product.id == product_id).with_for_update())).scalar_one_or_none()
    if row is None:
        raise HTTPException(404, '商品不存在')
    active = (await db.execute(select(SeckillActivity.id).where(SeckillActivity.active_product_id == product_id))).scalar_one_or_none()
    if active:
        raise HTTPException(409, '请先关闭该商品的秒杀活动，再修改库存或下架')
    return row


@app.put('/products/{product_id}/stock')
async def update_stock(product_id: int = Path(ge=1, le=2147483647), stock: int = Query(ge=0, le=100000),
    user=Depends(require_role('admin')), db: AsyncSession = Depends(get_db)):
    row = await locked_editable_product(db, product_id)
    row.stock = stock
    await db.commit()
    return ApiResponse.ok({'id': row.id, 'stock': row.stock})


@app.put('/products/{product_id}/delist')
async def delist(product_id: int = Path(ge=1, le=2147483647), user=Depends(require_role('admin')),
    db: AsyncSession = Depends(get_db)):
    row = await locked_editable_product(db, product_id)
    row.status = ProductStatus.OFF_SHELF
    await db.commit()
    return ApiResponse.ok(message='商品已下架')


app.get('/ready')(check_dependencies)
