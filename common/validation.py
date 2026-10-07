from decimal import Decimal
from typing import Annotated
from pydantic import AfterValidator, Field, StrictInt

ID = Annotated[StrictInt, Field(gt=0, le=2147483647)]
Stock = Annotated[StrictInt, Field(ge=0, le=100000)]


def check_money(value: Decimal) -> Decimal:
    if not value.is_finite() or value <= 0 or value > Decimal('99999999.99'):
        raise ValueError('金额必须在 0.01 至 99999999.99 之间')
    if value != value.quantize(Decimal('0.01')):
        raise ValueError('金额最多两位小数')
    return value.quantize(Decimal('0.01'))


Money = Annotated[Decimal, AfterValidator(check_money)]


def check_password(value: str) -> str:
    if len(value.encode('utf-8')) > 72:
        raise ValueError('密码的 UTF-8 编码不能超过 72 字节')
    return value


Password = Annotated[str, Field(min_length=8, max_length=72), AfterValidator(check_password)]
