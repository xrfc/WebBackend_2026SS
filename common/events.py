from typing import Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, StrictInt
from common.validation import ID


class OrderEvent(BaseModel):
    model_config = ConfigDict(extra='forbid')
    version: Literal[1]
    kind: Literal['order.create']
    request_id: UUID
    activity_id: UUID
    user_id: ID
    product_id: ID
    price_cents: StrictInt = Field(gt=0, le=9999999999)
