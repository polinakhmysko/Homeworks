from pydantic import BaseModel
from typing import Optional
from decimal import Decimal

class Product(BaseModel):
    product_name: str

class Order(BaseModel):
    order_id: str
    product_name: str
    quantity: int
    price_per_unit: Decimal
    total_price: Decimal
    product_id: Optional[int]