from pydantic import BaseModel, Field
from typing import Optional
from decimal import Decimal

# === User Models ===
class UserBase(BaseModel):
    user_id: str = Field(..., max_length=20)
    name: str
    surname: str
    age: int
    email: str
    phone: str

class User(UserBase):
    pass

class Card(BaseModel):
    card_number: str
    user_id: str

class Order(BaseModel):
    order_id: int
    quantity: int
    price_per_unit: Decimal
    total_price: Decimal
    card_number: str
    user_id: str = Field(..., max_length=20)
    product_id: Optional[int]

class Product(BaseModel):
    product: str