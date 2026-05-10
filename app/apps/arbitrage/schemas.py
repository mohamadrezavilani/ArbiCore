from pydantic import BaseModel, UUID4
from datetime import datetime
from typing import Optional

# ... (keep previous Opportunity schemas if any)

class ExchangeCreate(BaseModel):
    name: str
    base_url: str
    orderbook_endpoint: str

class ExchangeResponse(ExchangeCreate):
    id: UUID4
    is_active: bool
    created_at: datetime

class ExchangeSymbolCreate(BaseModel):
    exchange_id: UUID4
    original_symbol: str
    common_symbol: str
    price_conversion_factor: float = 1.0

class ExchangeSymbolResponse(ExchangeSymbolCreate):
    id: UUID4
    is_active: bool

class OrderbookSnapshotResponse(BaseModel):
    id: UUID4
    exchange_name: str
    common_symbol: str
    best_ask_price: Optional[float]
    best_ask_volume: Optional[float]
    best_bid_price: Optional[float]
    best_bid_volume: Optional[float]
    created_at: datetime

class ArbitrageOpportunityResponse(BaseModel):
    id: UUID4
    common_symbol: str
    exchange_a_name: str
    exchange_b_name: str
    trade_type: str
    price_a: float
    price_b: float
    profit_percent: float
    created_at: datetime