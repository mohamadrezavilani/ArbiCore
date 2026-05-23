from pydantic import BaseModel, UUID4
from datetime import datetime
from typing import Optional, List
from typing import Dict, Any, List, Optional
from datetime import datetime

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
    asks: Optional[List[List[float]]]   # added
    bids: Optional[List[List[float]]]   # added
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
    traded_volume: float          # new
    profit_quote: float           # new – actual profit in IRT or USDT
    created_at: datetime

class OpportunitySummaryItem(BaseModel):
    common_symbol: str
    total_opportunities: int
    sum_profit_percent: float
    avg_profit_percent: float
    total_estimated_profit_quote: float   # in IRT or USDT (based on symbol)
    quote_currency: str

class SystemStats(BaseModel):
    total_opportunities: int
    total_profit_irt: float
    total_profit_usdt: float
    last_scan_time: Optional[datetime]
    active_exchanges: int
    active_symbols: int

class ScanTriggerResponse(BaseModel):
    message: str
    task_id: Optional[str]   # if you implement background task ID

class SymbolSettingsCreate(BaseModel):
    common_symbol: str
    min_profit_percent: float
    is_active: bool = True
    opportunistic_rebalance_enabled: bool = False
    opportunistic_rebalance_max_loss_percent: float = 0.5

class RiskSettingsResponse(BaseModel):
    id: UUID4
    common_symbol: str
    min_profit_percent: float
    cutoff_threshold: float
    min_trade_percent: float
    min_trade_factor: float
    valuability_factor: float
    default_network_id: Optional[UUID4]
    is_active: bool
    opportunistic_rebalance_enabled: bool
    opportunistic_rebalance_max_loss_percent: float

class RiskSettingsUpdate(BaseModel):
    min_profit_percent: Optional[float] = None
    cutoff_threshold: Optional[float] = None
    min_trade_percent: Optional[float] = None
    min_trade_factor: Optional[float] = None
    valuability_factor: Optional[float] = None
    default_network_id: Optional[UUID4] = None
    is_active: Optional[bool] = None
    opportunistic_rebalance_enabled: Optional[bool] = None
    opportunistic_rebalance_max_loss_percent: Optional[float] = None

class SymbolSettingsResponse(SymbolSettingsCreate):
    id: UUID4
    created_at: datetime
    updated_at: datetime

class NetworkResponse(BaseModel):
    id: UUID4
    symbol: str
    network_name: str
    fee_per_transfer: float
    is_active: bool



class RejectedOpportunityResponse(BaseModel):
    id: UUID4
    common_symbol: str
    exchange_a_name: str
    exchange_b_name: str
    trade_type: str
    rejection_reason: str
    details: Optional[dict]
    created_at: datetime

class RebalanceLogResponse(BaseModel):
    id: UUID4
    common_symbol: Optional[str]
    currency: Optional[str]
    from_exchange: str
    to_exchange: str
    amount_sent: float
    network_fee: float
    net_received: float
    reason: str
    created_at: datetime


class DashboardResponse(BaseModel):
    timestamp: datetime
    balances: Dict[str, Any]  # {"base": [...], "quote": [...]}
    opportunities: Dict[str, Any]  # {"executed_today": int, "rejected_today": int, "last_24h_profit": float}
    rebalances: Dict[str, Any]  # {"last_24h_count": int, "last_24h_total_sent": float}
    system_health: Dict[str, Any]  # {"active_exchanges": int, "active_symbols": int, "last_scan_time": str}

# Add at the end of schemas.py
class ActionLogResponse(BaseModel):
    id: UUID4
    timestamp: datetime
    action_type: str  # trade, rebalance, rejection
    details: Dict[str, Any]

class RealizedProfitResponse(BaseModel):
    currency: str
    days: int
    trade_profit: float
    network_fees: float
    net_profit: float
    since: datetime