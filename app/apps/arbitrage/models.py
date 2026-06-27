import uuid
from datetime import datetime
from typing import Optional
import enum
from sqlalchemy import String, Numeric, ForeignKey, JSON, UniqueConstraint, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, UUIDMixin, TimestampMixin

class Exchange(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "exchanges"
    name: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    base_url: Mapped[str] = mapped_column(String(200))
    orderbook_endpoint: Mapped[str] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(default=True)
    mode: Mapped[str] = mapped_column(String(20), default="simulator")

    symbols: Mapped[list["ExchangeSymbol"]] = relationship(back_populates="exchange", cascade="all, delete-orphan")
    base_inventories: Mapped[list["BaseInventory"]] = relationship(back_populates="exchange", cascade="all, delete-orphan")
    quote_inventories: Mapped[list["QuoteInventory"]] = relationship(back_populates="exchange", cascade="all, delete-orphan")
    fees: Mapped[list["ExchangeFee"]] = relationship(back_populates="exchange", cascade="all, delete-orphan")


class ExchangeFee(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "exchange_fees"
    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    quote_currency: Mapped[str] = mapped_column(String(10), nullable=False)
    taker_fee: Mapped[float] = mapped_column(Numeric(10, 6), default=0.0)
    maker_fee: Mapped[float] = mapped_column(Numeric(10, 6), default=0.0)
    exchange: Mapped["Exchange"] = relationship(back_populates="fees")


class ExchangeSymbol(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "exchange_symbols"
    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    original_symbol: Mapped[str] = mapped_column(String(50))
    common_symbol: Mapped[str] = mapped_column(String(50))
    price_conversion_factor: Mapped[float] = mapped_column(Numeric(10, 5), default=1.0)
    is_active: Mapped[bool] = mapped_column(default=True)
    exchange: Mapped["Exchange"] = relationship(back_populates="symbols")


class OrderbookSnapshot(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "orderbook_snapshots"
    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    symbol_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchange_symbols.id"), nullable=False)
    best_ask_price: Mapped[float] = mapped_column(Numeric(20, 10), nullable=True)
    best_ask_volume: Mapped[float] = mapped_column(Numeric(20, 10), nullable=True)
    best_bid_price: Mapped[float] = mapped_column(Numeric(20, 10), nullable=True)
    best_bid_volume: Mapped[float] = mapped_column(Numeric(20, 10), nullable=True)
    asks: Mapped[list] = mapped_column(JSON, nullable=True)
    bids: Mapped[list] = mapped_column(JSON, nullable=True)
    raw_data: Mapped[dict] = mapped_column(JSON, nullable=True)


class ArbitrageOpportunity(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "arbitrage_opportunities"
    common_symbol: Mapped[str] = mapped_column(String(50), index=True)
    exchange_a_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"))
    exchange_b_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"))
    trade_type: Mapped[str] = mapped_column(String(50))
    price_a: Mapped[float] = mapped_column(Numeric(20, 10))
    price_b: Mapped[float] = mapped_column(Numeric(20, 10))
    profit_percent: Mapped[float] = mapped_column(Numeric(10, 4))
    traded_volume: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    profit_quote: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    executions: Mapped[list["OrderExecution"]] = relationship(back_populates="opportunity", cascade="all, delete-orphan")

class BaseInventory(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "base_inventories"
    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    common_symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    balance: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    exchange: Mapped["Exchange"] = relationship(back_populates="base_inventories")
    __table_args__ = (CheckConstraint('balance >= 0', name='check_base_balance_non_negative'),)


class QuoteInventory(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "quote_inventories"
    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    currency: Mapped[str] = mapped_column(String(10), nullable=False)
    balance: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    exchange: Mapped["Exchange"] = relationship(back_populates="quote_inventories")
    __table_args__ = (CheckConstraint('balance >= 0', name='check_quote_balance_non_negative'),)


class Network(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "networks"
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    network_name: Mapped[str] = mapped_column(String(50), nullable=False)
    fee_per_transfer: Mapped[float] = mapped_column(Numeric(20, 10), default=0.0)
    is_active: Mapped[bool] = mapped_column(default=True)


class SymbolArbitrageSettings(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "symbol_arbitrage_settings"
    common_symbol: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    min_profit_percent: Mapped[float] = mapped_column(Numeric(10, 4), default=0.5)
    is_active: Mapped[bool] = mapped_column(default=True)

    cutoff_threshold: Mapped[float] = mapped_column(Numeric(10, 6), default=0.1)
    min_trade_percent: Mapped[float] = mapped_column(Numeric(10, 6), default=0.2)
    min_trade_factor: Mapped[float] = mapped_column(Numeric(10, 6), default=0.3)
    valuability_factor: Mapped[float] = mapped_column(Numeric(10, 6), default=1.0)

    default_network_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("networks.id"), nullable=True)
    default_network: Mapped[Optional["Network"]] = relationship()

    opportunistic_rebalance_enabled: Mapped[bool] = mapped_column(default=False)
    opportunistic_rebalance_max_loss_percent: Mapped[float] = mapped_column(Numeric(10, 6), default=0.5)

    # Market rebalancing (base asset)
    market_rebalance_enabled: Mapped[bool] = mapped_column(default=True)
    market_rebalance_amount_percent: Mapped[float] = mapped_column(Numeric(5,2), default=20.0)
    market_rebalance_max_spread_percent: Mapped[float] = mapped_column(Numeric(5,2), default=0.6)
    market_rebalance_imbalance_ratio: Mapped[float] = mapped_column(Numeric(5,2), default=0.25)
    market_rebalance_cooldown_seconds: Mapped[int] = mapped_column(default=300)
    last_rebalance_time: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    rebalance_pending: Mapped[bool] = mapped_column(default=False)

    # NEW: Quote rebalancing (IRT)
    quote_rebalance_enabled: Mapped[bool] = mapped_column(default=True)
    quote_rebalance_amount_percent: Mapped[float] = mapped_column(Numeric(5,2), default=20.0)
    quote_rebalance_max_spread_percent: Mapped[float] = mapped_column(Numeric(5,2), default=0.6)
    quote_rebalance_imbalance_ratio: Mapped[float] = mapped_column(Numeric(5,2), default=0.25)
    quote_rebalance_cooldown_seconds: Mapped[int] = mapped_column(default=300)
    last_quote_rebalance_time: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    quote_rebalance_pending: Mapped[bool] = mapped_column(default=False)


class RejectedOpportunity(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "rejected_opportunities"
    common_symbol: Mapped[str] = mapped_column(String(50), index=True)
    exchange_a_name: Mapped[str] = mapped_column(String(50))
    exchange_b_name: Mapped[str] = mapped_column(String(50))
    trade_type: Mapped[str] = mapped_column(String(50))
    rejection_reason: Mapped[str] = mapped_column(String(200))
    details: Mapped[dict] = mapped_column(JSON, nullable=True)


class RebalanceLog(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "rebalance_logs"
    common_symbol: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    currency: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    from_exchange: Mapped[str] = mapped_column(String(50))
    to_exchange: Mapped[str] = mapped_column(String(50))
    amount_sent: Mapped[float] = mapped_column(Numeric(20, 8))
    network_fee: Mapped[float] = mapped_column(Numeric(20, 8))
    net_received: Mapped[float] = mapped_column(Numeric(20, 8))
    reason: Mapped[str] = mapped_column(String(200))
    profit_quote: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)   # NEW


class ExchangePairWeight(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "exchange_pair_weights"
    exchange_a_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    exchange_b_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    weight: Mapped[float] = mapped_column(Numeric(3, 2), default=0.5)
    last_buy_exchange_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("exchanges.id"), nullable=True)
    last_sell_exchange_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("exchanges.id"), nullable=True)
    exchange_a: Mapped["Exchange"] = relationship(foreign_keys=[exchange_a_id])
    exchange_b: Mapped["Exchange"] = relationship(foreign_keys=[exchange_b_id])
    last_buy_exchange: Mapped[Optional["Exchange"]] = relationship(foreign_keys=[last_buy_exchange_id])
    last_sell_exchange: Mapped[Optional["Exchange"]] = relationship(foreign_keys=[last_sell_exchange_id])
    __table_args__ = (UniqueConstraint('exchange_a_id', 'exchange_b_id', name='uq_exchange_pair'),)

class OrderExecution(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "order_executions"
    opportunity_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("arbitrage_opportunities.id", ondelete="CASCADE"), nullable=False, index=True)
    exchange_name: Mapped[str] = mapped_column(String(50), nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=False)  # "buy" or "sell"
    price: Mapped[float] = mapped_column(Numeric(20, 10), nullable=False)
    volume: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    fee: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    client_order_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # for debugging

    # Relationship back to opportunity (optional, for ORM ease)
    opportunity: Mapped["ArbitrageOpportunity"] = relationship(back_populates="executions")
