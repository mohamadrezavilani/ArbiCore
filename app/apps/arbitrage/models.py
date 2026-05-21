import uuid
from typing import Optional
import enum
from sqlalchemy import String, Numeric, ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, UUIDMixin, TimestampMixin

class Exchange(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "exchanges"
    name: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    base_url: Mapped[str] = mapped_column(String(200))
    orderbook_endpoint: Mapped[str] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(default=True)
    mode: Mapped[str] = mapped_column(String(20), default="simulator")   # "simulator" or "live"

    symbols: Mapped[list["ExchangeSymbol"]] = relationship(back_populates="exchange", cascade="all, delete-orphan")
    base_inventories: Mapped[list["BaseInventory"]] = relationship(back_populates="exchange", cascade="all, delete-orphan")
    quote_inventories: Mapped[list["QuoteInventory"]] = relationship(back_populates="exchange", cascade="all, delete-orphan")
    fees: Mapped[list["ExchangeFee"]] = relationship(back_populates="exchange", cascade="all, delete-orphan")


class ExchangeFee(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "exchange_fees"
    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    quote_currency: Mapped[str] = mapped_column(String(10), nullable=False)  # 'IRT' or 'USDT'
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

class BaseInventory(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "base_inventories"
    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    common_symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    balance: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    exchange: Mapped["Exchange"] = relationship(back_populates="base_inventories")

class QuoteInventory(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "quote_inventories"
    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    currency: Mapped[str] = mapped_column(String(10), nullable=False)  # 'IRT' or 'USDT'
    balance: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)
    exchange: Mapped["Exchange"] = relationship(back_populates="quote_inventories")

class Network(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "networks"
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)   # common_symbol
    network_name: Mapped[str] = mapped_column(String(50), nullable=False)
    fee_per_transfer: Mapped[float] = mapped_column(Numeric(20, 10), default=0.0)   # in base currency
    is_active: Mapped[bool] = mapped_column(default=True)

class SymbolArbitrageSettings(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "symbol_arbitrage_settings"
    common_symbol: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    min_profit_percent: Mapped[float] = mapped_column(Numeric(10, 4), default=0.5)
    is_active: Mapped[bool] = mapped_column(default=True)

    # Risk parameters
    cutoff_threshold: Mapped[float] = mapped_column(Numeric(10, 6), default=0.1)
    min_trade_percent: Mapped[float] = mapped_column(Numeric(10, 6), default=0.2)
    min_trade_factor: Mapped[float] = mapped_column(Numeric(10, 6), default=0.3)
    valuability_factor: Mapped[float] = mapped_column(Numeric(10, 6), default=1.0)

    # Default network to use for transferring this symbol
    default_network_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("networks.id"), nullable=True)
    default_network: Mapped[Optional["Network"]] = relationship()

class RejectedOpportunity(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "rejected_opportunities"

    common_symbol: Mapped[str] = mapped_column(String(50), index=True)
    exchange_a_name: Mapped[str] = mapped_column(String(50))
    exchange_b_name: Mapped[str] = mapped_column(String(50))
    trade_type: Mapped[str] = mapped_column(String(50))  # e.g., "buy_on_wallex_sell_on_nobitex"
    rejection_reason: Mapped[str] = mapped_column(String(200))
    details: Mapped[dict] = mapped_column(JSON, nullable=True)  # store prices, fees, thresholds, etc.