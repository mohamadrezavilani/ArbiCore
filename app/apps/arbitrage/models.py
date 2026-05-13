import uuid
from sqlalchemy import String, Numeric, ForeignKey, DateTime, Boolean, JSON, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime
from uuid import uuid4
from app.models.base import Base, UUIDMixin, TimestampMixin

class Exchange(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "exchanges"

    name: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    base_url: Mapped[str] = mapped_column(String(200))
    orderbook_endpoint: Mapped[str] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(default=True)
    taker_fee: Mapped[float] = mapped_column(Numeric(10, 6), default=0.0)   # fee when you take an order
    maker_fee: Mapped[float] = mapped_column(Numeric(10, 6), default=0.0)   # fee when you place a limit order

    symbols: Mapped[list["ExchangeSymbol"]] = relationship(back_populates="exchange", cascade="all, delete-orphan")

class ExchangeSymbol(Base, UUIDMixin, TimestampMixin):
    """Symbols per exchange, with optional mapping and conversion factor."""
    __tablename__ = "exchange_symbols"

    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    original_symbol: Mapped[str] = mapped_column(String(50))      # e.g. "TONTMN" on Wallex
    common_symbol: Mapped[str] = mapped_column(String(50))        # e.g. "TONIRT" for comparison
    price_conversion_factor: Mapped[float] = mapped_column(Numeric(10, 5), default=1.0)  # e.g. 10.0 for TMN -> IRT
    is_active: Mapped[bool] = mapped_column(default=True)

    # Relation
    exchange: Mapped["Exchange"] = relationship(back_populates="symbols")

class OrderbookSnapshot(Base, UUIDMixin, TimestampMixin):
    """Stores each polled order book snapshot."""
    __tablename__ = "orderbook_snapshots"

    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    symbol_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchange_symbols.id"), nullable=False)
    best_ask_price: Mapped[float] = mapped_column(Numeric(20, 10), nullable=True)
    best_ask_volume: Mapped[float] = mapped_column(Numeric(20, 10), nullable=True)
    best_bid_price: Mapped[float] = mapped_column(Numeric(20, 10), nullable=True)
    best_bid_volume: Mapped[float] = mapped_column(Numeric(20, 10), nullable=True)
    raw_data: Mapped[dict] = mapped_column(JSON, nullable=True)   # store full response if needed

class ArbitrageOpportunity(Base, UUIDMixin, TimestampMixin):
    """Detected arbitrage opportunity between two exchanges on a common symbol."""
    __tablename__ = "arbitrage_opportunities"

    common_symbol: Mapped[str] = mapped_column(String(50), index=True)
    exchange_a_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"))
    exchange_b_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"))
    trade_type: Mapped[str] = mapped_column(String(50))  # "buy" or "sell"
    price_a: Mapped[float] = mapped_column(Numeric(20, 10))
    price_b: Mapped[float] = mapped_column(Numeric(20, 10))
    profit_percent: Mapped[float] = mapped_column(Numeric(10, 4))
    is_executed: Mapped[bool] = mapped_column(default=False)