import uuid
from sqlalchemy import String, Numeric, ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, UUIDMixin, TimestampMixin

class Exchange(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "exchanges"

    name: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    base_url: Mapped[str] = mapped_column(String(200))
    orderbook_endpoint: Mapped[str] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(default=True)

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
    """Base currency inventory (e.g., TON, USDT) on an exchange."""
    __tablename__ = "base_inventories"

    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    common_symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    balance: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)

    exchange: Mapped["Exchange"] = relationship(back_populates="base_inventories")

class QuoteInventory(Base, UUIDMixin, TimestampMixin):
    """Quote currency inventory (IRT or USDT) on an exchange."""
    __tablename__ = "quote_inventories"

    exchange_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchanges.id"), nullable=False)
    currency: Mapped[str] = mapped_column(String(10), nullable=False)  # 'IRT' or 'USDT'
    balance: Mapped[float] = mapped_column(Numeric(20, 8), default=0.0)

    exchange: Mapped["Exchange"] = relationship(back_populates="quote_inventories")

class SymbolArbitrageSettings(Base, UUIDMixin, TimestampMixin):
    """Per‑symbol arbitrage settings (minimum profit percentage required)."""
    __tablename__ = "symbol_arbitrage_settings"

    common_symbol: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    min_profit_percent: Mapped[float] = mapped_column(Numeric(10, 4), default=0.5)
    is_active: Mapped[bool] = mapped_column(default=True)