from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.apps.arbitrage.models import Exchange, BaseInventory, QuoteInventory

async def get_base_balance(db: AsyncSession, exchange_name: str, common_symbol: str) -> float:
    exch = await db.execute(select(Exchange).where(Exchange.name == exchange_name))
    exch = exch.scalar_one_or_none()
    if not exch:
        return 0.0
    inv = await db.execute(select(BaseInventory).where(
        BaseInventory.exchange_id == exch.id,
        BaseInventory.common_symbol == common_symbol
    ))
    inv = inv.scalar_one_or_none()
    return float(inv.balance) if inv else 0.0

async def update_base_balance(db: AsyncSession, exchange_name: str, common_symbol: str, delta: float) -> None:
    exch = await db.execute(select(Exchange).where(Exchange.name == exchange_name))
    exch = exch.scalar_one_or_none()
    if not exch:
        return
    inv = await db.execute(select(BaseInventory).where(
        BaseInventory.exchange_id == exch.id,
        BaseInventory.common_symbol == common_symbol
    ))
    inv = inv.scalar_one_or_none()
    if inv:
        inv.balance += delta
    else:
        db.add(BaseInventory(exchange_id=exch.id, common_symbol=common_symbol, balance=delta))

async def get_quote_balance(db: AsyncSession, exchange_name: str, currency: str) -> float:
    exch = await db.execute(select(Exchange).where(Exchange.name == exchange_name))
    exch = exch.scalar_one_or_none()
    if not exch:
        return 0.0
    inv = await db.execute(select(QuoteInventory).where(
        QuoteInventory.exchange_id == exch.id,
        QuoteInventory.currency == currency
    ))
    inv = inv.scalar_one_or_none()
    return float(inv.balance) if inv else 0.0

async def update_quote_balance(db: AsyncSession, exchange_name: str, currency: str, delta: float) -> None:
    exch = await db.execute(select(Exchange).where(Exchange.name == exchange_name))
    exch = exch.scalar_one_or_none()
    if not exch:
        return
    inv = await db.execute(select(QuoteInventory).where(
        QuoteInventory.exchange_id == exch.id,
        QuoteInventory.currency == currency
    ))
    inv = inv.scalar_one_or_none()
    if inv:
        inv.balance += delta
    else:
        db.add(QuoteInventory(exchange_id=exch.id, currency=currency, balance=delta))