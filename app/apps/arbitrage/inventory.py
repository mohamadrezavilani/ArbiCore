from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import logging
from app.apps.arbitrage.models import Exchange, BaseInventory, QuoteInventory

logger = logging.getLogger(__name__)

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
        new_balance = float(inv.balance) + delta
        if new_balance < 0:
            logger.warning(f"Attempted to set {exchange_name} {common_symbol} balance to {new_balance}, clamping to 0")
            new_balance = 0.0
        inv.balance = new_balance
    else:
        if delta > 0:
            db.add(BaseInventory(exchange_id=exch.id, common_symbol=common_symbol, balance=delta))
        elif delta < 0:
            logger.warning(f"Attempted to create negative {common_symbol} balance for {exchange_name} – ignoring")

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
        new_balance = float(inv.balance) + delta
        if new_balance < 0:
            logger.warning(f"Attempted to set {exchange_name} {currency} balance to {new_balance}, clamping to 0")
            new_balance = 0.0
        inv.balance = new_balance
    else:
        if delta > 0:
            db.add(QuoteInventory(exchange_id=exch.id, currency=currency, balance=delta))
        elif delta < 0:
            logger.warning(f"Attempted to create negative {currency} balance for {exchange_name} – ignoring")

async def set_base_balance(db: AsyncSession, exchange_name: str, common_symbol: str, new_balance: float):
    if new_balance < 0:
        logger.warning(f"set_base_balance called with negative {new_balance} for {exchange_name} {common_symbol}, setting to 0")
        new_balance = 0.0
    exch = await db.execute(select(Exchange).where(Exchange.name == exchange_name))
    exch = exch.scalar_one_or_none()
    if not exch: return
    inv = await db.execute(select(BaseInventory).where(
        BaseInventory.exchange_id == exch.id,
        BaseInventory.common_symbol == common_symbol
    ))
    inv = inv.scalar_one_or_none()
    if inv:
        inv.balance = new_balance
    else:
        db.add(BaseInventory(exchange_id=exch.id, common_symbol=common_symbol, balance=new_balance))

async def set_quote_balance(db: AsyncSession, exchange_name: str, currency: str, new_balance: float):
    if new_balance < 0:
        logger.warning(f"set_quote_balance called with negative {new_balance} for {exchange_name} {currency}, setting to 0")
        new_balance = 0.0
    exch = await db.execute(select(Exchange).where(Exchange.name == exchange_name))
    exch = exch.scalar_one_or_none()
    if not exch: return
    inv = await db.execute(select(QuoteInventory).where(
        QuoteInventory.exchange_id == exch.id,
        QuoteInventory.currency == currency
    ))
    inv = inv.scalar_one_or_none()
    if inv:
        inv.balance = new_balance
    else:
        db.add(QuoteInventory(exchange_id=exch.id, currency=currency, balance=new_balance))