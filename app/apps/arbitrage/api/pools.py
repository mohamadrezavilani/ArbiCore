from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from app.core.database import get_db
from app.apps.arbitrage.models import BaseInventory, QuoteInventory, ArbitrageOpportunity, RebalanceLog, Exchange
from typing import Dict, Any
from datetime import datetime, timedelta

router = APIRouter()

@router.get("/")
async def get_pools(db: AsyncSession = Depends(get_db)):
    """
    Returns total balances per quote currency (IRT, USDT) and per base asset.
    Also shows per-exchange breakdown.
    """
    # Quote pools
    quote_stmt = select(
        QuoteInventory.currency,
        func.sum(QuoteInventory.balance).label("total")
    ).group_by(QuoteInventory.currency)
    quote_result = await db.execute(quote_stmt)
    quote_pools = {row.currency: float(row.total) for row in quote_result.all()}

    # Base pools per symbol
    base_stmt = select(
        BaseInventory.common_symbol,
        func.sum(BaseInventory.balance).label("total")
    ).group_by(BaseInventory.common_symbol)
    base_result = await db.execute(base_stmt)
    base_pools = {row.common_symbol: float(row.total) for row in base_result.all()}

    # Per-exchange quote balances (for breakdown)
    per_exchange_quote = await db.execute(
        select(Exchange.name, QuoteInventory.currency, QuoteInventory.balance)
        .join(Exchange, Exchange.id == QuoteInventory.exchange_id)
    )
    per_exchange_quote_list = [
        {"exchange": r[0], "currency": r[1], "balance": float(r[2])}
        for r in per_exchange_quote.all()
    ]

    # Per-exchange base balances
    per_exchange_base = await db.execute(
        select(Exchange.name, BaseInventory.common_symbol, BaseInventory.balance)
        .join(Exchange, Exchange.id == BaseInventory.exchange_id)
    )
    per_exchange_base_list = [
        {"exchange": r[0], "symbol": r[1], "balance": float(r[2])}
        for r in per_exchange_base.all()
    ]

    return {
        "quote_pools": quote_pools,
        "base_pools": base_pools,
        "per_exchange": {
            "quote": per_exchange_quote_list,
            "base": per_exchange_base_list
        }
    }

@router.get("/profit/realized")
async def get_realized_profit(
    days: int = Query(7, ge=1, le=90),
    currency: str = Query("IRT", pattern="^(IRT|USDT)$"),
    db: AsyncSession = Depends(get_db)
):
    """
    Net realized profit = profit from trades minus network fees paid during rebalancing,
    over the last N days.
    """
    since = datetime.utcnow() - timedelta(days=days)

    # Profit from trades (in the specified quote currency)
    if currency == "IRT":
        profit_stmt = select(
            func.sum(ArbitrageOpportunity.traded_volume * (ArbitrageOpportunity.price_b - ArbitrageOpportunity.price_a))
        ).where(
            ArbitrageOpportunity.created_at >= since,
            ArbitrageOpportunity.common_symbol.like("%IRT")
        )
    else:  # USDT
        profit_stmt = select(
            func.sum(ArbitrageOpportunity.traded_volume * (ArbitrageOpportunity.price_b - ArbitrageOpportunity.price_a))
        ).where(
            ArbitrageOpportunity.created_at >= since,
            ArbitrageOpportunity.common_symbol.like("%USDT")
        )
    trade_profit = (await db.execute(profit_stmt)).scalar() or 0.0

    # Network fees paid in rebalancing (for the same currency)
    # RebalanceLog stores fees in the base/quote currency? In our model, network_fee is in the transferred asset units.
    # For IRT/USDT, the rebalance log may have currency = 'IRT' or 'USDT'.
    fee_stmt = select(func.sum(RebalanceLog.network_fee)).where(
        RebalanceLog.created_at >= since,
        RebalanceLog.currency == currency
    )
    network_fees = (await db.execute(fee_stmt)).scalar() or 0.0

    net_profit = float(trade_profit) - float(network_fees)

    return {
        "currency": currency,
        "days": days,
        "trade_profit": float(trade_profit),
        "network_fees": float(network_fees),
        "net_profit": float(net_profit),
        "since": since.isoformat()
    }