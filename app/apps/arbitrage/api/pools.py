from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.core.database import get_db
from app.apps.arbitrage.models import BaseInventory, QuoteInventory, ArbitrageOpportunity, RebalanceLog, OrderbookSnapshot, ExchangeSymbol
from datetime import datetime, timedelta
from typing import Dict, Any
from app.apps.arbitrage.models import Exchange

router = APIRouter()

@router.get("/")
async def get_pools(db: AsyncSession = Depends(get_db)):
    # Quote pools
    quote_stmt = select(
        QuoteInventory.currency,
        func.sum(QuoteInventory.balance).label("total")
    ).group_by(QuoteInventory.currency)
    quote_result = await db.execute(quote_stmt)
    quote_pools = {row.currency: float(row.total) for row in quote_result.all()}

    # Base pools
    base_stmt = select(
        BaseInventory.common_symbol,
        func.sum(BaseInventory.balance).label("total")
    ).group_by(BaseInventory.common_symbol)
    base_result = await db.execute(base_stmt)
    base_pools = {row.common_symbol: float(row.total) for row in base_result.all()}

    # Per-exchange breakdown
    per_exchange_quote = await db.execute(
        select(Exchange.name, QuoteInventory.currency, QuoteInventory.balance)
        .join(Exchange, Exchange.id == QuoteInventory.exchange_id)
    )
    per_exchange_quote_list = [
        {"exchange": r[0], "currency": r[1], "balance": float(r[2])}
        for r in per_exchange_quote.all()
    ]
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
    since = datetime.utcnow() - timedelta(days=days)

    # Trade profit
    if currency == "IRT":
        profit_stmt = select(
            func.sum(ArbitrageOpportunity.traded_volume * (ArbitrageOpportunity.price_b - ArbitrageOpportunity.price_a))
        ).where(
            ArbitrageOpportunity.created_at >= since,
            ArbitrageOpportunity.common_symbol.like("%IRT")
        )
    else:
        profit_stmt = select(
            func.sum(ArbitrageOpportunity.traded_volume * (ArbitrageOpportunity.price_b - ArbitrageOpportunity.price_a))
        ).where(
            ArbitrageOpportunity.created_at >= since,
            ArbitrageOpportunity.common_symbol.like("%USDT")
        )
    trade_profit = (await db.execute(profit_stmt)).scalar() or 0.0

    # Network fees from quote rebalances (same currency)
    fee_stmt_quote = select(func.sum(RebalanceLog.network_fee)).where(
        RebalanceLog.created_at >= since,
        RebalanceLog.currency == currency
    )
    network_fees_quote = (await db.execute(fee_stmt_quote)).scalar() or 0.0

    # Base rebalancing fees (converted from USDT to IRT if needed)
    network_fees_base_converted = 0.0
    if currency == "IRT":
        base_fees_stmt = select(
            RebalanceLog.created_at,
            RebalanceLog.network_fee
        ).where(
            RebalanceLog.created_at >= since,
            RebalanceLog.currency.is_(None),
            RebalanceLog.common_symbol.like("%IRT")
        )
        base_fees = await db.execute(base_fees_stmt)
        rows = base_fees.all()
        for row in rows:
            fee_date = row.created_at.date()
            day_start = datetime.combine(fee_date, datetime.min.time())
            day_end = datetime.combine(fee_date, datetime.max.time())
            price_stmt = select(
                func.avg(OrderbookSnapshot.best_bid_price)
            ).join(
                ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id
            ).where(
                ExchangeSymbol.common_symbol == "USDTIRT",
                OrderbookSnapshot.created_at.between(day_start, day_end)
            )
            avg_price = (await db.execute(price_stmt)).scalar()
            if avg_price:
                network_fees_base_converted += float(row.network_fee) * float(avg_price)
            else:
                # Fallback: last known price
                last_price_stmt = select(OrderbookSnapshot.best_bid_price).join(
                    ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id
                ).where(
                    ExchangeSymbol.common_symbol == "USDTIRT"
                ).order_by(OrderbookSnapshot.created_at.desc()).limit(1)
                last_price = (await db.execute(last_price_stmt)).scalar()
                if last_price:
                    network_fees_base_converted += row.network_fee * float(last_price)
                else:
                    network_fees_base_converted += row.network_fee * 50000  # fallback rate

    total_network_fees = network_fees_quote + network_fees_base_converted
    net_profit = float(trade_profit) - float(total_network_fees)

    return {
        "currency": currency,
        "days": days,
        "trade_profit": float(trade_profit),
        "network_fees_quote": float(network_fees_quote),
        "network_fees_base_converted": float(network_fees_base_converted),
        "total_network_fees": float(total_network_fees),
        "net_profit": float(net_profit),
        "since": since.isoformat()
    }