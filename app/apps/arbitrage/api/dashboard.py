from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from datetime import datetime, timedelta
from app.core.database import get_db
from app.apps.arbitrage.models import (
    Exchange, ExchangeSymbol, OrderbookSnapshot, ArbitrageOpportunity,
    BaseInventory, QuoteInventory, RejectedOpportunity, RebalanceLog
)
from app.apps.arbitrage import schemas

router = APIRouter()

@router.get("/", response_model=schemas.DashboardResponse)
async def get_dashboard(db: AsyncSession = Depends(get_db)):
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = now - timedelta(days=1)

    # Balances
    base_result = await db.execute(
        select(Exchange.name, BaseInventory.common_symbol, BaseInventory.balance)
        .join(Exchange, Exchange.id == BaseInventory.exchange_id)
    )
    quote_result = await db.execute(
        select(Exchange.name, QuoteInventory.currency, QuoteInventory.balance)
        .join(Exchange, Exchange.id == QuoteInventory.exchange_id)
    )
    balances = {
        "base": [{"exchange": r[0], "symbol": r[1], "balance": float(r[2])} for r in base_result.all()],
        "quote": [{"exchange": r[0], "currency": r[1], "balance": float(r[2])} for r in quote_result.all()]
    }

    # Opportunities
    executed_today = await db.execute(
        select(func.count(ArbitrageOpportunity.id))
        .where(ArbitrageOpportunity.created_at >= today_start)
    )
    rejected_today = await db.execute(
        select(func.count(RejectedOpportunity.id))
        .where(RejectedOpportunity.created_at >= today_start)
    )
    profit_24h = await db.execute(
        select(func.sum(ArbitrageOpportunity.traded_volume * (ArbitrageOpportunity.price_b - ArbitrageOpportunity.price_a)))
        .where(ArbitrageOpportunity.created_at >= yesterday)
    )
    opportunities = {
        "executed_today": executed_today.scalar() or 0,
        "rejected_today": rejected_today.scalar() or 0,
        "last_24h_profit": float(profit_24h.scalar() or 0.0)
    }

    # Rebalances
    rebalance_count = await db.execute(
        select(func.count(RebalanceLog.id))
        .where(RebalanceLog.created_at >= yesterday)
    )
    rebalance_total_sent = await db.execute(
        select(func.sum(RebalanceLog.amount_sent))
        .where(RebalanceLog.created_at >= yesterday)
    )
    rebalances = {
        "last_24h_count": rebalance_count.scalar() or 0,
        "last_24h_total_sent": float(rebalance_total_sent.scalar() or 0.0)
    }

    # System health
    active_ex = await db.execute(
        select(func.count()).select_from(Exchange).where(Exchange.is_active == True)
    )
    active_sym = await db.execute(
        select(func.count()).select_from(ExchangeSymbol).where(ExchangeSymbol.is_active == True)
    )
    last_scan_result = await db.execute(select(func.max(OrderbookSnapshot.created_at)))
    last_scan_val = last_scan_result.scalar()
    system_health = {
        "active_exchanges": active_ex.scalar() or 0,
        "active_symbols": active_sym.scalar() or 0,
        "last_scan_time": last_scan_val.isoformat() if last_scan_val else None
    }

    return schemas.DashboardResponse(
        timestamp=now,
        balances=balances,
        opportunities=opportunities,
        rebalances=rebalances,
        system_health=system_health
    )