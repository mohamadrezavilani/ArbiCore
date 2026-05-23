from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, union_all, func, cast, String
from sqlalchemy.orm import aliased
from app.core.database import get_db
from app.apps.arbitrage.models import ArbitrageOpportunity, RebalanceLog, RejectedOpportunity, Exchange
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel

router = APIRouter()

class ActionItem(BaseModel):
    id: str
    timestamp: datetime
    action_type: str
    details: dict

@router.get("/", response_model=List[ActionItem])
async def get_actions(
    limit: int = Query(50, ge=1, le=500),
    action_type: Optional[str] = Query(None, pattern="^(trade|rebalance|rejection)$"),
    db: AsyncSession = Depends(get_db)
):
    ExchangeA = aliased(Exchange)
    ExchangeB = aliased(Exchange)
    trades_query = select(
        ArbitrageOpportunity.id.label("id"),
        ArbitrageOpportunity.created_at.label("timestamp"),
        cast("trade", String).label("action_type"),
        func.json_build_object(
            "common_symbol", ArbitrageOpportunity.common_symbol,
            "exchange_a", ExchangeA.name,
            "exchange_b", ExchangeB.name,
            "trade_type", ArbitrageOpportunity.trade_type,
            "price_a", ArbitrageOpportunity.price_a,
            "price_b", ArbitrageOpportunity.price_b,
            "profit_percent", ArbitrageOpportunity.profit_percent,
            "traded_volume", ArbitrageOpportunity.traded_volume,
            "profit_quote", ArbitrageOpportunity.traded_volume * (ArbitrageOpportunity.price_b - ArbitrageOpportunity.price_a)
        ).label("details")
    ).join(
        ExchangeA, ArbitrageOpportunity.exchange_a_id == ExchangeA.id
    ).join(
        ExchangeB, ArbitrageOpportunity.exchange_b_id == ExchangeB.id
    )

    rebalances_query = select(
        RebalanceLog.id.label("id"),
        RebalanceLog.created_at.label("timestamp"),
        cast("rebalance", String).label("action_type"),
        func.json_build_object(
            "common_symbol", RebalanceLog.common_symbol,
            "currency", RebalanceLog.currency,
            "from_exchange", RebalanceLog.from_exchange,
            "to_exchange", RebalanceLog.to_exchange,
            "amount_sent", RebalanceLog.amount_sent,
            "network_fee", RebalanceLog.network_fee,
            "net_received", RebalanceLog.net_received,
            "reason", RebalanceLog.reason
        ).label("details")
    )

    rejections_query = select(
        RejectedOpportunity.id.label("id"),
        RejectedOpportunity.created_at.label("timestamp"),
        cast("rejection", String).label("action_type"),
        func.json_build_object(
            "common_symbol", RejectedOpportunity.common_symbol,
            "exchange_a", RejectedOpportunity.exchange_a_name,
            "exchange_b", RejectedOpportunity.exchange_b_name,
            "trade_type", RejectedOpportunity.trade_type,
            "rejection_reason", RejectedOpportunity.rejection_reason,
            "details", RejectedOpportunity.details
        ).label("details")
    )

    combined = union_all(trades_query, rebalances_query, rejections_query).alias("combined")
    stmt = select(
        combined.c.id,
        combined.c.timestamp,
        combined.c.action_type,
        combined.c.details
    ).order_by(combined.c.timestamp.desc()).limit(limit)

    if action_type:
        stmt = stmt.where(combined.c.action_type == action_type)

    result = await db.execute(stmt)
    rows = result.all()
    return [
        ActionItem(
            id=str(row.id),
            timestamp=row.timestamp,
            action_type=row.action_type,
            details=row.details
        )
        for row in rows
    ]