from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from app.core.database import get_db
from app.apps.arbitrage.models import OrderExecution, ArbitrageOpportunity
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List

router = APIRouter()

class ExecutionResponse(BaseModel):
    id: str
    opportunity_id: str
    common_symbol: str
    exchange_name: str
    side: str
    price: float
    volume: float
    fee: float
    client_order_id: Optional[str]
    created_at: datetime

@router.get("/", response_model=List[ExecutionResponse])
async def get_executions(
    limit: int = Query(50, ge=1, le=500),
    symbol: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Fetch recent order executions with optional symbol filter."""
    stmt = (
        select(OrderExecution, ArbitrageOpportunity.common_symbol)
        .join(ArbitrageOpportunity, OrderExecution.opportunity_id == ArbitrageOpportunity.id)
        .order_by(desc(OrderExecution.created_at))
        .limit(limit)
    )
    if symbol:
        stmt = stmt.where(ArbitrageOpportunity.common_symbol == symbol)
    result = await db.execute(stmt)
    rows = result.all()
    return [
        ExecutionResponse(
            id=str(row.OrderExecution.id),
            opportunity_id=str(row.OrderExecution.opportunity_id),
            common_symbol=row.common_symbol,
            exchange_name=row.OrderExecution.exchange_name,
            side=row.OrderExecution.side,
            price=float(row.OrderExecution.price),
            volume=float(row.OrderExecution.volume),
            fee=float(row.OrderExecution.fee),
            client_order_id=row.OrderExecution.client_order_id,
            created_at=row.OrderExecution.created_at
        )
        for row in rows
    ]