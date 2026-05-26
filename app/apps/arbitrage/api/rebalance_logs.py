from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.apps.arbitrage.models import RebalanceLog
from app.core.database import get_db
from app.apps.arbitrage.services.orderbook_fetcher import OrderbookFetcher
from app.apps.arbitrage.services.rebalancer import Rebalancer
from app.apps.arbitrage.services.opportunity_logger import OpportunityLogger
from app.apps.arbitrage.services.trade_executor import TradeExecutor
from pydantic import BaseModel
from typing import Optional

router = APIRouter()


class RebalanceResult(BaseModel):
    rebalanced: bool
    reason: str
    details: Optional[dict] = None


@router.post("/rebalance/{symbol}", response_model=RebalanceResult)
async def force_rebalance(symbol: str, db: AsyncSession = Depends(get_db)):
    """Manually trigger market rebalancing for a symbol."""
    # Fetch current orderbooks
    fetcher = OrderbookFetcher()
    exchange_orderbooks = await fetcher.fetch_all(db)

    if symbol not in exchange_orderbooks:
        return RebalanceResult(
            rebalanced=False,
            reason=f"Symbol '{symbol}' not found in current orderbooks",
            details={"available_symbols": list(exchange_orderbooks.keys())}
        )

    # Determine quote currency
    if symbol.endswith("IRT"):
        quote_currency = "IRT"
    elif symbol.endswith("USDT"):
        quote_currency = "USDT"
    else:
        return RebalanceResult(rebalanced=False, reason=f"Cannot determine quote currency for {symbol}")

    rebalancer = Rebalancer(OpportunityLogger(), TradeExecutor(OpportunityLogger()))
    success, reason = await rebalancer.rebalance_symbol_if_needed(
        db, symbol, quote_currency, exchange_orderbooks[symbol]
    )
    await db.commit()
    return RebalanceResult(rebalanced=success, reason=reason)


# Keep existing GET endpoint for logs
@router.get("/")
async def get_rebalance_logs(limit: int = 100, db: AsyncSession = Depends(get_db)):
    stmt = select(RebalanceLog).order_by(RebalanceLog.created_at.desc()).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()