from sqlalchemy import select
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app.apps.arbitrage.models import SymbolArbitrageSettings, Network
from app.apps.arbitrage import schemas


router = APIRouter()

@router.get("/risk-context/{symbol}")
async def get_risk_context(symbol: str, db: AsyncSession = Depends(get_db)):
    from sqlalchemy import func
    from app.apps.arbitrage.models import BaseInventory, Exchange, Network

    # Get settings
    stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == symbol)
    settings = (await db.execute(stmt)).scalar_one_or_none()
    if not settings:
        raise HTTPException(404, "Symbol not found")

    # Get max base pool
    max_base_stmt = select(func.max(BaseInventory.balance)).join(Exchange).where(
        BaseInventory.common_symbol == symbol,
        Exchange.is_active == True
    )
    max_base = (await db.execute(max_base_stmt)).scalar() or 0.0

    # Get network fee
    network_fee_base = 0.0
    if settings.default_network_id:
        net_stmt = select(Network.fee_per_transfer).where(Network.id == settings.default_network_id)
        net_fee = (await db.execute(net_stmt)).scalar()
        network_fee_base = float(net_fee) if net_fee else 0.0

    # Get current balances
    balance_stmt = select(Exchange.name, BaseInventory.balance).join(Exchange).where(
        BaseInventory.common_symbol == symbol,
        Exchange.is_active == True
    )
    balances = await db.execute(balance_stmt)
    balance_list = [{"exchange": name, "balance": float(bal)} for name, bal in balances.all()]

    return {
        "symbol": symbol,
        "settings": {
            "market_rebalance_enabled": settings.market_rebalance_enabled,
            "market_rebalance_amount_percent": settings.market_rebalance_amount_percent,
            "market_rebalance_max_spread_percent": settings.market_rebalance_max_spread_percent,
            "market_rebalance_imbalance_ratio": settings.market_rebalance_imbalance_ratio,
            "market_rebalance_cooldown_seconds": settings.market_rebalance_cooldown_seconds,
            "last_rebalance_time": settings.last_rebalance_time
        },
        "max_base_pool": max_base,
        "network_fee_base": network_fee_base,
        "current_base_balances": balance_list
    }