from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app.apps.arbitrage.models import Exchange, BaseInventory, QuoteInventory

router = APIRouter()

@router.get("/")
async def get_balances(db: AsyncSession = Depends(get_db)):
    base_result = await db.execute(
        select(Exchange.name, BaseInventory.common_symbol, BaseInventory.balance)
        .join(Exchange, Exchange.id == BaseInventory.exchange_id)
    )
    quote_result = await db.execute(
        select(Exchange.name, QuoteInventory.currency, QuoteInventory.balance)
        .join(Exchange, Exchange.id == QuoteInventory.exchange_id)
    )
    return {
        "base_balances": [{"exchange": r[0], "symbol": r[1], "balance": float(r[2])} for r in base_result.all()],
        "quote_balances": [{"exchange": r[0], "currency": r[1], "balance": float(r[2])} for r in quote_result.all()]
    }