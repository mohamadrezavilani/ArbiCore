from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app.apps.arbitrage.models import OrderbookSnapshot, Exchange, ExchangeSymbol
from app.apps.arbitrage import schemas

router = APIRouter()

@router.get("/", response_model=list[schemas.OrderbookSnapshotResponse])
async def get_snapshots(limit: int = 20, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(
            OrderbookSnapshot.id,
            OrderbookSnapshot.best_ask_price,
            OrderbookSnapshot.best_ask_volume,
            OrderbookSnapshot.best_bid_price,
            OrderbookSnapshot.best_bid_volume,
            OrderbookSnapshot.asks,
            OrderbookSnapshot.bids,
            OrderbookSnapshot.created_at,
            Exchange.name.label("exchange_name"),
            ExchangeSymbol.common_symbol.label("common_symbol")
        )
        .join(Exchange, OrderbookSnapshot.exchange_id == Exchange.id)
        .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
        .order_by(OrderbookSnapshot.created_at.desc())
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.all()
    return [
        schemas.OrderbookSnapshotResponse(
            id=row.id,
            exchange_name=row.exchange_name,
            common_symbol=row.common_symbol,
            best_ask_price=row.best_ask_price,
            best_ask_volume=row.best_ask_volume,
            best_bid_price=row.best_bid_price,
            best_bid_volume=row.best_bid_volume,
            asks=row.asks,
            bids=row.bids,
            created_at=row.created_at
        )
        for row in rows
    ]