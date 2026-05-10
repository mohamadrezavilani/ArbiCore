from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.apps.arbitrage import models, schemas, services
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.apps.arbitrage.models import OrderbookSnapshot, Exchange, ExchangeSymbol
from app.apps.arbitrage.schemas import OrderbookSnapshotResponse

router = APIRouter()

@router.post("/exchanges", response_model=schemas.ExchangeResponse)
async def create_exchange(data: schemas.ExchangeCreate, db: AsyncSession = Depends(get_db)):
    exchange = models.Exchange(**data.dict())
    db.add(exchange)
    await db.commit()
    await db.refresh(exchange)
    return exchange

@router.post("/exchange-symbols", response_model=schemas.ExchangeSymbolResponse)
async def add_symbol(data: schemas.ExchangeSymbolCreate, db: AsyncSession = Depends(get_db)):
    symbol = models.ExchangeSymbol(**data.dict())
    db.add(symbol)
    await db.commit()
    await db.refresh(symbol)
    return symbol

@router.get("/snapshots", response_model=list[OrderbookSnapshotResponse])
async def get_snapshots(limit: int = 20, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(
            OrderbookSnapshot.id,
            OrderbookSnapshot.best_ask_price,
            OrderbookSnapshot.best_ask_volume,
            OrderbookSnapshot.best_bid_price,
            OrderbookSnapshot.best_bid_volume,
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
    # Convert each row to a dict that matches the response schema
    return [
        OrderbookSnapshotResponse(
            id=row.id,
            exchange_name=row.exchange_name,
            common_symbol=row.common_symbol,
            best_ask_price=row.best_ask_price,
            best_ask_volume=row.best_ask_volume,
            best_bid_price=row.best_bid_price,
            best_bid_volume=row.best_bid_volume,
            created_at=row.created_at
        )
        for row in rows
    ]

@router.get("/opportunities", response_model=list[schemas.ArbitrageOpportunityResponse])
async def get_opportunities(limit: int = 20, db: AsyncSession = Depends(get_db)):
    stmt = select(models.ArbitrageOpportunity).order_by(models.ArbitrageOpportunity.created_at.desc()).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()

@router.get("/exchanges", response_model=list[schemas.ExchangeResponse])
async def list_exchanges(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Exchange))
    return result.scalars().all()

@router.get("/exchange-symbols", response_model=list[schemas.ExchangeSymbolResponse])
async def list_symbols(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.ExchangeSymbol))
    return result.scalars().all()