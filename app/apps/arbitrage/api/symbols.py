from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app.apps.arbitrage import models, schemas

router = APIRouter()

@router.get("/", response_model=list[schemas.ExchangeSymbolResponse])
async def list_symbols(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.ExchangeSymbol))
    return result.scalars().all()

@router.post("/", response_model=schemas.ExchangeSymbolResponse)
async def add_symbol(data: schemas.ExchangeSymbolCreate, db: AsyncSession = Depends(get_db)):
    symbol = models.ExchangeSymbol(**data.dict())
    db.add(symbol)
    await db.commit()
    await db.refresh(symbol)
    return symbol

@router.get("/{symbol_id}", response_model=schemas.ExchangeSymbolResponse)
async def get_symbol(symbol_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.ExchangeSymbol).where(models.ExchangeSymbol.id == symbol_id))
    symbol = result.scalar_one_or_none()
    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return symbol

@router.put("/{symbol_id}", response_model=schemas.ExchangeSymbolResponse)
async def update_symbol(symbol_id: str, data: schemas.ExchangeSymbolCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.ExchangeSymbol).where(models.ExchangeSymbol.id == symbol_id))
    symbol = result.scalar_one_or_none()
    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")
    for key, value in data.dict().items():
        setattr(symbol, key, value)
    await db.commit()
    await db.refresh(symbol)
    return symbol

@router.delete("/{symbol_id}")
async def delete_symbol(symbol_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.ExchangeSymbol).where(models.ExchangeSymbol.id == symbol_id))
    symbol = result.scalar_one_or_none()
    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")
    await db.delete(symbol)
    await db.commit()
    return {"message": "Symbol deleted"}