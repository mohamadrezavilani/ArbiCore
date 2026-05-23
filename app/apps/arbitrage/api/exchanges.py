from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app.apps.arbitrage import models, schemas

router = APIRouter()

@router.get("/", response_model=list[schemas.ExchangeResponse])
async def list_exchanges(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Exchange))
    return result.scalars().all()

@router.post("/", response_model=schemas.ExchangeResponse)
async def create_exchange(data: schemas.ExchangeCreate, db: AsyncSession = Depends(get_db)):
    exchange = models.Exchange(**data.dict())
    db.add(exchange)
    await db.commit()
    await db.refresh(exchange)
    return exchange

@router.get("/{exchange_id}", response_model=schemas.ExchangeResponse)
async def get_exchange(exchange_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Exchange).where(models.Exchange.id == exchange_id))
    exchange = result.scalar_one_or_none()
    if not exchange:
        raise HTTPException(status_code=404, detail="Exchange not found")
    return exchange

@router.put("/{exchange_id}", response_model=schemas.ExchangeResponse)
async def update_exchange(exchange_id: str, data: schemas.ExchangeCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Exchange).where(models.Exchange.id == exchange_id))
    exchange = result.scalar_one_or_none()
    if not exchange:
        raise HTTPException(status_code=404, detail="Exchange not found")
    for key, value in data.dict().items():
        setattr(exchange, key, value)
    await db.commit()
    await db.refresh(exchange)
    return exchange

@router.delete("/{exchange_id}")
async def delete_exchange(exchange_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Exchange).where(models.Exchange.id == exchange_id))
    exchange = result.scalar_one_or_none()
    if not exchange:
        raise HTTPException(status_code=404, detail="Exchange not found")
    await db.delete(exchange)
    await db.commit()
    return {"message": "Exchange deleted"}