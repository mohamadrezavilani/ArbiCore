from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app.apps.arbitrage.models import SymbolArbitrageSettings, Network
from app.apps.arbitrage import schemas

router = APIRouter()

# Symbol Arbitrage Settings
@router.get("/", response_model=list[schemas.SymbolSettingsResponse])
async def get_arbitrage_settings(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SymbolArbitrageSettings))
    return result.scalars().all()

@router.post("/", response_model=schemas.SymbolSettingsResponse)
async def create_or_update_setting(data: schemas.SymbolSettingsCreate, db: AsyncSession = Depends(get_db)):
    stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == data.common_symbol)
    existing = await db.execute(stmt)
    setting = existing.scalar_one_or_none()
    if setting:
        setting.min_profit_percent = data.min_profit_percent
        setting.is_active = data.is_active
    else:
        setting = SymbolArbitrageSettings(**data.dict())
        db.add(setting)
    await db.commit()
    await db.refresh(setting)
    return setting

@router.get("/risk", response_model=list[schemas.RiskSettingsResponse])
async def get_risk_settings(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SymbolArbitrageSettings))
    return result.scalars().all()

@router.put("/risk/{symbol}", response_model=schemas.RiskSettingsResponse)
async def update_risk_settings(symbol: str, data: schemas.RiskSettingsUpdate, db: AsyncSession = Depends(get_db)):
    stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == symbol)
    settings_obj = (await db.execute(stmt)).scalar_one_or_none()
    if not settings_obj:
        raise HTTPException(status_code=404, detail="Symbol not found")
    for key, value in data.dict(exclude_unset=True).items():
        setattr(settings_obj, key, value)
    await db.commit()
    await db.refresh(settings_obj)
    return settings_obj

# Networks
@router.get("/networks", response_model=list[schemas.NetworkResponse])
async def get_networks(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Network))
    return result.scalars().all()