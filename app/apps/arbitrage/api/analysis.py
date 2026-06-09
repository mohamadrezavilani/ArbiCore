from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.apps.arbitrage.services.analysis_service import AnalysisService

router = APIRouter()


@router.get("/spread-history")
async def get_spread_history(
    symbol: str = Query("USDTIRT"),
    exchange: str = Query(None),
    hours: int = Query(24, ge=1, le=168),
    interval_minutes: int = Query(10, ge=1, le=60),
    db: AsyncSession = Depends(get_db)
):
    data = await AnalysisService.get_spread_history(db, symbol, exchange, hours, interval_minutes)
    return data


@router.get("/liquidity-depth")
async def get_liquidity_depth(
    symbol: str = Query("USDTIRT"),
    exchange: str = Query(None),
    hours: int = Query(24, ge=1, le=168),
    depth_levels: int = Query(5, ge=1, le=20),
    db: AsyncSession = Depends(get_db)
):
    data = await AnalysisService.get_liquidity_depth(db, symbol, exchange, hours, depth_levels)
    return data


@router.get("/volatility")
async def get_price_volatility(
    symbol: str = Query("USDTIRT"),
    exchange: str = Query(None),
    hours: int = Query(24, ge=1, le=168),
    db: AsyncSession = Depends(get_db)
):
    data = await AnalysisService.get_price_volatility(db, symbol, exchange, hours)
    return data


@router.get("/cross-exchange-spread")
async def get_cross_exchange_spread(
    symbol: str = Query("USDTIRT"),
    hours: int = Query(24, ge=1, le=168),
    db: AsyncSession = Depends(get_db)
):
    data = await AnalysisService.get_cross_exchange_spread(db, symbol, hours)
    return data