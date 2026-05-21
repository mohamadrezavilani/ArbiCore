from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from app.core.database import get_db
from app.apps.arbitrage import models, schemas
from app.apps.arbitrage.models import (
    OrderbookSnapshot, Exchange, ExchangeSymbol, ArbitrageOpportunity,
    BaseInventory, QuoteInventory, ExchangeFee, SymbolArbitrageSettings, Network
)
from app.apps.arbitrage.schemas import NetworkResponse, RiskSettingsResponse, RiskSettingsUpdate

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

@router.get("/snapshots", response_model=list[schemas.OrderbookSnapshotResponse])
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

@router.get("/opportunities", response_model=list[schemas.ArbitrageOpportunityResponse])
async def get_opportunities(limit: int = 20, db: AsyncSession = Depends(get_db)):
    # Correct: price_a and price_b already include fees, so profit = volume * (price_b - price_a)
    raw_sql = text("""
        SELECT
            ao.id,
            ao.common_symbol,
            ao.trade_type,
            ao.price_a,
            ao.price_b,
            ao.profit_percent,
            ao.traded_volume,
            ao.created_at,
            ea.name AS exchange_a_name,
            eb.name AS exchange_b_name,
            (ao.traded_volume * (ao.price_b - ao.price_a)) AS profit_quote
        FROM arbitrage_opportunities ao
        JOIN exchanges ea ON ao.exchange_a_id = ea.id
        JOIN exchanges eb ON ao.exchange_b_id = eb.id
        ORDER BY ao.created_at DESC
        LIMIT :limit
    """)
    result = await db.execute(raw_sql, {"limit": limit})
    rows = result.all()
    return [
        schemas.ArbitrageOpportunityResponse(
            id=row.id,
            common_symbol=row.common_symbol,
            exchange_a_name=row.exchange_a_name,
            exchange_b_name=row.exchange_b_name,
            trade_type=row.trade_type,
            price_a=row.price_a,
            price_b=row.price_b,
            profit_percent=row.profit_percent,
            traded_volume=row.traded_volume,
            profit_quote=float(row.profit_quote) if row.profit_quote is not None else 0.0,
            created_at=row.created_at
        )
        for row in rows
    ]

@router.get("/opportunities/summary", response_model=list[schemas.OpportunitySummaryItem])
async def get_opportunity_summary(db: AsyncSession = Depends(get_db)):
    raw_sql = text("""
        SELECT
            common_symbol,
            COUNT(*) AS total_opportunities,
            SUM(profit_percent) AS sum_profit_percent,
            AVG(profit_percent) AS avg_profit_percent,
            SUM(traded_volume * (price_b - price_a)) AS total_estimated_profit_quote
        FROM arbitrage_opportunities
        GROUP BY common_symbol
    """)
    result = await db.execute(raw_sql)
    rows = result.all()
    summary = []
    for row in rows:
        if row.common_symbol.endswith("IRT"):
            quote = "IRT"
        elif row.common_symbol.endswith("USDT"):
            quote = "USDT"
        else:
            quote = "UNKNOWN"
        summary.append(
            schemas.OpportunitySummaryItem(
                common_symbol=row.common_symbol,
                total_opportunities=row.total_opportunities,
                sum_profit_percent=float(row.sum_profit_percent) if row.sum_profit_percent else 0.0,
                avg_profit_percent=float(row.avg_profit_percent) if row.avg_profit_percent else 0.0,
                total_estimated_profit_quote=float(row.total_estimated_profit_quote) if row.total_estimated_profit_quote else 0.0,
                quote_currency=quote
            )
        )
    return summary

@router.get("/exchanges", response_model=list[schemas.ExchangeResponse])
async def list_exchanges(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Exchange))
    return result.scalars().all()

@router.get("/exchange-symbols", response_model=list[schemas.ExchangeSymbolResponse])
async def list_symbols(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.ExchangeSymbol))
    return result.scalars().all()

@router.get("/balances")
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

@router.get("/stats", response_model=schemas.SystemStats)
async def get_system_stats(db: AsyncSession = Depends(get_db)):
    raw_sql = text("""
        SELECT
            COALESCE(SUM(CASE WHEN common_symbol LIKE '%IRT' THEN traded_volume * (price_b - price_a) ELSE 0 END), 0) AS total_profit_irt,
            COALESCE(SUM(CASE WHEN common_symbol LIKE '%USDT' THEN traded_volume * (price_b - price_a) ELSE 0 END), 0) AS total_profit_usdt
        FROM arbitrage_opportunities
    """)
    result = await db.execute(raw_sql)
    profit_row = result.first()
    profit_irt = float(profit_row.total_profit_irt) if profit_row else 0.0
    profit_usdt = float(profit_row.total_profit_usdt) if profit_row else 0.0

    total_opp = await db.execute(select(func.count(ArbitrageOpportunity.id)))
    total_opp = total_opp.scalar() or 0

    last_scan = await db.execute(select(func.max(OrderbookSnapshot.created_at)))
    last_scan = last_scan.scalar()

    active_ex = await db.execute(select(func.count()).select_from(Exchange).where(Exchange.is_active == True))
    active_ex = active_ex.scalar() or 0

    active_sym = await db.execute(select(func.count()).select_from(ExchangeSymbol).where(ExchangeSymbol.is_active == True))
    active_sym = active_sym.scalar() or 0

    return schemas.SystemStats(
        total_opportunities=total_opp,
        total_profit_irt=round(profit_irt, 2),
        total_profit_usdt=round(profit_usdt, 2),
        last_scan_time=last_scan,
        active_exchanges=active_ex,
        active_symbols=active_sym
    )

@router.get("/settings", response_model=list[schemas.SymbolSettingsResponse])
async def get_arbitrage_settings(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SymbolArbitrageSettings))
    return result.scalars().all()

@router.post("/settings", response_model=schemas.SymbolSettingsResponse)
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

@router.get("/networks", response_model=list[NetworkResponse])
async def get_networks(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Network))
    return result.scalars().all()

@router.get("/risk-settings", response_model=list[RiskSettingsResponse])
async def get_risk_settings(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SymbolArbitrageSettings))
    return result.scalars().all()

@router.put("/risk-settings/{symbol}", response_model=RiskSettingsResponse)
async def update_risk_settings(symbol: str, data: RiskSettingsUpdate, db: AsyncSession = Depends(get_db)):
    stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == symbol)
    settings_obj = (await db.execute(stmt)).scalar_one_or_none()
    if not settings_obj:
        raise HTTPException(status_code=404, detail="Symbol not found")
    for key, value in data.dict(exclude_unset=True).items():
        setattr(settings_obj, key, value)
    await db.commit()
    await db.refresh(settings_obj)
    return settings_obj

from app.apps.arbitrage.models import RejectedOpportunity

@router.get("/rejected-opportunities", response_model=list[schemas.RejectedOpportunityResponse])
async def get_rejected_opportunities(limit: int = 50, db: AsyncSession = Depends(get_db)):
    stmt = select(RejectedOpportunity).order_by(RejectedOpportunity.created_at.desc()).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()