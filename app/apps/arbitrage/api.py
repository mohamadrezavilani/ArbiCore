from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.apps.arbitrage import models, schemas, services
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.apps.arbitrage.models import OrderbookSnapshot, Exchange, ExchangeSymbol, ArbitrageOpportunity
from app.apps.arbitrage.schemas import OrderbookSnapshotResponse
from app.apps.arbitrage.models import BaseInventory, QuoteInventory
from sqlalchemy.orm import aliased

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
    ExchangeA = aliased(Exchange)
    ExchangeB = aliased(Exchange)

    stmt = (
        select(
            ArbitrageOpportunity.id,
            ArbitrageOpportunity.common_symbol,
            ArbitrageOpportunity.trade_type,
            ArbitrageOpportunity.price_a,
            ArbitrageOpportunity.price_b,
            ArbitrageOpportunity.profit_percent,
            ArbitrageOpportunity.traded_volume,
            ArbitrageOpportunity.created_at,
            ExchangeA.name.label("exchange_a_name"),
            ExchangeB.name.label("exchange_b_name"),
            (ArbitrageOpportunity.traded_volume *
             (ArbitrageOpportunity.price_b * (1 - ExchangeB.taker_fee) -
              ArbitrageOpportunity.price_a * (1 + ExchangeA.taker_fee))
             ).label("profit_quote")
        )
        .join(ExchangeA, ArbitrageOpportunity.exchange_a_id == ExchangeA.id)
        .join(ExchangeB, ArbitrageOpportunity.exchange_b_id == ExchangeB.id)
        .order_by(ArbitrageOpportunity.created_at.desc())
        .limit(limit)
    )
    result = await db.execute(stmt)
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
            profit_quote=float(row.profit_quote) if row.profit_quote else 0.0,
            created_at=row.created_at
        )
        for row in rows
    ]

@router.get("/opportunities/summary", response_model=list[schemas.OpportunitySummaryItem])
async def get_opportunity_summary(db: AsyncSession = Depends(get_db)):
    ExchangeA = aliased(Exchange)
    ExchangeB = aliased(Exchange)

    stmt = (
        select(
            ArbitrageOpportunity.common_symbol,
            func.count(ArbitrageOpportunity.id).label("total"),
            func.sum(ArbitrageOpportunity.profit_percent).label("sum_percent"),
            func.avg(ArbitrageOpportunity.profit_percent).label("avg_percent"),
            func.sum(
                ArbitrageOpportunity.traded_volume *
                (ArbitrageOpportunity.price_b * (1 - ExchangeB.taker_fee) -
                 ArbitrageOpportunity.price_a * (1 + ExchangeA.taker_fee))
            ).label("total_profit_quote")
        )
        .join(ExchangeA, ArbitrageOpportunity.exchange_a_id == ExchangeA.id)
        .join(ExchangeB, ArbitrageOpportunity.exchange_b_id == ExchangeB.id)
        .group_by(ArbitrageOpportunity.common_symbol)
    )
    result = await db.execute(stmt)
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
                total_opportunities=row.total,
                sum_profit_percent=float(row.sum_percent) if row.sum_percent else 0.0,
                avg_profit_percent=float(row.avg_percent) if row.avg_percent else 0.0,
                total_estimated_profit_quote=float(row.total_profit_quote) if row.total_profit_quote else 0.0,
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

from app.apps.arbitrage.models import BaseInventory, QuoteInventory

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
    ExchangeA = aliased(Exchange)
    ExchangeB = aliased(Exchange)

    # Total opportunities
    total_opp = await db.execute(select(func.count(ArbitrageOpportunity.id)))
    total_opp = total_opp.scalar() or 0

    # Total profit in IRT
    profit_irt_stmt = (
        select(func.sum(
            ArbitrageOpportunity.traded_volume *
            (ArbitrageOpportunity.price_b * (1 - ExchangeB.taker_fee) -
             ArbitrageOpportunity.price_a * (1 + ExchangeA.taker_fee))
        ))
        .join(ExchangeA, ArbitrageOpportunity.exchange_a_id == ExchangeA.id)
        .join(ExchangeB, ArbitrageOpportunity.exchange_b_id == ExchangeB.id)
        .where(ArbitrageOpportunity.common_symbol.like("%IRT"))
    )
    profit_irt = await db.execute(profit_irt_stmt)
    profit_irt = float(profit_irt.scalar() or 0.0)

    # Total profit in USDT
    profit_usdt_stmt = (
        select(func.sum(
            ArbitrageOpportunity.traded_volume *
            (ArbitrageOpportunity.price_b * (1 - ExchangeB.taker_fee) -
             ArbitrageOpportunity.price_a * (1 + ExchangeA.taker_fee))
        ))
        .join(ExchangeA, ArbitrageOpportunity.exchange_a_id == ExchangeA.id)
        .join(ExchangeB, ArbitrageOpportunity.exchange_b_id == ExchangeB.id)
        .where(ArbitrageOpportunity.common_symbol.like("%USDT"))
    )
    profit_usdt = await db.execute(profit_usdt_stmt)
    profit_usdt = float(profit_usdt.scalar() or 0.0)

    # Last scan time
    last_scan = await db.execute(select(func.max(OrderbookSnapshot.created_at)))
    last_scan = last_scan.scalar()

    # Active exchanges count
    active_ex = await db.execute(select(func.count()).select_from(Exchange).where(Exchange.is_active == True))
    active_ex = active_ex.scalar() or 0

    # Active symbols count
    active_sym = await db.execute(
        select(func.count()).select_from(ExchangeSymbol).where(ExchangeSymbol.is_active == True))
    active_sym = active_sym.scalar() or 0

    return schemas.SystemStats(
        total_opportunities=total_opp,
        total_profit_irt=round(profit_irt, 2),
        total_profit_usdt=round(profit_usdt, 2),
        last_scan_time=last_scan,
        active_exchanges=active_ex,
        active_symbols=active_sym
    )