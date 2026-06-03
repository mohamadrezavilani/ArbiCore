from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from app.core.database import get_db
from app.apps.arbitrage.models import ArbitrageOpportunity, RejectedOpportunity
from app.apps.arbitrage import schemas

router = APIRouter()

@router.get("/", response_model=list[schemas.ArbitrageOpportunityResponse])
async def get_opportunities(limit: int = 20, db: AsyncSession = Depends(get_db)):
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
            ao.profit_quote
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

@router.get("/summary", response_model=list[schemas.OpportunitySummaryItem])
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

@router.get("/rejected", response_model=list[schemas.RejectedOpportunityResponse])
async def get_rejected_opportunities(limit: int = 50, db: AsyncSession = Depends(get_db)):
    stmt = select(RejectedOpportunity).order_by(RejectedOpportunity.created_at.desc()).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()