from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app.apps.arbitrage.models import RebalanceLog

router = APIRouter()

@router.get("/")
async def get_rebalance_logs(limit: int = 100, db: AsyncSession = Depends(get_db)):
    stmt = select(RebalanceLog).order_by(RebalanceLog.created_at.desc()).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()