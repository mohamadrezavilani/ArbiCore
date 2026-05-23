import logging
from typing import Optional, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from app.apps.arbitrage.models import RejectedOpportunity, RebalanceLog

logger = logging.getLogger(__name__)

class OpportunityLogger:
    @staticmethod
    async def log_rejected_opportunity(
        db: AsyncSession,
        common_symbol: str,
        exchange_a: str,
        exchange_b: str,
        trade_type: str,
        reason: str,
        details: Optional[Dict] = None
    ):
        if details and details.get('reason') and 'No profit' not in details['reason']:
            logger.info(f"❌ Rejected {common_symbol} {trade_type}: {reason} | details={details}")
        rejected = RejectedOpportunity(
            common_symbol=common_symbol,
            exchange_a_name=exchange_a,
            exchange_b_name=exchange_b,
            trade_type=trade_type,
            rejection_reason=reason,
            details=details or {}
        )
        db.add(rejected)
        await db.flush()

    @staticmethod
    async def log_rebalance(
        db: AsyncSession,
        common_symbol: Optional[str],
        currency: Optional[str],
        from_exch: str,
        to_exch: str,
        amount_sent: float,
        fee: float,
        net: float,
        reason: str
    ):
        log = RebalanceLog(
            common_symbol=common_symbol if common_symbol else None,
            currency=currency if currency else None,
            from_exchange=from_exch,
            to_exchange=to_exch,
            amount_sent=amount_sent,
            network_fee=fee,
            net_received=net,
            reason=reason
        )
        db.add(log)
        await db.flush()