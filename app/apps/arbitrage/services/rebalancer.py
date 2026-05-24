import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Tuple, Optional, List
from app.apps.arbitrage.models import (
    Exchange, BaseInventory, QuoteInventory, SymbolArbitrageSettings, Network, ExchangeSymbol, ExchangeFee
)
from app.apps.arbitrage.inventory import update_base_balance, update_quote_balance, get_base_balance, get_quote_balance
from app.exchanges.factory import get_exchange_client
from .opportunity_logger import OpportunityLogger

logger = logging.getLogger(__name__)

class Rebalancer:
    def __init__(self, logger: OpportunityLogger, trade_executor=None):
        self.logger = logger
        self.trade_executor = trade_executor

    # ---------- Base asset rebalancing ----------
    async def rebalance_symbol_if_needed(
        self,
        db: AsyncSession,
        common_symbol: str,
        threshold_ratio: float = 0.1
    ):
        # (Your existing implementation for base assets)
        stmt = (
            select(Exchange.name, Exchange.id, BaseInventory.balance)
            .join(BaseInventory, BaseInventory.exchange_id == Exchange.id)
            .where(BaseInventory.common_symbol == common_symbol)
            .where(Exchange.is_active == True)
        )
        result = await db.execute(stmt)
        rows = result.all()
        if not rows or len(rows) < 2:
            return

        balances = [(r.name, float(r.balance)) for r in rows]
        avg_balance = sum(b for _, b in balances) / len(balances)
        min_balance = min(b for _, b in balances)

        if min_balance >= threshold_ratio * avg_balance:
            return

        poorest = min(balances, key=lambda x: x[1])
        richest = max(balances, key=lambda x: x[1])

        transfer_amount = min(richest[1] * 0.75, richest[1] - 1e-6)
        if transfer_amount <= 0:
            return

        # Get network fee
        settings_stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == common_symbol)
        settings_obj = (await db.execute(settings_stmt)).scalar_one_or_none()
        network_fee = 0.0
        if settings_obj and settings_obj.default_network_id:
            net_stmt = select(Network).where(Network.id == settings_obj.default_network_id)
            net = (await db.execute(net_stmt)).scalar_one_or_none()
            if net:
                network_fee = float(net.fee_per_transfer)

        # Simple transfer (network fee > 0; opportunistic rebalancing can be added later)
        if transfer_amount <= network_fee:
            logger.info(f"Transfer amount {transfer_amount:.4f} {common_symbol} <= network fee {network_fee}, skipping")
            return

        net_received = transfer_amount - network_fee

        await update_base_balance(db, richest[0], common_symbol, -transfer_amount)
        await update_base_balance(db, poorest[0], common_symbol, net_received)

        await self.logger.log_rebalance(
            db, common_symbol, None, richest[0], poorest[0],
            transfer_amount, network_fee, net_received,
            f"base_balance_{common_symbol}_below_{threshold_ratio*100:.0f}%_avg"
        )
        logger.info(
            f"🔄 Rebalanced {common_symbol}: sent {transfer_amount:.4f} from {richest[0]} to {poorest[0]} "
            f"(network fee {network_fee:.4f}), net received {net_received:.4f}"
        )

    # ---------- Quote currency rebalancing (IRT, USDT) ----------
    async def rebalance_quote_if_needed(
        self,
        db: AsyncSession,
        currency: str,           # "IRT" or "USDT"
        threshold_ratio: float = 0.1
    ):
        """
        If any exchange's quote balance (IRT or USDT) is less than threshold_ratio * average_balance,
        transfer from the richest exchange to the poorest.
        For now, uses simple transfer (network fee = 0 for fiat/internal transfers).
        """
        stmt = (
            select(Exchange.name, Exchange.id, QuoteInventory.balance)
            .join(QuoteInventory, QuoteInventory.exchange_id == Exchange.id)
            .where(QuoteInventory.currency == currency)
            .where(Exchange.is_active == True)
        )
        result = await db.execute(stmt)
        rows = result.all()
        if not rows or len(rows) < 2:
            return

        balances = [(r.name, float(r.balance)) for r in rows]
        avg_balance = sum(b for _, b in balances) / len(balances)
        min_balance = min(b for _, b in balances)

        if min_balance >= threshold_ratio * avg_balance:
            return

        poorest = min(balances, key=lambda x: x[1])
        richest = max(balances, key=lambda x: x[1])

        transfer_amount = richest[1] * 0.8
        if transfer_amount <= 0:
            return

        # For quote transfers, assume zero network fee (internal transfer)
        network_fee = 0.0

        if transfer_amount <= network_fee:
            return

        net_received = transfer_amount - network_fee

        await update_quote_balance(db, richest[0], currency, -transfer_amount)
        await update_quote_balance(db, poorest[0], currency, net_received)

        await self.logger.log_rebalance(
            db, None, currency, richest[0], poorest[0],
            transfer_amount, network_fee, net_received,
            f"quote_balance_{currency}_below_{threshold_ratio*100:.0f}%_avg"
        )
        logger.info(
            f"🔄 Rebalanced {currency}: sent {transfer_amount:.4f} from {richest[0]} to {poorest[0]} "
            f"(network fee {network_fee:.4f}), net received {net_received:.4f}"
        )