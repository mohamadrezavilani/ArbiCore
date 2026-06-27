import logging
from typing import Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.apps.arbitrage.models import Exchange, ExchangeSymbol, BaseInventory, QuoteInventory
from app.exchanges.factory import get_exchange_client

logger = logging.getLogger(__name__)


class BalanceSyncService:
    """Fetches real balances from live exchanges and updates local inventory."""

    @staticmethod
    async def sync_all_balances(db: AsyncSession) -> Dict[str, Any]:
        """Sync balances for all active exchanges with mode='live'."""
        stmt = select(Exchange).where(Exchange.is_active == True, Exchange.mode == "live")
        result = await db.execute(stmt)
        exchanges = result.scalars().all()
        if not exchanges:
            logger.warning("No live exchanges found to sync.")
            return {"message": "No live exchanges"}
        summary = {}
        for exchange in exchanges:
            try:
                summary[exchange.name] = await BalanceSyncService.sync_exchange_balance(db, exchange.name)
            except Exception as e:
                logger.exception(f"Failed to sync balances for {exchange.name}: {e}")
                summary[exchange.name] = {"error": str(e)}
        await db.commit()
        return summary

    @staticmethod
    async def sync_exchange_balance(db: AsyncSession, exchange_name: str) -> Dict[str, float]:
        """Sync balances for a single exchange by name."""
        stmt = select(Exchange).where(Exchange.name == exchange_name, Exchange.is_active == True, Exchange.mode == "live")
        exchange = (await db.execute(stmt)).scalar_one_or_none()
        if not exchange:
            raise ValueError(f"Exchange {exchange_name} not found or not live")
        client = get_exchange_client(exchange_name)
        if not client:
            raise ValueError(f"No client for {exchange_name}")
        balances = await client.get_balances()
        # Update quote inventory (IRT, USDT)
        if "IRT" in balances:
            await BalanceSyncService._update_quote_inventory(db, exchange.id, "IRT", balances["IRT"])
        if "USDT" in balances:
            symbol_stmt = select(ExchangeSymbol).where(
                ExchangeSymbol.exchange_id == exchange.id,
                ExchangeSymbol.common_symbol == "USDTIRT"
            )
            symbol = (await db.execute(symbol_stmt)).scalar_one_or_none()
            if symbol:
                await BalanceSyncService._update_base_inventory(db, exchange.id, "USDTIRT", balances["USDT"])
            else:
                logger.warning(f"No symbol mapping for USDT on {exchange_name}, skipping base update.")
        await db.commit()
        return balances

    @staticmethod
    async def _update_quote_inventory(db: AsyncSession, exchange_id: str, currency: str, balance: float):
        stmt = select(QuoteInventory).where(
            QuoteInventory.exchange_id == exchange_id,
            QuoteInventory.currency == currency
        )
        inv = (await db.execute(stmt)).scalar_one_or_none()
        if inv:
            inv.balance = balance
        else:
            db.add(QuoteInventory(exchange_id=exchange_id, currency=currency, balance=balance))
        logger.info(f"Updated {currency} balance for exchange {exchange_id} to {balance}")

    @staticmethod
    async def _update_base_inventory(db: AsyncSession, exchange_id: str, common_symbol: str, balance: float):
        stmt = select(BaseInventory).where(
            BaseInventory.exchange_id == exchange_id,
            BaseInventory.common_symbol == common_symbol
        )
        inv = (await db.execute(stmt)).scalar_one_or_none()
        if inv:
            inv.balance = balance
        else:
            db.add(BaseInventory(exchange_id=exchange_id, common_symbol=common_symbol, balance=balance))
        logger.info(f"Updated {common_symbol} balance for exchange {exchange_id} to {balance}")