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
        """
        Sync balances for all active exchanges with mode='live'.
        Returns summary: {'exchange_name': {'base': {'USDT': balance}, 'quote': {'IRT': balance}}}
        """
        # Get all live exchanges
        stmt = select(Exchange).where(Exchange.is_active == True, Exchange.mode == "live")
        result = await db.execute(stmt)
        exchanges = result.scalars().all()

        if not exchanges:
            logger.warning("No live exchanges found to sync.")
            return {"message": "No live exchanges"}

        summary = {}

        for exchange in exchanges:
            client = get_exchange_client(exchange.name)
            if not client:
                logger.error(f"No client for exchange {exchange.name}, skipping.")
                continue

            try:
                balances = await client.get_balances()
                logger.info(f"Fetched balances for {exchange.name}: {balances}")

                # Update quote inventories (IRT, USDT)
                quote_currencies = ["IRT", "USDT"]  # We'll treat both as quote? Actually USDT is base, but we store it as base.
                # We'll treat USDT as base (since it's the base asset in USDTIRT)
                # So only IRT is quote.
                # Update QuoteInventory for IRT
                if "IRT" in balances:
                    await BalanceSyncService._update_quote_inventory(db, exchange.id, "IRT", balances["IRT"])
                # Update BaseInventory for USDT
                if "USDT" in balances:
                    # Find the common_symbol for this exchange that uses USDT as base
                    # We'll assume there's a symbol with common_symbol "USDTIRT"
                    symbol_stmt = select(ExchangeSymbol).where(
                        ExchangeSymbol.exchange_id == exchange.id,
                        ExchangeSymbol.common_symbol == "USDTIRT"
                    )
                    sym_result = await db.execute(symbol_stmt)
                    symbol = sym_result.scalar_one_or_none()
                    if symbol:
                        await BalanceSyncService._update_base_inventory(db, exchange.id, "USDTIRT", balances["USDT"])
                    else:
                        logger.warning(f"No symbol mapping for USDT on {exchange.name}, skipping base update.")

                # Also update any other base assets (if we had other symbols)
                # For simplicity, we only handle USDTIRT for now.

                summary[exchange.name] = balances
                await db.commit()  # commit after each exchange? better commit once after all

            except Exception as e:
                logger.exception(f"Failed to sync balances for {exchange.name}: {e}")
                summary[exchange.name] = {"error": str(e)}

        await db.commit()
        return summary

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