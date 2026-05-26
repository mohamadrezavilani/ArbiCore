import asyncio
import uuid
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Tuple, Optional
from app.apps.arbitrage.models import Exchange, ExchangeSymbol
from app.apps.arbitrage.inventory import (
    update_base_balance, update_quote_balance, set_base_balance, set_quote_balance
)
from app.exchanges.factory import get_exchange_client
from .opportunity_logger import OpportunityLogger

logger = logging.getLogger(__name__)

class TradeExecutor:
    def __init__(self, logger: OpportunityLogger):
        self.logger = logger

    async def sync_balances_from_exchange(self, db: AsyncSession, exchange_name: str, client):
        """After a live trade, replace database balances with real exchange balances."""
        real_balances = await client.get_balances()
        stmt = select(ExchangeSymbol).join(Exchange).where(Exchange.name == exchange_name)
        result = await db.execute(stmt)
        symbols = result.scalars().all()
        asset_to_common = {sym.original_symbol: sym.common_symbol for sym in symbols}
        for asset, balance in real_balances.items():
            if asset in ("IRT", "USDT"):
                await set_quote_balance(db, exchange_name, asset, balance)
            else:
                common = asset_to_common.get(asset, asset)
                await set_base_balance(db, exchange_name, common, balance)

    async def execute(
        self,
        db: AsyncSession,
        common_symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        volume: float,
        quote_currency: str,
        buy_client,      # can be None in simulator mode
        sell_client,     # can be None in simulator mode
        buy_exch_obj,
        sell_exch_obj,
        buy_fee_rate: float = 0.0,
        sell_fee_rate: float = 0.0,
        vwap_buy: Optional[float] = None,   # for simulator: pre‑computed VWAP buy price
        vwap_sell: Optional[float] = None   # for simulator: pre‑computed VWAP sell price
    ) -> Tuple[bool, float, float, float]:
        """
        Execute a trade: buy on buy_exchange, sell on sell_exchange.
        Returns (success, filled_volume, vwap_buy, vwap_sell).
        For simulator: uses provided vwap_buy/vwap_sell and updates balances directly.
        For live: places market orders and syncs balances.
        """
        # Determine mode
        buy_mode = (await db.execute(select(Exchange.mode).where(Exchange.name == buy_exchange))).scalar_one_or_none()
        sell_mode = (await db.execute(select(Exchange.mode).where(Exchange.name == sell_exchange))).scalar_one_or_none()
        is_live = (buy_mode == "live" and sell_mode == "live")

        if not is_live:
            # Simulator mode: update balances directly using VWAP and fees
            if vwap_buy is None or vwap_sell is None:
                logger.error("Simulator mode requires vwap_buy and vwap_sell parameters")
                return False, 0, 0, 0

            # Apply fees to prices (as in real execution)
            effective_buy_price = vwap_buy * (1 + buy_fee_rate)
            effective_sell_price = vwap_sell * (1 - sell_fee_rate)

            cost = volume * effective_buy_price
            revenue = volume * effective_sell_price

            # Update base balances
            await update_base_balance(db, buy_exchange, common_symbol, volume)
            await update_base_balance(db, sell_exchange, common_symbol, -volume)

            # Update quote balances
            await update_quote_balance(db, buy_exchange, quote_currency, -cost)
            await update_quote_balance(db, sell_exchange, quote_currency, revenue)

            logger.info(f"Simulator trade: {volume:.4f} {common_symbol} bought at {effective_buy_price:.2f}, sold at {effective_sell_price:.2f}")
            return True, volume, vwap_buy, vwap_sell

        # Live trading
        if buy_client is None or sell_client is None:
            logger.error("Live mode requires valid exchange clients")
            return False, 0, 0, 0

        timeout_initial = 5.0
        timeout_extended = 60.0
        poll_interval = 0.5

        short_uuid = uuid.uuid4().hex[:8]
        buy_order_id = f"buy_{short_uuid}"
        sell_order_id = f"sell_{short_uuid}"

        buy_task = asyncio.create_task(buy_client.place_market_order(
            symbol=common_symbol, side="buy", amount=volume, client_order_id=buy_order_id
        ))
        sell_task = asyncio.create_task(sell_client.place_market_order(
            symbol=common_symbol, side="sell", amount=volume, client_order_id=sell_order_id
        ))
        buy_result, sell_result = await asyncio.gather(buy_task, sell_task)

        if buy_result.status != "filled" or sell_result.status != "filled":
            if buy_result.status != "filled" and buy_result.order_id:
                await buy_client.cancel_order(buy_result.client_order_id)
            if sell_result.status != "filled" and sell_result.order_id:
                await sell_client.cancel_order(sell_result.client_order_id)
            await self.logger.log_rejected_opportunity(
                db, common_symbol, buy_exchange, sell_exchange,
                f"buy_on_{buy_exchange}_sell_on_{sell_exchange}",
                "Atomic trade failed (order placement)",
                {"buy_status": buy_result.status, "sell_status": sell_result.status}
            )
            return False, 0, 0, 0

        start = asyncio.get_event_loop().time()
        buy_filled = sell_filled = False
        while (asyncio.get_event_loop().time() - start) < timeout_initial:
            if not buy_filled:
                buy_status = await buy_client.order_status(buy_result.client_order_id)
                if buy_status.status == "filled":
                    buy_filled = True
                    buy_result = buy_status
            if not sell_filled:
                sell_status = await sell_client.order_status(sell_result.client_order_id)
                if sell_status.status == "filled":
                    sell_filled = True
                    sell_result = sell_status
            if buy_filled and sell_filled:
                break
            await asyncio.sleep(poll_interval)

        if buy_filled and sell_filled:
            vwap_buy = buy_result.filled_price
            vwap_sell = sell_result.filled_price
            filled_vol = min(buy_result.filled_volume, sell_result.filled_volume)
            await self.sync_balances_from_exchange(db, buy_exchange, buy_client)
            await self.sync_balances_from_exchange(db, sell_exchange, sell_client)
            return True, filled_vol, vwap_buy, vwap_sell

        # Extended wait for missing leg
        if buy_filled and not sell_filled:
            extended_start = asyncio.get_event_loop().time()
            while (asyncio.get_event_loop().time() - extended_start) < timeout_extended:
                sell_status = await sell_client.order_status(sell_result.client_order_id)
                if sell_status.status == "filled":
                    sell_filled = True
                    sell_result = sell_status
                    break
                await asyncio.sleep(poll_interval)
            if not sell_filled:
                await sell_client.cancel_order(sell_result.client_order_id)
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exchange, sell_exchange,
                    f"buy_on_{buy_exchange}_sell_on_{sell_exchange}",
                    "Second leg (sell) did not fill within extended timeout",
                    {"buy_filled": buy_filled, "sell_filled": sell_filled}
                )
                return False, 0, 0, 0
        elif sell_filled and not buy_filled:
            extended_start = asyncio.get_event_loop().time()
            while (asyncio.get_event_loop().time() - extended_start) < timeout_extended:
                buy_status = await buy_client.order_status(buy_result.client_order_id)
                if buy_status.status == "filled":
                    buy_filled = True
                    buy_result = buy_status
                    break
                await asyncio.sleep(poll_interval)
            if not buy_filled:
                await buy_client.cancel_order(buy_result.client_order_id)
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exchange, sell_exchange,
                    f"buy_on_{buy_exchange}_sell_on_{sell_exchange}",
                    "Second leg (buy) did not fill within extended timeout",
                    {"buy_filled": buy_filled, "sell_filled": sell_filled}
                )
                return False, 0, 0, 0
        else:
            await buy_client.cancel_order(buy_result.client_order_id)
            await sell_client.cancel_order(sell_result.client_order_id)
            await self.logger.log_rejected_opportunity(
                db, common_symbol, buy_exchange, sell_exchange,
                f"buy_on_{buy_exchange}_sell_on_{sell_exchange}",
                "Neither leg filled within initial timeout",
                {"buy_filled": buy_filled, "sell_filled": sell_filled}
            )
            return False, 0, 0, 0

        # Both filled after extended wait
        vwap_buy = buy_result.filled_price
        vwap_sell = sell_result.filled_price
        filled_vol = min(buy_result.filled_volume, sell_result.filled_volume)
        await self.sync_balances_from_exchange(db, buy_exchange, buy_client)
        await self.sync_balances_from_exchange(db, sell_exchange, sell_client)
        return True, filled_vol, vwap_buy, vwap_sell

    async def update_balances_simulator(
        self,
        db: AsyncSession,
        buy_exch: str,
        sell_exch: str,
        common_symbol: str,
        quote_currency: str,
        volume: float,
        cost: float,
        revenue: float
    ):
        """Legacy method – kept for compatibility. Prefer using execute() with simulator mode."""
        await update_base_balance(db, buy_exch, common_symbol, volume)
        await update_base_balance(db, sell_exch, common_symbol, -volume)
        await update_quote_balance(db, buy_exch, quote_currency, -cost)
        await update_quote_balance(db, sell_exch, quote_currency, revenue)