import asyncio
import uuid
import logging
from typing import Tuple, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.apps.arbitrage.models import Exchange, ExchangeSymbol
from app.apps.arbitrage.inventory import set_base_balance, set_quote_balance
from .opportunity_logger import OpportunityLogger

logger = logging.getLogger(__name__)

class TradeExecutor:
    def __init__(self, logger: OpportunityLogger):
        self.logger = logger

    async def sync_balances_from_exchange(self, db: AsyncSession, exchange_name: str, client):
        try:
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
        except Exception as e:
            logger.error(f"Failed to sync balances for {exchange_name}: {e}")

    async def execute_and_get_deltas(
        self,
        db: AsyncSession,
        common_symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        volume: float,
        quote_currency: str,
        buy_client: Optional[Any],
        sell_client: Optional[Any],
        buy_exch_obj_id: Optional[str],
        sell_exch_obj_id: Optional[str],
        buy_fee_rate: float,
        sell_fee_rate: float,
        vwap_buy: float,
        vwap_sell: float,
        is_live: bool
    ) -> Tuple[bool, float, float, float, float, float, float, float, float]:
        """
        Returns:
            success, filled_vol, vwap_buy, vwap_sell,
            base_delta_buy, base_delta_sell, quote_delta_buy, quote_delta_sell,
            net_profit (in quote currency)
        """
        if not is_live:
            # Simulator mode: compute deltas and net profit
            effective_buy = vwap_buy * (1 + buy_fee_rate)
            effective_sell = vwap_sell * (1 - sell_fee_rate)
            cost = volume * effective_buy
            revenue = volume * effective_sell
            net_profit = revenue - cost
            base_delta_buy = volume
            base_delta_sell = -volume
            quote_delta_buy = -cost
            quote_delta_sell = revenue
            logger.info(f"Simulator trade: {volume:.4f} {common_symbol} buy@{buy_exchange} {effective_buy:.2f} sell@{sell_exchange} {effective_sell:.2f} net_profit={net_profit:.2f}")
            return True, volume, vwap_buy, vwap_sell, base_delta_buy, base_delta_sell, quote_delta_buy, quote_delta_sell, net_profit

        # Live mode (simplified – net profit calculation would need actual fill prices)
        if buy_client is None or sell_client is None:
            logger.error("Live mode requires valid exchange clients")
            return False, 0, 0, 0, 0, 0, 0, 0, 0

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
            return False, 0, 0, 0, 0, 0, 0, 0, 0

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
            vwap_buy_final = buy_result.filled_price
            vwap_sell_final = sell_result.filled_price
            filled_vol = min(buy_result.filled_volume, sell_result.filled_volume)
            cost = filled_vol * vwap_buy_final
            revenue = filled_vol * vwap_sell_final
            net_profit = revenue - cost  # note: fees are already deducted in filled_price? Usually market orders include fee in filled_price? Depends on exchange. For simplicity, assume net.
            await self.sync_balances_from_exchange(db, buy_exchange, buy_client)
            await self.sync_balances_from_exchange(db, sell_exchange, sell_client)
            return True, filled_vol, vwap_buy_final, vwap_sell_final, 0.0, 0.0, 0.0, 0.0, net_profit

        # Extended wait (keep existing logic)
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
                return False, 0, 0, 0, 0, 0, 0, 0, 0
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
                return False, 0, 0, 0, 0, 0, 0, 0, 0
        else:
            await buy_client.cancel_order(buy_result.client_order_id)
            await sell_client.cancel_order(sell_result.client_order_id)
            await self.logger.log_rejected_opportunity(
                db, common_symbol, buy_exchange, sell_exchange,
                f"buy_on_{buy_exchange}_sell_on_{sell_exchange}",
                "Neither leg filled within initial timeout",
                {"buy_filled": buy_filled, "sell_filled": sell_filled}
            )
            return False, 0, 0, 0, 0, 0, 0, 0, 0

        # Both filled after extended wait
        vwap_buy_final = buy_result.filled_price
        vwap_sell_final = sell_result.filled_price
        filled_vol = min(buy_result.filled_volume, sell_result.filled_volume)
        cost = filled_vol * vwap_buy_final
        revenue = filled_vol * vwap_sell_final
        net_profit = revenue - cost
        await self.sync_balances_from_exchange(db, buy_exchange, buy_client)
        await self.sync_balances_from_exchange(db, sell_exchange, sell_client)
        return True, filled_vol, vwap_buy_final, vwap_sell_final, 0.0, 0.0, 0.0, 0.0, net_profit