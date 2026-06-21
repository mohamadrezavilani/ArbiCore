import asyncio
import uuid
import logging
from decimal import Decimal, getcontext
from typing import Tuple, Optional, Any, List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.apps.arbitrage.models import Exchange, ExchangeSymbol
from app.apps.arbitrage.inventory import set_base_balance, set_quote_balance
from .opportunity_logger import OpportunityLogger

# Set high precision for Decimal
getcontext().prec = 28

logger = logging.getLogger(__name__)


class TradeExecutor:
    def __init__(self, logger: OpportunityLogger):
        self.logger = logger

    async def sync_balances_from_exchange(self, db: AsyncSession, exchange_name: str, client):
        """
        Fetch real balances from a live exchange and update local inventory.
        """
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
            logger.info(f"[SYNC] Synced balances for {exchange_name}: {real_balances}")
        except Exception as e:
            logger.error(f"[SYNC] Failed to sync balances for {exchange_name}: {e}")

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
        Atomic trade execution with robust partial fill handling.
        Returns:
            success, filled_vol, vwap_buy, vwap_sell,
            base_delta_buy, base_delta_sell, quote_delta_buy, quote_delta_sell, net_profit
        """
        # ---------- Simulator mode (unchanged) ----------
        if not is_live:
            logger.info(f"[SIM] Simulating trade: {common_symbol} | buy={buy_exchange} | sell={sell_exchange} | vol={volume:.4f}")
            vol_dec = Decimal(str(volume))
            buy_price_dec = Decimal(str(vwap_buy))
            sell_price_dec = Decimal(str(vwap_sell))
            buy_fee_dec = Decimal(str(buy_fee_rate))
            sell_fee_dec = Decimal(str(sell_fee_rate))

            effective_buy = buy_price_dec * (1 + buy_fee_dec)
            effective_sell = sell_price_dec * (1 - sell_fee_dec)

            cost = vol_dec * effective_buy
            revenue = vol_dec * effective_sell
            net_profit = revenue - cost

            base_delta_buy = float(vol_dec)
            base_delta_sell = -float(vol_dec)
            quote_delta_buy = -float(cost)
            quote_delta_sell = float(revenue)

            logger.info(
                f"[SIM] Result: filled={volume:.4f} | buy_vwap={vwap_buy:.2f} | sell_vwap={vwap_sell:.2f} | "
                f"net_profit={net_profit:.2f} {quote_currency}"
            )
            return (
                True,
                volume,
                vwap_buy,
                vwap_sell,
                base_delta_buy,
                base_delta_sell,
                quote_delta_buy,
                quote_delta_sell,
                float(net_profit)
            )

        # ---------- LIVE MODE WITH ROBUST ATOMIC EXECUTION ----------
        if buy_client is None or sell_client is None:
            logger.error("[LIVE] Trade aborted: missing exchange client(s)")
            return False, 0, 0, 0, 0, 0, 0, 0, 0

        # Configuration
        POLL_INTERVAL = 0.5          # seconds between status checks
        MAX_DURATION = 300           # 5 minutes maximum
        MIN_FILL_THRESHOLD = 0.01    # 1% tolerance for full fill

        target_volume = volume
        start_time = asyncio.get_event_loop().time()

        logger.info(
            f"[LIVE] STARTING ATOMIC TRADE | symbol={common_symbol} | "
            f"buy={buy_exchange} (fee={buy_fee_rate:.4f}) | sell={sell_exchange} (fee={sell_fee_rate:.4f}) | "
            f"target_vol={target_volume:.4f} | max_duration={MAX_DURATION}s"
        )
        logger.info(f"[LIVE] Initial prices: buy_vwap={vwap_buy:.2f} | sell_vwap={vwap_sell:.2f}")

        # We'll maintain a list of orders for each side
        buy_orders: List[Dict] = []   # each: {'client_order_id': str, 'filled_vol': float, 'status': str, 'result': OrderResult}
        sell_orders: List[Dict] = []

        # Helper to place a new order and add to the list
        async def place_order_side(side: str, amount: float, client: Any, orders_list: List[Dict]) -> str:
            cid = f"{side}_{uuid.uuid4().hex[:8]}"
            logger.info(f"[LIVE] Placing {side.upper()} order: cid={cid} | amount={amount:.4f} | exchange={client.__class__.__name__}")
            try:
                order_result = await client.place_market_order(
                    symbol=common_symbol,
                    side=side,
                    amount=amount,
                    client_order_id=cid
                )
                orders_list.append({
                    'client_order_id': cid,
                    'filled_vol': order_result.filled_volume,
                    'status': order_result.status,
                    'result': order_result
                })
                logger.info(
                    f"[LIVE] Order placed: cid={cid} | initial_status={order_result.status} | "
                    f"filled={order_result.filled_volume:.4f} | price={order_result.filled_price:.2f}"
                )
                return cid
            except Exception as e:
                logger.error(f"[LIVE] Failed to place {side} order: cid={cid} | error={e}")
                raise

        # Place initial orders
        try:
            await place_order_side('buy', target_volume, buy_client, buy_orders)
            await place_order_side('sell', target_volume, sell_client, sell_orders)
        except Exception as e:
            logger.error(f"[LIVE] Initial order placement failed: {e}")
            # Try to cancel any orders that might have been placed
            await self._cancel_orders(buy_orders, buy_client, "buy")
            await self._cancel_orders(sell_orders, sell_client, "sell")
            return False, 0, 0, 0, 0, 0, 0, 0, 0

        last_log_time = start_time
        iteration = 0

        # Polling loop
        while (asyncio.get_event_loop().time() - start_time) < MAX_DURATION:
            iteration += 1
            now = asyncio.get_event_loop().time()

            # Update status for all pending orders
            for order in buy_orders + sell_orders:
                if order['status'] not in ('filled', 'cancelled', 'failed'):
                    try:
                        client = buy_client if order in buy_orders else sell_client
                        status = await client.order_status(order['client_order_id'])
                        old_filled = order['filled_vol']
                        new_filled = status.filled_volume
                        if abs(new_filled - old_filled) > 0.0001 or status.status != order['status']:
                            logger.info(
                                f"[LIVE] Status update: cid={order['client_order_id']} | "
                                f"status={status.status} | filled={new_filled:.4f} (delta={new_filled - old_filled:.4f}) | "
                                f"price={status.filled_price:.2f}"
                            )
                        order['filled_vol'] = new_filled
                        order['status'] = status.status
                        order['result'] = status
                    except Exception as e:
                        logger.warning(f"[LIVE] Status check failed for cid={order['client_order_id']}: {e}")

            # Calculate total filled per side
            total_buy_filled = sum(o['filled_vol'] for o in buy_orders)
            total_sell_filled = sum(o['filled_vol'] for o in sell_orders)

            # Log summary every 10 seconds
            if now - last_log_time >= 10:
                logger.info(
                    f"[LIVE] Poll # {iteration} | elapsed={now - start_time:.1f}s | "
                    f"buy_filled={total_buy_filled:.4f}/{target_volume:.4f} | "
                    f"sell_filled={total_sell_filled:.4f}/{target_volume:.4f} | "
                    f"buy_orders={len(buy_orders)} | sell_orders={len(sell_orders)}"
                )
                last_log_time = now

            # Check if both sides are fully filled (within threshold)
            if total_buy_filled >= target_volume * (1 - MIN_FILL_THRESHOLD) and \
               total_sell_filled >= target_volume * (1 - MIN_FILL_THRESHOLD):
                filled_vol = min(total_buy_filled, total_sell_filled)
                buy_vwap = self._compute_vwap(buy_orders)
                sell_vwap = self._compute_vwap(sell_orders)
                logger.info(
                    f"[LIVE] ✅ TRADE SUCCESS | filled={filled_vol:.4f} | "
                    f"buy_vwap={buy_vwap:.2f} | sell_vwap={sell_vwap:.2f} | "
                    f"elapsed={now - start_time:.1f}s | iterations={iteration}"
                )
                return True, filled_vol, buy_vwap, sell_vwap, 0.0, 0.0, 0.0, 0.0, 0.0

            # Now handle imbalances
            buy_fully_filled = total_buy_filled >= target_volume * (1 - MIN_FILL_THRESHOLD)
            sell_fully_filled = total_sell_filled >= target_volume * (1 - MIN_FILL_THRESHOLD)

            if buy_fully_filled and not sell_fully_filled:
                # Buy side is complete, need to fill sell side to match buy amount
                target_sell_volume = total_buy_filled - total_sell_filled
                if target_sell_volume > 0.001:
                    logger.warning(
                        f"[LIVE] ⚠️ IMBALANCE: buy complete ({total_buy_filled:.4f}), sell missing {target_sell_volume:.4f}. "
                        f"Cancelling pending sell orders and placing new sell for {target_sell_volume:.4f}"
                    )
                    await self._cancel_orders(sell_orders, sell_client, "sell")
                    sell_orders.clear()
                    await place_order_side('sell', target_sell_volume, sell_client, sell_orders)
                else:
                    logger.info(f"[LIVE] Sell missing amount {target_sell_volume:.4f} is too small, skipping adjustment")

            elif sell_fully_filled and not buy_fully_filled:
                target_buy_volume = total_sell_filled - total_buy_filled
                if target_buy_volume > 0.001:
                    logger.warning(
                        f"[LIVE] ⚠️ IMBALANCE: sell complete ({total_sell_filled:.4f}), buy missing {target_buy_volume:.4f}. "
                        f"Cancelling pending buy orders and placing new buy for {target_buy_volume:.4f}"
                    )
                    await self._cancel_orders(buy_orders, buy_client, "buy")
                    buy_orders.clear()
                    await place_order_side('buy', target_buy_volume, buy_client, buy_orders)
                else:
                    logger.info(f"[LIVE] Buy missing amount {target_buy_volume:.4f} is too small, skipping adjustment")

            else:
                # Both partially filled (or not filled at all)
                if total_buy_filled == 0 and total_sell_filled > 0:
                    logger.warning(
                        f"[LIVE] ⚠️ IMBALANCE: buy_filled=0, sell_filled={total_sell_filled:.4f}. "
                        f"Cancelling buy orders and placing new buy for {total_sell_filled:.4f}"
                    )
                    await self._cancel_orders(buy_orders, buy_client, "buy")
                    buy_orders.clear()
                    await place_order_side('buy', total_sell_filled, buy_client, buy_orders)

                elif total_sell_filled == 0 and total_buy_filled > 0:
                    logger.warning(
                        f"[LIVE] ⚠️ IMBALANCE: sell_filled=0, buy_filled={total_buy_filled:.4f}. "
                        f"Cancelling sell orders and placing new sell for {total_buy_filled:.4f}"
                    )
                    await self._cancel_orders(sell_orders, sell_client, "sell")
                    sell_orders.clear()
                    await place_order_side('sell', total_buy_filled, sell_client, sell_orders)

                # else both have some but not fully filled – just wait

            await asyncio.sleep(POLL_INTERVAL)

        # Timeout reached – cancel all open orders and report failure
        elapsed = asyncio.get_event_loop().time() - start_time
        logger.error(
            f"[LIVE] ❌ TRADE TIMEOUT after {elapsed:.1f}s | "
            f"buy_filled={total_buy_filled:.4f} | sell_filled={total_sell_filled:.4f} | "
            f"iterations={iteration}. Cancelling all open orders."
        )
        await self._cancel_orders(buy_orders, buy_client, "buy")
        await self._cancel_orders(sell_orders, sell_client, "sell")
        return False, 0, 0, 0, 0, 0, 0, 0, 0

    async def _cancel_orders(self, orders: List[Dict], client, side_label: str):
        """Cancel all orders in the list that are still pending."""
        if not orders:
            return
        for order in orders:
            if order['status'] not in ('filled', 'cancelled', 'failed'):
                cid = order['client_order_id']
                try:
                    logger.info(f"[LIVE] Cancelling {side_label} order: cid={cid}")
                    await client.cancel_order(cid)
                    order['status'] = 'cancelled'
                    logger.info(f"[LIVE] Cancelled {side_label} order: cid={cid}")
                except Exception as e:
                    logger.warning(f"[LIVE] Failed to cancel {side_label} order cid={cid}: {e}")

    def _compute_vwap(self, orders: List[Dict]) -> float:
        """
        Compute volume-weighted average price from all order results stored in the list.
        """
        total_vol = 0.0
        total_value = 0.0
        for order in orders:
            res = order.get('result')
            if res and res.filled_volume > 0 and res.filled_price > 0:
                total_vol += res.filled_volume
                total_value += res.filled_volume * res.filled_price
        vwap = total_value / total_vol if total_vol > 0 else 0.0
        if total_vol > 0:
            logger.debug(f"[VWAP] computed {vwap:.2f} from {total_vol:.4f} vol over {len(orders)} orders")
        return vwap