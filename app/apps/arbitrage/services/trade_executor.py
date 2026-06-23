import asyncio
import uuid
import logging
import sys
from decimal import Decimal, getcontext
from typing import Tuple, Optional, Any, List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.apps.arbitrage.models import Exchange, ExchangeSymbol
from app.apps.arbitrage.inventory import set_base_balance, set_quote_balance
from .opportunity_logger import OpportunityLogger

# Set high precision for Decimal
getcontext().prec = 28

# Force UTF‑8 for Windows console to avoid Unicode errors
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

logger = logging.getLogger(__name__)

# Minimum order size in USDT for live trading
MIN_ORDER_SIZE = {
    "wallex": 2.0,     # Wallex minimum order is 2 USDT
    "bitpin": 2.0,     # Bitpin also requires at least 2 USDT (verify)
    "nobitex": 2.0,    # adjust if needed
}


class TradeExecutor:
    def __init__(self, logger: OpportunityLogger):
        self.logger = logger

    async def sync_balances_from_exchange(self, db: AsyncSession, exchange_name: str, client):
        """Fetch real balances from a live exchange and update local inventory."""
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
        Returns: success, filled_vol, vwap_buy, vwap_sell,
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
            logger.error("Live mode requires valid exchange clients")
            return False, 0, 0, 0, 0, 0, 0, 0, 0

        # Check minimum order size for both exchanges
        min_buy = MIN_ORDER_SIZE.get(buy_exchange, 2.0)
        min_sell = MIN_ORDER_SIZE.get(sell_exchange, 2.0)
        if volume < min_buy or volume < min_sell:
            logger.warning(
                f"[LIVE] Volume {volume:.4f} is below minimum order size: "
                f"{buy_exchange} needs {min_buy}, {sell_exchange} needs {min_sell}. Rejecting trade."
            )
            return False, 0, 0, 0, 0, 0, 0, 0, 0

        # Fetch original symbols for the exchanges (critical fix)
        stmt = select(ExchangeSymbol.original_symbol).where(
            ExchangeSymbol.exchange_id == buy_exch_obj_id,
            ExchangeSymbol.common_symbol == common_symbol
        )
        buy_original = (await db.execute(stmt)).scalar_one_or_none()

        stmt = select(ExchangeSymbol.original_symbol).where(
            ExchangeSymbol.exchange_id == sell_exch_obj_id,
            ExchangeSymbol.common_symbol == common_symbol
        )
        sell_original = (await db.execute(stmt)).scalar_one_or_none()

        if not buy_original or not sell_original:
            logger.error(f"[LIVE] Missing original symbol for {common_symbol} on buy/sell exchanges")
            return False, 0, 0, 0, 0, 0, 0, 0, 0

        # Configuration
        POLL_INTERVAL = 0.5
        MAX_DURATION = 300
        MIN_FILL_THRESHOLD = 0.01

        target_volume = volume
        start_time = asyncio.get_event_loop().time()

        logger.info(
            f"[LIVE] STARTING ATOMIC TRADE | symbol={common_symbol} (buy_orig={buy_original}, sell_orig={sell_original}) | "
            f"buy={buy_exchange} (fee={buy_fee_rate:.4f}) | sell={sell_exchange} (fee={sell_fee_rate:.4f}) | "
            f"target_vol={target_volume:.4f} | max_duration={MAX_DURATION}s"
        )
        logger.info(f"[LIVE] Initial prices: buy_vwap={vwap_buy:.2f} | sell_vwap={vwap_sell:.2f}")

        buy_orders: List[Dict] = []
        sell_orders: List[Dict] = []

        async def place_order_side(side: str, amount: float, client: Any, orders_list: List[Dict], orig_symbol: str) -> str:
            cid = f"{side}_{uuid.uuid4().hex[:8]}"
            logger.info(f"[LIVE] Placing {side.upper()} order: cid={cid} | amount={amount:.4f} | exchange={client.__class__.__name__} | symbol={orig_symbol}")
            try:
                order_result = await client.place_market_order(
                    symbol=orig_symbol,
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
                error_msg = str(e).encode('ascii', errors='replace').decode()
                logger.error(f"[LIVE] Failed to place {side} order: cid={cid} | error={error_msg}")
                raise

        # Place initial orders with correct original symbols
        try:
            await place_order_side('buy', target_volume, buy_client, buy_orders, buy_original)
            await place_order_side('sell', target_volume, sell_client, sell_orders, sell_original)
        except Exception as e:
            sanitised = str(e).encode('ascii', errors='replace').decode()
            logger.error(f"[LIVE] Initial order placement failed: {sanitised}")
            await self._cancel_orders(buy_orders, buy_client, "buy")
            await self._cancel_orders(sell_orders, sell_client, "sell")
            return False, 0, 0, 0, 0, 0, 0, 0, 0

        last_log_time = start_time
        iteration = 0

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

            total_buy_filled = sum(o['filled_vol'] for o in buy_orders)
            total_sell_filled = sum(o['filled_vol'] for o in sell_orders)

            if now - last_log_time >= 10:
                logger.info(
                    f"[LIVE] Poll # {iteration} | elapsed={now - start_time:.1f}s | "
                    f"buy_filled={total_buy_filled:.4f}/{target_volume:.4f} | "
                    f"sell_filled={total_sell_filled:.4f}/{target_volume:.4f} | "
                    f"buy_orders={len(buy_orders)} | sell_orders={len(sell_orders)}"
                )
                last_log_time = now

            # Both sides fully filled?
            if total_buy_filled >= target_volume * (1 - MIN_FILL_THRESHOLD) and \
               total_sell_filled >= target_volume * (1 - MIN_FILL_THRESHOLD):
                filled_vol = min(total_buy_filled, total_sell_filled)
                buy_vwap_final = self._compute_vwap(buy_orders)
                sell_vwap_final = self._compute_vwap(sell_orders)
                logger.info(
                    f"[LIVE] ✅ TRADE SUCCESS | filled={filled_vol:.4f} | "
                    f"buy_vwap={buy_vwap_final:.2f} | sell_vwap={sell_vwap_final:.2f} | "
                    f"elapsed={now - start_time:.1f}s | iterations={iteration}"
                )
                return True, filled_vol, buy_vwap_final, sell_vwap_final, 0.0, 0.0, 0.0, 0.0, 0.0

            # Handle imbalances
            buy_fully_filled = total_buy_filled >= target_volume * (1 - MIN_FILL_THRESHOLD)
            sell_fully_filled = total_sell_filled >= target_volume * (1 - MIN_FILL_THRESHOLD)

            if buy_fully_filled and not sell_fully_filled:
                target_sell_volume = total_buy_filled - total_sell_filled
                if target_sell_volume > 0.001:
                    logger.warning(
                        f"[LIVE] ⚠️ IMBALANCE: buy complete ({total_buy_filled:.4f}), sell missing {target_sell_volume:.4f}. "
                        f"Cancelling pending sell orders and placing new sell for {target_sell_volume:.4f}"
                    )
                    await self._cancel_orders(sell_orders, sell_client, "sell")
                    sell_orders.clear()
                    await place_order_side('sell', target_sell_volume, sell_client, sell_orders, sell_original)
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
                    await place_order_side('buy', target_buy_volume, buy_client, buy_orders, buy_original)
                else:
                    logger.info(f"[LIVE] Buy missing amount {target_buy_volume:.4f} is too small, skipping adjustment")

            else:
                if total_buy_filled == 0 and total_sell_filled > 0:
                    logger.warning(
                        f"[LIVE] ⚠️ IMBALANCE: buy_filled=0, sell_filled={total_sell_filled:.4f}. "
                        f"Cancelling buy orders and placing new buy for {total_sell_filled:.4f}"
                    )
                    await self._cancel_orders(buy_orders, buy_client, "buy")
                    buy_orders.clear()
                    await place_order_side('buy', total_sell_filled, buy_client, buy_orders, buy_original)

                elif total_sell_filled == 0 and total_buy_filled > 0:
                    logger.warning(
                        f"[LIVE] ⚠️ IMBALANCE: sell_filled=0, buy_filled={total_buy_filled:.4f}. "
                        f"Cancelling sell orders and placing new sell for {total_buy_filled:.4f}"
                    )
                    await self._cancel_orders(sell_orders, sell_client, "sell")
                    sell_orders.clear()
                    await place_order_side('sell', total_buy_filled, sell_client, sell_orders, sell_original)

            await asyncio.sleep(POLL_INTERVAL)

        elapsed = asyncio.get_event_loop().time() - start_time
        total_buy_filled = sum(o['filled_vol'] for o in buy_orders)
        total_sell_filled = sum(o['filled_vol'] for o in sell_orders)
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
        total_vol = 0.0
        total_value = 0.0
        for order in orders:
            res = order.get('result')
            if res and res.filled_volume > 0 and res.filled_price > 0:
                total_vol += res.filled_volume
                total_value += res.filled_volume * res.filled_price
        return total_value / total_vol if total_vol > 0 else 0.0