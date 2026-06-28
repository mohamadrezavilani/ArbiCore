import asyncio
import uuid
import logging
import sys
import json
from decimal import Decimal, getcontext
from typing import Tuple, Optional, Any, List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.apps.arbitrage.models import ExchangeSymbol
from app.apps.arbitrage.services.opportunity_logger import OpportunityLogger
from app.apps.arbitrage.services.balance_sync import BalanceSyncService
from app.exchanges.base import OrderResult

getcontext().prec = 28
logger = logging.getLogger(__name__)

# Setup execution audit logger
execution_logger = logging.getLogger("execution_audit")
execution_logger.setLevel(logging.INFO)
if not execution_logger.handlers:
    try:
        from logging.handlers import RotatingFileHandler
        fh = RotatingFileHandler("logs/executions.log", maxBytes=10_485_760, backupCount=5)
        fh.setFormatter(logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        execution_logger.addHandler(fh)
    except Exception:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(logging.Formatter('EXEC_AUDIT: %(message)s'))
        execution_logger.addHandler(console)

MIN_ORDER_SIZE = {
    "wallex": 2.0,
    "bitpin": 2.0,
    "nobitex": 2.0,
}

class TradeExecutor:
    def __init__(self, logger: OpportunityLogger):
        self.logger = logger

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
        limit_price_buy: float,
        limit_price_sell: float,
        is_live: bool,
        buy_price_factor: float = 1.0,
        sell_price_factor: float = 1.0
    ) -> Tuple[bool, float, float, float, float, float, float, float, float, List[Dict], List[Dict]]:
        """
        Execute a pair of limit orders (buy on one exchange, sell on another) and return deltas.
        This version handles pending/partial fills, cancels on timeout, and always syncs balances.
        """
        if not is_live:
            # Simulation mode (unchanged)
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
            buy_execs = [{"price": vwap_buy, "volume": volume, "fee": 0.0, "exchange_name": buy_exchange, "side": "buy"}]
            sell_execs = [{"price": vwap_sell, "volume": volume, "fee": 0.0, "exchange_name": sell_exchange, "side": "sell"}]
            logger.info(f"[SIM] Result: filled={volume:.4f} | buy_vwap={vwap_buy:.2f} | sell_vwap={vwap_sell:.2f} | net_profit={net_profit:.2f} {quote_currency}")
            return True, volume, vwap_buy, vwap_sell, base_delta_buy, base_delta_sell, quote_delta_buy, quote_delta_sell, float(net_profit), buy_execs, sell_execs

        # --- Live mode ---
        # Validate minimum order sizes
        min_buy = MIN_ORDER_SIZE.get(buy_exchange, 2.0)
        min_sell = MIN_ORDER_SIZE.get(sell_exchange, 2.0)
        volume = round(volume, 2)
        if volume < min_buy or volume < min_sell:
            logger.warning(f"[LIVE] Volume {volume:.4f} below min: {buy_exchange}={min_buy}, {sell_exchange}={min_sell}")
            return False, 0, 0, 0, 0, 0, 0, 0, 0, [], []

        # Get original symbols
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
            logger.error(f"[LIVE] Missing original symbol for {common_symbol}")
            return False, 0, 0, 0, 0, 0, 0, 0, 0, [], []

        # Place orders
        buy_orders = []   # list of dicts with client_order_id, filled_vol, status, result
        sell_orders = []
        placed = {"buy": False, "sell": False}

        try:
            # Buy order
            buy_cid = f"buy_{uuid.uuid4().hex[:8]}"
            native_price_buy = limit_price_buy / buy_price_factor
            native_price_buy = int(round(native_price_buy))
            buy_result = await buy_client.place_limit_order(
                symbol=buy_original,
                side="buy",
                amount=volume,
                client_order_id=buy_cid,
                price=float(native_price_buy)
            )
            buy_orders.append({
                'client_order_id': buy_cid,
                'filled_vol': buy_result.filled_volume,
                'status': buy_result.status,
                'result': buy_result
            })
            placed['buy'] = True
            logger.info(f"[LIVE] Buy order placed: {buy_cid} status={buy_result.status} filled={buy_result.filled_volume:.4f}")

            # Sell order
            sell_cid = f"sell_{uuid.uuid4().hex[:8]}"
            native_price_sell = limit_price_sell / sell_price_factor
            native_price_sell = int(round(native_price_sell))
            sell_result = await sell_client.place_limit_order(
                symbol=sell_original,
                side="sell",
                amount=volume,
                client_order_id=sell_cid,
                price=float(native_price_sell)
            )
            sell_orders.append({
                'client_order_id': sell_cid,
                'filled_vol': sell_result.filled_volume,
                'status': sell_result.status,
                'result': sell_result
            })
            placed['sell'] = True
            logger.info(f"[LIVE] Sell order placed: {sell_cid} status={sell_result.status} filled={sell_result.filled_volume:.4f}")

        except Exception as e:
            sanitised = str(e).encode('ascii', errors='replace').decode()
            logger.error(f"[LIVE] Order placement failed: {sanitised}")
            if placed.get('buy'):
                await self._cancel_orders(buy_orders, buy_client, "buy")
            if placed.get('sell'):
                await self._cancel_orders(sell_orders, sell_client, "sell")
            # Sync balances to correct inventory
            try:
                await BalanceSyncService.sync_exchange_balance(db, buy_exchange)
                await BalanceSyncService.sync_exchange_balance(db, sell_exchange)
            except Exception as sync_err:
                logger.error(f"[SYNC] Failed to sync after placement failure: {sync_err}")
            return False, 0, 0, 0, 0, 0, 0, 0, 0, [], []

        # --- Polling loop ---
        POLL_INTERVAL = 0.5
        MAX_DURATION = 300  # seconds
        MIN_FILL_THRESHOLD = 0.01  # 1%
        start_time = asyncio.get_event_loop().time()
        last_log_time = start_time
        iteration = 0

        while (asyncio.get_event_loop().time() - start_time) < MAX_DURATION:
            iteration += 1
            now = asyncio.get_event_loop().time()

            # Update status of all orders
            for order in buy_orders + sell_orders:
                if order['status'] not in ('filled', 'cancelled', 'failed'):
                    try:
                        client = buy_client if order in buy_orders else sell_client
                        status = await client.order_status(order['client_order_id'])
                        old_filled = order['filled_vol']
                        new_filled = status.filled_volume
                        if abs(new_filled - old_filled) > 0.0001 or status.status != order['status']:
                            logger.info(f"[LIVE] Status update: cid={order['client_order_id']} status={status.status} filled={new_filled:.4f}")
                        order['filled_vol'] = new_filled
                        order['status'] = status.status
                        order['result'] = status
                    except Exception as e:
                        logger.warning(f"[LIVE] Status check failed for cid={order['client_order_id']}: {e}")

            total_buy_filled = sum(o['filled_vol'] for o in buy_orders)
            total_sell_filled = sum(o['filled_vol'] for o in sell_orders)

            if now - last_log_time >= 10:
                logger.info(f"[LIVE] Poll #{iteration} | elapsed={now-start_time:.1f}s | buy={total_buy_filled:.4f}/{volume:.4f} | sell={total_sell_filled:.4f}/{volume:.4f}")
                last_log_time = now

            # Check if both sides are sufficiently filled
            if total_buy_filled >= volume * (1 - MIN_FILL_THRESHOLD) and total_sell_filled >= volume * (1 - MIN_FILL_THRESHOLD):
                filled_vol = min(total_buy_filled, total_sell_filled)
                filled_vol = round(filled_vol, 2)
                buy_vwap = self._compute_vwap(buy_orders)
                sell_vwap = self._compute_vwap(sell_orders)
                buy_fees = sum(o['result'].fee for o in buy_orders if o.get('result') and o['result'].fee is not None)
                sell_fees = sum(o['result'].fee for o in sell_orders if o.get('result') and o['result'].fee is not None)
                revenue = filled_vol * sell_vwap
                cost = filled_vol * buy_vwap
                net_profit = revenue - cost - buy_fees - sell_fees
                buy_execs = self._extract_executions(buy_orders, buy_exchange, "buy")
                sell_execs = self._extract_executions(sell_orders, sell_exchange, "sell")

                logger.info(f"[LIVE] ✅ Full fill achieved | filled={filled_vol:.4f} | net_profit={net_profit:.2f} {quote_currency}")
                # Sync balances
                try:
                    await BalanceSyncService.sync_exchange_balance(db, buy_exchange)
                    await BalanceSyncService.sync_exchange_balance(db, sell_exchange)
                except Exception as sync_err:
                    logger.error(f"[SYNC] Failed to sync after full fill: {sync_err}")
                return True, filled_vol, buy_vwap, sell_vwap, 0.0, 0.0, 0.0, 0.0, net_profit, buy_execs, sell_execs

            await asyncio.sleep(POLL_INTERVAL)

        # --- Timeout: cancel remaining orders and return partial result if any fills exist ---
        elapsed = asyncio.get_event_loop().time() - start_time
        logger.warning(f"[LIVE] ⏰ Timeout after {elapsed:.1f}s. Cancelling remaining orders.")
        await self._cancel_orders(buy_orders, buy_client, "buy")
        await self._cancel_orders(sell_orders, sell_client, "sell")

        total_buy_filled = sum(o['filled_vol'] for o in buy_orders)
        total_sell_filled = sum(o['filled_vol'] for o in sell_orders)
        filled_vol = min(total_buy_filled, total_sell_filled)
        filled_vol = round(filled_vol, 2)

        # Sync balances after timeout
        try:
            await BalanceSyncService.sync_exchange_balance(db, buy_exchange)
            await BalanceSyncService.sync_exchange_balance(db, sell_exchange)
        except Exception as sync_err:
            logger.error(f"[SYNC] Failed to sync after timeout: {sync_err}")

        if filled_vol > 0.01:
            buy_vwap = self._compute_vwap(buy_orders)
            sell_vwap = self._compute_vwap(sell_orders)
            buy_fees = sum(o['result'].fee for o in buy_orders if o.get('result') and o['result'].fee is not None)
            sell_fees = sum(o['result'].fee for o in sell_orders if o.get('result') and o['result'].fee is not None)
            revenue = filled_vol * sell_vwap
            cost = filled_vol * buy_vwap
            net_profit = revenue - cost - buy_fees - sell_fees
            buy_execs = self._extract_executions(buy_orders, buy_exchange, "buy")
            sell_execs = self._extract_executions(sell_orders, sell_exchange, "sell")
            logger.info(f"[LIVE] ⚠️ Partial fill: filled={filled_vol:.4f} | net_profit={net_profit:.2f} {quote_currency}")
            return True, filled_vol, buy_vwap, sell_vwap, 0.0, 0.0, 0.0, 0.0, net_profit, buy_execs, sell_execs
        else:
            logger.info("[LIVE] ❌ No fills – trade aborted.")
            return False, 0, 0, 0, 0, 0, 0, 0, 0, [], []

    # ---------- Helper methods ----------
    async def _cancel_orders(self, orders: List[Dict], client, side_label: str):
        for order in orders:
            if order['status'] not in ('filled', 'cancelled', 'failed'):
                try:
                    await client.cancel_order(order['client_order_id'])
                    order['status'] = 'cancelled'
                    logger.info(f"[LIVE] Cancelled {side_label} order: cid={order['client_order_id']}")
                except Exception as e:
                    logger.warning(f"[LIVE] Cancel failed for {order['client_order_id']}: {e}")

    def _compute_vwap(self, orders: List[Dict]) -> float:
        total_vol = 0.0
        total_value = 0.0
        for order in orders:
            res = order.get('result')
            if res and hasattr(res, 'executions'):
                for exec_item in res.executions:
                    vol = exec_item.get('volume', 0)
                    price = exec_item.get('price', 0)
                    if vol > 0 and price > 0:
                        total_vol += vol
                        total_value += vol * price
        return total_value / total_vol if total_vol > 0 else 0.0

    def _extract_executions(self, orders: List[Dict], exchange_name: str, side: str) -> List[Dict]:
        execs = []
        for order in orders:
            res = order.get('result')
            client_order_id = order.get('client_order_id')
            if res and hasattr(res, 'executions'):
                for exec_item in res.executions:
                    execs.append({
                        "exchange_name": exchange_name,
                        "side": side,
                        "price": exec_item.get('price', 0),
                        "volume": exec_item.get('volume', 0),
                        "fee": exec_item.get('fee', 0),
                        "client_order_id": client_order_id
                    })
        return execs