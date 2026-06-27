import asyncio
import uuid
import logging
import sys
import json
from decimal import Decimal, getcontext
from typing import Tuple, Optional, Any, List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.apps.arbitrage.models import Exchange, ExchangeSymbol
from app.apps.arbitrage.inventory import set_base_balance, set_quote_balance
from .opportunity_logger import OpportunityLogger
from .balance_sync import BalanceSyncService  # NEW

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

getcontext().prec = 28
logger = logging.getLogger(__name__)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

MIN_ORDER_SIZE = {
    "wallex": 2.0,
    "bitpin": 2.0,
    "nobitex": 2.0,
}


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
        limit_price_buy: float,
        limit_price_sell: float,
        is_live: bool,
        buy_price_factor: float = 1.0,
        sell_price_factor: float = 1.0
    ) -> Tuple[bool, float, float, float, float, float, float, float, float, List[Dict], List[Dict]]:

        if not is_live:
            logger.info(
                f"[SIM] Simulating trade: {common_symbol} | buy={buy_exchange} | sell={sell_exchange} | vol={volume:.4f}"
            )
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
                float(net_profit),
                buy_execs,
                sell_execs
            )

        if buy_client is None or sell_client is None:
            logger.error("Live mode requires valid exchange clients")
            return False, 0, 0, 0, 0, 0, 0, 0, 0, [], []

        min_buy = MIN_ORDER_SIZE.get(buy_exchange, 2.0)
        min_sell = MIN_ORDER_SIZE.get(sell_exchange, 2.0)
        volume = round(volume, 2)
        if volume < min_buy or volume < min_sell:
            logger.warning(
                f"[LIVE] Volume {volume:.4f} below min: {buy_exchange}={min_buy}, {sell_exchange}={min_sell}. Rejecting."
            )
            return False, 0, 0, 0, 0, 0, 0, 0, 0, [], []

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

        POLL_INTERVAL = 0.5
        MAX_DURATION = 300
        MIN_FILL_THRESHOLD = 0.01

        target_volume = volume
        start_time = asyncio.get_event_loop().time()

        logger.info(
            f"[LIVE] STARTING | symbol={common_symbol} (buy_orig={buy_original}, sell_orig={sell_original}) | "
            f"buy={buy_exchange} (fee={buy_fee_rate:.4f}) | sell={sell_exchange} (fee={sell_fee_rate:.4f}) | "
            f"target_vol={target_volume:.4f} | limit_buy={limit_price_buy:.2f} | limit_sell={limit_price_sell:.2f}"
        )

        buy_orders: List[Dict] = []
        sell_orders: List[Dict] = []

        async def place_order_side(side: str, amount: float, client: Any, orders_list: List[Dict], orig_symbol: str, limit_price: float, price_factor: float) -> str:
            amount = round(amount, 2)
            if amount < MIN_ORDER_SIZE.get(client.__class__.__name__.lower().replace('client',''), 2.0):
                raise ValueError(f"Amount {amount} below minimum")
            cid = f"{side}_{uuid.uuid4().hex[:8]}"
            native_price = limit_price / price_factor
            native_price = int(round(native_price))
            logger.info(f"[LIVE] Placing {side.upper()} order: cid={cid} | amount={amount:.4f} | native_price={native_price} (factor={price_factor})")
            try:
                order_result = await client.place_market_order(
                    symbol=orig_symbol,
                    side=side,
                    amount=amount,
                    client_order_id=cid,
                    price=float(native_price)
                )
                orders_list.append({
                    'client_order_id': cid,
                    'filled_vol': order_result.filled_volume,
                    'status': order_result.status,
                    'result': order_result
                })
                logger.info(f"[LIVE] Order placed: cid={cid} | status={order_result.status} | filled={order_result.filled_volume:.4f} | executions={order_result.executions}")
                return cid
            except Exception as e:
                error_msg = str(e)
                try:
                    error_msg = error_msg.encode('utf-8', errors='replace').decode('utf-8')
                except Exception:
                    pass
                logger.error(f"[LIVE] Failed to place {side} order: cid={cid} | error={error_msg}")
                raise

        try:
            await place_order_side('buy', target_volume, buy_client, buy_orders, buy_original, limit_price_buy, buy_price_factor)
            await place_order_side('sell', target_volume, sell_client, sell_orders, sell_original, limit_price_sell, sell_price_factor)
        except Exception as e:
            sanitised = str(e).encode('ascii', errors='replace').decode()
            logger.error(f"[LIVE] Initial placement failed: {sanitised}")
            await self._cancel_orders(buy_orders, buy_client, "buy")
            await self._cancel_orders(sell_orders, sell_client, "sell")
            return False, 0, 0, 0, 0, 0, 0, 0, 0, [], []

        last_log_time = start_time
        iteration = 0

        while (asyncio.get_event_loop().time() - start_time) < MAX_DURATION:
            iteration += 1
            now = asyncio.get_event_loop().time()

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
                                f"status={status.status} | filled={new_filled:.4f} | price={status.filled_price:.2f} | executions={status.executions}"
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
                    f"[LIVE] Poll #{iteration} | elapsed={now - start_time:.1f}s | "
                    f"buy={total_buy_filled:.4f}/{target_volume:.4f} | sell={total_sell_filled:.4f}/{target_volume:.4f}"
                )
                last_log_time = now

            if total_buy_filled >= target_volume * (1 - MIN_FILL_THRESHOLD) and \
               total_sell_filled >= target_volume * (1 - MIN_FILL_THRESHOLD):
                filled_vol = min(total_buy_filled, total_sell_filled)
                filled_vol = round(filled_vol, 2)
                buy_vwap_final = self._compute_vwap(buy_orders)
                sell_vwap_final = self._compute_vwap(sell_orders)

                buy_fees = sum(o['result'].fee for o in buy_orders if o.get('result') and o['result'].fee is not None)
                sell_fees = sum(o['result'].fee for o in sell_orders if o.get('result') and o['result'].fee is not None)

                revenue = filled_vol * sell_vwap_final
                cost = filled_vol * buy_vwap_final
                net_profit = revenue - cost - buy_fees - sell_fees

                buy_execs = self._extract_executions(buy_orders, buy_exchange, "buy")
                sell_execs = self._extract_executions(sell_orders, sell_exchange, "sell")

                audit_data = {
                    "common_symbol": common_symbol,
                    "buy_exchange": buy_exchange,
                    "sell_exchange": sell_exchange,
                    "filled_volume": filled_vol,
                    "buy_vwap": buy_vwap_final,
                    "sell_vwap": sell_vwap_final,
                    "net_profit": net_profit,
                    "quote_currency": quote_currency,
                    "buy_executions": buy_execs,
                    "sell_executions": sell_execs
                }
                execution_logger.info(json.dumps(audit_data))

                logger.info(
                    f"[LIVE] ✅ TRADE SUCCESS | filled={filled_vol:.4f} | "
                    f"buy_vwap={buy_vwap_final:.2f} | sell_vwap={sell_vwap_final:.2f} | "
                    f"buy_fees={buy_fees:.2f} | sell_fees={sell_fees:.2f} | net_profit={net_profit:.2f} {quote_currency}"
                )

                # ====== SYNC BALANCES AFTER SUCCESSFUL TRADE ======
                try:
                    await BalanceSyncService.sync_exchange_balance(db, buy_exchange)
                    await BalanceSyncService.sync_exchange_balance(db, sell_exchange)
                    logger.info(f"[SYNC] Synced balances for {buy_exchange} and {sell_exchange} after trade")
                except Exception as e:
                    logger.error(f"[SYNC] Failed to sync balances after trade: {e}")

                return True, filled_vol, buy_vwap_final, sell_vwap_final, 0.0, 0.0, 0.0, 0.0, net_profit, buy_execs, sell_execs

            # Imbalance handling (unchanged)
            buy_fully_filled = total_buy_filled >= target_volume * (1 - MIN_FILL_THRESHOLD)
            sell_fully_filled = total_sell_filled >= target_volume * (1 - MIN_FILL_THRESHOLD)

            if buy_fully_filled and not sell_fully_filled:
                target_sell_volume = total_buy_filled - total_sell_filled
                target_sell_volume = round(target_sell_volume, 2)
                if target_sell_volume > 0.001:
                    logger.warning(f"[LIVE] IMBALANCE: buy complete, sell missing {target_sell_volume:.4f}")
                    await self._cancel_orders(sell_orders, sell_client, "sell")
                    sell_orders.clear()
                    await place_order_side('sell', target_sell_volume, sell_client, sell_orders, sell_original, limit_price_sell, sell_price_factor)

            elif sell_fully_filled and not buy_fully_filled:
                target_buy_volume = total_sell_filled - total_buy_filled
                target_buy_volume = round(target_buy_volume, 2)
                if target_buy_volume > 0.001:
                    logger.warning(f"[LIVE] IMBALANCE: sell complete, buy missing {target_buy_volume:.4f}")
                    await self._cancel_orders(buy_orders, buy_client, "buy")
                    buy_orders.clear()
                    await place_order_side('buy', target_buy_volume, buy_client, buy_orders, buy_original, limit_price_buy, buy_price_factor)

            else:
                if total_buy_filled == 0 and total_sell_filled > 0:
                    logger.warning(f"[LIVE] IMBALANCE: buy=0, sell={total_sell_filled:.4f}")
                    await self._cancel_orders(buy_orders, buy_client, "buy")
                    buy_orders.clear()
                    await place_order_side('buy', total_sell_filled, buy_client, buy_orders, buy_original, limit_price_buy, buy_price_factor)

                elif total_sell_filled == 0 and total_buy_filled > 0:
                    logger.warning(f"[LIVE] IMBALANCE: sell=0, buy={total_buy_filled:.4f}")
                    await self._cancel_orders(sell_orders, sell_client, "sell")
                    sell_orders.clear()
                    await place_order_side('sell', total_buy_filled, sell_client, sell_orders, sell_original, limit_price_sell, sell_price_factor)

            await asyncio.sleep(POLL_INTERVAL)

        elapsed = asyncio.get_event_loop().time() - start_time
        logger.error(f"[LIVE] ❌ TIMEOUT after {elapsed:.1f}s. Cancelling all.")
        await self._cancel_orders(buy_orders, buy_client, "buy")
        await self._cancel_orders(sell_orders, sell_client, "sell")
        return False, 0, 0, 0, 0, 0, 0, 0, 0, [], []

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