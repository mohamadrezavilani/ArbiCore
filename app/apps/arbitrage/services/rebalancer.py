import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.apps.arbitrage.models import (
    Exchange, BaseInventory, SymbolArbitrageSettings
)
from app.apps.arbitrage.inventory import get_quote_balance
from app.exchanges.factory import get_exchange_client
from app.apps.arbitrage.services.opportunity_logger import OpportunityLogger
from app.apps.arbitrage.services.trade_executor import TradeExecutor
from app.core.timezone import format_local_time

logger = logging.getLogger(__name__)

class Rebalancer:
    def __init__(self, logger: OpportunityLogger, trade_executor: Optional[TradeExecutor] = None):
        self.logger = logger
        self.trade_executor = trade_executor or TradeExecutor(logger)

    async def rebalance_symbol_if_needed(
        self,
        db: AsyncSession,
        common_symbol: str,
        quote_currency: str,
        exchange_orderbooks: Dict[str, Tuple[List[List[float]], List[List[float]]]],
    ) -> Tuple[bool, str]:
        """
        Returns (success, reason).
        success: True if a rebalance trade was executed.
        reason: Human-readable explanation.
        """
        logger.info(f"[REBALANCE] Checking {common_symbol} (quote={quote_currency})")

        # 1. Get balances and exchange modes
        stmt = (
            select(Exchange.name, Exchange.mode, BaseInventory.balance)
            .join(BaseInventory, BaseInventory.exchange_id == Exchange.id)
            .where(BaseInventory.common_symbol == common_symbol)
            .where(Exchange.is_active == True)
        )
        result = await db.execute(stmt)
        rows = result.all()
        if len(rows) < 2:
            reason = f"Only {len(rows)} exchange(s) with balance, need at least 2"
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        exchange_modes = {name: mode for name, mode, _ in rows}
        balances = [(name, float(bal)) for name, _, bal in rows]
        balances.sort(key=lambda x: x[1])
        poorest = balances[0]
        richest = balances[-1]
        avg_balance = sum(b for _, b in balances) / len(balances)

        logger.info(f"[REBALANCE] Balances: {balances}")
        logger.info(f"[REBALANCE] Avg={avg_balance:.2f}, poorest={poorest[0]}={poorest[1]:.2f}, richest={richest[0]}={richest[1]:.2f}")

        # 2. Get settings
        settings_stmt = select(SymbolArbitrageSettings).where(
            SymbolArbitrageSettings.common_symbol == common_symbol
        )
        settings = (await db.execute(settings_stmt)).scalar_one_or_none()
        if not settings:
            reason = f"No settings found for {common_symbol}"
            logger.warning(f"[REBALANCE] {reason}")
            return False, reason

        if not settings.market_rebalance_enabled:
            reason = f"market_rebalance_enabled=False for {common_symbol}"
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        imbalance_ratio = float(settings.market_rebalance_imbalance_ratio)
        trigger_threshold = imbalance_ratio * avg_balance
        logger.info(f"[REBALANCE] Imbalance ratio = {imbalance_ratio} (trigger when min < {trigger_threshold:.2f})")

        if poorest[1] >= trigger_threshold:
            reason = f"No imbalance: poorest {poorest[0]} balance {poorest[1]:.2f} >= {trigger_threshold:.2f}"
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        # 3. Cooldown check
        if settings.last_rebalance_time:
            cooldown_sec = settings.market_rebalance_cooldown_seconds
            next_allowed = settings.last_rebalance_time + timedelta(seconds=cooldown_sec)
            now = datetime.utcnow()
            if now < next_allowed:
                next_allowed_str = format_local_time(next_allowed)
                reason = f"Cooldown active until {next_allowed_str} (Tehran time)"
                logger.info(f"[REBALANCE] {reason}")
                return False, reason
            else:
                logger.info(f"[REBALANCE] Cooldown passed (last rebalance at {format_local_time(settings.last_rebalance_time)})")
        else:
            logger.info("[REBALANCE] No previous rebalance time, proceeding")

        # 4. Orderbook availability
        richest_ob = exchange_orderbooks.get(richest[0])
        poorest_ob = exchange_orderbooks.get(poorest[0])
        if not richest_ob or not poorest_ob:
            missing = []
            if not richest_ob: missing.append(richest[0])
            if not poorest_ob: missing.append(poorest[0])
            reason = f"Missing orderbook for {', '.join(missing)}"
            logger.warning(f"[REBALANCE] {reason}")
            return False, reason

        richest_asks, _ = richest_ob
        _, poorest_bids = poorest_ob
        if not richest_asks or not poorest_bids:
            reason = f"Incomplete levels: richest_asks={bool(richest_asks)}, poorest_bids={bool(poorest_bids)}"
            logger.warning(f"[REBALANCE] {reason}")
            return False, reason

        sell_price = richest_asks[0][0]
        buy_price = poorest_bids[0][0]
        spread_percent = abs(sell_price - buy_price) / buy_price * 100
        max_spread = float(settings.market_rebalance_max_spread_percent)
        logger.info(f"[REBALANCE] Prices: sell@{richest[0]}={sell_price:.2f}, buy@{poorest[0]}={buy_price:.2f}, spread={spread_percent:.3f}%, max_spread={max_spread}%")

        if spread_percent > max_spread:
            reason = f"Spread {spread_percent:.2f}% > {max_spread}% (sell@{richest[0]}={sell_price:.2f}, buy@{poorest[0]}={buy_price:.2f})"
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        # 5. Target amount
        target_amount = avg_balance * (float(settings.market_rebalance_amount_percent) / 100.0)
        logger.info(f"[REBALANCE] Target amount = {target_amount:.4f} ({settings.market_rebalance_amount_percent}% of avg)")

        # Respect available base on richest
        if target_amount > richest[1]:
            old = target_amount
            target_amount = richest[1] * 0.9
            logger.info(f"[REBALANCE] Reduced target from {old:.4f} to {target_amount:.4f} due to richest balance {richest[1]:.2f}")

        # Respect available quote on poorest
        poorest_quote_balance = await get_quote_balance(db, poorest[0], quote_currency)
        cost_estimate = target_amount * buy_price
        if cost_estimate > poorest_quote_balance:
            old = target_amount
            target_amount = poorest_quote_balance / buy_price * 0.95
            logger.info(f"[REBALANCE] Reduced target from {old:.4f} to {target_amount:.4f} due to poor quote balance {poorest_quote_balance:.2f} (need {cost_estimate:.2f})")

        if target_amount < 0.001:
            reason = f"Target amount too small ({target_amount:.6f})"
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        # 6. Execute trade – decide based on mode
        mode_richest = exchange_modes.get(richest[0], "simulator")
        mode_poorest = exchange_modes.get(poorest[0], "simulator")
        is_live = (mode_richest == "live" and mode_poorest == "live")

        if not is_live:
            # Simulator mode: directly call the trade executor with simulator parameters
            logger.info(f"[REBALANCE] Simulator mode: executing rebalance via TradeExecutor (no live APIs)")
            # We need fees for simulation. Fetch from exchange_fees table.
            from app.apps.arbitrage.models import ExchangeFee
            buy_fee = 0.0
            sell_fee = 0.0
            fee_stmt = select(ExchangeFee.taker_fee).join(Exchange).where(
                Exchange.name == poorest[0],
                ExchangeFee.quote_currency == quote_currency
            )
            fee_res = await db.execute(fee_stmt)
            buy_fee = float(fee_res.scalar() or 0.0)
            fee_stmt = select(ExchangeFee.taker_fee).join(Exchange).where(
                Exchange.name == richest[0],
                ExchangeFee.quote_currency == quote_currency
            )
            fee_res = await db.execute(fee_stmt)
            sell_fee = float(fee_res.scalar() or 0.0)

            success, filled_vol, vwap_buy, vwap_sell = await self.trade_executor.execute(
                db=db,
                common_symbol=common_symbol,
                buy_exchange=poorest[0],
                sell_exchange=richest[0],
                volume=target_amount,
                quote_currency=quote_currency,
                buy_client=None,
                sell_client=None,
                buy_exch_obj=None,
                sell_exch_obj=None,
                buy_fee_rate=buy_fee,
                sell_fee_rate=sell_fee,
                vwap_buy=buy_price,
                vwap_sell=sell_price
            )
        else:
            # Live mode: create real clients and execute
            buy_client = get_exchange_client(poorest[0])
            sell_client = get_exchange_client(richest[0])
            if not buy_client or not sell_client:
                reason = f"Client creation failed for {poorest[0]} or {richest[0]}"
                logger.error(f"[REBALANCE] {reason}")
                return False, reason

            exch_poorest = (await db.execute(select(Exchange).where(Exchange.name == poorest[0]))).scalar_one_or_none()
            exch_richest = (await db.execute(select(Exchange).where(Exchange.name == richest[0]))).scalar_one_or_none()
            if not exch_poorest or not exch_richest:
                reason = f"Exchange objects not found for {poorest[0]} or {richest[0]}"
                logger.error(f"[REBALANCE] {reason}")
                return False, reason

            # Fetch fees for live mode (though executor will also fetch, but pass anyway)
            from app.apps.arbitrage.models import ExchangeFee
            buy_fee = 0.0
            sell_fee = 0.0
            fee_stmt = select(ExchangeFee.taker_fee).join(Exchange).where(
                Exchange.name == poorest[0],
                ExchangeFee.quote_currency == quote_currency
            )
            fee_res = await db.execute(fee_stmt)
            buy_fee = float(fee_res.scalar() or 0.0)
            fee_stmt = select(ExchangeFee.taker_fee).join(Exchange).where(
                Exchange.name == richest[0],
                ExchangeFee.quote_currency == quote_currency
            )
            fee_res = await db.execute(fee_stmt)
            sell_fee = float(fee_res.scalar() or 0.0)

            logger.info(f"[REBALANCE] Live mode: executing rebalance via TradeExecutor")
            success, filled_vol, vwap_buy, vwap_sell = await self.trade_executor.execute(
                db=db,
                common_symbol=common_symbol,
                buy_exchange=poorest[0],
                sell_exchange=richest[0],
                volume=target_amount,
                quote_currency=quote_currency,
                buy_client=buy_client,
                sell_client=sell_client,
                buy_exch_obj=exch_poorest,
                sell_exch_obj=exch_richest,
                buy_fee_rate=buy_fee,
                sell_fee_rate=sell_fee,
                vwap_buy=None,   # not needed for live
                vwap_sell=None
            )

        if not success:
            reason = "Trade execution failed (see logs for details)"
            logger.error(f"[REBALANCE] {reason}")
            return False, reason

        # 7. Log rebalance
        await self.logger.log_rebalance(
            db,
            common_symbol=common_symbol,
            currency=None,
            from_exch=richest[0],
            to_exch=poorest[0],
            amount_sent=filled_vol,
            fee=0.0,
            net=filled_vol,
            reason=f"market_rebalance_{common_symbol}_imbalance_{imbalance_ratio}"
        )

        # 8. Update last rebalance time and commit
        settings.last_rebalance_time = datetime.utcnow()
        await db.commit()

        reason = f"Rebalance executed: sold {filled_vol:.4f} on {richest[0]} at VWAP {vwap_sell:.2f}, bought on {poorest[0]} at VWAP {vwap_buy:.2f}"
        logger.info(f"[REBALANCE] {reason}")
        return True, reason