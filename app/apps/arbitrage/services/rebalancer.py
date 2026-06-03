import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.apps.arbitrage.models import (
    Exchange, BaseInventory, SymbolArbitrageSettings, ExchangeFee, QuoteInventory
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
        If imbalance exists but cannot rebalance now, sets rebalance_pending=True.
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

        # 3. Check imbalance – set pending flag if needed
        if poorest[1] < trigger_threshold:
            settings.rebalance_pending = True
            await db.flush()
            logger.info(f"[REBALANCE] Imbalance detected, set pending flag for {common_symbol}")
        else:
            # No imbalance, clear pending if it was set
            if settings.rebalance_pending:
                settings.rebalance_pending = False
                await db.flush()
                logger.info(f"[REBALANCE] No imbalance, cleared pending flag for {common_symbol}")
            reason = f"No imbalance: poorest {poorest[0]} balance {poorest[1]:.2f} >= {trigger_threshold:.2f}"
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        # 4. Cooldown check – only if pending, we still need to wait, but keep pending flag
        if settings.last_rebalance_time:
            cooldown_sec = settings.market_rebalance_cooldown_seconds
            next_allowed = settings.last_rebalance_time + timedelta(seconds=cooldown_sec)
            now = datetime.utcnow()
            if now < next_allowed:
                next_allowed_str = format_local_time(next_allowed)
                reason = f"Cooldown active until {next_allowed_str} (Tehran time)"
                logger.info(f"[REBALANCE] {reason}")
                # Keep pending flag, but don't execute
                return False, reason
            else:
                logger.info(f"[REBALANCE] Cooldown passed (last rebalance at {format_local_time(settings.last_rebalance_time)})")
        else:
            logger.info("[REBALANCE] No previous rebalance time, proceeding")

        # 5. Orderbook availability
        richest_ob = exchange_orderbooks.get(richest[0])
        poorest_ob = exchange_orderbooks.get(poorest[0])
        if not richest_ob or not poorest_ob:
            missing = []
            if not richest_ob: missing.append(richest[0])
            if not poorest_ob: missing.append(poorest[0])
            reason = f"Missing orderbook for {', '.join(missing)}"
            logger.warning(f"[REBALANCE] {reason}")
            # Keep pending flag, will retry later
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
            reason = f"Spread {spread_percent:.2f}% > {max_spread}% – will retry later (pending flag remains)"
            logger.info(f"[REBALANCE] {reason}")
            # Keep pending flag, do not execute
            return False, reason

        # 6. Target amount
        target_amount = avg_balance * (float(settings.market_rebalance_amount_percent) / 100.0)
        logger.info(f"[REBALANCE] Target amount = {target_amount:.4f} ({settings.market_rebalance_amount_percent}% of avg)")

        if target_amount > richest[1]:
            old = target_amount
            target_amount = richest[1] * 0.9
            logger.info(f"[REBALANCE] Reduced target from {old:.4f} to {target_amount:.4f} due to richest balance {richest[1]:.2f}")

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

        # 7. Execute trade
        mode_richest = exchange_modes.get(richest[0], "simulator")
        mode_poorest = exchange_modes.get(poorest[0], "simulator")
        is_live = (mode_richest == "live" and mode_poorest == "live")

        buy_client = None
        sell_client = None
        buy_exch_obj_id = None
        sell_exch_obj_id = None
        if is_live:
            buy_client = get_exchange_client(poorest[0])
            sell_client = get_exchange_client(richest[0])
            if not buy_client or not sell_client:
                reason = f"Client creation failed for {poorest[0]} or {richest[0]}"
                logger.error(f"[REBALANCE] {reason}")
                return False, reason
            stmt = select(Exchange.id).where(Exchange.name == poorest[0])
            buy_exch_obj_id = (await db.execute(stmt)).scalar_one_or_none()
            stmt = select(Exchange.id).where(Exchange.name == richest[0])
            sell_exch_obj_id = (await db.execute(stmt)).scalar_one_or_none()
            if not buy_exch_obj_id or not sell_exch_obj_id:
                reason = f"Exchange IDs not found for {poorest[0]} or {richest[0]}"
                logger.error(f"[REBALANCE] {reason}")
                return False, reason

        # Fetch fees
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

        success, filled_vol, vwap_buy, vwap_sell, _, _, _, _, _ = await self.trade_executor.execute_and_get_deltas(
            db=db,
            common_symbol=common_symbol,
            buy_exchange=poorest[0],
            sell_exchange=richest[0],
            volume=target_amount,
            quote_currency=quote_currency,
            buy_client=buy_client,
            sell_client=sell_client,
            buy_exch_obj_id=buy_exch_obj_id,
            sell_exch_obj_id=sell_exch_obj_id,
            buy_fee_rate=buy_fee,
            sell_fee_rate=sell_fee,
            vwap_buy=buy_price,
            vwap_sell=sell_price,
            is_live=is_live
        )

        if not success:
            reason = "Trade execution failed (see logs for details)"
            logger.error(f"[REBALANCE] {reason}")
            # Keep pending flag
            return False, reason

        # 8. Log rebalance
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

        # 9. Update last rebalance time and clear pending flag
        settings.last_rebalance_time = datetime.utcnow()
        settings.rebalance_pending = False
        await db.commit()

        reason = f"Rebalance executed: sold {filled_vol:.4f} on {richest[0]} at VWAP {vwap_sell:.2f}, bought on {poorest[0]} at VWAP {vwap_buy:.2f}"
        logger.info(f"[REBALANCE] {reason}")
        return True, reason

    async def rebalance_quote_if_needed(
        self,
        db: AsyncSession,
        common_symbol: str,
        quote_currency: str,
        exchange_orderbooks: Dict[str, Tuple[List[List[float]], List[List[float]]]],
    ) -> Tuple[bool, str]:
        """
        Rebalance quote currency (IRT) by trading USDT in the opposite direction.
        When an exchange has too little IRT, we sell USDT there (gain IRT) and buy USDT on the exchange with surplus IRT.
        """
        logger.info(f"[QUOTE REBALANCE] Checking {common_symbol} quote={quote_currency}")

        # 1. Get current quote balances (IRT)
        stmt = (
            select(Exchange.name, Exchange.mode, QuoteInventory.balance)
            .join(QuoteInventory, QuoteInventory.exchange_id == Exchange.id)
            .where(QuoteInventory.currency == quote_currency)
            .where(Exchange.is_active == True)
        )
        result = await db.execute(stmt)
        rows = result.all()
        if len(rows) < 2:
            reason = f"Only {len(rows)} exchange(s) with {quote_currency} balance, need at least 2"
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        exchange_modes = {name: mode for name, mode, _ in rows}
        balances = [(name, float(bal)) for name, _, bal in rows]
        balances.sort(key=lambda x: x[1])
        poorest = balances[0]      # exchange with smallest IRT
        richest = balances[-1]     # exchange with largest IRT
        avg_balance = sum(b for _, b in balances) / len(balances)

        logger.info(f"[QUOTE REBALANCE] Quote balances: {balances}")
        logger.info(f"[QUOTE REBALANCE] Avg={avg_balance:.2f}, poorest={poorest[0]}={poorest[1]:.2f}, richest={richest[0]}={richest[1]:.2f}")

        # 2. Get settings
        settings_stmt = select(SymbolArbitrageSettings).where(
            SymbolArbitrageSettings.common_symbol == common_symbol
        )
        settings = (await db.execute(settings_stmt)).scalar_one_or_none()
        if not settings:
            reason = f"No settings found for {common_symbol}"
            logger.warning(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        if not settings.quote_rebalance_enabled:
            reason = f"quote_rebalance_enabled=False for {common_symbol}"
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        imbalance_ratio = float(settings.quote_rebalance_imbalance_ratio)
        trigger_threshold = imbalance_ratio * avg_balance
        logger.info(f"[QUOTE REBALANCE] Imbalance ratio = {imbalance_ratio} (trigger when min < {trigger_threshold:.2f})")

        # 3. Set pending flag if imbalance exists
        if poorest[1] < trigger_threshold:
            settings.quote_rebalance_pending = True
            await db.flush()
            logger.info(f"[QUOTE REBALANCE] Imbalance detected, set pending flag")
        else:
            if settings.quote_rebalance_pending:
                settings.quote_rebalance_pending = False
                await db.flush()
                logger.info(f"[QUOTE REBALANCE] No imbalance, cleared pending flag")
            reason = f"No imbalance: poorest {poorest[0]} balance {poorest[1]:.2f} >= {trigger_threshold:.2f}"
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        # 4. Cooldown check
        if settings.last_quote_rebalance_time:
            cooldown_sec = settings.quote_rebalance_cooldown_seconds
            next_allowed = settings.last_quote_rebalance_time + timedelta(seconds=cooldown_sec)
            now = datetime.utcnow()
            if now < next_allowed:
                next_allowed_str = format_local_time(next_allowed)
                reason = f"Cooldown active until {next_allowed_str}"
                logger.info(f"[QUOTE REBALANCE] {reason}")
                return False, reason
            else:
                logger.info(f"[QUOTE REBALANCE] Cooldown passed")
        else:
            logger.info("[QUOTE REBALANCE] No previous rebalance time, proceeding")

        # 5. Orderbook availability
        poorest_ob = exchange_orderbooks.get(poorest[0])
        richest_ob = exchange_orderbooks.get(richest[0])
        if not poorest_ob or not richest_ob:
            missing = []
            if not poorest_ob: missing.append(poorest[0])
            if not richest_ob: missing.append(richest[0])
            reason = f"Missing orderbook for {', '.join(missing)}"
            logger.warning(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        _, poorest_bids = poorest_ob   # sell USDT – use bid side
        richest_asks, _ = richest_ob   # buy USDT – use ask side
        if not poorest_bids or not richest_asks:
            reason = f"Incomplete levels: poorest_bids={bool(poorest_bids)}, richest_asks={bool(richest_asks)}"
            logger.warning(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        sell_price = poorest_bids[0][0]    # price at which we sell USDT on the poor IRT exchange
        buy_price = richest_asks[0][0]     # price at which we buy USDT on the rich IRT exchange
        spread_percent = abs(sell_price - buy_price) / buy_price * 100
        max_spread = float(settings.quote_rebalance_max_spread_percent)
        logger.info(f"[QUOTE REBALANCE] Prices: sell@{poorest[0]}={sell_price:.2f}, buy@{richest[0]}={buy_price:.2f}, spread={spread_percent:.3f}%, max_spread={max_spread}%")

        if spread_percent > max_spread:
            reason = f"Spread {spread_percent:.2f}% > {max_spread}% – will retry later (pending flag remains)"
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        # 6. Target amount in USDT
        target_irt = avg_balance * (float(settings.quote_rebalance_amount_percent) / 100.0)
        target_usdt = target_irt / buy_price   # amount of USDT to buy on richest (and sell on poorest)
        logger.info(f"[QUOTE REBALANCE] Target amount = {target_usdt:.4f} USDT ({settings.quote_rebalance_amount_percent}% of avg quote in IRT)")

        # Respect available USDT on the poorest exchange (need to sell there)
        stmt = select(BaseInventory.balance).join(Exchange).where(
            Exchange.name == poorest[0],
            BaseInventory.common_symbol == common_symbol
        )
        bal_result = await db.execute(stmt)
        available_base = float(bal_result.scalar_one_or_none() or 0.0)
        if target_usdt > available_base:
            old = target_usdt
            target_usdt = available_base * 0.9
            logger.info(f"[QUOTE REBALANCE] Reduced target from {old:.4f} to {target_usdt:.4f} due to low base balance on {poorest[0]}")

        # Respect available IRT on the richest exchange (need to pay with IRT)
        stmt = select(QuoteInventory.balance).join(Exchange).where(
            Exchange.name == richest[0],
            QuoteInventory.currency == quote_currency
        )
        bal_result = await db.execute(stmt)
        available_quote_richest = float(bal_result.scalar_one_or_none() or 0.0)
        cost_estimate = target_usdt * buy_price
        if cost_estimate > available_quote_richest:
            old = target_usdt
            target_usdt = (available_quote_richest / buy_price) * 0.95
            logger.info(f"[QUOTE REBALANCE] Reduced target from {old:.4f} to {target_usdt:.4f} due to low quote balance on {richest[0]}")

        if target_usdt < 0.001:
            reason = f"Target amount too small ({target_usdt:.6f} USDT)"
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        # 7. Execute trade: buy USDT on richest exchange, sell USDT on poorest exchange
        mode_poorest = exchange_modes.get(poorest[0], "simulator")
        mode_richest = exchange_modes.get(richest[0], "simulator")
        is_live = (mode_poorest == "live" and mode_richest == "live")

        buy_client = None
        sell_client = None
        buy_exch_obj_id = None
        sell_exch_obj_id = None
        if is_live:
            buy_client = get_exchange_client(richest[0])   # buy USDT on rich IRT exchange
            sell_client = get_exchange_client(poorest[0])  # sell USDT on poor IRT exchange
            if not buy_client or not sell_client:
                reason = f"Client creation failed for {richest[0]} or {poorest[0]}"
                logger.error(f"[QUOTE REBALANCE] {reason}")
                return False, reason
            stmt = select(Exchange.id).where(Exchange.name == richest[0])
            buy_exch_obj_id = (await db.execute(stmt)).scalar_one_or_none()
            stmt = select(Exchange.id).where(Exchange.name == poorest[0])
            sell_exch_obj_id = (await db.execute(stmt)).scalar_one_or_none()
            if not buy_exch_obj_id or not sell_exch_obj_id:
                reason = f"Exchange IDs not found"
                return False, reason

        # Fetch fees
        buy_fee = 0.0
        sell_fee = 0.0
        fee_stmt = select(ExchangeFee.taker_fee).join(Exchange).where(
            Exchange.name == richest[0],
            ExchangeFee.quote_currency == quote_currency
        )
        fee_res = await db.execute(fee_stmt)
        buy_fee = float(fee_res.scalar() or 0.0)
        fee_stmt = select(ExchangeFee.taker_fee).join(Exchange).where(
            Exchange.name == poorest[0],
            ExchangeFee.quote_currency == quote_currency
        )
        fee_res = await db.execute(fee_stmt)
        sell_fee = float(fee_res.scalar() or 0.0)

        # Execute
        success, filled_vol, vwap_buy, vwap_sell, b_delta_buy, b_delta_sell, q_delta_buy, q_delta_sell, net_profit = \
            await self.trade_executor.execute_and_get_deltas(
                db=db,
                common_symbol=common_symbol,
                buy_exchange=richest[0],      # buy USDT on rich IRT exchange
                sell_exchange=poorest[0],     # sell USDT on poor IRT exchange
                volume=target_usdt,
                quote_currency=quote_currency,
                buy_client=buy_client,
                sell_client=sell_client,
                buy_exch_obj_id=buy_exch_obj_id,
                sell_exch_obj_id=sell_exch_obj_id,
                buy_fee_rate=buy_fee,
                sell_fee_rate=sell_fee,
                vwap_buy=buy_price,
                vwap_sell=sell_price,
                is_live=is_live
            )

        if not success:
            reason = "Trade execution failed"
            logger.error(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        # 8. Log rebalance
        await self.logger.log_rebalance(
            db,
            common_symbol=None,
            currency=quote_currency,
            from_exch=richest[0],   # IRT moves from rich to poor (actually via USDT trade)
            to_exch=poorest[0],
            amount_sent=filled_vol,
            fee=0.0,
            net=filled_vol,
            reason=f"quote_rebalance_{common_symbol}_imbalance_{imbalance_ratio}"
        )

        # 9. Update last rebalance time and clear pending flag
        settings.last_quote_rebalance_time = datetime.utcnow()
        settings.quote_rebalance_pending = False
        await db.commit()

        reason = f"Quote rebalance executed: sold {filled_vol:.4f} {common_symbol} on {poorest[0]} at {vwap_sell:.2f}, bought on {richest[0]} at {vwap_buy:.2f}"
        logger.info(f"[QUOTE REBALANCE] {reason}")
        return True, reason