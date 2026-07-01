import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.apps.arbitrage.models import (
    Exchange, BaseInventory, SymbolArbitrageSettings, ExchangeFee, QuoteInventory,
    ArbitrageOpportunity, OrderExecution
)
from app.apps.arbitrage.inventory import update_base_balance, update_quote_balance, get_quote_balance
from app.exchanges.factory import get_exchange_client
from app.apps.arbitrage.services.opportunity_logger import OpportunityLogger
from app.apps.arbitrage.services.trade_executor import TradeExecutor
from app.core.timezone import format_local_time

logger = logging.getLogger(__name__)


class Rebalancer:
    def __init__(self, logger: OpportunityLogger, trade_executor: Optional[TradeExecutor] = None):
        self.logger = logger
        self.trade_executor = trade_executor or TradeExecutor(logger)

    # ----------------------------------------------------------------------
    # Base asset rebalancing (USDT)
    # ----------------------------------------------------------------------
    async def rebalance_symbol_if_needed(
        self,
        db: AsyncSession,
        common_symbol: str,
        quote_currency: str,
        exchange_orderbooks: Dict[str, Tuple[List[List[float]], List[List[float]]]],
    ) -> Tuple[bool, str]:
        logger.info(f"[REBALANCE] Checking {common_symbol} (quote={quote_currency})")

        # 0. Get settings early so we can update monitoring fields
        settings_stmt = select(SymbolArbitrageSettings).where(
            SymbolArbitrageSettings.common_symbol == common_symbol
        )
        settings = (await db.execute(settings_stmt)).scalar_one_or_none()
        if not settings:
            reason = f"No settings found for {common_symbol}"
            logger.warning(f"[REBALANCE] {reason}")
            return False, reason

        # Record check time and default reason
        now = datetime.utcnow()
        settings.last_rebalance_check_time = now
        settings.last_rebalance_spread = None
        settings.last_rebalance_reason = "Checking..."

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
            settings.last_rebalance_reason = reason
            await db.commit()
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        exchange_modes = {name: mode for name, mode, _ in rows}
        balances = [(name, float(bal)) for name, _, bal in rows]
        balances.sort(key=lambda x: x[1])
        poorest = balances[0]
        richest = balances[-1]

        logger.info(f"[REBALANCE] Balances: {balances}")
        logger.info(f"[REBALANCE] richest={richest[0]}={richest[1]:.2f}, poorest={poorest[0]}={poorest[1]:.2f}")

        # Direction guard: only rebalance if Wallex has more USDT and Bitpin has less
        if richest[0] != 'wallex' or poorest[0] != 'bitpin':
            reason = f"Skipping: richest={richest[0]}, poorest={poorest[0]} – not the expected direction (Wallex rich, Bitpin poor)"
            settings.last_rebalance_reason = reason
            await db.commit()
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        if not settings.market_rebalance_enabled:
            reason = f"market_rebalance_enabled=False for {common_symbol}"
            settings.last_rebalance_reason = reason
            await db.commit()
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        # 2. Cooldown
        if settings.last_rebalance_time:
            cooldown_sec = settings.market_rebalance_cooldown_seconds
            next_allowed = settings.last_rebalance_time + timedelta(seconds=cooldown_sec)
            now_utc = datetime.utcnow()
            if now_utc < next_allowed:
                next_allowed_str = format_local_time(next_allowed)
                reason = f"Cooldown active until {next_allowed_str}"
                settings.last_rebalance_reason = reason
                await db.commit()
                logger.info(f"[REBALANCE] {reason}")
                return False, reason
            else:
                logger.info(f"[REBALANCE] Cooldown passed (last rebalance at {format_local_time(settings.last_rebalance_time)})")
        else:
            logger.info("[REBALANCE] No previous rebalance time, proceeding")

        # 3. Orderbook availability
        richest_ob = exchange_orderbooks.get(richest[0])
        poorest_ob = exchange_orderbooks.get(poorest[0])
        if not richest_ob or not poorest_ob:
            missing = []
            if not richest_ob: missing.append(richest[0])
            if not poorest_ob: missing.append(poorest[0])
            reason = f"Missing orderbook for {', '.join(missing)}"
            settings.last_rebalance_reason = reason
            await db.commit()
            logger.warning(f"[REBALANCE] {reason}")
            return False, reason

        # Sell USDT on richest (Wallex) -> use its BID side
        # Buy USDT on poorest (Bitpin) -> use its ASK side
        _, richest_bids = richest_ob
        poorest_asks, _ = poorest_ob

        if not richest_bids or not poorest_asks:
            reason = f"Incomplete levels: richest_bids={bool(richest_bids)}, poorest_asks={bool(poorest_asks)}"
            settings.last_rebalance_reason = reason
            await db.commit()
            logger.warning(f"[REBALANCE] {reason}")
            return False, reason

        sell_price = richest_bids[0][0]
        buy_price = poorest_asks[0][0]
        spread_percent = (buy_price - sell_price) / sell_price * 100
        max_spread = float(settings.market_rebalance_max_spread_percent)
        logger.info(f"[REBALANCE] Prices: sell@{richest[0]}={sell_price:.2f}, buy@{poorest[0]}={buy_price:.2f}, spread={spread_percent:.3f}%, max_spread={max_spread}%")

        # Store spread for monitoring
        settings.last_rebalance_spread = spread_percent

        if spread_percent > max_spread:
            reason = f"Spread {spread_percent:.2f}% > {max_spread}% – will retry later (pending flag remains)"
            settings.last_rebalance_reason = reason
            settings.rebalance_pending = True  # keep pending flag
            await db.commit()
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        # 4. Target amount = ALL USDT on the richest exchange (Wallex)
        target_amount = richest[1]
        logger.info(f"[REBALANCE] Target amount = {target_amount:.4f} (all USDT from Wallex)")

        # Ensure we have enough IRT on Bitpin to buy
        poorest_quote_balance = await get_quote_balance(db, poorest[0], quote_currency)
        cost_estimate = target_amount * buy_price
        if cost_estimate > poorest_quote_balance:
            old = target_amount
            target_amount = (poorest_quote_balance / buy_price) * 0.95
            logger.info(f"[REBALANCE] Reduced target from {old:.4f} to {target_amount:.4f} due to low quote balance {poorest_quote_balance:.2f} (need {cost_estimate:.2f})")

        if target_amount < 0.001:
            reason = f"Target amount too small ({target_amount:.6f})"
            settings.last_rebalance_reason = reason
            await db.commit()
            logger.info(f"[REBALANCE] {reason}")
            return False, reason

        # 5. Execute trade
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
                settings.last_rebalance_reason = reason
                await db.commit()
                logger.error(f"[REBALANCE] {reason}")
                return False, reason
            stmt = select(Exchange.id).where(Exchange.name == poorest[0])
            buy_exch_obj_id = (await db.execute(stmt)).scalar_one_or_none()
            stmt = select(Exchange.id).where(Exchange.name == richest[0])
            sell_exch_obj_id = (await db.execute(stmt)).scalar_one_or_none()
            if not buy_exch_obj_id or not sell_exch_obj_id:
                reason = f"Exchange IDs not found for {poorest[0]} or {richest[0]}"
                settings.last_rebalance_reason = reason
                await db.commit()
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

        # Execute and get all 11 values
        success, filled_vol, vwap_buy, vwap_sell, base_delta_buy, base_delta_sell, quote_delta_buy, quote_delta_sell, net_profit, buy_execs, sell_execs = \
            await self.trade_executor.execute_and_get_deltas(
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
                limit_price_buy=buy_price,
                limit_price_sell=sell_price,
                is_live=is_live
            )

        if not success:
            reason = "Trade execution failed (see logs for details)"
            settings.last_rebalance_reason = reason
            await db.commit()
            logger.error(f"[REBALANCE] {reason}")
            return False, reason

        # Apply deltas to database
        await update_base_balance(db, poorest[0], common_symbol, base_delta_buy)
        await update_base_balance(db, richest[0], common_symbol, base_delta_sell)
        await update_quote_balance(db, poorest[0], quote_currency, quote_delta_buy)
        await update_quote_balance(db, richest[0], quote_currency, quote_delta_sell)

        # 6. Create an ArbitrageOpportunity for this rebalance (so executions are linked)
        profit_percent = (net_profit / (filled_vol * vwap_buy)) * 100 if filled_vol > 0 and vwap_buy > 0 else 0.0
        opp = ArbitrageOpportunity(
            common_symbol=common_symbol,
            exchange_a_id=buy_exch_obj_id,
            exchange_b_id=sell_exch_obj_id,
            trade_type="rebalance_base",
            price_a=vwap_buy,
            price_b=vwap_sell,
            profit_percent=profit_percent,
            traded_volume=filled_vol,
            profit_quote=net_profit
        )
        db.add(opp)
        await db.flush()

        # Add executions
        all_execs = buy_execs + sell_execs
        for exec_data in all_execs:
            exec_record = OrderExecution(
                opportunity_id=opp.id,
                exchange_name=exec_data["exchange_name"],
                side=exec_data["side"],
                price=exec_data["price"],
                volume=exec_data["volume"],
                fee=exec_data["fee"],
                client_order_id=exec_data.get("client_order_id")
            )
            db.add(exec_record)

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
            reason=f"market_rebalance_{common_symbol}_full_swap",
            profit_quote=net_profit
        )

        # 8. Update last rebalance time and clear pending flag
        settings.last_rebalance_time = datetime.utcnow()
        settings.rebalance_pending = False
        reason = f"✅ Executed: moved {filled_vol:.4f} USDT from {richest[0]} to {poorest[0]}, net_profit={net_profit:.2f} {quote_currency}"
        settings.last_rebalance_reason = reason
        await db.commit()

        logger.info(f"[REBALANCE] {reason}")
        return True, reason

    # ----------------------------------------------------------------------
    # Quote asset rebalancing (IRT)
    # ----------------------------------------------------------------------
    async def rebalance_quote_if_needed(
        self,
        db: AsyncSession,
        common_symbol: str,
        quote_currency: str,
        exchange_orderbooks: Dict[str, Tuple[List[List[float]], List[List[float]]]],
    ) -> Tuple[bool, str]:
        logger.info(f"[QUOTE REBALANCE] Checking {common_symbol} quote={quote_currency}")

        # Get settings early
        settings_stmt = select(SymbolArbitrageSettings).where(
            SymbolArbitrageSettings.common_symbol == common_symbol
        )
        settings = (await db.execute(settings_stmt)).scalar_one_or_none()
        if not settings:
            reason = f"No settings found for {common_symbol}"
            logger.warning(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        now = datetime.utcnow()
        settings.last_quote_rebalance_check_time = now
        settings.last_quote_rebalance_spread = None
        settings.last_quote_rebalance_reason = "Checking..."

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
            settings.last_quote_rebalance_reason = reason
            await db.commit()
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        exchange_modes = {name: mode for name, mode, _ in rows}
        balances = [(name, float(bal)) for name, _, bal in rows]
        balances.sort(key=lambda x: x[1])
        poorest = balances[0]
        richest = balances[-1]

        logger.info(f"[QUOTE REBALANCE] Quote balances: {balances}")
        logger.info(f"[QUOTE REBALANCE] richest={richest[0]}={richest[1]:.2f}, poorest={poorest[0]}={poorest[1]:.2f}")

        # Direction guard: only rebalance if Bitpin has more IRT and Wallex has less
        if richest[0] != 'bitpin' or poorest[0] != 'wallex':
            reason = f"Skipping: richest={richest[0]}, poorest={poorest[0]} – not the expected direction (Bitpin rich, Wallex poor)"
            settings.last_quote_rebalance_reason = reason
            await db.commit()
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        if not settings.quote_rebalance_enabled:
            reason = f"quote_rebalance_enabled=False for {common_symbol}"
            settings.last_quote_rebalance_reason = reason
            await db.commit()
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        # 2. Cooldown
        if settings.last_quote_rebalance_time:
            cooldown_sec = settings.quote_rebalance_cooldown_seconds
            next_allowed = settings.last_quote_rebalance_time + timedelta(seconds=cooldown_sec)
            now_utc = datetime.utcnow()
            if now_utc < next_allowed:
                next_allowed_str = format_local_time(next_allowed)
                reason = f"Cooldown active until {next_allowed_str}"
                settings.last_quote_rebalance_reason = reason
                await db.commit()
                logger.info(f"[QUOTE REBALANCE] {reason}")
                return False, reason
            else:
                logger.info(f"[QUOTE REBALANCE] Cooldown passed")
        else:
            logger.info("[QUOTE REBALANCE] No previous rebalance time, proceeding")

        # 3. Orderbook availability
        poorest_ob = exchange_orderbooks.get(poorest[0])
        richest_ob = exchange_orderbooks.get(richest[0])
        if not poorest_ob or not richest_ob:
            missing = []
            if not poorest_ob: missing.append(poorest[0])
            if not richest_ob: missing.append(richest[0])
            reason = f"Missing orderbook for {', '.join(missing)}"
            settings.last_quote_rebalance_reason = reason
            await db.commit()
            logger.warning(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        # Sell USDT on poorest IRT exchange (Bitpin) -> use its BID side
        # Buy USDT on richest IRT exchange (Wallex) -> use its ASK side
        _, poorest_bids = poorest_ob
        richest_asks, _ = richest_ob

        if not poorest_bids or not richest_asks:
            reason = f"Incomplete levels: poorest_bids={bool(poorest_bids)}, richest_asks={bool(richest_asks)}"
            settings.last_quote_rebalance_reason = reason
            await db.commit()
            logger.warning(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        sell_price = poorest_bids[0][0]
        buy_price = richest_asks[0][0]
        spread_percent = (buy_price - sell_price) / sell_price * 100
        max_spread = float(settings.quote_rebalance_max_spread_percent)
        logger.info(f"[QUOTE REBALANCE] Prices: sell@{poorest[0]}={sell_price:.2f}, buy@{richest[0]}={buy_price:.2f}, spread={spread_percent:.3f}%, max_spread={max_spread}%")

        settings.last_quote_rebalance_spread = spread_percent

        if spread_percent > max_spread:
            reason = f"Spread {spread_percent:.2f}% > {max_spread}% – will retry later (pending flag remains)"
            settings.last_quote_rebalance_reason = reason
            settings.quote_rebalance_pending = True
            await db.commit()
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        # 4. Target amount = ALL IRT on the richest exchange (Bitpin) converted to USDT
        target_irt = richest[1]
        target_usdt = target_irt / buy_price
        logger.info(f"[QUOTE REBALANCE] Target amount = {target_usdt:.4f} USDT (all IRT from Bitpin)")

        # Respect available USDT on Bitpin (need to sell USDT there)
        stmt = select(BaseInventory.balance).join(Exchange).where(
            Exchange.name == poorest[0],
            BaseInventory.common_symbol == common_symbol
        )
        bal_result = await db.execute(stmt)
        available_base = float(bal_result.scalar_one_or_none() or 0.0)
        if target_usdt > available_base:
            old = target_usdt
            target_usdt = available_base * 0.95
            logger.info(f"[QUOTE REBALANCE] Reduced target from {old:.4f} to {target_usdt:.4f} due to low base balance on {poorest[0]}")

        if target_usdt < 0.001:
            reason = f"Target amount too small ({target_usdt:.6f} USDT)"
            settings.last_quote_rebalance_reason = reason
            await db.commit()
            logger.info(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        # 5. Execute trade
        mode_poorest = exchange_modes.get(poorest[0], "simulator")
        mode_richest = exchange_modes.get(richest[0], "simulator")
        is_live = (mode_poorest == "live" and mode_richest == "live")

        buy_client = None
        sell_client = None
        buy_exch_obj_id = None
        sell_exch_obj_id = None
        if is_live:
            buy_client = get_exchange_client(richest[0])
            sell_client = get_exchange_client(poorest[0])
            if not buy_client or not sell_client:
                reason = f"Client creation failed for {richest[0]} or {poorest[0]}"
                settings.last_quote_rebalance_reason = reason
                await db.commit()
                logger.error(f"[QUOTE REBALANCE] {reason}")
                return False, reason
            stmt = select(Exchange.id).where(Exchange.name == richest[0])
            buy_exch_obj_id = (await db.execute(stmt)).scalar_one_or_none()
            stmt = select(Exchange.id).where(Exchange.name == poorest[0])
            sell_exch_obj_id = (await db.execute(stmt)).scalar_one_or_none()
            if not buy_exch_obj_id or not sell_exch_obj_id:
                reason = f"Exchange IDs not found"
                settings.last_quote_rebalance_reason = reason
                await db.commit()
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

        # Execute and get all 11 values
        success, filled_vol, vwap_buy, vwap_sell, base_delta_buy, base_delta_sell, quote_delta_buy, quote_delta_sell, net_profit, buy_execs, sell_execs = \
            await self.trade_executor.execute_and_get_deltas(
                db=db,
                common_symbol=common_symbol,
                buy_exchange=richest[0],
                sell_exchange=poorest[0],
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
                limit_price_buy=buy_price,
                limit_price_sell=sell_price,
                is_live=is_live
            )

        if not success:
            reason = "Trade execution failed"
            settings.last_quote_rebalance_reason = reason
            await db.commit()
            logger.error(f"[QUOTE REBALANCE] {reason}")
            return False, reason

        # Apply deltas to database
        await update_base_balance(db, poorest[0], common_symbol, base_delta_sell)
        await update_base_balance(db, richest[0], common_symbol, base_delta_buy)
        await update_quote_balance(db, poorest[0], quote_currency, quote_delta_sell)
        await update_quote_balance(db, richest[0], quote_currency, quote_delta_buy)

        # 6. Create an ArbitrageOpportunity for this quote rebalance
        profit_percent = (net_profit / (filled_vol * vwap_buy)) * 100 if filled_vol > 0 and vwap_buy > 0 else 0.0
        opp = ArbitrageOpportunity(
            common_symbol=common_symbol,
            exchange_a_id=buy_exch_obj_id,
            exchange_b_id=sell_exch_obj_id,
            trade_type="rebalance_quote",
            price_a=vwap_buy,
            price_b=vwap_sell,
            profit_percent=profit_percent,
            traded_volume=filled_vol,
            profit_quote=net_profit
        )
        db.add(opp)
        await db.flush()

        all_execs = buy_execs + sell_execs
        for exec_data in all_execs:
            exec_record = OrderExecution(
                opportunity_id=opp.id,
                exchange_name=exec_data["exchange_name"],
                side=exec_data["side"],
                price=exec_data["price"],
                volume=exec_data["volume"],
                fee=exec_data["fee"],
                client_order_id=exec_data.get("client_order_id")
            )
            db.add(exec_record)

        # 7. Log rebalance
        await self.logger.log_rebalance(
            db,
            common_symbol=None,
            currency=quote_currency,
            from_exch=poorest[0],
            to_exch=richest[0],
            amount_sent=filled_vol,
            fee=0.0,
            net=filled_vol,
            reason=f"quote_rebalance_{common_symbol}_full_swap",
            profit_quote=net_profit
        )

        # 8. Update last rebalance time and clear pending flag
        settings.last_quote_rebalance_time = datetime.utcnow()
        settings.quote_rebalance_pending = False
        reason = f"✅ Executed: moved {filled_vol:.4f} USDT from {poorest[0]} to {richest[0]}, net_profit={net_profit:.2f} {quote_currency}"
        settings.last_quote_rebalance_reason = reason
        await db.commit()

        logger.info(f"[QUOTE REBALANCE] {reason}")
        return True, reason