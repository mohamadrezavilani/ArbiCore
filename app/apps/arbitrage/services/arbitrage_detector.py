import logging
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.apps.arbitrage.models import (
    Exchange, ExchangeFee, SymbolArbitrageSettings, Network,
    ArbitrageOpportunity, BaseInventory, QuoteInventory
)
from app.exchanges.factory import get_exchange_client
from .opportunity_logger import OpportunityLogger
from .risk_manager import RiskManager
from .trade_executor import TradeExecutor
from .rebalancer import Rebalancer
from .pair_weight import get_pair_weight

logger = logging.getLogger(__name__)

MIN_ORDER_SIZE = 2.0   # USDT minimum for Wallex/Bitpin


class ArbitrageDetector:
    def __init__(
            self,
            logger: OpportunityLogger,
            risk_manager: RiskManager,
            trade_executor: TradeExecutor,
            rebalancer: Rebalancer
    ):
        self.logger = logger
        self.risk_manager = risk_manager
        self.trade_executor = trade_executor
        self.rebalancer = rebalancer
        self.MAX_LEVELS = 20   # sweep up to 20 levels

    @staticmethod
    def _max_volume_from_quote(available_quote: float, ask_price: float) -> float:
        if available_quote <= 0 or ask_price <= 0:
            return 0.0
        d_quote = Decimal(str(available_quote))
        d_price = Decimal(str(ask_price))
        max_vol_dec = (d_quote / d_price).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
        return float(max_vol_dec)

    @staticmethod
    def _max_volume_from_base(available_base: float) -> float:
        if available_base <= 0:
            return 0.0
        d_base = Decimal(str(available_base))
        return float(d_base.quantize(Decimal('0.00000001'), rounding=ROUND_DOWN))

    def _get_cumulative_levels(self, levels: List[List[float]], max_volume: float, max_levels: int = None) -> Tuple[List[float], List[float], float, float]:
        """
        Sweep orderbook levels to accumulate up to max_volume.
        Returns: (prices, volumes, total_volume, vwap)
        """
        if max_levels is None:
            max_levels = self.MAX_LEVELS
        cum_vol = 0.0
        cum_value = 0.0
        prices = []
        vols = []
        for price, vol in levels[:max_levels]:
            if cum_vol >= max_volume:
                break
            remaining = max_volume - cum_vol
            take = min(vol, remaining)
            cum_vol += take
            cum_value += take * price
            prices.append(price)
            vols.append(take)
            if cum_vol >= max_volume:
                break
        if cum_vol == 0:
            return [], [], 0.0, 0.0
        vwap = cum_value / cum_vol
        return prices, vols, cum_vol, vwap

    async def detect_for_symbol(
            self,
            db: AsyncSession,
            common_symbol: str,
            exchange_orderbooks: Dict[str, Tuple[List[List[float]], List[List[float]]]]
    ) -> Tuple[bool, Dict[str, float], Dict[str, float], List[ArbitrageOpportunity]]:

        if common_symbol.endswith("IRT"):
            quote_currency = "IRT"
        elif common_symbol.endswith("USDT"):
            quote_currency = "USDT"
        else:
            return False, {}, {}, []

        # Get settings
        stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == common_symbol)
        settings = (await db.execute(stmt)).scalar_one_or_none()
        if not settings or not settings.is_active:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, "", "", "global",
                f"Symbol {common_symbol} not active or no settings",
                {"settings_found": settings is not None}
            )
            return False, {}, {}, []

        exchange_names = list(exchange_orderbooks.keys())

        # Pre-fetch all static data
        exchange_modes = {}
        base_balances = {}
        quote_balances = {}
        taker_fees = {}
        exchange_ids = {}
        pair_weights = {}

        for name in exchange_names:
            exchange_modes[name] = (await db.execute(
                select(Exchange.mode).where(Exchange.name == name)
            )).scalar_one_or_none() or "simulator"

            base_balances[name] = float((await db.execute(
                select(BaseInventory.balance).join(Exchange)
                .where(Exchange.name == name, BaseInventory.common_symbol == common_symbol)
            )).scalar_one_or_none() or 0.0)

            quote_balances[name] = float((await db.execute(
                select(QuoteInventory.balance).join(Exchange)
                .where(Exchange.name == name, QuoteInventory.currency == quote_currency)
            )).scalar_one_or_none() or 0.0)

            taker_fees[name] = float((await db.execute(
                select(ExchangeFee.taker_fee).join(Exchange)
                .where(Exchange.name == name, ExchangeFee.quote_currency == quote_currency)
            )).scalar_one_or_none() or 0.0)

            exchange_ids[name] = (await db.execute(
                select(Exchange.id).where(Exchange.name == name)
            )).scalar_one_or_none()

        for buy_ex in exchange_names:
            for sell_ex in exchange_names:
                if buy_ex != sell_ex:
                    pair_weights[(buy_ex, sell_ex)] = await get_pair_weight(db, buy_ex, sell_ex)

        network_fee_base = await self._get_network_fee_base(db, settings)
        max_base_pool = await self._get_max_base_pool(db, common_symbol)

        # Build asks/bids with effective prices
        asks = []  # (exchange, raw_price, volume, effective_price, fee)
        bids = []
        for exch_name, (ask_levels, bid_levels) in exchange_orderbooks.items():
            fee = taker_fees.get(exch_name, 0.0)
            for price, vol in ask_levels:
                effective = price * (1 + fee)
                asks.append((exch_name, price, vol, effective, fee))
            for price, vol in bid_levels:
                effective = price * (1 - fee)
                bids.append((exch_name, price, vol, effective, fee))

        if not asks or not bids:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, "", "", "global",
                "No orderbook levels", {"asks": len(asks), "bids": len(bids)}
            )
            return False, {}, {}, []

        asks.sort(key=lambda x: x[3])
        bids.sort(key=lambda x: x[3], reverse=True)

        avail_quote = quote_balances.copy()
        avail_base = base_balances.copy()

        base_deltas = {ex: 0.0 for ex in exchange_names}
        quote_deltas = {ex: 0.0 for ex in exchange_names}
        opportunities = []

        i, j = 0, 0
        while i < len(asks) and j < len(bids):
            buy_exch, ask_price, ask_vol, eff_ask, ask_fee = asks[i]
            sell_exch, bid_price, bid_vol, eff_bid, bid_fee = bids[j]

            if buy_exch == sell_exch:
                i += 1
                j += 1
                continue

            if eff_ask >= eff_bid:
                break  # no profit possible further due to sorted order

            weight = pair_weights.get((buy_exch, sell_exch), 0.5)

            # Determine maximum possible volume from balances
            available_quote = avail_quote.get(buy_exch, 0.0)
            available_base = avail_base.get(sell_exch, 0.0)

            max_vol_by_quote = self._max_volume_from_quote(available_quote, ask_price)
            max_vol_by_base = self._max_volume_from_base(available_base)

            # We want to trade at least MIN_ORDER_SIZE if possible
            desired_vol = min(max_vol_by_quote, max_vol_by_base)

            # Safety: don't use more than 25% of quote balance to avoid draining
            max_safe_quote = available_quote
            max_vol_safe = self._max_volume_from_quote(max_safe_quote, ask_price)
            desired_vol = min(desired_vol, max_vol_safe)

            # logger.info(f"max_safe_quote : {max_safe_quote:.4f} , max_vol_safe: {max_vol_safe:.4f}")

            if desired_vol < MIN_ORDER_SIZE:
                reason = f"Desired volume {desired_vol:.4f} below minimum {MIN_ORDER_SIZE}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {"available_quote": available_quote, "available_base": available_base}
                )
                i += 1
                j += 1
                continue

            # Now sweep orderbook to see how much we can get at VWAP
            buy_original_levels = exchange_orderbooks[buy_exch][0]  # asks
            sell_original_levels = exchange_orderbooks[sell_exch][1]  # bids

            # Get cumulative up to desired_vol (or max levels)
            buy_prices, buy_vols, total_buy_vol, vwap_buy = self._get_cumulative_levels(buy_original_levels, desired_vol, self.MAX_LEVELS)
            sell_prices, sell_vols, total_sell_vol, vwap_sell = self._get_cumulative_levels(sell_original_levels, desired_vol, self.MAX_LEVELS)

            if total_buy_vol < MIN_ORDER_SIZE or total_sell_vol < MIN_ORDER_SIZE:
                reason = f"Insufficient depth: buy_depth={total_buy_vol:.4f}, sell_depth={total_sell_vol:.4f}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {"buy_depth": total_buy_vol, "sell_depth": total_sell_vol}
                )
                i += 1
                j += 1
                continue

            # The volume we can trade is the minimum of the two cumulative volumes
            max_vol = min(total_buy_vol, total_sell_vol)

            # Ensure we meet minimum order size
            if max_vol < MIN_ORDER_SIZE:
                reason = f"Max volume {max_vol:.4f} below minimum {MIN_ORDER_SIZE}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {"max_vol": max_vol, "min_order": MIN_ORDER_SIZE}
                )
                i += 1
                j += 1
                continue

            # Apply profit check using VWAP prices
            effective_buy = vwap_buy * (1 + ask_fee)
            effective_sell = vwap_sell * (1 - bid_fee)
            profit_percent = (effective_sell - effective_buy) / effective_buy * 100

            if profit_percent < float(settings.min_profit_percent):
                reason = f"Profit {profit_percent:.4f}% below minimum {settings.min_profit_percent}%"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {"vwap_buy": vwap_buy, "vwap_sell": vwap_sell, "min_profit": settings.min_profit_percent}
                )
                i += 1
                j += 1
                continue

            net_gain = max_vol * (effective_sell - effective_buy)

            # Risk manager decides how much of max_vol to trade
            trade_pct = self.risk_manager.calculate_trade_percent(
                net_gain=net_gain,
                network_commission_quote=0.0,
                params=settings,
                vol=max_vol,
                weight=weight,
                current_price=vwap_buy,
                network_fee_base=network_fee_base,
                max_base_pool=max_base_pool
            )

            if trade_pct <= 0:
                reason = f"Risk manager rejected: trade_pct={trade_pct:.4f}, net_gain={net_gain:.6f}, weight={weight:.3f}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason, {"net_gain": net_gain, "trade_pct": trade_pct}
                )
                i += 1
                j += 1
                continue

            volume = max_vol * trade_pct

            # Safety: ensure we don't exceed available balances
            if volume > desired_vol:
                volume = desired_vol
            logger.info(f"volume : {volume:.4f} = max_vol: {max_vol} * trade_pct: {trade_pct}")

            if volume < MIN_ORDER_SIZE:
                reason = f"Final volume {volume:.4f} below minimum {MIN_ORDER_SIZE}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {"volume": volume, "min_order": MIN_ORDER_SIZE}
                )
                i += 1
                j += 1
                continue

            # Determine worst prices for limit orders
            buy_limit_price = buy_prices[-1] if buy_prices else vwap_buy
            sell_limit_price = sell_prices[-1] if sell_prices else vwap_sell

            is_live = (exchange_modes.get(buy_exch) == "live" and exchange_modes.get(sell_exch) == "live")

            success, filled_vol, vwap_buy_exec, vwap_sell_exec, b_delta_buy, b_delta_sell, q_delta_buy, q_delta_sell, net_profit = \
                await self.trade_executor.execute_and_get_deltas(
                    db=db, common_symbol=common_symbol,
                    buy_exchange=buy_exch, sell_exchange=sell_exch,
                    volume=volume, quote_currency=quote_currency,
                    buy_client=get_exchange_client(buy_exch) if is_live else None,
                    sell_client=get_exchange_client(sell_exch) if is_live else None,
                    buy_exch_obj_id=exchange_ids.get(buy_exch),
                    sell_exch_obj_id=exchange_ids.get(sell_exch),
                    buy_fee_rate=ask_fee, sell_fee_rate=bid_fee,
                    vwap_buy=vwap_buy, vwap_sell=vwap_sell,
                    limit_price_buy=buy_limit_price,
                    limit_price_sell=sell_limit_price,
                    is_live=is_live
                )

            if success:
                if not is_live:
                    base_deltas[buy_exch] += b_delta_buy
                    base_deltas[sell_exch] += b_delta_sell
                    quote_deltas[buy_exch] += q_delta_buy
                    quote_deltas[sell_exch] += q_delta_sell

                # Update available balances for future loops
                avail_quote[buy_exch] = max(0.0, avail_quote[buy_exch] + q_delta_buy)
                avail_base[buy_exch] = max(0.0, avail_base[buy_exch] + b_delta_buy)
                avail_quote[sell_exch] = max(0.0, avail_quote[sell_exch] + q_delta_sell)
                avail_base[sell_exch] = max(0.0, avail_base[sell_exch] + b_delta_sell)

                opp = ArbitrageOpportunity(
                    common_symbol=common_symbol,
                    exchange_a_id=exchange_ids[buy_exch],
                    exchange_b_id=exchange_ids[sell_exch],
                    trade_type=f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    price_a=vwap_buy_exec,
                    price_b=vwap_sell_exec,
                    profit_percent=((filled_vol * vwap_sell_exec - filled_vol * vwap_buy_exec) / (filled_vol * vwap_buy_exec)) * 100 if filled_vol > 0 else 0,
                    traded_volume=filled_vol,
                    profit_quote=net_profit
                )
                opportunities.append(opp)

                # Advance indices to avoid reusing same levels
                i += 1
                j += 1
            else:
                i += 1
                j += 1

        return len(opportunities) > 0, base_deltas, quote_deltas, opportunities

    async def _get_network_fee_base(self, db: AsyncSession, settings: SymbolArbitrageSettings) -> float:
        if not settings.default_network_id:
            return 0.0
        fee = (await db.execute(
            select(Network.fee_per_transfer).where(Network.id == settings.default_network_id))).scalar_one_or_none()
        return float(fee) if fee else 0.0

    async def _get_max_base_pool(self, db: AsyncSession, common_symbol: str) -> float:
        max_bal = (await db.execute(
            select(func.max(BaseInventory.balance))
            .join(Exchange)
            .where(BaseInventory.common_symbol == common_symbol, Exchange.is_active == True)
        )).scalar()
        return float(max_bal) if max_bal else 0.0