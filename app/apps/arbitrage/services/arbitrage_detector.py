import logging
from typing import Dict, List, Tuple, Optional, Any
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

    async def detect_for_symbol(
        self,
        db: AsyncSession,
        common_symbol: str,
        exchange_orderbooks: Dict[str, Tuple[List[List[float]], List[List[float]]]]
    ) -> Tuple[bool, Dict[str, float], Dict[str, float], List[ArbitrageOpportunity]]:
        """
        Returns:
            - any_trade: bool
            - base_deltas: {exchange: delta}
            - quote_deltas: {exchange: delta}
            - opportunities: list of ArbitrageOpportunity objects to add
        """
        # Determine quote currency
        if common_symbol.endswith("IRT"):
            quote_currency = "IRT"
        elif common_symbol.endswith("USDT"):
            quote_currency = "USDT"
        else:
            return False, {}, {}, []

        # ========== FORCE TRADE FOR TESTING (set to False for production) ==========
        FORCE_TRADE = False                     # Set to True to enable forced test
        FORCE_SYMBOL = "USDTIRT"
        FORCE_MULTIPLIER = 0.998
        FORCE_BID_MULTIPLIER = 1.002

        if FORCE_TRADE and common_symbol == FORCE_SYMBOL:
            exch_list = list(exchange_orderbooks.keys())
            if len(exch_list) >= 2:
                buy_ex = exch_list[0]
                sell_ex = exch_list[1]
                buy_asks, buy_bids = exchange_orderbooks[buy_ex]
                sell_asks, sell_bids = exchange_orderbooks[sell_ex]
                if buy_asks and sell_bids:
                    original_ask = buy_asks[0][0]
                    buy_asks[0][0] = original_ask * FORCE_MULTIPLIER
                    original_bid = sell_bids[0][0]
                    sell_bids[0][0] = original_bid * FORCE_BID_MULTIPLIER
                    exchange_orderbooks[buy_ex] = (buy_asks, buy_bids)
                    exchange_orderbooks[sell_ex] = (sell_asks, sell_bids)
                    logger.info(f"[TEST] Forced profitable spread: buy on {buy_ex} ask={buy_asks[0][0]:.2f}, sell on {sell_ex} bid={sell_bids[0][0]:.2f}")
        # ========== END FORCE TRADE ==========

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

        # ------------------------------------------------------------
        # Pre-fetch all static data
        # ------------------------------------------------------------
        exchange_modes = {}
        for name in exchange_names:
            stmt = select(Exchange.mode).where(Exchange.name == name)
            mode = (await db.execute(stmt)).scalar_one_or_none()
            exchange_modes[name] = mode or "simulator"

        base_balances = {}
        quote_balances = {}
        for ex_name in exchange_names:
            stmt = select(BaseInventory.balance).join(Exchange).where(
                Exchange.name == ex_name,
                BaseInventory.common_symbol == common_symbol
            )
            bal = (await db.execute(stmt)).scalar_one_or_none()
            base_balances[ex_name] = float(bal) if bal else 0.0

            stmt = select(QuoteInventory.balance).join(Exchange).where(
                Exchange.name == ex_name,
                QuoteInventory.currency == quote_currency
            )
            bal = (await db.execute(stmt)).scalar_one_or_none()
            quote_balances[ex_name] = float(bal) if bal else 0.0

        # Pair weights
        pair_weights = {}
        for buy_ex in exchange_names:
            for sell_ex in exchange_names:
                if buy_ex != sell_ex:
                    pair_weights[(buy_ex, sell_ex)] = await get_pair_weight(db, buy_ex, sell_ex)

        # Taker fees
        taker_fees = {}
        for ex_name in exchange_names:
            stmt = select(ExchangeFee.taker_fee).join(Exchange).where(
                Exchange.name == ex_name,
                ExchangeFee.quote_currency == quote_currency
            )
            fee = (await db.execute(stmt)).scalar_one_or_none()
            taker_fees[ex_name] = float(fee) if fee else 0.0

        network_fee_base = await self._get_network_fee_base(db, settings)
        max_base_pool = await self._get_max_base_pool(db, common_symbol)

        exchange_ids = {}
        for ex_name in exchange_names:
            stmt = select(Exchange.id).where(Exchange.name == ex_name)
            eid = (await db.execute(stmt)).scalar_one_or_none()
            exchange_ids[ex_name] = eid

        # Build asks/bids with effective prices
        asks = []   # (exchange, raw_price, volume, effective_price, fee)
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
                "No orderbook levels",
                {"asks": len(asks), "bids": len(bids)}
            )
            return False, {}, {}, []

        asks.sort(key=lambda x: x[3])
        bids.sort(key=lambda x: x[3], reverse=True)

        # Working copies of balances
        avail_quote = quote_balances.copy()
        avail_base = base_balances.copy()

        base_deltas = {ex: 0.0 for ex in exchange_names}
        quote_deltas = {ex: 0.0 for ex in exchange_names}
        opportunities = []

        i, j = 0, 0
        SAFETY = 0.999999

        while i < len(asks) and j < len(bids):
            buy_exch, ask_price, ask_vol, eff_ask, ask_fee = asks[i]
            sell_exch, bid_price, bid_vol, eff_bid, bid_fee = bids[j]

            if buy_exch == sell_exch:
                i += 1
                j += 1
                continue

            if eff_ask >= eff_bid:
                reason = f"No profit: buy effective {eff_ask:.6f} >= sell effective {eff_bid:.6f}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {"ask_price": ask_price, "bid_price": bid_price, "buy_fee": ask_fee, "sell_fee": bid_fee}
                )
                break

            weight = pair_weights.get((buy_exch, sell_exch), 0.5)

            # ----- Balance checks with safety margin -----
            available_quote = avail_quote.get(buy_exch, 0.0)
            if available_quote <= 0:
                max_vol_by_quote = 0.0
            else:
                max_vol_by_quote = (available_quote / ask_price) * SAFETY

            available_base = avail_base.get(sell_exch, 0.0)
            if available_base <= 0:
                max_vol_by_base = 0.0
            else:
                max_vol_by_base = available_base * SAFETY

            max_vol = min(ask_vol, bid_vol, max_vol_by_quote, max_vol_by_base)

            if max_vol <= 0:
                reason = f"Insufficient balance: quote on {buy_exch}={available_quote:.2f}, base on {sell_exch}={available_base:.4f}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {"required_quote": max_vol * ask_price if max_vol else 0, "available_quote": available_quote,
                     "required_base": max_vol, "available_base": available_base}
                )
                i += 1
                j += 1
                continue

            net_gain = max_vol * (eff_bid - eff_ask)
            trade_pct = self.risk_manager.calculate_trade_percent(
                net_gain=net_gain,
                network_commission_quote=0.0,
                params=settings,
                vol=max_vol,
                weight=weight,
                current_price=ask_price,
                network_fee_base=network_fee_base,
                max_base_pool=max_base_pool
            )
            if trade_pct <= 0:
                reason = f"Risk manager rejected: trade_pct={trade_pct:.4f}, net_gain={net_gain:.6f}, weight={weight:.3f}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {
                        "net_gain": net_gain,
                        "trade_pct": trade_pct,
                        "weight": weight,
                        "cutoff_threshold": float(settings.cutoff_threshold),
                        "min_trade_percent": float(settings.min_trade_percent)
                    }
                )
                i += 1
                j += 1
                continue

            volume = max_vol * trade_pct
            if volume < 1e-6:
                reason = f"Volume too small: {volume:.8f}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {"max_vol": max_vol, "trade_pct": trade_pct}
                )
                i += 1
                j += 1
                continue

            # Execute trade
            is_live = (exchange_modes.get(buy_exch) == "live" and exchange_modes.get(sell_exch) == "live")
            success, filled_vol, vwap_buy, vwap_sell, base_delta_buy, base_delta_sell, quote_delta_buy, quote_delta_sell, net_profit = \
                await self.trade_executor.execute_and_get_deltas(
                    db=db,
                    common_symbol=common_symbol,
                    buy_exchange=buy_exch,
                    sell_exchange=sell_exch,
                    volume=volume,
                    quote_currency=quote_currency,
                    buy_client=get_exchange_client(buy_exch) if is_live else None,
                    sell_client=get_exchange_client(sell_exch) if is_live else None,
                    buy_exch_obj_id=exchange_ids.get(buy_exch),
                    sell_exch_obj_id=exchange_ids.get(sell_exch),
                    buy_fee_rate=ask_fee,
                    sell_fee_rate=bid_fee,
                    vwap_buy=ask_price,
                    vwap_sell=bid_price,
                    is_live=is_live
                )

            if success:
                if not is_live:
                    base_deltas[buy_exch] += base_delta_buy
                    base_deltas[sell_exch] += base_delta_sell
                    quote_deltas[buy_exch] += quote_delta_buy
                    quote_deltas[sell_exch] += quote_delta_sell

                # Update working balances with clamping to zero
                avail_quote[buy_exch] = max(0.0, avail_quote[buy_exch] + quote_delta_buy)
                avail_base[buy_exch] = max(0.0, avail_base[buy_exch] + base_delta_buy)
                avail_quote[sell_exch] = max(0.0, avail_quote[sell_exch] + quote_delta_sell)
                avail_base[sell_exch] = max(0.0, avail_base[sell_exch] + base_delta_sell)

                opp = ArbitrageOpportunity(
                    common_symbol=common_symbol,
                    exchange_a_id=exchange_ids[buy_exch],
                    exchange_b_id=exchange_ids[sell_exch],
                    trade_type=f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    price_a=vwap_buy,
                    price_b=vwap_sell,
                    profit_percent=((filled_vol * vwap_sell - filled_vol * vwap_buy) / (
                                filled_vol * vwap_buy)) * 100 if filled_vol > 0 else 0,
                    traded_volume=filled_vol,
                    profit_quote=net_profit  # <-- store net profit
                )
                opportunities.append(opp)

                # Update orderbook pointers
                asks[i] = (buy_exch, ask_price, ask_vol - filled_vol, eff_ask, ask_fee)
                bids[j] = (sell_exch, bid_price, bid_vol - filled_vol, eff_bid, bid_fee)
                if asks[i][2] <= 0:
                    i += 1
                if bids[j][2] <= 0:
                    j += 1
            else:
                i += 1
                j += 1

        any_trade = len(opportunities) > 0
        return any_trade, base_deltas, quote_deltas, opportunities

    async def _get_network_fee_base(self, db: AsyncSession, settings: SymbolArbitrageSettings) -> float:
        if not settings.default_network_id:
            return 0.0
        net_stmt = select(Network.fee_per_transfer).where(Network.id == settings.default_network_id)
        net_fee = await db.execute(net_stmt)
        fee = net_fee.scalar_one_or_none()
        return float(fee) if fee else 0.0

    async def _get_max_base_pool(self, db: AsyncSession, common_symbol: str) -> float:
        stmt = select(func.max(BaseInventory.balance)).join(Exchange).where(
            BaseInventory.common_symbol == common_symbol,
            Exchange.is_active == True
        )
        result = await db.execute(stmt)
        max_bal = result.scalar()
        return float(max_bal) if max_bal else 0.0