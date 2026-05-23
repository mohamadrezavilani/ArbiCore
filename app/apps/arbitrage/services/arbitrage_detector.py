import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Dict, List, Tuple
from app.apps.arbitrage.models import (
    Exchange, ExchangeFee, SymbolArbitrageSettings, Network, ArbitrageOpportunity
)
from app.apps.arbitrage.inventory import get_base_balance, get_quote_balance
from app.exchanges.factory import get_exchange_client
from .opportunity_logger import OpportunityLogger
from .risk_manager import RiskManager
from .trade_executor import TradeExecutor
from .rebalancer import Rebalancer

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
    ):
        """
        exchange_orderbooks: { exchange_name: (ask_levels, bid_levels) }
        """
        if common_symbol.endswith("IRT"):
            quote_currency = "IRT"
        elif common_symbol.endswith("USDT"):
            quote_currency = "USDT"
        else:
            logger.warning(f"Unknown quote currency for symbol {common_symbol}")
            return

        async def get_taker_fee(exchange_name: str) -> float:
            exch = await db.execute(select(Exchange).where(Exchange.name == exchange_name))
            exch_obj = exch.scalar_one_or_none()
            if not exch_obj:
                return 0.0
            fee_stmt = select(ExchangeFee).where(
                ExchangeFee.exchange_id == exch_obj.id,
                ExchangeFee.quote_currency == quote_currency
            )
            fee_rec = await db.execute(fee_stmt)
            fee = fee_rec.scalar_one_or_none()
            return float(fee.taker_fee) if fee else 0.0

        # Get global settings for this symbol
        settings_stmt = select(SymbolArbitrageSettings).where(
            SymbolArbitrageSettings.common_symbol == common_symbol
        )
        settings = (await db.execute(settings_stmt)).scalar_one_or_none()
        if not settings or not settings.is_active:
            return

        network_fee_base = 0.0
        if settings.default_network_id:
            net_stmt = select(Network).where(Network.id == settings.default_network_id)
            net = (await db.execute(net_stmt)).scalar_one_or_none()
            if net:
                network_fee_base = float(net.fee_per_transfer)

        exchange_names = list(exchange_orderbooks.keys())
        for i in range(len(exchange_names)):
            for j in range(i + 1, len(exchange_names)):
                name_a = exchange_names[i]
                name_b = exchange_names[j]
                a_asks, a_bids = exchange_orderbooks[name_a]
                b_asks, b_bids = exchange_orderbooks[name_b]

                # Direction A -> B: buy on A, sell on B
                if a_asks and b_bids:
                    await self._process_direction(
                        db, common_symbol, quote_currency,
                        name_a, a_asks, name_b, b_bids,
                        await get_taker_fee(name_a), await get_taker_fee(name_b),
                        settings, network_fee_base
                    )
                # Direction B -> A: buy on B, sell on A
                if b_asks and a_bids:
                    await self._process_direction(
                        db, common_symbol, quote_currency,
                        name_b, b_asks, name_a, a_bids,
                        await get_taker_fee(name_b), await get_taker_fee(name_a),
                        settings, network_fee_base
                    )

    async def _process_direction(
        self,
        db: AsyncSession,
        common_symbol: str,
        quote_currency: str,
        buy_exch: str,
        buy_levels: List[List[float]],
        sell_exch: str,
        sell_levels: List[List[float]],
        buy_fee: float,
        sell_fee: float,
        settings: SymbolArbitrageSettings,
        network_fee_base: float
    ):
        # Compute max trade volume, cost, revenue
        vol, cost, rev, gross_gain, reason = await self._compute_max_trade(
            db, common_symbol, quote_currency,
            buy_exch, sell_exch, buy_levels, sell_levels, buy_fee, sell_fee
        )
        trade_type = f"buy_on_{buy_exch}_sell_on_{sell_exch}"
        if vol <= 0:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, buy_exch, sell_exch, trade_type,
                f"No volume: {reason}",
                {"vol": vol, "reason": reason}
            )
            return

        vwap_buy = cost / vol
        vwap_sell = rev / vol
        network_fee_quote = network_fee_base * vwap_buy
        trade_pct = self.risk_manager.calculate_trade_percent(gross_gain, network_fee_quote, settings)
        if trade_pct <= 0:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, buy_exch, sell_exch, trade_type,
                "Trade percent <= 0 (risk threshold not met)",
                {"trade_pct": trade_pct, "gross_gain": gross_gain, "network_fee_quote": network_fee_quote}
            )
            return

        actual_vol = vol * trade_pct
        if actual_vol < 1e-6:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, buy_exch, sell_exch, trade_type,
                f"Actual volume {actual_vol} below minimum threshold (1e-6)",
                {"actual_vol": actual_vol, "vol": vol, "trade_pct": trade_pct}
            )
            return

        actual_cost = cost * trade_pct
        actual_rev = rev * trade_pct
        gross_profit_pct = ((actual_rev - actual_cost) / actual_cost) * 100 if actual_cost else 0
        min_profit = float(settings.min_profit_percent)
        if gross_profit_pct < min_profit:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, buy_exch, sell_exch, trade_type,
                f"Gross profit {gross_profit_pct:.2f}% below min {min_profit}%",
                {"gross_profit_pct": gross_profit_pct, "min_profit_percent": min_profit}
            )
            return

        # Execute trade
        buy_client = get_exchange_client(buy_exch)
        sell_client = get_exchange_client(sell_exch)
        if not buy_client or not sell_client:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, buy_exch, sell_exch, trade_type,
                "Could not create exchange clients",
                {"buy_client": buy_client is not None, "sell_client": sell_client is not None}
            )
            return

        exch_a_obj = (await db.execute(select(Exchange).where(Exchange.name == buy_exch))).scalar_one_or_none()
        exch_b_obj = (await db.execute(select(Exchange).where(Exchange.name == sell_exch))).scalar_one_or_none()
        if not exch_a_obj or not exch_b_obj:
            return

        # Determine if live or simulator
        buy_mode = (await db.execute(select(Exchange.mode).where(Exchange.name == buy_exch))).scalar_one_or_none()
        sell_mode = (await db.execute(select(Exchange.mode).where(Exchange.name == sell_exch))).scalar_one_or_none()
        is_live = (buy_mode == "live" and sell_mode == "live")

        if is_live:
            success, filled_vol, final_vwap_buy, final_vwap_sell = await self.trade_executor.execute(
                db, common_symbol, buy_exch, sell_exch, actual_vol, quote_currency,
                buy_client, sell_client, exch_a_obj, exch_b_obj, buy_fee, sell_fee
            )
            if not success:
                return
            # Use actual filled volume and prices
            actual_vol = filled_vol
            actual_cost = actual_vol * final_vwap_buy
            actual_rev = actual_vol * final_vwap_sell
            gross_profit_pct = ((actual_rev - actual_cost) / actual_cost) * 100 if actual_cost else 0
        else:
            # Simulator: just update balances directly
            await self.trade_executor.update_balances_simulator(
                db, buy_exch, sell_exch, common_symbol, quote_currency,
                actual_vol, actual_cost, actual_rev
            )
            final_vwap_buy = vwap_buy
            final_vwap_sell = vwap_sell

        # Record successful opportunity
        opp = ArbitrageOpportunity(
            common_symbol=common_symbol,
            exchange_a_id=exch_a_obj.id,
            exchange_b_id=exch_b_obj.id,
            trade_type=trade_type,
            price_a=final_vwap_buy,
            price_b=final_vwap_sell,
            profit_percent=gross_profit_pct,
            traded_volume=actual_vol,
        )
        db.add(opp)

        # Run rebalancing after trade
        await self.rebalancer.rebalance_symbol_if_needed(db, common_symbol, threshold_ratio=0.1)
        await self.rebalancer.rebalance_quote_if_needed(db, quote_currency, threshold_ratio=0.1)

        logger.info(
            f"✅ Executed {actual_vol:.4f} {common_symbol} (risk {trade_pct:.1%} of max {vol:.4f}) "
            f"buy {buy_exch} @{final_vwap_buy:.2f} sell {sell_exch} @{final_vwap_sell:.2f} "
            f"profit {gross_profit_pct:.2f}%"
        )

    async def _compute_max_trade(
        self,
        db: AsyncSession,
        common_symbol: str,
        quote_currency: str,
        buy_exch: str,
        sell_exch: str,
        buy_levels: List[List[float]],
        sell_levels: List[List[float]],
        buy_fee: float,
        sell_fee: float
    ) -> Tuple[float, float, float, float, str]:
        vol = cost = rev = 0.0
        reason = "Unknown"
        avail_base = await get_base_balance(db, sell_exch, common_symbol)
        if avail_base <= 0:
            return 0, 0, 0, 0, f"Insufficient base balance on {sell_exch} ({avail_base})"
        avail_quote = await get_quote_balance(db, buy_exch, quote_currency)
        if avail_quote <= 0:
            return 0, 0, 0, 0, f"Insufficient quote balance on {buy_exch} ({avail_quote} {quote_currency})"

        i_buy = i_sell = 0
        buy_cpy = [l[:] for l in buy_levels]
        sell_cpy = [l[:] for l in sell_levels]

        while i_buy < len(buy_cpy) and i_sell < len(sell_cpy):
            bprice, bvol = buy_cpy[i_buy]
            sprice, svol = sell_cpy[i_sell]
            cost_unit = bprice * (1 + buy_fee)
            rev_unit = sprice * (1 - sell_fee)
            if rev_unit <= cost_unit:
                reason = f"No profit: buy {cost_unit:.2f} vs sell {rev_unit:.2f}"
                break
            take = min(bvol, svol, avail_base - vol, avail_quote / cost_unit)
            if take <= 0:
                if bvol <= 0:
                    i_buy += 1
                if svol <= 0:
                    i_sell += 1
                continue
            vol += take
            cost += take * cost_unit
            rev += take * rev_unit
            buy_cpy[i_buy][1] -= take
            sell_cpy[i_sell][1] -= take
            if vol >= avail_base or cost >= avail_quote:
                reason = f"Reached inventory limit (base={avail_base}, quote={avail_quote})"
                break
        else:
            if i_buy >= len(buy_cpy):
                reason = "Buy order book exhausted"
            elif i_sell >= len(sell_cpy):
                reason = "Sell order book exhausted"
            else:
                reason = "No profitable levels found"
        return vol, cost, rev, rev - cost, reason