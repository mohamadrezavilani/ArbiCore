import logging
from typing import Dict, List, Tuple, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.apps.arbitrage.models import (
    Exchange, ExchangeFee, SymbolArbitrageSettings, Network,
    ArbitrageOpportunity, BaseInventory, QuoteInventory
)
from app.apps.arbitrage.inventory import get_base_balance, get_quote_balance
from app.exchanges.factory import get_exchange_client
from .opportunity_logger import OpportunityLogger
from .risk_manager import RiskManager
from .trade_executor import TradeExecutor
from .rebalancer import Rebalancer
from .pair_weight import get_pair_weight, update_pair_weight

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
    ) -> bool:
        # Determine quote currency
        if common_symbol.endswith("IRT"):
            quote_currency = "IRT"
        elif common_symbol.endswith("USDT"):
            quote_currency = "USDT"
        else:
            logger.warning(f"Unknown quote currency for symbol {common_symbol}")
            return False

        # Get global settings for this symbol
        settings_stmt = select(SymbolArbitrageSettings).where(
            SymbolArbitrageSettings.common_symbol == common_symbol
        )
        settings = (await db.execute(settings_stmt)).scalar_one_or_none()
        if not settings or not settings.is_active:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, "", "", "global",
                f"Symbol {common_symbol} not active or no settings",
                {"settings_found": settings is not None}
            )
            return False

        # Fetch current balances and modes for all active exchanges
        exchange_info = {}
        stmt = select(Exchange.name, Exchange.mode).where(Exchange.is_active == True)
        exchanges = await db.execute(stmt)
        for exch_name, mode in exchanges.all():
            quote_bal = await get_quote_balance(db, exch_name, quote_currency)
            base_bal = await get_base_balance(db, exch_name, common_symbol)
            exchange_info[exch_name] = {
                "quote": float(quote_bal),
                "base": float(base_bal),
                "mode": mode
            }

        # Build asks and bids lists with fees
        asks = []   # (exchange, price, volume, effective_price, fee)
        bids = []
        for exch_name, (ask_levels, bid_levels) in exchange_orderbooks.items():
            fee = await self._get_taker_fee(db, exch_name, quote_currency)
            if fee is None:
                continue
            for price, vol in ask_levels:
                # effective = price * (1 + fee)
                effective = price
                asks.append((exch_name, price, vol, effective, fee))
            for price, vol in bid_levels:
                effective = price
                # effective = price * (1 - fee)
                bids.append((exch_name, price, vol, effective, fee))

        if not asks or not bids:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, "", "", "global",
                "No orderbook levels available for any exchange",
                {"asks_count": len(asks), "bids_count": len(bids)}
            )
            return False

        asks.sort(key=lambda x: x[3])
        bids.sort(key=lambda x: x[3], reverse=True)

        avail_quote = {exch: exchange_info[exch]["quote"] for exch in exchange_info}
        avail_base = {exch: exchange_info[exch]["base"] for exch in exchange_info}

        i, j = 0, 0
        matches = []
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

            weight = await get_pair_weight(db, buy_exch, sell_exch)

            max_vol = min(ask_vol, bid_vol)
            needed_quote = max_vol * ask_price
            if needed_quote > avail_quote.get(buy_exch, 0):
                max_vol = avail_quote[buy_exch] / ask_price
            if max_vol > avail_base.get(sell_exch, 0):
                max_vol = avail_base[sell_exch]

            if max_vol <= 0:
                reason = f"Insufficient balance: quote on {buy_exch}={avail_quote.get(buy_exch,0):.2f}, base on {sell_exch}={avail_base.get(sell_exch,0):.4f}"
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    reason,
                    {"required_quote": needed_quote, "available_quote": avail_quote.get(buy_exch,0),
                     "required_base": max_vol, "available_base": avail_base.get(sell_exch,0)}
                )
                if ask_vol <= 0:
                    i += 1
                if bid_vol <= 0:
                    j += 1
                continue

            net_gain = max_vol * (eff_bid - eff_ask)
            network_fee_base = await self._get_network_fee_base(db, settings)
            max_base_pool = await self._get_max_base_pool(db, common_symbol)

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
                # Convert Decimal settings to float before logging
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

            matches.append((buy_exch, sell_exch, volume, ask_price, bid_price, ask_fee, bid_fee))
            avail_quote[buy_exch] -= volume * ask_price
            avail_base[sell_exch] -= volume
            asks[i] = (buy_exch, ask_price, ask_vol - volume, eff_ask, ask_fee)
            bids[j] = (sell_exch, bid_price, bid_vol - volume, eff_bid, bid_fee)
            if asks[i][2] <= 0:
                i += 1
            if bids[j][2] <= 0:
                j += 1

        any_trade = False
        for (buy_exch, sell_exch, volume, ask_price, bid_price, ask_fee, bid_fee) in matches:
            success = await self._execute_match(
                db, common_symbol, quote_currency,
                buy_exch, sell_exch, volume, ask_price, bid_price,
                ask_fee, bid_fee, settings, exchange_info, exchange_orderbooks
            )
            if success:
                any_trade = True
                await update_pair_weight(db, buy_exch, sell_exch)

        return any_trade

    async def _get_taker_fee(self, db: AsyncSession, exchange_name: str, quote_currency: str) -> Optional[float]:
        exch = await db.execute(select(Exchange).where(Exchange.name == exchange_name))
        exch_obj = exch.scalar_one_or_none()
        if not exch_obj:
            return None
        fee_stmt = select(ExchangeFee.taker_fee).where(
            ExchangeFee.exchange_id == exch_obj.id,
            ExchangeFee.quote_currency == quote_currency
        )
        fee = await db.execute(fee_stmt)
        fee_val = fee.scalar_one_or_none()
        return float(fee_val) if fee_val is not None else 0.0

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

    async def _execute_match(
        self,
        db: AsyncSession,
        common_symbol: str,
        quote_currency: str,
        buy_exch: str,
        sell_exch: str,
        volume: float,
        ask_price: float,
        bid_price: float,
        buy_fee: float,
        sell_fee: float,
        settings: SymbolArbitrageSettings,
        exchange_info: Dict,
        exchange_orderbooks: Dict
    ) -> bool:
        """Returns True if execution succeeded."""
        buy_client = get_exchange_client(buy_exch)
        sell_client = get_exchange_client(sell_exch)
        if not buy_client or not sell_client:
            await self.logger.log_rejected_opportunity(
                db, common_symbol, buy_exch, sell_exch,
                f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                "Could not create exchange clients",
                {"buy_client_exists": buy_client is not None, "sell_client_exists": sell_client is not None}
            )
            return False

        exch_buy = (await db.execute(select(Exchange).where(Exchange.name == buy_exch))).scalar_one_or_none()
        exch_sell = (await db.execute(select(Exchange).where(Exchange.name == sell_exch))).scalar_one_or_none()
        if not exch_buy or not exch_sell:
            return False

        is_live = (exchange_info.get(buy_exch, {}).get("mode") == "live" and
                   exchange_info.get(sell_exch, {}).get("mode") == "live")

        if not is_live:
            # Simulator
            effective_buy = ask_price
            # effective_buy = ask_price * (1 + buy_fee)
            effective_sell = bid_price
            # effective_sell = bid_price * (1 - sell_fee)
            cost = volume * effective_buy
            revenue = volume * effective_sell
            from app.apps.arbitrage.inventory import update_base_balance, update_quote_balance
            await update_base_balance(db, buy_exch, common_symbol, volume)
            await update_base_balance(db, sell_exch, common_symbol, -volume)
            await update_quote_balance(db, buy_exch, quote_currency, -cost)
            await update_quote_balance(db, sell_exch, quote_currency, revenue)
            opp = ArbitrageOpportunity(
                common_symbol=common_symbol,
                exchange_a_id=exch_buy.id,
                exchange_b_id=exch_sell.id,
                trade_type=f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                price_a=effective_buy,
                price_b=effective_sell,
                profit_percent=((revenue - cost) / cost) * 100 if cost else 0,
                traded_volume=volume
            )
            db.add(opp)
            await db.commit()
            logger.info(f"✅ Simulator trade: {volume:.4f} {common_symbol} buy@{buy_exch} {effective_buy:.2f} sell@{sell_exch} {effective_sell:.2f}")
            return True
        else:
            # Live mode
            success, filled_vol, vwap_buy, vwap_sell = await self.trade_executor.execute(
                db=db,
                common_symbol=common_symbol,
                buy_exchange=buy_exch,
                sell_exchange=sell_exch,
                volume=volume,
                quote_currency=quote_currency,
                buy_client=buy_client,
                sell_client=sell_client,
                buy_exch_obj=exch_buy,
                sell_exch_obj=exch_sell,
                buy_fee_rate=buy_fee,
                sell_fee_rate=sell_fee,
                vwap_buy=ask_price,
                vwap_sell=bid_price
            )
            if success:
                opp = ArbitrageOpportunity(
                    common_symbol=common_symbol,
                    exchange_a_id=exch_buy.id,
                    exchange_b_id=exch_sell.id,
                    trade_type=f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    price_a=vwap_buy,
                    price_b=vwap_sell,
                    profit_percent=((filled_vol * vwap_sell - filled_vol * vwap_buy) / (filled_vol * vwap_buy)) * 100,
                    traded_volume=filled_vol
                )
                db.add(opp)
                await db.commit()
                logger.info(f"✅ Live trade: {filled_vol:.4f} {common_symbol} buy@{buy_exch} {vwap_buy:.2f} sell@{sell_exch} {vwap_sell:.2f}")
                return True
            else:
                await self.logger.log_rejected_opportunity(
                    db, common_symbol, buy_exch, sell_exch,
                    f"buy_on_{buy_exch}_sell_on_{sell_exch}",
                    "Live trade execution failed",
                    {"volume": volume, "ask_price": ask_price, "bid_price": bid_price}
                )
                return False