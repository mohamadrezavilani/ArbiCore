import logging
import aiohttp
import asyncio
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import Dict, Any, Optional, Tuple, List
from app.core.config import settings
from app.apps.arbitrage.models import Exchange, ExchangeSymbol, OrderbookSnapshot, ArbitrageOpportunity, ExchangeFee, \
    SymbolArbitrageSettings, Network
from app.apps.arbitrage.inventory import (
    get_base_balance, update_base_balance, set_base_balance,
    get_quote_balance, update_quote_balance, set_quote_balance
)
from app.exchanges.factory import get_exchange_client

logger = logging.getLogger(__name__)

class ArbitrageService:
    def __init__(self):
        self.poll_interval = settings.ARBITRAGE_CHECK_INTERVAL_SECONDS

    # ---------- Helper: sync real balances after live trade ----------
    async def sync_balances_from_exchange(self, db: AsyncSession, exchange_name: str, client):
        """After a live trade, replace database balances with real exchange balances."""
        real_balances = await client.get_balances()
        # Fetch mapping from original_symbol to common_symbol for this exchange
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

    # ---------- Exchange-specific fetch & extraction ----------
    # (all fetch and extract methods remain unchanged – omitted for brevity, but include them as in your previous version)
    async def fetch_wallex_orderbook(self, session: aiohttp.ClientSession, symbol: str) -> Optional[Dict[str, Any]]:
        url = "https://api.wallex.ir/v1/depth"
        params = {"symbol": symbol}
        try:
            async with session.get(url, params=params, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if data.get("success"):
                    return data["result"]
                else:
                    logger.warning(f"[Wallex] API error for {symbol}: {data.get('message', 'Unknown')}")
                    return None
        except Exception as e:
            logger.error(f"[Wallex] Error fetching {symbol}: {e}")
            return None

    def wallex_extract_levels(self, orderbook: Dict[str, Any]) -> Tuple[Optional[List[List[float]]], Optional[List[List[float]]]]:
        asks = orderbook.get("ask", [])
        bids = orderbook.get("bid", [])
        ask_levels = [[float(a["price"]), float(a["quantity"])] for a in asks] if asks else []
        bid_levels = [[float(b["price"]), float(b["quantity"])] for b in bids] if bids else []
        return ask_levels, bid_levels

    async def fetch_nobitex_orderbook(self, session: aiohttp.ClientSession, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"https://apiv2.nobitex.ir/v3/orderbook/{symbol}"
        try:
            async with session.get(url, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if data.get("status") == "ok":
                    return data
                else:
                    logger.warning(f"[Nobitex] API error for {symbol}: {data.get('status', 'Unknown')}")
                    return None
        except Exception as e:
            logger.error(f"[Nobitex] Error fetching {symbol}: {e}")
            return None

    def nobitex_extract_levels(self, orderbook: Dict[str, Any]) -> Tuple[Optional[List[List[float]]], Optional[List[List[float]]]]:
        asks = orderbook.get("asks", [])
        bids = orderbook.get("bids", [])
        ask_levels = [[float(price), float(vol)] for price, vol in asks] if asks else []
        bid_levels = [[float(price), float(vol)] for price, vol in bids] if bids else []
        return ask_levels, bid_levels

    async def fetch_bitpin_orderbook(self, session: aiohttp.ClientSession, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"https://api.bitpin.org/api/v1/mth/orderbook/{symbol}/"
        try:
            async with session.get(url, timeout=10) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logger.error(f"[Bitpin] Error fetching {symbol}: {e}")
            return None

    def bitpin_extract_levels(self, orderbook: Dict[str, Any]) -> Tuple[Optional[List[List[float]]], Optional[List[List[float]]]]:
        asks = orderbook.get("asks", [])
        bids = orderbook.get("bids", [])
        ask_levels = [[float(price), float(vol)] for price, vol in asks] if asks else []
        bid_levels = [[float(price), float(vol)] for price, vol in bids] if bids else []
        return ask_levels, bid_levels

    # ---------- Storage helper ----------
    async def store_orderbook_snapshot(
        self, db: AsyncSession, exchange_name: str, symbol_original: str, common_symbol: str,
        ask_price: Optional[float], ask_vol: Optional[float],
        bid_price: Optional[float], bid_vol: Optional[float],
        ask_levels: List[List[float]], bid_levels: List[List[float]],
        raw_data: Optional[Dict] = None
    ):
        exch_stmt = select(Exchange).where(Exchange.name == exchange_name)
        exch_result = await db.execute(exch_stmt)
        exchange = exch_result.scalar_one_or_none()
        if not exchange:
            logger.warning(f"Exchange '{exchange_name}' not found in DB")
            return
        sym_stmt = select(ExchangeSymbol).where(
            ExchangeSymbol.exchange_id == exchange.id,
            ExchangeSymbol.original_symbol == symbol_original
        )
        sym_result = await db.execute(sym_stmt)
        symbol = sym_result.scalar_one_or_none()
        if not symbol:
            logger.warning(f"Symbol '{symbol_original}' for exchange '{exchange_name}' not found")
            return
        snapshot = OrderbookSnapshot(
            exchange_id=exchange.id,
            symbol_id=symbol.id,
            best_ask_price=ask_price,
            best_ask_volume=ask_vol,
            best_bid_price=bid_price,
            best_bid_volume=bid_vol,
            asks=ask_levels,
            bids=bid_levels,
            raw_data=raw_data
        )
        db.add(snapshot)

    # ---------- Risk formula ----------
    def calculate_trade_percent(self, net_gain: float, network_commission_quote: float,
                                params: SymbolArbitrageSettings) -> float:
        cutoff = float(params.cutoff_threshold)
        min_trade_pct = float(params.min_trade_percent)
        min_trade_factor = float(params.min_trade_factor)
        valuability_factor = float(params.valuability_factor)

        min_threshold = min_trade_factor * network_commission_quote
        full_threshold = valuability_factor * network_commission_quote

        if net_gain <= 0:
            return 0.0
        if net_gain < cutoff:
            return 0.0
        if net_gain <= min_threshold:
            return min_trade_pct
        if net_gain >= full_threshold:
            return 1.0

        if full_threshold > min_threshold:
            slope = (1.0 - min_trade_pct) / (full_threshold - min_threshold)
            return min_trade_pct + slope * (net_gain - min_threshold)
        return min_trade_pct

    # ---------- Core arbitrage detection (supports both simulator & live) ----------
    async def detect_arbitrage_between(
        self,
        db: AsyncSession,
        common_symbol: str,
        exchange_a_name: str,
        a_ask_levels: List[List[float]],
        a_bid_levels: List[List[float]],
        exchange_b_name: str,
        b_ask_levels: List[List[float]],
        b_bid_levels: List[List[float]],
    ) -> None:
        # Determine quote currency
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

        exch_a = await db.execute(select(Exchange).where(Exchange.name == exchange_a_name))
        exch_a_obj = exch_a.scalar_one_or_none()
        exch_b = await db.execute(select(Exchange).where(Exchange.name == exchange_b_name))
        exch_b_obj = exch_b.scalar_one_or_none()
        if not exch_a_obj or not exch_b_obj:
            return

        settings_stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == common_symbol)
        settings = (await db.execute(settings_stmt)).scalar_one_or_none()
        if not settings or not settings.is_active:
            return

        network_fee_base = 0.0
        if settings.default_network_id:
            net_stmt = select(Network).where(Network.id == settings.default_network_id)
            net = (await db.execute(net_stmt)).scalar_one_or_none()
            if net:
                network_fee_base = float(net.fee_per_transfer)

        a_fee = await get_taker_fee(exchange_a_name)
        b_fee = await get_taker_fee(exchange_b_name)

        async def compute_max_trade(buy_exch, sell_exch, buy_levels, sell_levels, buy_fee, sell_fee):
            vol = cost = rev = 0.0
            avail_base = await get_base_balance(db, sell_exch, common_symbol)
            if avail_base <= 0:
                return 0,0,0,0
            avail_quote = await get_quote_balance(db, buy_exch, quote_currency)
            if avail_quote <= 0:
                return 0,0,0,0

            i_buy = i_sell = 0
            buy_cpy = [l[:] for l in buy_levels]
            sell_cpy = [l[:] for l in sell_levels]

            while i_buy < len(buy_cpy) and i_sell < len(sell_cpy):
                bprice, bvol = buy_cpy[i_buy]
                sprice, svol = sell_cpy[i_sell]
                cost_unit = bprice * (1 + buy_fee)
                rev_unit = sprice * (1 - sell_fee)
                if rev_unit <= cost_unit:
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
                    break
            return vol, cost, rev, rev - cost

        async def process_opp(buy_exch, sell_exch, buy_levels, sell_levels, buy_fee, sell_fee,
                              buy_exch_obj, sell_exch_obj, prefix):
            vol, cost, rev, gross_gain = await compute_max_trade(buy_exch, sell_exch, buy_levels, sell_levels, buy_fee, sell_fee)
            if vol <= 0:
                return
            vwap_buy = cost / vol
            vwap_sell = rev / vol
            network_fee_quote = network_fee_base * vwap_buy
            trade_pct = self.calculate_trade_percent(gross_gain, network_fee_quote, settings)
            if trade_pct <= 0:
                logger.info(f"Cannot create opportunity for {common_symbol} cause {trade_pct} <= 0")
                return

            actual_vol = vol * trade_pct
            actual_cost = cost * trade_pct
            actual_rev = rev * trade_pct
            gross_profit_pct = ((actual_rev - actual_cost) / actual_cost) * 100 if actual_cost else 0
            if gross_profit_pct < float(settings.min_profit_percent):
                logger.info(f"Cannot create opportunity for {common_symbol} cause {gross_profit_pct} < {settings.min_profit_percent}")
                return

            # Check trading mode from Exchange table
            buy_mode = (await db.execute(select(Exchange.mode).where(Exchange.name == buy_exch))).scalar_one_or_none()
            sell_mode = (await db.execute(select(Exchange.mode).where(Exchange.name == sell_exch))).scalar_one_or_none()
            is_live = (buy_mode == "live" and sell_mode == "live")

            if is_live:
                # LIVE TRADING – atomic concurrent orders
                buy_client = get_exchange_client(buy_exch)
                sell_client = get_exchange_client(sell_exch)
                if not buy_client or not sell_client:
                    logger.error(f"Cannot create clients for {buy_exch} / {sell_exch}")
                    return

                short_uuid = uuid.uuid4().hex[:8]
                buy_order_id = f"buy_{short_uuid}"
                sell_order_id = f"sell_{short_uuid}"

                # Place both orders concurrently
                buy_task = asyncio.create_task(buy_client.place_market_order(
                    symbol=common_symbol, side="buy", amount=actual_vol, client_order_id=buy_order_id
                ))
                sell_task = asyncio.create_task(sell_client.place_market_order(
                    symbol=common_symbol, side="sell", amount=actual_vol, client_order_id=sell_order_id
                ))
                buy_result, sell_result = await asyncio.gather(buy_task, sell_task)

                buy_ok = buy_result.status == "filled"
                sell_ok = sell_result.status == "filled"

                if not buy_ok or not sell_ok:
                    # Cancel any order that is not filled
                    if not buy_ok and buy_result.order_id:
                        await buy_client.cancel_order(buy_result.client_order_id)
                    if not sell_ok and sell_result.order_id:
                        await sell_client.cancel_order(sell_result.client_order_id)
                    logger.error(f"Atomic trade failed: buy={buy_result.status}, sell={sell_result.status}")
                    return

                # Both succeeded – use actual fill prices
                vwap_buy = buy_result.filled_price
                vwap_sell = sell_result.filled_price
                actual_vol = min(buy_result.filled_volume, sell_result.filled_volume)
                actual_cost = actual_vol * vwap_buy
                actual_rev = actual_vol * vwap_sell
                gross_profit_pct = ((actual_rev - actual_cost) / actual_cost) * 100 if actual_cost else 0

                # Sync real balances
                await self.sync_balances_from_exchange(db, buy_exch, buy_client)
                await self.sync_balances_from_exchange(db, sell_exch, sell_client)
            else:
                # SIMULATOR MODE
                await update_base_balance(db, buy_exch, common_symbol, actual_vol)
                await update_base_balance(db, sell_exch, common_symbol, -actual_vol)
                await update_quote_balance(db, buy_exch, quote_currency, -actual_cost)
                await update_quote_balance(db, sell_exch, quote_currency, actual_rev)

            # Record opportunity (same for both modes)
            opp = ArbitrageOpportunity(
                common_symbol=common_symbol,
                exchange_a_id=buy_exch_obj.id,
                exchange_b_id=sell_exch_obj.id,
                trade_type=f"{prefix}_{buy_exch}_sell_on_{sell_exch}",
                price_a=vwap_buy,
                price_b=vwap_sell,
                profit_percent=gross_profit_pct,
                traded_volume=actual_vol,
            )
            db.add(opp)
            logger.info(
                f"✅ Executed {actual_vol:.4f} {common_symbol} (risk {trade_pct:.1%} of max {vol:.4f}) "
                f"buy {buy_exch} @{vwap_buy:.2f} sell {sell_exch} @{vwap_sell:.2f} "
                f"profit {gross_profit_pct:.2f}% (network fee {network_fee_quote:.2f} used for risk)"
            )

        # Check both directions
        if a_ask_levels and b_bid_levels:
            await process_opp(exchange_a_name, exchange_b_name, a_ask_levels, b_bid_levels, a_fee, b_fee,
                              exch_a_obj, exch_b_obj, "buy_on")
        if b_ask_levels and a_bid_levels:
            await process_opp(exchange_b_name, exchange_a_name, b_ask_levels, a_bid_levels, b_fee, a_fee,
                              exch_b_obj, exch_a_obj, "buy_on")

    # ---------- Main polling routine ----------
    async def poll_and_store(self, db: AsyncSession):
        stmt = (
            select(ExchangeSymbol)
            .where(ExchangeSymbol.is_active == True)
            .join(Exchange)
            .where(Exchange.is_active == True)
            .options(selectinload(ExchangeSymbol.exchange))
        )
        result = await db.execute(stmt)
        symbols = result.scalars().all()
        if not symbols:
            logger.warning("No active exchange symbols found. Please seed exchanges and symbols first.")
            return

        symbol_group: Dict[str, List[ExchangeSymbol]] = {}
        for sym in symbols:
            symbol_group.setdefault(sym.common_symbol, []).append(sym)

        async with aiohttp.ClientSession() as session:
            for common_symbol, exchange_symbols in symbol_group.items():
                exchange_data = {}
                for ex_sym in exchange_symbols:
                    exchange_name = ex_sym.exchange.name
                    original_symbol = ex_sym.original_symbol
                    factor = float(ex_sym.price_conversion_factor)

                    if exchange_name == "wallex":
                        ob = await self.fetch_wallex_orderbook(session, original_symbol)
                        if ob:
                            ask_levels, bid_levels = self.wallex_extract_levels(ob)
                            ask_levels = [[p * factor, v] for p, v in ask_levels] if ask_levels else []
                            bid_levels = [[p * factor, v] for p, v in bid_levels] if bid_levels else []
                            best_ask = ask_levels[0] if ask_levels else [None, None]
                            best_bid = bid_levels[0] if bid_levels else [None, None]
                            await self.store_orderbook_snapshot(
                                db, "wallex", original_symbol, common_symbol,
                                best_ask[0], best_ask[1], best_bid[0], best_bid[1],
                                ask_levels, bid_levels, ob
                            )
                            exchange_data["wallex"] = (ask_levels, bid_levels)
                    elif exchange_name == "nobitex":
                        ob = await self.fetch_nobitex_orderbook(session, original_symbol)
                        if ob:
                            ask_levels, bid_levels = self.nobitex_extract_levels(ob)
                            ask_levels = [[p * factor, v] for p, v in ask_levels] if ask_levels else []
                            bid_levels = [[p * factor, v] for p, v in bid_levels] if bid_levels else []
                            best_ask = ask_levels[0] if ask_levels else [None, None]
                            best_bid = bid_levels[0] if bid_levels else [None, None]
                            await self.store_orderbook_snapshot(
                                db, "nobitex", original_symbol, common_symbol,
                                best_ask[0], best_ask[1], best_bid[0], best_bid[1],
                                ask_levels, bid_levels, ob
                            )
                            exchange_data["nobitex"] = (ask_levels, bid_levels)
                    elif exchange_name == "bitpin":
                        ob = await self.fetch_bitpin_orderbook(session, original_symbol)
                        if ob:
                            ask_levels, bid_levels = self.bitpin_extract_levels(ob)
                            ask_levels = [[p * factor, v] for p, v in ask_levels] if ask_levels else []
                            bid_levels = [[p * factor, v] for p, v in bid_levels] if bid_levels else []
                            best_ask = ask_levels[0] if ask_levels else [None, None]
                            best_bid = bid_levels[0] if bid_levels else [None, None]
                            await self.store_orderbook_snapshot(
                                db, "bitpin", original_symbol, common_symbol,
                                best_ask[0], best_ask[1], best_bid[0], best_bid[1],
                                ask_levels, bid_levels, ob
                            )
                            exchange_data["bitpin"] = (ask_levels, bid_levels)

                exchange_names = list(exchange_data.keys())
                for i in range(len(exchange_names)):
                    for j in range(i + 1, len(exchange_names)):
                        name_a = exchange_names[i]
                        name_b = exchange_names[j]
                        a_ask_levels, a_bid_levels = exchange_data[name_a]
                        b_ask_levels, b_bid_levels = exchange_data[name_b]
                        await self.detect_arbitrage_between(
                            db, common_symbol,
                            name_a, a_ask_levels, a_bid_levels,
                            name_b, b_ask_levels, b_bid_levels
                        )

        await db.commit()