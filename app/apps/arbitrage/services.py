import logging
import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import Dict, Any, Optional, Tuple, List
from app.core.config import settings
from app.apps.arbitrage.models import Exchange, ExchangeSymbol, OrderbookSnapshot, ArbitrageOpportunity, ExchangeFee, SymbolArbitrageSettings
from app.apps.arbitrage.inventory import get_base_balance, update_base_balance, get_quote_balance, update_quote_balance

logger = logging.getLogger(__name__)

class ArbitrageService:
    def __init__(self):
        self.poll_interval = settings.ARBITRAGE_CHECK_INTERVAL_SECONDS

    # ---------- Wallex ----------
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
        """Return (asks, bids) as lists of [price, volume]."""
        asks = orderbook.get("ask", [])
        bids = orderbook.get("bid", [])
        ask_levels = [[float(a["price"]), float(a["quantity"])] for a in asks] if asks else []
        bid_levels = [[float(b["price"]), float(b["quantity"])] for b in bids] if bids else []
        return ask_levels, bid_levels

    # ---------- Nobitex ----------
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

    # ---------- Bitpin ----------
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

    # ---------- Storage helpers ----------
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

    # ---------- Multi‑level arbitrage detection ----------
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
        """
        Multi‑level arbitrage detection.
        For opportunity 1: buy on A (use A's asks) and sell on B (use B's bids).
        For opportunity 2: buy on B (use B's asks) and sell on A (use A's bids).
        """
        # Determine quote currency from symbol suffix
        if common_symbol.endswith("IRT"):
            quote_currency = "IRT"
        elif common_symbol.endswith("USDT"):
            quote_currency = "USDT"
        else:
            logger.warning(f"Unknown quote currency for symbol {common_symbol}")
            return

        # Helper to fetch taker fee for an exchange given quote_currency
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

        # Get exchange IDs
        exch_a = await db.execute(select(Exchange).where(Exchange.name == exchange_a_name))
        exch_a_obj = exch_a.scalar_one_or_none()
        exch_b = await db.execute(select(Exchange).where(Exchange.name == exchange_b_name))
        exch_b_obj = exch_b.scalar_one_or_none()
        if not exch_a_obj or not exch_b_obj:
            return

        a_taker_fee = await get_taker_fee(exchange_a_name)
        b_taker_fee = await get_taker_fee(exchange_b_name)

        # ---------- Opportunity 1: buy on A (using A's asks), sell on B (using B's bids) ----------
        if a_ask_levels and b_bid_levels:
            total_volume = 0.0
            total_cost = 0.0
            total_revenue = 0.0

            # Get available base on sell side (B)
            available_base = await get_base_balance(db, exchange_b_name, common_symbol)
            if available_base <= 0:
                logger.info(f"⏭️ Skip {common_symbol}: insufficient base on {exchange_b_name} ({available_base})")
                return

            # Get available quote on buy side (A)
            available_quote = await get_quote_balance(db, exchange_a_name, quote_currency)

            # Iterate through ask levels (sorted ascending) and bid levels (sorted descending)
            i_ask = 0
            i_bid = 0
            while i_ask < len(a_ask_levels) and i_bid < len(b_bid_levels):
                ask_price, ask_vol = a_ask_levels[i_ask]
                bid_price, bid_vol = b_bid_levels[i_bid]

                buy_cost_per_unit = ask_price * (1 + a_taker_fee)
                sell_revenue_per_unit = bid_price * (1 - b_taker_fee)

                # If this level is not profitable, stop (further levels are worse)
                if sell_revenue_per_unit <= buy_cost_per_unit:
                    break

                # How much can we take from this level?
                max_volume_this_level = min(
                    ask_vol, bid_vol,
                    available_base - total_volume,          # remaining base to sell
                    available_quote / buy_cost_per_unit     # remaining quote to buy
                )
                if max_volume_this_level <= 0:
                    # Move to next level on whichever side is exhausted
                    if ask_vol <= 0:
                        i_ask += 1
                    if bid_vol <= 0:
                        i_bid += 1
                    continue

                # Execute this partial level
                total_volume += max_volume_this_level
                total_cost += max_volume_this_level * buy_cost_per_unit
                total_revenue += max_volume_this_level * sell_revenue_per_unit

                # Reduce the level volumes
                a_ask_levels[i_ask][1] -= max_volume_this_level
                b_bid_levels[i_bid][1] -= max_volume_this_level

                # If we reached inventory limits, stop
                if total_volume >= available_base or total_cost >= available_quote:
                    break

            if total_volume > 0:
                profit_percent = (total_revenue - total_cost) / total_cost * 100
                min_profit = settings.ARBITRAGE_MIN_PROFIT_PERCENT
                stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == common_symbol)
                result = await db.execute(stmt)
                sym_settings = result.scalar_one_or_none()
                if sym_settings and sym_settings.is_active:
                    min_profit = sym_settings.min_profit_percent
                if profit_percent >= min_profit:
                    # VWAP prices
                    vwap_buy = total_cost / total_volume if total_volume else 0
                    vwap_sell = total_revenue / total_volume if total_volume else 0
                    opp = ArbitrageOpportunity(
                        common_symbol=common_symbol,
                        exchange_a_id=exch_a_obj.id,
                        exchange_b_id=exch_b_obj.id,
                        trade_type=f"buy_on_{exchange_a_name}_sell_on_{exchange_b_name}",
                        price_a=vwap_buy,
                        price_b=vwap_sell,
                        profit_percent=profit_percent,
                        traded_volume=total_volume,
                    )
                    db.add(opp)

                    # Update inventories
                    await update_base_balance(db, exchange_a_name, common_symbol, total_volume)
                    await update_base_balance(db, exchange_b_name, common_symbol, -total_volume)
                    await update_quote_balance(db, exchange_a_name, quote_currency, -total_cost)
                    await update_quote_balance(db, exchange_b_name, quote_currency, total_revenue)

                    logger.info(
                        f"✅ Executed {total_volume:.4f} {common_symbol} (buy on {exchange_a_name} VWAP {vwap_buy:.2f}, "
                        f"sell on {exchange_b_name} VWAP {vwap_sell:.2f}) profit {profit_percent:.2f}%"
                    )

        # ---------- Opportunity 2: buy on B (using B's asks), sell on A (using A's bids) ----------
        if b_ask_levels and a_bid_levels:
            total_volume = 0.0
            total_cost = 0.0
            total_revenue = 0.0

            available_base = await get_base_balance(db, exchange_a_name, common_symbol)
            if available_base <= 0:
                return

            available_quote = await get_quote_balance(db, exchange_b_name, quote_currency)

            i_ask = 0
            i_bid = 0
            while i_ask < len(b_ask_levels) and i_bid < len(a_bid_levels):
                ask_price, ask_vol = b_ask_levels[i_ask]
                bid_price, bid_vol = a_bid_levels[i_bid]

                buy_cost_per_unit = ask_price * (1 + b_taker_fee)
                sell_revenue_per_unit = bid_price * (1 - a_taker_fee)

                if sell_revenue_per_unit <= buy_cost_per_unit:
                    break

                max_volume_this_level = min(
                    ask_vol, bid_vol,
                    available_base - total_volume,
                    available_quote / buy_cost_per_unit
                )
                if max_volume_this_level <= 0:
                    if ask_vol <= 0:
                        i_ask += 1
                    if bid_vol <= 0:
                        i_bid += 1
                    continue

                total_volume += max_volume_this_level
                total_cost += max_volume_this_level * buy_cost_per_unit
                total_revenue += max_volume_this_level * sell_revenue_per_unit

                b_ask_levels[i_ask][1] -= max_volume_this_level
                a_bid_levels[i_bid][1] -= max_volume_this_level

                if total_volume >= available_base or total_cost >= available_quote:
                    break

            if total_volume > 0:
                profit_percent = (total_revenue - total_cost) / total_cost * 100
                min_profit = settings.ARBITRAGE_MIN_PROFIT_PERCENT
                stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == common_symbol)
                result = await db.execute(stmt)
                sym_settings = result.scalar_one_or_none()
                if sym_settings and sym_settings.is_active:
                    min_profit = sym_settings.min_profit_percent
                if profit_percent >= min_profit:
                    vwap_buy = total_cost / total_volume if total_volume else 0
                    vwap_sell = total_revenue / total_volume if total_volume else 0
                    opp = ArbitrageOpportunity(
                        common_symbol=common_symbol,
                        exchange_a_id=exch_b_obj.id,
                        exchange_b_id=exch_a_obj.id,
                        trade_type=f"buy_on_{exchange_b_name}_sell_on_{exchange_a_name}",
                        price_a=vwap_buy,
                        price_b=vwap_sell,
                        profit_percent=profit_percent,
                        traded_volume=total_volume,
                    )
                    db.add(opp)

                    await update_base_balance(db, exchange_b_name, common_symbol, total_volume)
                    await update_base_balance(db, exchange_a_name, common_symbol, -total_volume)
                    await update_quote_balance(db, exchange_b_name, quote_currency, -total_cost)
                    await update_quote_balance(db, exchange_a_name, quote_currency, total_revenue)

                    logger.info(
                        f"✅ Executed {total_volume:.4f} {common_symbol} (buy on {exchange_b_name} VWAP {vwap_buy:.2f}, "
                        f"sell on {exchange_a_name} VWAP {vwap_sell:.2f}) profit {profit_percent:.2f}%"
                    )

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

        # Group symbols by their common name
        symbol_group: Dict[str, List[ExchangeSymbol]] = {}
        for sym in symbols:
            symbol_group.setdefault(sym.common_symbol, []).append(sym)

        async with aiohttp.ClientSession() as session:
            for common_symbol, exchange_symbols in symbol_group.items():
                exchange_data = {}  # key = exchange_name, value = (ask_levels, bid_levels, best_ask_p, best_ask_v, best_bid_p, best_bid_v)

                for ex_sym in exchange_symbols:
                    exchange_name = ex_sym.exchange.name
                    original_symbol = ex_sym.original_symbol
                    factor = float(ex_sym.price_conversion_factor)

                    if exchange_name == "wallex":
                        ob = await self.fetch_wallex_orderbook(session, original_symbol)
                        if ob:
                            ask_levels, bid_levels = self.wallex_extract_levels(ob)
                            # Apply conversion factor to prices
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
                        else:
                            logger.debug(f"Failed to fetch wallex {original_symbol}")

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
                        else:
                            logger.debug(f"Failed to fetch nobitex {original_symbol}")

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
                        else:
                            logger.debug(f"Failed to fetch bitpin {original_symbol}")

                # Compare every pair of exchanges that have data
                exchange_names = list(exchange_data.keys())
                for i in range(len(exchange_names)):
                    for j in range(i + 1, len(exchange_names)):
                        name_a = exchange_names[i]
                        name_b = exchange_names[j]
                        a_ask_levels, a_bid_levels = exchange_data[name_a]
                        b_ask_levels, b_bid_levels = exchange_data[name_b]
                        # For each pair, we check both directions (the function will evaluate both opportunities)
                        await self.detect_arbitrage_between(
                            db, common_symbol,
                            name_a, a_ask_levels, a_bid_levels,
                            name_b, b_ask_levels, b_bid_levels
                        )

        await db.commit()
        # logger.info("✅ Polling cycle completed")