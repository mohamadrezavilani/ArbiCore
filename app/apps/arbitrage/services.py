import logging
import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import Dict, Any, Optional, Tuple, List
from app.core.config import settings
from app.apps.arbitrage.models import Exchange, ExchangeSymbol, OrderbookSnapshot, ArbitrageOpportunity

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

    def wallex_best_prices(self, orderbook: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        asks = orderbook.get("ask", [])
        best_ask = asks[0] if asks else None
        ask_price = best_ask["price"] if best_ask else None
        ask_vol = best_ask["quantity"] if best_ask else None

        bids = orderbook.get("bid", [])
        best_bid = bids[0] if bids else None
        bid_price = best_bid["price"] if best_bid else None
        bid_vol = best_bid["quantity"] if best_bid else None
        return ask_price, ask_vol, bid_price, bid_vol

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

    def nobitex_best_prices(self, orderbook: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        asks = orderbook.get("asks", [])
        best_ask = asks[0] if asks else None
        ask_price = float(best_ask[0]) if best_ask else None
        ask_vol = float(best_ask[1]) if best_ask else None

        bids = orderbook.get("bids", [])
        best_bid = bids[0] if bids else None
        bid_price = float(best_bid[0]) if best_bid else None
        bid_vol = float(best_bid[1]) if best_bid else None
        return ask_price, ask_vol, bid_price, bid_vol

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

    def bitpin_best_prices(self, orderbook: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        asks = orderbook.get("asks", [])
        best_ask = asks[0] if asks else None
        ask_price = float(best_ask[0]) if best_ask else None
        ask_vol = float(best_ask[1]) if best_ask else None

        bids = orderbook.get("bids", [])
        best_bid = bids[0] if bids else None
        bid_price = float(best_bid[0]) if best_bid else None
        bid_vol = float(best_bid[1]) if best_bid else None
        return ask_price, ask_vol, bid_price, bid_vol

    # ---------- Storage helpers ----------
    async def store_orderbook_snapshot(
        self, db: AsyncSession, exchange_name: str, symbol_original: str, common_symbol: str,
        ask_price: Optional[float], ask_vol: Optional[float],
        bid_price: Optional[float], bid_vol: Optional[float],
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
            raw_data=raw_data
        )
        db.add(snapshot)

    # ---------- Generic fee‑aware arbitrage detection ----------
    async def detect_arbitrage_between(
        self,
        db: AsyncSession,
        common_symbol: str,
        exchange_a_name: str,
        a_prices: Tuple[Optional[float], Optional[float], Optional[float], Optional[float]],
        a_taker_fee: float,
        exchange_b_name: str,
        b_prices: Tuple[Optional[float], Optional[float], Optional[float], Optional[float]],
        b_taker_fee: float,
    ) -> None:
        """
        Detect arbitrage opportunities between two exchanges, accounting for taker fees.
        a_prices = (ask_price, ask_volume, bid_price, bid_volume)
        """
        a_ask, a_ask_v, a_bid, a_bid_v = a_prices
        b_ask, b_ask_v, b_bid, b_bid_v = b_prices

        # Fetch exchange IDs from DB
        exch_a = await db.execute(select(Exchange).where(Exchange.name == exchange_a_name))
        exch_a_obj = exch_a.scalar_one_or_none()
        exch_b = await db.execute(select(Exchange).where(Exchange.name == exchange_b_name))
        exch_b_obj = exch_b.scalar_one_or_none()
        if not exch_a_obj or not exch_b_obj:
            logger.warning(f"Exchange {exchange_a_name} or {exchange_b_name} not found")
            return

        # Opportunity 1: buy on A (ask), sell on B (bid)
        if a_ask and b_bid:
            buy_cost = a_ask * (1 + a_taker_fee)
            sell_revenue = b_bid * (1 - b_taker_fee)
            if sell_revenue > buy_cost:
                profit_percent = (sell_revenue - buy_cost) / buy_cost * 100
                if profit_percent >= settings.ARBITRAGE_MIN_PROFIT_PERCENT:
                    opp = ArbitrageOpportunity(
                        common_symbol=common_symbol,
                        exchange_a_id=exch_a_obj.id,
                        exchange_b_id=exch_b_obj.id,
                        trade_type=f"buy_on_{exchange_a_name}_sell_on_{exchange_b_name}",
                        price_a=a_ask,
                        price_b=b_bid,
                        profit_percent=profit_percent,
                    )
                    db.add(opp)
                    logger.info(
                        f"✅ Opportunity: buy {common_symbol} on {exchange_a_name} @{a_ask} (fee {a_taker_fee:.4f}), "
                        f"sell on {exchange_b_name} @{b_bid} (fee {b_taker_fee:.4f}), net profit {profit_percent:.2f}%"
                    )

        # Opportunity 2: buy on B (ask), sell on A (bid)
        if b_ask and a_bid:
            buy_cost = b_ask * (1 + b_taker_fee)
            sell_revenue = a_bid * (1 - a_taker_fee)
            if sell_revenue > buy_cost:
                profit_percent = (sell_revenue - buy_cost) / buy_cost * 100
                if profit_percent >= settings.ARBITRAGE_MIN_PROFIT_PERCENT:
                    opp = ArbitrageOpportunity(
                        common_symbol=common_symbol,
                        exchange_a_id=exch_b_obj.id,
                        exchange_b_id=exch_a_obj.id,
                        trade_type=f"buy_on_{exchange_b_name}_sell_on_{exchange_a_name}",
                        price_a=b_ask,
                        price_b=a_bid,
                        profit_percent=profit_percent,
                    )
                    db.add(opp)
                    logger.info(
                        f"✅ Opportunity: buy {common_symbol} on {exchange_b_name} @{b_ask} (fee {b_taker_fee:.4f}), "
                        f"sell on {exchange_a_name} @{a_bid} (fee {a_taker_fee:.4f}), net profit {profit_percent:.2f}%"
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
                # Store prices for each exchange
                exchange_data = {}

                for ex_sym in exchange_symbols:
                    exchange_name = ex_sym.exchange.name
                    original_symbol = ex_sym.original_symbol
                    factor = float(ex_sym.price_conversion_factor)

                    if exchange_name == "wallex":
                        ob = await self.fetch_wallex_orderbook(session, original_symbol)
                        if ob:
                            ask_p, ask_v, bid_p, bid_v = self.wallex_best_prices(ob)
                            ask_p = ask_p * factor if ask_p else None
                            bid_p = bid_p * factor if bid_p else None
                            await self.store_orderbook_snapshot(
                                db, "wallex", original_symbol, common_symbol,
                                ask_p, ask_v, bid_p, bid_v, ob
                            )
                            exchange_data["wallex"] = (ask_p, ask_v, bid_p, bid_v)
                        else:
                            logger.debug(f"Failed to fetch wallex {original_symbol}")

                    elif exchange_name == "nobitex":
                        ob = await self.fetch_nobitex_orderbook(session, original_symbol)
                        if ob:
                            ask_p, ask_v, bid_p, bid_v = self.nobitex_best_prices(ob)
                            ask_p = ask_p * factor if ask_p else None
                            bid_p = bid_p * factor if bid_p else None
                            await self.store_orderbook_snapshot(
                                db, "nobitex", original_symbol, common_symbol,
                                ask_p, ask_v, bid_p, bid_v, ob
                            )
                            exchange_data["nobitex"] = (ask_p, ask_v, bid_p, bid_v)
                        else:
                            logger.debug(f"Failed to fetch nobitex {original_symbol}")

                    elif exchange_name == "bitpin":
                        ob = await self.fetch_bitpin_orderbook(session, original_symbol)
                        if ob:
                            ask_p, ask_v, bid_p, bid_v = self.bitpin_best_prices(ob)
                            ask_p = ask_p * factor if ask_p else None
                            bid_p = bid_p * factor if bid_p else None
                            await self.store_orderbook_snapshot(
                                db, "bitpin", original_symbol, common_symbol,
                                ask_p, ask_v, bid_p, bid_v, ob
                            )
                            exchange_data["bitpin"] = (ask_p, ask_v, bid_p, bid_v)
                        else:
                            logger.debug(f"Failed to fetch bitpin {original_symbol}")

                # Fetch taker fees for exchanges that have data
                fees = {}
                for name in exchange_data.keys():
                    exch = await db.execute(select(Exchange).where(Exchange.name == name))
                    exch_obj = exch.scalar_one_or_none()
                    if exch_obj:
                        fees[name] = float(exch_obj.taker_fee)

                # Compare every pair of exchanges that we have data for
                exchange_names = list(exchange_data.keys())
                for i in range(len(exchange_names)):
                    for j in range(i + 1, len(exchange_names)):
                        name_a = exchange_names[i]
                        name_b = exchange_names[j]
                        await self.detect_arbitrage_between(
                            db, common_symbol,
                            name_a, exchange_data[name_a], fees.get(name_a, 0),
                            name_b, exchange_data[name_b], fees.get(name_b, 0)
                        )

        await db.commit()
        logger.info(f"✅ Polling cycle completed – processed {len(symbols)} symbols")