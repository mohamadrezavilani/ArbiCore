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

    # ---------- Storage helpers ----------
    async def store_orderbook_snapshot(
        self, db: AsyncSession, exchange_name: str, symbol_original: str, common_symbol: str,
        ask_price: Optional[float], ask_vol: Optional[float],
        bid_price: Optional[float], bid_vol: Optional[float],
        raw_data: Optional[Dict] = None
    ):
        # Fetch exchange and symbol with proper async loading
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

    async def detect_and_save_opportunities(
        self, db: AsyncSession, common_symbol: str,
        wallex_ask_price, wallex_ask_vol, wallex_bid_price, wallex_bid_vol,
        nobitex_ask_price, nobitex_ask_vol, nobitex_bid_price, nobitex_bid_vol
    ):
        # Get exchange IDs (assuming wallex id=1, nobitex id=2 – better to query)
        wallex = await db.execute(select(Exchange).where(Exchange.name == "wallex"))
        wallex_exch = wallex.scalar_one_or_none()
        nobitex = await db.execute(select(Exchange).where(Exchange.name == "nobitex"))
        nobitex_exch = nobitex.scalar_one_or_none()
        if not wallex_exch or not nobitex_exch:
            logger.warning("Exchanges not found for opportunity detection")
            return

        # Buy on Wallex, sell on Nobitex
        if wallex_ask_price and nobitex_bid_price:
            profit = (nobitex_bid_price - wallex_ask_price) / wallex_ask_price * 100
            if profit >= settings.ARBITRAGE_MIN_PROFIT_PERCENT:
                opp = ArbitrageOpportunity(
                    common_symbol=common_symbol,
                    exchange_a_id=wallex_exch.id,
                    exchange_b_id=nobitex_exch.id,
                    trade_type="buy_on_wallex_sell_on_nobitex",
                    price_a=wallex_ask_price,
                    price_b=nobitex_bid_price,
                    profit_percent=profit
                )
                db.add(opp)
                logger.info(f"Opportunity found: buy {common_symbol} on Wallex @{wallex_ask_price}, sell on Nobitex @{nobitex_bid_price}, profit {profit:.2f}%")

        # Buy on Nobitex, sell on Wallex
        if nobitex_ask_price and wallex_bid_price:
            profit = (wallex_bid_price - nobitex_ask_price) / nobitex_ask_price * 100
            if profit >= settings.ARBITRAGE_MIN_PROFIT_PERCENT:
                opp = ArbitrageOpportunity(
                    common_symbol=common_symbol,
                    exchange_a_id=nobitex_exch.id,
                    exchange_b_id=wallex_exch.id,
                    trade_type="buy_on_nobitex_sell_on_wallex",
                    price_a=nobitex_ask_price,
                    price_b=wallex_bid_price,
                    profit_percent=profit
                )
                db.add(opp)
                logger.info(f"Opportunity found: buy {common_symbol} on Nobitex @{nobitex_ask_price}, sell on Wallex @{wallex_bid_price}, profit {profit:.2f}%")

    # ---------- Main polling routine ----------
    async def poll_and_store(self, db: AsyncSession):
        # Get all active exchange symbols with exchange data preloaded
        stmt = (
            select(ExchangeSymbol)
            .where(ExchangeSymbol.is_active == True)
            .join(Exchange)
            .where(Exchange.is_active == True)
            .options(selectinload(ExchangeSymbol.exchange))   # <-- eager load exchange
        )
        result = await db.execute(stmt)
        symbols = result.scalars().all()

        if not symbols:
            logger.warning("No active exchange symbols found. Please seed exchanges and symbols first.")
            return

        # Group by common_symbol
        symbol_group: Dict[str, List[ExchangeSymbol]] = {}
        for sym in symbols:
            symbol_group.setdefault(sym.common_symbol, []).append(sym)

        async with aiohttp.ClientSession() as session:
            for common_symbol, exchange_symbols in symbol_group.items():
                wallex_data = None
                nobitex_data = None

                for ex_sym in exchange_symbols:
                    exchange_name = ex_sym.exchange.name  # now works because eager loaded
                    original_symbol = ex_sym.original_symbol
                    factor = float(ex_sym.price_conversion_factor)

                    if exchange_name == "wallex":
                        ob = await self.fetch_wallex_orderbook(session, original_symbol)
                        if ob:
                            ask_p, ask_v, bid_p, bid_v = self.wallex_best_prices(ob)
                            # Apply conversion factor
                            ask_p = ask_p * factor if ask_p else None
                            bid_p = bid_p * factor if bid_p else None
                            await self.store_orderbook_snapshot(
                                db, "wallex", original_symbol, common_symbol,
                                ask_p, ask_v, bid_p, bid_v, ob
                            )
                            wallex_data = (ask_p, ask_v, bid_p, bid_v)
                        else:
                            logger.debug(f"Failed to fetch wallex {original_symbol}")
                    elif exchange_name == "nobitex":
                        ob = await self.fetch_nobitex_orderbook(session, original_symbol)
                        if ob:
                            ask_p, ask_v, bid_p, bid_v = self.nobitex_best_prices(ob)
                            await self.store_orderbook_snapshot(
                                db, "nobitex", original_symbol, common_symbol,
                                ask_p, ask_v, bid_p, bid_v, ob
                            )
                            nobitex_data = (ask_p, ask_v, bid_p, bid_v)
                        else:
                            logger.debug(f"Failed to fetch nobitex {original_symbol}")

                # If both exchanges have data for this common symbol, detect opportunities
                if wallex_data and nobitex_data:
                    w_ask, w_ask_v, w_bid, w_bid_v = wallex_data
                    n_ask, n_ask_v, n_bid, n_bid_v = nobitex_data
                    await self.detect_and_save_opportunities(
                        db, common_symbol,
                        w_ask, w_ask_v, w_bid, w_bid_v,
                        n_ask, n_ask_v, n_bid, n_bid_v
                    )

        await db.commit()
        logger.info(f"Polling cycle completed – stored snapshots for {len(symbols)} symbols")