import asyncio
import logging
from typing import Dict, List, Tuple, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.apps.arbitrage.models import Exchange, ExchangeSymbol, OrderbookSnapshot
from app.exchanges.factory import get_exchange_client

logger = logging.getLogger(__name__)

class OrderbookFetcher:
    async def fetch_all(self, db: AsyncSession, timeout_per_exchange: float = 10.0) -> Dict[str, Dict[str, Tuple[List[List[float]], List[List[float]]]]]:
        """
        Fetch orderbooks for all active symbols on all active exchanges in parallel.
        Returns: {common_symbol: {exchange_name: (ask_levels, bid_levels)}}
        """
        # Get all active exchange symbols
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
            logger.warning("No active exchange symbols found.")
            return {}

        # Group by common_symbol
        symbol_group: Dict[str, List[ExchangeSymbol]] = {}
        for sym in symbols:
            symbol_group.setdefault(sym.common_symbol, []).append(sym)

        # Cache clients per exchange
        clients = {}
        for ex_sym in symbols:
            ex_name = ex_sym.exchange.name
            if ex_name not in clients:
                clients[ex_name] = get_exchange_client(ex_name)

        # Fetch all symbols in parallel
        tasks = []
        for common_symbol, ex_symbols in symbol_group.items():
            tasks.append(self._fetch_for_symbol(common_symbol, ex_symbols, clients, db, timeout_per_exchange))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        final_data = {}
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Error fetching symbol group: {res}")
                continue
            common_symbol, exchange_data = res
            if exchange_data:
                final_data[common_symbol] = exchange_data

        return final_data

    async def _fetch_for_symbol(
        self,
        common_symbol: str,
        exchange_symbols: List[ExchangeSymbol],
        clients: Dict[str, Any],
        db: AsyncSession,
        timeout_per_exchange: float
    ) -> Tuple[str, Dict[str, Tuple[List[List[float]], List[List[float]]]]]:
        """Fetch orderbooks for a single common_symbol from all its exchanges in parallel."""
        exchange_data = {}
        fetch_tasks = []
        for ex_sym in exchange_symbols:
            ex_name = ex_sym.exchange.name
            client = clients.get(ex_name)
            if not client:
                continue
            fetch_tasks.append(self._fetch_one(ex_sym, client, timeout_per_exchange))

        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                logger.warning(f"Fetch error for {common_symbol}: {res}")
                continue
            if res is None:
                continue
            ex_name, ask_levels, bid_levels, snapshot = res
            exchange_data[ex_name] = (ask_levels, bid_levels)
            db.add(snapshot)   # will be committed later in the main transaction
        return common_symbol, exchange_data

    async def _fetch_one(self, ex_sym: ExchangeSymbol, client, timeout: float):
        """Fetch a single orderbook, apply conversion, create snapshot object."""
        ex_name = ex_sym.exchange.name
        original_symbol = ex_sym.original_symbol
        factor = float(ex_sym.price_conversion_factor)
        try:
            raw_ob = await asyncio.wait_for(client.fetch_orderbook(original_symbol), timeout=timeout)
            if not raw_ob:
                return None
            ask_levels, bid_levels = client.extract_levels(raw_ob)
            # Apply price conversion factor
            ask_levels = [[p * factor, v] for p, v in ask_levels] if ask_levels else []
            bid_levels = [[p * factor, v] for p, v in bid_levels] if bid_levels else []
            best_ask = ask_levels[0] if ask_levels else [None, None]
            best_bid = bid_levels[0] if bid_levels else [None, None]

            snapshot = OrderbookSnapshot(
                exchange_id=ex_sym.exchange_id,
                symbol_id=ex_sym.id,
                best_ask_price=best_ask[0],
                best_ask_volume=best_ask[1],
                best_bid_price=best_bid[0],
                best_bid_volume=best_bid[1],
                asks=ask_levels,
                bids=bid_levels,
                raw_data=raw_ob
            )
            return (ex_name, ask_levels, bid_levels, snapshot)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching orderbook for {ex_name} {original_symbol}")
        except Exception as e:
            logger.error(f"Error fetching {ex_name} {original_symbol}: {e}")
        return None