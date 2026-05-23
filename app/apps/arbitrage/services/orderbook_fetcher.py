import logging
from typing import Dict, List, Tuple, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.apps.arbitrage.models import Exchange, ExchangeSymbol, OrderbookSnapshot
from app.exchanges.factory import get_exchange_client

logger = logging.getLogger(__name__)

class OrderbookFetcher:
    async def fetch_all(self, db: AsyncSession) -> Dict[str, Dict[str, Tuple[List[List[float]], List[List[float]]]]]:
        """
        Fetch orderbooks for all active symbols on all active exchanges.
        Returns a nested dict:
            {
                common_symbol: {
                    exchange_name: (ask_levels, bid_levels)
                }
            }
        Also stores snapshots in the database.
        """
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

        result_data = {}
        # Reuse clients per exchange
        clients = {}
        for common_symbol, exchange_symbols in symbol_group.items():
            exchange_data = {}
            for ex_sym in exchange_symbols:
                exchange_name = ex_sym.exchange.name
                original_symbol = ex_sym.original_symbol
                factor = float(ex_sym.price_conversion_factor)

                if exchange_name not in clients:
                    client = get_exchange_client(exchange_name)
                    if not client:
                        logger.warning(f"No client found for exchange {exchange_name}")
                        continue
                    clients[exchange_name] = client

                client = clients[exchange_name]
                raw_ob = await client.fetch_orderbook(original_symbol)
                if raw_ob:
                    ask_levels, bid_levels = client.extract_levels(raw_ob)
                    # Apply price conversion factor
                    ask_levels = [[p * factor, v] for p, v in ask_levels] if ask_levels else []
                    bid_levels = [[p * factor, v] for p, v in bid_levels] if bid_levels else []
                    best_ask = ask_levels[0] if ask_levels else [None, None]
                    best_bid = bid_levels[0] if bid_levels else [None, None]
                    # Store snapshot
                    await self._store_snapshot(
                        db, exchange_name, original_symbol, common_symbol,
                        best_ask[0], best_ask[1], best_bid[0], best_bid[1],
                        ask_levels, bid_levels, raw_ob
                    )
                    exchange_data[exchange_name] = (ask_levels, bid_levels)
            if exchange_data:
                result_data[common_symbol] = exchange_data
        return result_data

    async def _store_snapshot(
        self,
        db: AsyncSession,
        exchange_name: str,
        symbol_original: str,
        common_symbol: str,
        ask_price: Optional[float],
        ask_vol: Optional[float],
        bid_price: Optional[float],
        bid_vol: Optional[float],
        ask_levels: List[List[float]],
        bid_levels: List[List[float]],
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