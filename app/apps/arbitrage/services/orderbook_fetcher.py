# app/apps/arbitrage/services/orderbook_fetcher.py

import asyncio
import logging
import time
from typing import Dict, List, Tuple, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.apps.arbitrage.models import Exchange, ExchangeSymbol, OrderbookSnapshot
from app.exchanges.factory import get_exchange_client
from app.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# Track which exchange/keys have already been warned about missing timestamps
_warned_keys = set()


class OrderbookFetcher:
    # Per-exchange rate limiters
    _rate_limiters: Dict[str, RateLimiter] = {}
    _lock = asyncio.Lock()

    # ---- ROTATION STATE (class-level, persists across instances) ----
    _all_symbol_groups: List[str] = []  # List of common_symbol strings
    _rotation_index: int = 0
    _cache_timestamp: float = 0.0
    CACHE_TTL_SECONDS: float = 300.0  # Refresh symbol list every 5 minutes

    @classmethod
    async def _get_limiter(cls, exchange_name: str) -> RateLimiter:
        async with cls._lock:
            if exchange_name not in cls._rate_limiters:
                if exchange_name == "bitpin":
                    min_interval = 1.0  # 1 second → 60/min
                else:
                    min_interval = 0.2  # 5 requests per second
                cls._rate_limiters[exchange_name] = RateLimiter(min_interval)
            return cls._rate_limiters[exchange_name]

    async def fetch_all(
            self,
            db: AsyncSession,
            timeout_per_exchange: float = 10.0,
            symbols_per_cycle: Optional[int] = None
    ) -> Dict[str, Tuple[Dict[str, Tuple[List[List[float]], List[List[float]]]], float]]:
        """
        Fetch orderbooks for all active symbols on all active exchanges.

        Args:
            db: Database session
            timeout_per_exchange: Timeout per exchange API call
            symbols_per_cycle: If set > 0, rotates through symbols in batches.
                               If None or 0, fetches ALL symbols (original behavior).

        Returns a dict mapping common_symbol -> (exchange_orderbooks, max_timestamp).
        """
        # ---- Step 1: Get all active symbols (with caching) ----
        now = time.time()
        cache_stale = (now - self._cache_timestamp) > self.CACHE_TTL_SECONDS

        if cache_stale or not self._all_symbol_groups:
            stmt = (
                select(ExchangeSymbol)
                .where(ExchangeSymbol.is_active == True)
                .join(Exchange)
                .where(Exchange.is_active == True)
                .options(selectinload(ExchangeSymbol.exchange))
            )
            result = await db.execute(stmt)
            all_symbols = result.scalars().all()

            # Group by common_symbol
            symbol_groups: Dict[str, List[ExchangeSymbol]] = {}
            for sym in all_symbols:
                symbol_groups.setdefault(sym.common_symbol, []).append(sym)

            self._all_symbol_groups = sorted(symbol_groups.keys())
            self._cache_timestamp = now

            logger.info(
                f"Refreshed symbol cache: {len(self._all_symbol_groups)} unique symbols, "
                f"{len(all_symbols)} total exchange-symbol pairs"
            )
        else:
            # Rebuild symbol_groups from cached list
            stmt = (
                select(ExchangeSymbol)
                .where(ExchangeSymbol.is_active == True)
                .join(Exchange)
                .where(Exchange.is_active == True)
                .options(selectinload(ExchangeSymbol.exchange))
            )
            result = await db.execute(stmt)
            all_symbols = result.scalars().all()

            symbol_groups: Dict[str, List[ExchangeSymbol]] = {}
            for sym in all_symbols:
                symbol_groups.setdefault(sym.common_symbol, []).append(sym)

        if not self._all_symbol_groups:
            logger.warning("No active exchange symbols found.")
            return {}

        total_groups = len(self._all_symbol_groups)

        # ---- Step 2: Determine which symbols to fetch this cycle ----
        if symbols_per_cycle and symbols_per_cycle > 0 and total_groups > symbols_per_cycle:
            # ROTATION MODE
            start = self._rotation_index
            end = min(start + symbols_per_cycle, total_groups)
            selected_common = self._all_symbol_groups[start:end]

            # Wrap around if we hit the end
            wrap_count = 0
            if end >= total_groups and symbols_per_cycle > (end - start):
                remaining = symbols_per_cycle - (end - start)
                if remaining > 0:
                    selected_common.extend(self._all_symbol_groups[:remaining])
                    self._rotation_index = remaining
                    wrap_count = remaining
                else:
                    self._rotation_index = 0
            else:
                self._rotation_index = end

            # Flatten to ExchangeSymbol list
            cycle_symbols = []
            for cs in selected_common:
                cycle_symbols.extend(symbol_groups.get(cs, []))

            logger.info(
                f"[ROTATION] Fetching {len(selected_common)} symbol groups "
                f"({start}-{end - 1}{f' + 0-{wrap_count - 1}' if wrap_count else ''}/"
                f"{total_groups}), next start={self._rotation_index}, "
                f"{len(cycle_symbols)} exchange-symbols total"
            )
        else:
            # FETCH ALL MODE (original behavior)
            cycle_symbols = []
            for symbols in symbol_groups.values():
                cycle_symbols.extend(symbols)
            selected_common = self._all_symbol_groups
            logger.info(
                f"Fetching all {total_groups} symbol groups across "
                f"{len(set(s.exchange.name for s in cycle_symbols))} exchanges"
            )

        if not cycle_symbols:
            logger.warning("No symbols selected for this cycle.")
            return {}

        # ---- Step 3: Create clients for each exchange ----
        clients = {}
        for ex_sym in cycle_symbols:
            ex_name = ex_sym.exchange.name
            if ex_name not in clients:
                clients[ex_name] = get_exchange_client(ex_name)

        # ---- Step 4: Rebuild symbol_groups for selected symbols only ----
        selected_groups: Dict[str, List[ExchangeSymbol]] = {}
        for sym in cycle_symbols:
            selected_groups.setdefault(sym.common_symbol, []).append(sym)

        # ---- Step 5: Fetch each symbol group in parallel ----
        tasks = []
        for common_symbol, ex_symbols in selected_groups.items():
            tasks.append(
                self._fetch_for_symbol(
                    common_symbol, ex_symbols, clients, db, timeout_per_exchange
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        final_data = {}
        success_count = 0
        fail_count = 0

        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Error fetching symbol group: {res}")
                fail_count += 1
                continue
            if res is None:
                fail_count += 1
                continue

            common_symbol, exchange_data, max_ts = res
            if exchange_data:
                final_data[common_symbol] = (exchange_data, max_ts)
                success_count += 1
            else:
                fail_count += 1

        logger.info(
            f"[FETCH RESULT] {success_count} successful pairs, "
            f"{fail_count} failed, {len(selected_groups)} total in {time.time() - now:.1f}s"
        )
        return final_data

    async def _fetch_for_symbol(
            self,
            common_symbol: str,
            exchange_symbols: List[ExchangeSymbol],
            clients: Dict[str, Any],
            db: AsyncSession,
            timeout_per_exchange: float
    ) -> Tuple[str, Dict[str, Tuple[List[List[float]], List[List[float]]]], float]:
        """
        Fetch orderbooks for a single common symbol from all exchanges that have it.
        """
        exchange_data = {}
        fetch_tasks = []
        max_timestamp = 0.0

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
            ex_name, ask_levels, bid_levels, snapshot, ts = res
            exchange_data[ex_name] = (ask_levels, bid_levels)
            db.add(snapshot)
            if ts and ts > max_timestamp:
                max_timestamp = ts

        if max_timestamp > 0:
            logger.debug(f"[TIME] Symbol {common_symbol}: max timestamp = {max_timestamp:.2f}")

        return common_symbol, exchange_data, max_timestamp

    async def _fetch_one(self, ex_sym: ExchangeSymbol, client, timeout: float):
        """
        Fetch a single orderbook with retries.
        Applies rate limiting before each attempt (including retries).
        """
        ex_name = ex_sym.exchange.name
        original_symbol = ex_sym.original_symbol
        factor = float(ex_sym.price_conversion_factor)
        max_retries = 3
        base_delay = 1.0

        # Acquire the rate limiter for this exchange
        limiter = await self._get_limiter(ex_name)

        for attempt in range(max_retries):
            # Enforce rate limit before each attempt
            await limiter.acquire()

            try:
                raw_ob = await asyncio.wait_for(
                    client.fetch_orderbook(original_symbol),
                    timeout=timeout
                )
                if not raw_ob:
                    # No data – retry after delay
                    if attempt < max_retries - 1:
                        wait = base_delay * (2 ** attempt)
                        logger.warning(
                            f"Empty orderbook for {ex_name} {original_symbol}, "
                            f"retrying in {wait:.1f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait)
                        continue
                    else:
                        logger.error(
                            f"Empty orderbook for {ex_name} {original_symbol} "
                            f"after {max_retries} attempts"
                        )
                        return None

                ts = self._extract_timestamp(raw_ob, ex_name)
                ask_levels, bid_levels = client.extract_levels(raw_ob)
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
                return (ex_name, ask_levels, bid_levels, snapshot, ts)

            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait = base_delay * (2 ** attempt)
                    logger.warning(
                        f"Timeout fetching {ex_name} {original_symbol}, "
                        f"retrying in {wait:.1f}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(
                        f"Timeout fetching {ex_name} {original_symbol} "
                        f"after {max_retries} attempts"
                    )
                    return None

            except Exception as e:
                if attempt < max_retries - 1:
                    wait = base_delay * (2 ** attempt)
                    logger.warning(
                        f"Error fetching {ex_name} {original_symbol}: {e}, "
                        f"retrying in {wait:.1f}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(
                        f"Error fetching {ex_name} {original_symbol}: {e} "
                        f"after {max_retries} attempts"
                    )
                    return None

        return None

    def _extract_timestamp(self, raw_ob: Dict[str, Any], exchange_name: str) -> float:
        """
        Attempt to extract a timestamp (seconds since epoch) from various common keys.
        If none found, logs the available keys once and returns current system time.
        """
        possible_keys = ['timestamp', 'time', 'server_time', 'update_time', 'created_at', 'date']
        for key in possible_keys:
            val = raw_ob.get(key)
            if val:
                if isinstance(val, (int, float)):
                    if val > 1e12:  # milliseconds
                        return val / 1000.0
                    return float(val)
                elif isinstance(val, str):
                    try:
                        if val.isdigit():
                            num = float(val)
                            if num > 1e12:
                                return num / 1000.0
                            return num
                    except:
                        pass

        # No key found – log once per exchange and key set
        key_id = (exchange_name, tuple(sorted(raw_ob.keys())))
        if key_id not in _warned_keys:
            logger.warning(
                f"[TIME] No timestamp key found in {exchange_name} response. "
                f"Keys: {list(raw_ob.keys())}"
            )
            _warned_keys.add(key_id)

        return time.time()