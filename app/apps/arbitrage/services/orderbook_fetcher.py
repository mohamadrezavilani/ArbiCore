import asyncio
import logging
import time
from typing import Dict, List, Tuple, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from app.apps.arbitrage.models import Exchange, ExchangeSymbol, OrderbookSnapshot
from app.exchanges.factory import get_exchange_client

logger = logging.getLogger(__name__)

# Track which exchange/keys have already been warned about missing timestamps
_warned_keys = set()

# Track symbols that consistently fail to avoid retrying them
_failed_symbols_cache: Dict[str, set] = {}  # exchange_name -> set of common_symbols
_failed_cache_ttl = 300  # 5 minutes


class OrderbookFetcher:
    """
    Fetches orderbook snapshots from multiple exchanges with proper rate limiting.

    Rate Limits:
    - Bitpin: 60 requests/minute = 1 req/sec
    - Wallex/Nobitex: 5 requests/second
    """

    # Per-exchange rate limiting state (class-level, shared across instances)
    _rate_limiters: Dict[str, asyncio.Semaphore] = {}
    _last_request_time: Dict[str, float] = {}
    _rate_lock = asyncio.Lock()

    @classmethod
    async def _get_rate_limiter(cls, exchange_name: str) -> asyncio.Semaphore:
        """Get or create a semaphore for the exchange."""
        async with cls._rate_lock:
            if exchange_name not in cls._rate_limiters:
                cls._rate_limiters[exchange_name] = asyncio.Semaphore(1)
                cls._last_request_time[exchange_name] = 0.0
            return cls._rate_limiters[exchange_name]

    async def _rate_limited_fetch(self, exchange_name: str, fetch_coro):
        """
        Execute a fetch coroutine with proper rate limiting.
        Ensures minimum interval between requests to the same exchange.
        """
        semaphore = await self._get_rate_limiter(exchange_name)

        # Minimum interval between requests
        if exchange_name == "bitpin":
            min_interval = 1.05  # 60/min = 1/sec, add buffer
        else:
            min_interval = 0.21   # 5/sec = 0.2s, add buffer

        async with semaphore:
            # Check how long since last request
            async with self._rate_lock:
                last_time = self._last_request_time.get(exchange_name, 0.0)

            now = time.time()
            elapsed = now - last_time

            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                logger.debug(f"[RATE LIMIT] {exchange_name}: sleeping {sleep_time:.3f}s (last was {elapsed:.3f}s ago)")
                await asyncio.sleep(sleep_time)

            try:
                result = await fetch_coro
                async with self._rate_lock:
                    self._last_request_time[exchange_name] = time.time()
                return result
            except Exception as e:
                # Still update last request time on failure to prevent hammering
                async with self._rate_lock:
                    self._last_request_time[exchange_name] = time.time()
                raise

    async def fetch_all(self, db: AsyncSession, timeout_per_exchange: float = 10.0) -> Dict[str, Tuple[Dict[str, Tuple[List[List[float]], List[List[float]]]], float]]:
        """
        Fetch orderbooks for all active cross-exchange symbols.

        OPTIMIZATIONS:
        1. Only fetches symbols present on 2+ exchanges (arbitrage candidates)
        2. Limits concurrent fetches to prevent event loop overload
        3. Proper per-exchange rate limiting
        4. Skips recently-failed symbols to avoid wasted API calls

        Returns: {common_symbol: ({exchange_name: (asks, bids)}, max_timestamp)}
        """
        # Get all active symbols with exchange info
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

        # CRITICAL OPTIMIZATION: Only keep symbols on 2+ exchanges
        arbitrage_symbols = {
            sym: exs for sym, exs in symbol_group.items()
            if len(exs) >= 2
        }

        # Filter out recently-failed symbols
        now = time.time()
        valid_symbols = {}
        skipped_failed = 0
        for sym, exs in arbitrage_symbols.items():
            recently_failed = False
            for ex in exs:
                ex_name = ex.exchange.name
                if ex_name in _failed_symbols_cache:
                    if sym in _failed_symbols_cache[ex_name]:
                        recently_failed = True
                        break
            if recently_failed:
                skipped_failed += 1
            else:
                valid_symbols[sym] = exs

        skipped = len(symbol_group) - len(arbitrage_symbols)
        if skipped > 0:
            logger.info(f"[OPTIMIZATION] Skipped {skipped} single-exchange symbols, processing {len(arbitrage_symbols)} cross-exchange pairs")
        if skipped_failed > 0:
            logger.debug(f"[CACHE] Skipped {skipped_failed} recently-failed symbols")

        if not valid_symbols:
            logger.warning("No valid cross-exchange symbols to fetch.")
            return {}

        # Create exchange clients (cached per exchange)
        clients = {}
        for ex_sym in symbols:
            ex_name = ex_sym.exchange.name
            if ex_name not in clients:
                clients[ex_name] = get_exchange_client(ex_name)
                if clients[ex_name] is None:
                    logger.error(f"Failed to create client for {ex_name}")

        # Fetch symbols with controlled concurrency
        concurrency_limit = asyncio.Semaphore(20)

        async def fetch_limited(common_symbol, ex_symbols):
            async with concurrency_limit:
                return await self._fetch_for_symbol(common_symbol, ex_symbols, clients, db, timeout_per_exchange)

        tasks = [
            fetch_limited(sym, exs)
            for sym, exs in valid_symbols.items()
        ]

        logger.info(f"Starting fetch for {len(tasks)} symbol groups across {len(clients)} exchanges")
        start_time = time.time()

        results = await asyncio.gather(*tasks, return_exceptions=True)

        elapsed = time.time() - start_time

        final_data = {}
        success = 0
        failed = 0
        empty = 0

        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Symbol group exception: {res}")
                failed += 1
                continue
            common_symbol, exchange_data, max_ts = res
            if exchange_data and len(exchange_data) >= 2:
                final_data[common_symbol] = (exchange_data, max_ts)
                success += 1
            elif exchange_data:
                empty += 1  # Only got data from 1 exchange
            else:
                failed += 1

        logger.info(
            f"[FETCH RESULT] {success} successful pairs, {empty} single-exchange, {failed} failed, "
            f"{len(tasks)} total in {elapsed:.1f}s"
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
        """Fetch orderbooks for one symbol from all its exchanges."""
        exchange_data = {}
        max_timestamp = 0.0

        for ex_sym in exchange_symbols:
            ex_name = ex_sym.exchange.name
            client = clients.get(ex_name)
            if not client:
                logger.debug(f"No client for {ex_name}")
                continue

            try:
                res = await self._fetch_one(ex_sym, client, timeout_per_exchange)
                if res is None:
                    continue
                ex_name, ask_levels, bid_levels, snapshot, ts = res
                exchange_data[ex_name] = (ask_levels, bid_levels)
                db.add(snapshot)
                if ts and ts > max_timestamp:
                    max_timestamp = ts
            except Exception as e:
                logger.warning(f"Fetch failed for {common_symbol} on {ex_name}: {e}")
                continue

        return common_symbol, exchange_data, max_timestamp

    async def _fetch_one(self, ex_sym: ExchangeSymbol, client, timeout: float):
        """Fetch a single orderbook with retries and rate limiting."""
        ex_name = ex_sym.exchange.name
        original_symbol = ex_sym.original_symbol
        common_symbol = ex_sym.common_symbol
        factor = float(ex_sym.price_conversion_factor)
        max_retries = 3
        base_delay = 1.0

        for attempt in range(max_retries):
            try:
                # Rate-limited fetch with timeout
                fetch_coro = asyncio.wait_for(
                    client.fetch_orderbook(original_symbol),
                    timeout=timeout
                )
                raw_ob = await self._rate_limited_fetch(ex_name, fetch_coro)

                if not raw_ob:
                    # Check if it's a 404 (permanent failure, don't retry)
                    # The client returns None on 404, so we can't distinguish here
                    # But we can check after retries exhausted
                    if attempt < max_retries - 1:
                        wait = base_delay * (2 ** attempt)
                        logger.warning(
                            f"Empty orderbook {ex_name}/{original_symbol}, "
                            f"retry in {wait:.1f}s ({attempt+1}/{max_retries})"
                        )
                        await asyncio.sleep(wait)
                        continue
                    logger.error(f"Empty orderbook {ex_name}/{original_symbol} after {max_retries} retries")
                    # Cache this failure
                    if ex_name not in _failed_symbols_cache:
                        _failed_symbols_cache[ex_name] = set()
                    _failed_symbols_cache[ex_name].add(common_symbol)
                    return None

                ts = self._extract_timestamp(raw_ob, ex_name)
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
                return (ex_name, ask_levels, bid_levels, snapshot, ts)

            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait = base_delay * (2 ** attempt)
                    logger.warning(
                        f"Timeout {ex_name}/{original_symbol}, retry in {wait:.1f}s ({attempt+1}/{max_retries})"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"Timeout {ex_name}/{original_symbol} after {max_retries} retries")
                    return None

            except Exception as e:
                error_str = str(e).lower()
                # Check for permanent errors (404, 403, etc.)
                is_permanent = any(code in error_str for code in ['404', '403', 'not found', 'forbidden'])

                if is_permanent:
                    logger.warning(f"Permanent error {ex_name}/{original_symbol}: {e} - caching failure")
                    if ex_name not in _failed_symbols_cache:
                        _failed_symbols_cache[ex_name] = set()
                    _failed_symbols_cache[ex_name].add(common_symbol)
                    return None

                if attempt < max_retries - 1:
                    wait = base_delay * (2 ** attempt)
                    logger.warning(
                        f"Error {ex_name}/{original_symbol}: {e}, retry in {wait:.1f}s ({attempt+1}/{max_retries})"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"Error {ex_name}/{original_symbol}: {e} after {max_retries} retries")
                    return None

        return None

    def _extract_timestamp(self, raw_ob: Dict[str, Any], exchange_name: str) -> float:
        """Extract timestamp from exchange response, fallback to current time."""
        possible_keys = ['timestamp', 'time', 'server_time', 'update_time', 'created_at', 'date', 'ts']
        for key in possible_keys:
            val = raw_ob.get(key)
            if val is not None:
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
                    except (ValueError, AttributeError):
                        pass

        # Log missing timestamp once per exchange response shape
        key_id = (exchange_name, tuple(sorted(raw_ob.keys())))
        if key_id not in _warned_keys:
            logger.warning(
                f"[TIME] No timestamp in {exchange_name} response. "
                f"Keys: {list(raw_ob.keys())[:10]}... Using system time."
            )
            _warned_keys.add(key_id)

        return time.time()