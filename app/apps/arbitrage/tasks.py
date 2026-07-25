# app/apps/arbitrage/tasks.py

import asyncio
import logging
import time

from app.apps.arbitrage.services.orderbook_fetcher import OrderbookFetcher
from app.core.database import AsyncSessionLocal
from app.apps.arbitrage.services import ArbitrageService

logger = logging.getLogger(__name__)

# ---- TUNABLE PARAMETERS ----
UPDATE_INTERVAL_SECONDS = 10
SYMBOLS_PER_CYCLE = 5  # Fetch 5 symbol groups per cycle


# With ~255 symbols: full rotation every ~255/5 * 10s = 510s (~8.5 min)


async def periodic_arbitrage_poll():
    service = ArbitrageService()

    # Initial fetch — small batch to get started quickly
    async with AsyncSessionLocal() as db:
        try:
            exchange_data = await service.fetcher.fetch_all(
                db,
                symbols_per_cycle=SYMBOLS_PER_CYCLE
            )
            if exchange_data:
                service.last_fetch_timestamp = max(
                    (ts for (_, (_, ts)) in exchange_data.items()),
                    default=0.0
                )
                logger.info(
                    f"Initial fetch: {len(exchange_data)} symbols, "
                    f"max ts={service.last_fetch_timestamp:.2f}"
                )
            else:
                logger.warning("Initial fetch returned no data.")
        except Exception as e:
            logger.exception(f"Initial fetch failed: {e}")
            service.last_fetch_timestamp = 0.0

    cycle_count = 0
    while True:
        try:
            try:
                async with asyncio.timeout(120):
                    async with AsyncSessionLocal() as db:
                        await service.poll_and_store(db)
            except asyncio.TimeoutError:
                logger.error("Arbitrage poll timed out after 120 seconds – skipping this cycle")
                await asyncio.sleep(UPDATE_INTERVAL_SECONDS)
                continue
            except Exception as e:
                logger.exception(f"Error in poll_and_store: {e}")
                await asyncio.sleep(UPDATE_INTERVAL_SECONDS)
                continue

            cycle_count += 1
            now = time.time()

            if service.last_fetch_timestamp > 0:
                next_fetch = service.last_fetch_timestamp + UPDATE_INTERVAL_SECONDS
                if next_fetch > now:
                    sleep_seconds = next_fetch - now
                    if sleep_seconds > 60:
                        logger.warning(
                            f"Computed sleep {sleep_seconds:.1f}s is too large, "
                            f"using fixed interval {UPDATE_INTERVAL_SECONDS}s"
                        )
                        sleep_seconds = UPDATE_INTERVAL_SECONDS
                    await asyncio.sleep(sleep_seconds)
                    continue
                else:
                    if now - next_fetch > 2:
                        logger.warning(
                            f"[SCHEDULE] Missed exact boundary by {now - next_fetch:.1f}s. "
                            f"Using fixed interval."
                        )
                    await asyncio.sleep(UPDATE_INTERVAL_SECONDS)
            else:
                if not hasattr(service, '_no_ts_warned') or time.time() - service._no_ts_warned > 300:
                    logger.warning("[SCHEDULE] No timestamp available – using fixed interval.")
                    service._no_ts_warned = time.time()
                await asyncio.sleep(UPDATE_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            logger.info("Poll task cancelled, exiting gracefully.")
            break
        except Exception as e:
            logger.exception(f"Unhandled error in periodic_arbitrage_poll: {e}")
            await asyncio.sleep(UPDATE_INTERVAL_SECONDS)


async def collect_all_snapshots():
    """Periodically fetch orderbooks for all symbols with rotation."""
    from app.core.database import AsyncSessionLocal
    import traceback

    fetcher = OrderbookFetcher()

    while True:
        cycle_start = time.time()
        try:
            logger.info("[COLLECTOR] Starting fetch cycle")
            async with AsyncSessionLocal() as db:
                # USE ROTATION: fetch batch per cycle
                await fetcher.fetch_all(
                    db,
                    symbols_per_cycle=SYMBOLS_PER_CYCLE
                )
                await db.commit()
                logger.info("[COLLECTOR] ✅ Commit successful")
        except Exception as e:
            logger.exception(f"[COLLECTOR] Error in fetch cycle: {e}\n{traceback.format_exc()}")

        elapsed = time.time() - cycle_start
        sleep_time = max(0, UPDATE_INTERVAL_SECONDS - elapsed)
        logger.info(
            f"[COLLECTOR] Cycle finished in {elapsed:.1f}s, "
            f"sleeping {sleep_time:.1f}s..."
        )
        await asyncio.sleep(sleep_time)