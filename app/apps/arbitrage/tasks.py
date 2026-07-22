import asyncio
import logging
import time
import traceback

from app.apps.arbitrage.services.orderbook_fetcher import OrderbookFetcher
from app.core.database import AsyncSessionLocal
from app.apps.arbitrage.services import ArbitrageService

logger = logging.getLogger(__name__)

UPDATE_INTERVAL_SECONDS = 10


async def periodic_arbitrage_poll():
    """
    Main arbitrage polling loop.
    Fetches orderbooks, detects opportunities, executes trades, and rebalances.
    """
    service = ArbitrageService()

    # Initial fetch to get baseline timestamp
    async with AsyncSessionLocal() as db:
        try:
            exchange_data = await service.fetcher.fetch_all(db)
            if exchange_data:
                service.last_fetch_timestamp = max(
                    (ts for (_, (_, ts)) in exchange_data.items()), default=0.0
                )
                logger.info(f"Initial fetch: {len(exchange_data)} symbols, max ts={service.last_fetch_timestamp:.2f}")
            else:
                logger.warning("Initial fetch returned no data.")
        except Exception as e:
            logger.exception(f"Initial fetch failed: {e}")
            service.last_fetch_timestamp = 0.0

    cycle_count = 0
    while True:
        cycle_start = time.time()
        try:
            # Run one poll cycle with timeout
            try:
                async with asyncio.timeout(300):  # 5 minute timeout for full cycle
                    async with AsyncSessionLocal() as db:
                        await service.poll_and_store(db)
            except asyncio.TimeoutError:
                logger.error("Arbitrage poll timed out after 300s – skipping cycle")
                await asyncio.sleep(UPDATE_INTERVAL_SECONDS)
                continue
            except Exception as e:
                logger.exception(f"Error in poll_and_store: {e}")
                await asyncio.sleep(UPDATE_INTERVAL_SECONDS)
                continue

            cycle_count += 1
            now = time.time()
            cycle_duration = now - cycle_start
            logger.info(f"[CYCLE] Completed cycle {cycle_count} in {cycle_duration:.1f}s")

            # Schedule next fetch based on timestamp if available
            if service.last_fetch_timestamp > 0:
                next_fetch = service.last_fetch_timestamp + UPDATE_INTERVAL_SECONDS
                if next_fetch > now:
                    sleep_seconds = next_fetch - now
                    # Clamp to avoid excessive sleep
                    if sleep_seconds > 60:
                        logger.warning(
                            f"Computed sleep {sleep_seconds:.1f}s too large, using {UPDATE_INTERVAL_SECONDS}s"
                        )
                        sleep_seconds = UPDATE_INTERVAL_SECONDS
                    logger.debug(f"Sleeping {sleep_seconds:.1f}s until next cycle")
                    await asyncio.sleep(sleep_seconds)
                    continue
                else:
                    if now - next_fetch > 2:
                        logger.warning(
                            f"[SCHEDULE] Missed boundary by {now - next_fetch:.1f}s"
                        )
                    await asyncio.sleep(UPDATE_INTERVAL_SECONDS)
            else:
                if not hasattr(service, '_no_ts_warned') or time.time() - service._no_ts_warned > 300:
                    logger.warning("[SCHEDULE] No timestamp available – using fixed interval")
                    service._no_ts_warned = time.time()
                await asyncio.sleep(UPDATE_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            logger.info("Poll task cancelled, exiting gracefully.")
            break
        except Exception as e:
            logger.exception(f"Unhandled error in periodic_arbitrage_poll: {e}")
            await asyncio.sleep(UPDATE_INTERVAL_SECONDS)


async def collect_all_snapshots():
    """
    Dedicated snapshot collector.
    Ensures orderbook snapshots are committed even if arbitrage detection fails.
    """
    cycle = 0
    while True:
        cycle += 1
        cycle_start = time.time()

        try:
            logger.info(f"[COLLECTOR #{cycle}] Starting fetch cycle")

            async with AsyncSessionLocal() as db:
                fetcher = OrderbookFetcher()

                try:
                    exchange_data = await fetcher.fetch_all(db)

                    if exchange_data:
                        logger.info(
                            f"[COLLECTOR #{cycle}] Fetched {len(exchange_data)} symbols, committing..."
                        )
                    else:
                        logger.warning(f"[COLLECTOR #{cycle}] No data fetched, nothing to commit")

                except Exception as fetch_err:
                    logger.exception(f"[COLLECTOR #{cycle}] Fetch failed: {fetch_err}")
                    exchange_data = None

                # ALWAYS try to commit, even if fetch returned empty
                # (snapshots may have been added to session before failure)
                try:
                    await db.commit()

                    if exchange_data:
                        logger.info(
                            f"[COLLECTOR #{cycle}] ✅ Commit successful: {len(exchange_data)} symbols saved"
                        )
                    else:
                        logger.info(f"[COLLECTOR #{cycle}] Commit completed (no new data)")

                except Exception as commit_err:
                    logger.exception(f"[COLLECTOR #{cycle}] ❌ Commit failed: {commit_err}")
                    try:
                        await db.rollback()
                    except Exception as rb_err:
                        logger.error(f"[COLLECTOR #{cycle}] Rollback failed: {rb_err}")

        except Exception as e:
            logger.exception(f"[COLLECTOR #{cycle}] Fatal error: {e}\n{traceback.format_exc()}")

        cycle_duration = time.time() - cycle_start
        logger.info(f"[COLLECTOR #{cycle}] Cycle finished in {cycle_duration:.1f}s, sleeping {UPDATE_INTERVAL_SECONDS}s...")

        # Sleep remaining time (don't sleep negative if cycle took longer than interval)
        sleep_time = max(0, UPDATE_INTERVAL_SECONDS - (time.time() - cycle_start))
        await asyncio.sleep(sleep_time)