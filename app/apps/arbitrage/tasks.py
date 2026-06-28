import asyncio
import logging
import time
from app.core.database import AsyncSessionLocal
from app.apps.arbitrage.services import ArbitrageService

logger = logging.getLogger(__name__)

UPDATE_INTERVAL_SECONDS = 10


async def periodic_arbitrage_poll():
    service = ArbitrageService()

    # Initial fetch to get a baseline timestamp
    async with AsyncSessionLocal() as db:
        try:
            exchange_data = await service.fetcher.fetch_all(db)
            if exchange_data:
                service.last_fetch_timestamp = max(
                    (ts for (_, (_, ts)) in exchange_data.items()), default=0.0
                )
                logger.info(f"Initial fetch: global max timestamp = {service.last_fetch_timestamp:.2f}")
            else:
                logger.warning("Initial fetch returned no data.")
        except Exception as e:
            logger.exception(f"Initial fetch failed: {e}")
            service.last_fetch_timestamp = 0.0

    cycle_count = 0
    while True:
        try:
            # Run one poll cycle with timeout
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

            # Schedule next fetch based on timestamp if available
            if service.last_fetch_timestamp > 0:
                next_fetch = service.last_fetch_timestamp + UPDATE_INTERVAL_SECONDS
                if next_fetch > now:
                    sleep_seconds = next_fetch - now
                    # Clamp to avoid excessive sleep (e.g., if timestamp is in future)
                    if sleep_seconds > 60:
                        logger.warning(
                            f"Computed sleep {sleep_seconds:.1f}s is too large, using fixed interval {UPDATE_INTERVAL_SECONDS}s")
                        sleep_seconds = UPDATE_INTERVAL_SECONDS
                    await asyncio.sleep(sleep_seconds)
                    continue
                else:
                    # Only log if the deviation is significant (> 2 seconds)
                    if now - next_fetch > 2:
                        logger.warning(
                            f"[SCHEDULE] Missed exact boundary by {now - next_fetch:.1f}s. Using fixed interval.")
                    await asyncio.sleep(UPDATE_INTERVAL_SECONDS)
            else:
                # No timestamp – log this once per startup or if it repeats often?
                # We'll log only if we haven't logged in the last 5 minutes to avoid spam.
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