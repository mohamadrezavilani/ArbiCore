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
            async with asyncio.timeout(120):
                async with AsyncSessionLocal() as db:
                    await service.poll_and_store(db)

            cycle_count += 1
            now = time.time()
            if service.last_fetch_timestamp > 0:
                next_fetch = service.last_fetch_timestamp + UPDATE_INTERVAL_SECONDS
                if next_fetch > now:
                    sleep_seconds = next_fetch - now
                    # Log schedule only every 10 cycles or if deviation is > 0.5s
                    if cycle_count % 10 == 0 or abs(sleep_seconds - UPDATE_INTERVAL_SECONDS) > 0.5:
                        pass
                        # logger.info(f"[SCHEDULE] Next fetch at {next_fetch:.2f} (sleep {sleep_seconds:.2f}s)")
                    await asyncio.sleep(sleep_seconds)
                    continue
                else:
                    logger.warning(f"[SCHEDULE] Missed exact boundary (now {now:.2f} > {next_fetch:.2f}). Using fixed interval.")
                    await asyncio.sleep(service.poll_interval)
            else:
                logger.warning("[SCHEDULE] No timestamp available – using fixed interval.")
                await asyncio.sleep(service.poll_interval)
        except asyncio.TimeoutError:
            logger.error("Arbitrage poll timed out after 120 seconds – skipping this cycle")
            await asyncio.sleep(service.poll_interval)
        except Exception as e:
            logger.exception(f"Error in arbitrage polling cycle: {e}")
            await asyncio.sleep(service.poll_interval)