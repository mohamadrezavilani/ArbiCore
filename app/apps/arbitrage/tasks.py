import asyncio
import logging
from app.core.database import AsyncSessionLocal
from app.apps.arbitrage.services import ArbitrageService

logger = logging.getLogger(__name__)

async def periodic_arbitrage_poll():
    service = ArbitrageService()
    while True:
        try:
            # Use a longer timeout (120 seconds) while performance improves
            async with asyncio.timeout(120):
                async with AsyncSessionLocal() as db:
                    await service.poll_and_store(db)
        except asyncio.TimeoutError:
            logger.error("Arbitrage poll timed out after 120 seconds – skipping this cycle")
        except Exception as e:
            logger.exception(f"Error in arbitrage polling cycle: {e}")
        await asyncio.sleep(service.poll_interval)