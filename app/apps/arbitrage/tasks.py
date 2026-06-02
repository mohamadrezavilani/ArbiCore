import asyncio
import logging
from app.core.database import AsyncSessionLocal
from app.apps.arbitrage.services import ArbitrageService

logger = logging.getLogger(__name__)

async def periodic_arbitrage_poll():
    service = ArbitrageService()
    while True:
        try:
            async with asyncio.timeout(30):   # Python 3.11+
                async with AsyncSessionLocal() as db:
                    await service.poll_and_store(db)
        except asyncio.TimeoutError:
            logging.error("Arbitrage poll timed out after 30 seconds – skipping this cycle")
        except Exception as e:
            logging.exception(f"Error in arbitrage polling cycle: {e}")
        await asyncio.sleep(service.poll_interval)