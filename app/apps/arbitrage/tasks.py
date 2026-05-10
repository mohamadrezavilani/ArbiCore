import asyncio
import logging
from app.core.database import AsyncSessionLocal
from app.apps.arbitrage.services import ArbitrageService

logger = logging.getLogger(__name__)

async def periodic_arbitrage_poll():
    service = ArbitrageService()
    while True:
        try:
            async with AsyncSessionLocal() as db:
                await service.poll_and_store(db)
        except Exception as e:
            logger.exception(f"Error in arbitrage polling cycle: {e}")
        await asyncio.sleep(service.poll_interval)