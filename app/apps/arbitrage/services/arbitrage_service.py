import logging
from sqlalchemy.ext.asyncio import AsyncSession
from .orderbook_fetcher import OrderbookFetcher
from .arbitrage_detector import ArbitrageDetector
from .opportunity_logger import OpportunityLogger
from .risk_manager import RiskManager
from .trade_executor import TradeExecutor
from .rebalancer import Rebalancer
from app.core.config import settings

logger = logging.getLogger(__name__)

class ArbitrageService:
    def __init__(self):
        self.poll_interval = settings.ARBITRAGE_CHECK_INTERVAL_SECONDS
        self.logger = OpportunityLogger()
        self.risk_manager = RiskManager()
        self.trade_executor = TradeExecutor(self.logger)
        self.rebalancer = Rebalancer(self.logger)
        self.fetcher = OrderbookFetcher()
        self.detector = ArbitrageDetector(
            logger=self.logger,
            risk_manager=self.risk_manager,
            trade_executor=self.trade_executor,
            rebalancer=self.rebalancer
        )

    async def poll_and_store(self, db: AsyncSession):
        # Fetch orderbooks and store snapshots
        exchange_orderbooks = await self.fetcher.fetch_all(db)
        if not exchange_orderbooks:
            return

        # Detect arbitrage for each common symbol
        for common_symbol, orderbooks in exchange_orderbooks.items():
            await self.detector.detect_for_symbol(db, common_symbol, orderbooks)

        await db.commit()