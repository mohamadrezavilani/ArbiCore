import logging
from sqlalchemy.ext.asyncio import AsyncSession
from .orderbook_fetcher import OrderbookFetcher
from .arbitrage_detector import ArbitrageDetector
from .opportunity_logger import OpportunityLogger
from .risk_manager import RiskManager
from .trade_executor import TradeExecutor
from .rebalancer import Rebalancer
from app.apps.arbitrage.inventory import update_base_balance, update_quote_balance
from app.core.config import settings

logger = logging.getLogger(__name__)

class ArbitrageService:
    def __init__(self):
        self.poll_interval = settings.ARBITRAGE_CHECK_INTERVAL_SECONDS
        self.logger = OpportunityLogger()
        self.risk_manager = RiskManager()
        self.trade_executor = TradeExecutor(self.logger)
        self.rebalancer = Rebalancer(self.logger, self.trade_executor)
        self.fetcher = OrderbookFetcher()
        self.detector = ArbitrageDetector(
            logger=self.logger,
            risk_manager=self.risk_manager,
            trade_executor=self.trade_executor,
            rebalancer=self.rebalancer
        )

    async def poll_and_store(self, db: AsyncSession):
        exchange_orderbooks = await self.fetcher.fetch_all(db)
        if not exchange_orderbooks:
            return

        all_base_deltas = {}
        all_quote_deltas = {}
        all_opportunities = []
        traded_symbols = []

        for common_symbol, orderbooks in exchange_orderbooks.items():
            any_trade, base_deltas, quote_deltas, opportunities = await self.detector.detect_for_symbol(
                db, common_symbol, orderbooks
            )
            if any_trade:
                traded_symbols.append(common_symbol)
                # Merge deltas
                for ex, delta in base_deltas.items():
                    all_base_deltas[(ex, common_symbol)] = all_base_deltas.get((ex, common_symbol), 0.0) + delta
                for ex, delta in quote_deltas.items():
                    quote_currency = "IRT" if common_symbol.endswith("IRT") else "USDT"
                    all_quote_deltas[(ex, quote_currency)] = all_quote_deltas.get((ex, quote_currency), 0.0) + delta
                all_opportunities.extend(opportunities)

        # Apply all balance updates in bulk
        for (ex, sym), delta in all_base_deltas.items():
            if abs(delta) > 1e-8:
                await update_base_balance(db, ex, sym, delta)
        for (ex, cur), delta in all_quote_deltas.items():
            if abs(delta) > 1e-8:
                await update_quote_balance(db, ex, cur, delta)

        db.add_all(all_opportunities)
        await db.commit()

        # Rebalance only symbols that had trades
        for common_symbol in traded_symbols:
            orderbooks = exchange_orderbooks.get(common_symbol)
            if orderbooks:
                quote_currency = "IRT" if common_symbol.endswith("IRT") else "USDT"
                await self.rebalancer.rebalance_symbol_if_needed(db, common_symbol, quote_currency, orderbooks)

        await db.commit()