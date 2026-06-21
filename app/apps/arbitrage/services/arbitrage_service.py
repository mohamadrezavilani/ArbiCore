import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from .orderbook_fetcher import OrderbookFetcher
from .arbitrage_detector import ArbitrageDetector
from .opportunity_logger import OpportunityLogger
from .risk_manager import RiskManager
from .trade_executor import TradeExecutor
from .rebalancer import Rebalancer
from app.apps.arbitrage.inventory import update_base_balance, update_quote_balance
from app.apps.arbitrage.models import SymbolArbitrageSettings
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
        self.last_fetch_timestamp = 0.0

    async def poll_and_store(self, db: AsyncSession):
        exchange_data = await self.fetcher.fetch_all(db)
        if not exchange_data:
            return

        global_max_ts = 0.0
        all_base_deltas = {}
        all_quote_deltas = {}
        all_opportunities = []
        traded_symbols = []

        for common_symbol, (orderbooks, max_ts) in exchange_data.items():
            if max_ts > global_max_ts:
                global_max_ts = max_ts

            any_trade, base_deltas, quote_deltas, opportunities = await self.detector.detect_for_symbol(
                db, common_symbol, orderbooks
            )
            if any_trade:
                traded_symbols.append(common_symbol)
                for ex, delta in base_deltas.items():
                    all_base_deltas[(ex, common_symbol)] = all_base_deltas.get((ex, common_symbol), 0.0) + delta
                for ex, delta in quote_deltas.items():
                    quote_currency = "IRT" if common_symbol.endswith("IRT") else "USDT"
                    all_quote_deltas[(ex, quote_currency)] = all_quote_deltas.get((ex, quote_currency), 0.0) + delta
                all_opportunities.extend(opportunities)

        for (ex, sym), delta in all_base_deltas.items():
            if abs(delta) > 1e-8:
                await update_base_balance(db, ex, sym, delta)
        for (ex, cur), delta in all_quote_deltas.items():
            if abs(delta) > 1e-8:
                await update_quote_balance(db, ex, cur, delta)

        db.add_all(all_opportunities)
        await db.commit()

        # Rebalance base
        for common_symbol in traded_symbols:
            orderbooks = exchange_data.get(common_symbol)
            if orderbooks:
                orderbooks_dict = orderbooks[0]
                quote_currency = "IRT" if common_symbol.endswith("IRT") else "USDT"
                await self.rebalancer.rebalance_symbol_if_needed(db, common_symbol, quote_currency, orderbooks_dict)

        pending_stmt = select(SymbolArbitrageSettings.common_symbol).where(
            SymbolArbitrageSettings.rebalance_pending == True,
            SymbolArbitrageSettings.common_symbol.in_(exchange_data.keys())
        )
        pending_result = await db.execute(pending_stmt)
        pending_symbols = pending_result.scalars().all()
        for sym in pending_symbols:
            if sym not in traded_symbols:
                orderbooks = exchange_data.get(sym)
                if orderbooks:
                    orderbooks_dict = orderbooks[0]
                    quote_currency = "IRT" if sym.endswith("IRT") else "USDT"
                    await self.rebalancer.rebalance_symbol_if_needed(db, sym, quote_currency, orderbooks_dict)

        # Rebalance quote
        for common_symbol in traded_symbols:
            orderbooks = exchange_data.get(common_symbol)
            if orderbooks:
                orderbooks_dict = orderbooks[0]
                quote_currency = "IRT" if common_symbol.endswith("IRT") else "USDT"
                await self.rebalancer.rebalance_quote_if_needed(db, common_symbol, quote_currency, orderbooks_dict)

        pending_quote_stmt = select(SymbolArbitrageSettings.common_symbol).where(
            SymbolArbitrageSettings.quote_rebalance_pending == True,
            SymbolArbitrageSettings.common_symbol.in_(exchange_data.keys())
        )
        pending_quote_result = await db.execute(pending_quote_stmt)
        pending_quote_symbols = pending_quote_result.scalars().all()
        for sym in pending_quote_symbols:
            if sym not in traded_symbols:
                orderbooks = exchange_data.get(sym)
                if orderbooks:
                    orderbooks_dict = orderbooks[0]
                    quote_currency = "IRT" if sym.endswith("IRT") else "USDT"
                    await self.rebalancer.rebalance_quote_if_needed(db, sym, quote_currency, orderbooks_dict)

        await db.commit()

        self.last_fetch_timestamp = global_max_ts
        if global_max_ts > 0:
            pass
            # logger.info(f"[TIME] Global max timestamp across all symbols: {global_max_ts:.2f} (UTC)")
        else:
            logger.warning("[TIME] No valid timestamp received from any exchange – will fall back to fixed interval")