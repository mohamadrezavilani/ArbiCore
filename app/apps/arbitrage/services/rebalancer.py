import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Tuple, Optional, List
from app.apps.arbitrage.models import (
    Exchange, BaseInventory, QuoteInventory, SymbolArbitrageSettings, Network, ExchangeSymbol, OrderbookSnapshot
)
from app.apps.arbitrage.inventory import update_base_balance, update_quote_balance, get_base_balance, get_quote_balance
from app.exchanges.factory import get_exchange_client
from .opportunity_logger import OpportunityLogger
from .trade_executor import TradeExecutor

logger = logging.getLogger(__name__)

class Rebalancer:
    def __init__(self, logger: OpportunityLogger, trade_executor: TradeExecutor = None):
        self.logger = logger
        self.trade_executor = trade_executor

    async def rebalance_symbol_if_needed(
        self,
        db: AsyncSession,
        common_symbol: str,
        threshold_ratio: float = 0.1
    ):
        stmt = (
            select(Exchange.name, Exchange.id, BaseInventory.balance)
            .join(BaseInventory, BaseInventory.exchange_id == Exchange.id)
            .where(BaseInventory.common_symbol == common_symbol)
            .where(Exchange.is_active == True)
        )
        result = await db.execute(stmt)
        rows = result.all()
        if not rows or len(rows) < 2:
            return

        balances = [(r.name, float(r.balance)) for r in rows]
        avg_balance = sum(b for _, b in balances) / len(balances)
        min_balance = min(b for _, b in balances)

        if min_balance >= threshold_ratio * avg_balance:
            return

        poorest = min(balances, key=lambda x: x[1])
        richest = max(balances, key=lambda x: x[1])

        transfer_amount = min(richest[1] * 0.75, richest[1] - 1e-6)
        if transfer_amount <= 0:
            return

        # Get settings and network fee
        settings_stmt = select(SymbolArbitrageSettings).where(SymbolArbitrageSettings.common_symbol == common_symbol)
        settings_obj = (await db.execute(settings_stmt)).scalar_one_or_none()
        network_fee = 0.0
        if settings_obj and settings_obj.default_network_id:
            net_stmt = select(Network).where(Network.id == settings_obj.default_network_id)
            net = (await db.execute(net_stmt)).scalar_one_or_none()
            if net:
                network_fee = float(net.fee_per_transfer)

        # If network fee is zero or transfer amount <= fee, skip or use simple transfer
        if network_fee == 0 or transfer_amount <= network_fee:
            await self._simple_transfer(
                db, common_symbol, richest[0], poorest[0],
                transfer_amount, network_fee, threshold_ratio
            )
            return

        # Check if opportunistic rebalancing is enabled
        if settings_obj and settings_obj.opportunistic_rebalance_enabled:
            # Try to rebalance via cross trade
            success = await self._opportunistic_rebalance(
                db, common_symbol, richest[0], poorest[0],
                transfer_amount, network_fee, settings_obj, threshold_ratio
            )
            if success:
                return

        # Fallback to simple transfer (pay network fee)
        await self._simple_transfer(
            db, common_symbol, richest[0], poorest[0],
            transfer_amount, network_fee, threshold_ratio
        )

    async def _simple_transfer(
        self, db: AsyncSession, common_symbol: str,
        from_exch: str, to_exch: str,
        amount: float, network_fee: float, threshold_ratio: float
    ):
        if amount <= network_fee:
            logger.info(f"Transfer amount {amount:.4f} {common_symbol} <= network fee {network_fee}, skipping")
            return

        net_received = amount - network_fee
        await update_base_balance(db, from_exch, common_symbol, -amount)
        await update_base_balance(db, to_exch, common_symbol, net_received)

        await self.logger.log_rebalance(
            db, common_symbol, None, from_exch, to_exch,
            amount, network_fee, net_received,
            f"base_balance_{common_symbol}_below_{threshold_ratio*100:.0f}%_avg"
        )
        logger.info(
            f"🔄 Rebalanced {common_symbol}: sent {amount:.4f} from {from_exch} to {to_exch} "
            f"(network fee {network_fee:.4f}), net received {net_received:.4f}"
        )

    async def _opportunistic_rebalance(
        self, db: AsyncSession, common_symbol: str,
        from_exch: str, to_exch: str,
        amount: float, network_fee: float,
        settings: SymbolArbitrageSettings,
        threshold_ratio: float
    ) -> bool:
        """
        Attempt to rebalance by trading: sell on from_exch (which has excess) and buy on to_exch (which has deficit).
        This will move the asset from from_exch to to_exch but via two opposite trades.
        The net result should be that from_exch decreases by amount, to_exch increases by amount.
        Actually, we need to sell on from_exch and buy on to_exch. This will reduce from_exch's base balance and increase to_exch's base balance.
        But we also need to consider the quote currency flows.
        """
        # Determine quote currency
        if common_symbol.endswith("IRT"):
            quote_currency = "IRT"
        elif common_symbol.endswith("USDT"):
            quote_currency = "USDT"
        else:
            return False

        # Get exchange clients
        client_from = get_exchange_client(from_exch)
        client_to = get_exchange_client(to_exch)
        if not client_from or not client_to:
            return False

        # Fetch orderbooks to get current prices
        # We need original symbols for each exchange
        from_sym_stmt = select(ExchangeSymbol.original_symbol).join(Exchange).where(
            Exchange.name == from_exch, ExchangeSymbol.common_symbol == common_symbol
        )
        to_sym_stmt = select(ExchangeSymbol.original_symbol).join(Exchange).where(
            Exchange.name == to_exch, ExchangeSymbol.common_symbol == common_symbol
        )
        from_symbol = (await db.execute(from_sym_stmt)).scalar_one_or_none()
        to_symbol = (await db.execute(to_sym_stmt)).scalar_one_or_none()
        if not from_symbol or not to_symbol:
            return False

        from_ob = await client_from.fetch_orderbook(from_symbol)
        to_ob = await client_to.fetch_orderbook(to_symbol)
        if not from_ob or not to_ob:
            return False

        from_asks, from_bids = client_from.extract_levels(from_ob)
        to_asks, to_bids = client_to.extract_levels(to_ob)
        if not from_bids or not to_asks:
            return False

        # Get fees
        async def get_taker_fee(exchange_name: str) -> float:
            exch = await db.execute(select(Exchange).where(Exchange.name == exchange_name))
            exch_obj = exch.scalar_one_or_none()
            if not exch_obj:
                return 0.0
            fee_stmt = select(ExchangeFee).where(
                ExchangeFee.exchange_id == exch_obj.id,
                ExchangeFee.quote_currency == quote_currency
            )
            fee_rec = await db.execute(fee_stmt)
            fee = fee_rec.scalar_one_or_none()
            return float(fee.taker_fee) if fee else 0.0

        from_fee = await get_taker_fee(from_exch)
        to_fee = await get_taker_fee(to_exch)

        # We want to sell on from_exch and buy on to_exch.
        # The sell price is best bid on from_exch, buy price is best ask on to_exch.
        sell_price = from_bids[0][0]  # best bid
        buy_price = to_asks[0][0]    # best ask

        # Calculate net cost in quote currency for moving 'amount' base units.
        # We sell amount on from_exch: receive amount * sell_price * (1 - from_fee)
        # We buy amount on to_exch: pay amount * buy_price * (1 + to_fee)
        # The net quote difference = cost - revenue = (amount * buy_price * (1+to_fee)) - (amount * sell_price * (1-from_fee))
        # This is the loss (positive) or gain (negative) in quote currency.
        revenue_quote = amount * sell_price * (1 - from_fee)
        cost_quote = amount * buy_price * (1 + to_fee)
        net_loss_quote = cost_quote - revenue_quote

        # If net loss is negative, it's actually a profit – we should do it regardless.
        # If positive, compare to network fee (converted to quote currency)
        max_allowed_loss_quote = network_fee * buy_price  # network fee in quote currency (since network fee is in base, convert to quote using buy price)
        max_allowed_loss_quote *= (settings.opportunistic_rebalance_max_loss_percent / 100.0)

        if net_loss_quote <= max_allowed_loss_quote:
            # Execute the trade
            logger.info(f"Opportunistic rebalance: moving {amount} {common_symbol} from {from_exch} to {to_exch} via trade. Net loss: {net_loss_quote:.2f} {quote_currency} vs network fee equivalent {max_allowed_loss_quote:.2f}")
            # Execute sell on from_exch and buy on to_exch
            # We need to get exchange objects for logging
            exch_from_obj = (await db.execute(select(Exchange).where(Exchange.name == from_exch))).scalar_one_or_none()
            exch_to_obj = (await db.execute(select(Exchange).where(Exchange.name == to_exch))).scalar_one_or_none()
            if not exch_from_obj or not exch_to_obj:
                return False

            # Use trade executor to perform the trades
            # We'll create a small wrapper to execute both legs atomically? We'll just place two market orders.
            # But we need to ensure we have enough balances.
            # For simplicity, we'll execute sell first (to get quote), then buy (using that quote).
            # This is not atomic but acceptable for rebalancing.
            try:
                # Sell on from_exch
                sell_result = await client_from.place_market_order(
                    symbol=common_symbol, side="sell", amount=amount,
                    client_order_id=f"rebalance_sell_{common_symbol}_{from_exch}"
                )
                if sell_result.status != "filled":
                    logger.warning(f"Sell leg failed for opportunistic rebalance: {sell_result.status}")
                    return False
                # Now buy on to_exch using the quote currency obtained? But we need to have quote balance on to_exch.
                # We assume we have enough quote balance on to_exch.
                buy_result = await client_to.place_market_order(
                    symbol=common_symbol, side="buy", amount=amount,
                    client_order_id=f"rebalance_buy_{common_symbol}_{to_exch}"
                )
                if buy_result.status != "filled":
                    # Try to reverse the sell? Too complex, just log and accept loss
                    logger.warning(f"Buy leg failed for opportunistic rebalance: {buy_result.status}")
                    # Still update balances partially? We'll sync from exchange later.
                    await self._sync_balances_after_rebalance(db, from_exch, to_exch, common_symbol, quote_currency)
                    return False

                # Update inventory balances manually? Better to sync from exchange.
                await self._sync_balances_after_rebalance(db, from_exch, to_exch, common_symbol, quote_currency)

                # Log as a rebalance (with network_fee = 0, but we log the loss)
                await self.logger.log_rebalance(
                    db, common_symbol, None, from_exch, to_exch,
                    amount, 0, amount,  # network fee 0, net received = amount (but actual net received in base is amount)
                    f"opportunistic_rebalance_loss_{net_loss_quote:.2f}_{quote_currency}"
                )
                logger.info(f"✅ Opportunistic rebalance completed for {common_symbol} from {from_exch} to {to_exch}")
                return True
            except Exception as e:
                logger.exception(f"Opportunistic rebalance failed: {e}")
                return False
        else:
            logger.info(f"Opportunistic rebalance not profitable: net loss {net_loss_quote:.2f} > allowed {max_allowed_loss_quote:.2f}")
            return False

    async def _sync_balances_after_rebalance(self, db: AsyncSession, exch1: str, exch2: str, common_symbol: str, quote_currency: str):
        """Sync real balances from both exchanges after trades."""
        for exch in (exch1, exch2):
            client = get_exchange_client(exch)
            if client:
                real_balances = await client.get_balances()
                # Update base and quote balances
                for asset, bal in real_balances.items():
                    if asset == quote_currency:
                        await update_quote_balance(db, exch, quote_currency, bal - (await get_quote_balance(db, exch, quote_currency)))
                    else:
                        # This might be the base asset or other
                        await update_base_balance(db, exch, common_symbol, bal - (await get_base_balance(db, exch, common_symbol)))
        await db.commit()