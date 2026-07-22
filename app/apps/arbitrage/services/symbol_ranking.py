import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from decimal import Decimal
from collections import defaultdict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from app.apps.arbitrage.models import (
    OrderbookSnapshot, Exchange, ExchangeSymbol, ExchangeFee
)

logger = logging.getLogger(__name__)

class SymbolRankingService:
    """Analyze historical orderbook snapshots to rank arbitrage opportunities."""

    @staticmethod
    async def rank_symbols(
        db: AsyncSession,
        days: int = 3,
        min_profit_percent: float = 0.7,
        trade_notional_usdt: float = 100.0
    ) -> dict:
        """
        Compute ranking for all symbols based on historical snapshots.
        """
        since = datetime.utcnow() - timedelta(days=days)
        stmt = (
            select(
                OrderbookSnapshot,
                ExchangeSymbol.common_symbol,
                Exchange.name.label("exchange_name"),
                ExchangeSymbol.price_conversion_factor
            )
            .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
            .join(Exchange, OrderbookSnapshot.exchange_id == Exchange.id)
            .where(OrderbookSnapshot.created_at >= since)
            .where(Exchange.is_active == True)
        )
        result = await db.execute(stmt)
        rows = result.all()

        if not rows:
            return {"error": "No snapshot data available for the specified period."}

        # Group snapshots by symbol and time bucket (10-second intervals)
        snapshots_by_symbol = defaultdict(lambda: defaultdict(dict))
        for row in rows:
            snapshot = row[0]
            symbol = row.common_symbol
            exchange = row.exchange_name
            factor = float(row.price_conversion_factor)
            asks = snapshot.asks or []
            bids = snapshot.bids or []
            snapshots_by_symbol[symbol][snapshot.created_at][exchange] = (asks, bids)

        # Get taker fees
        fee_stmt = select(ExchangeFee, Exchange.name).join(Exchange)
        fee_result = await db.execute(fee_stmt)
        fees = {}
        for fee, exchange_name in fee_result:
            fees[(exchange_name, fee.quote_currency)] = float(fee.taker_fee)

        rankings = []
        for symbol, buckets in snapshots_by_symbol.items():
            if len(buckets) < 2:
                continue
            if symbol.endswith("IRT"):
                quote_currency = "IRT"
            elif symbol.endswith("USDT"):
                quote_currency = "USDT"
            else:
                continue

            usdt_price = await SymbolRankingService._get_usdt_price(db)

            total_opportunities = 0
            sum_profit_percent = 0.0
            total_profit_quote = 0.0
            max_profit = 0.0
            min_profit = float('inf')

            for timestamp, exchange_obs in sorted(buckets.items()):
                exchanges = list(exchange_obs.keys())
                if len(exchanges) < 2:
                    continue
                for buy_exch in exchanges:
                    for sell_exch in exchanges:
                        if buy_exch == sell_exch:
                            continue
                        buy_asks, _ = exchange_obs[buy_exch]
                        _, sell_bids = exchange_obs[sell_exch]
                        if not buy_asks or not sell_bids:
                            continue

                        best_ask = buy_asks[0][0]
                        if best_ask <= 0:
                            continue
                        # Convert notional to quote currency
                        if quote_currency == "IRT" and usdt_price:
                            notional_quote = trade_notional_usdt * usdt_price
                        else:
                            notional_quote = trade_notional_usdt
                        # Estimate volume needed
                        volume = notional_quote / best_ask
                        if volume <= 0:
                            continue

                        buy_vwap, buy_fill = SymbolRankingService._vwap_from_levels(buy_asks, volume)
                        sell_vwap, sell_fill = SymbolRankingService._vwap_from_levels(sell_bids, volume, side='bid')
                        if buy_fill == 0 or sell_fill == 0:
                            continue
                        trade_volume = min(buy_fill, sell_fill)
                        if trade_volume <= 0:
                            continue

                        buy_fee = fees.get((buy_exch, quote_currency), 0.0)
                        sell_fee = fees.get((sell_exch, quote_currency), 0.0)
                        effective_buy = buy_vwap * (1 + buy_fee)
                        effective_sell = sell_vwap * (1 - sell_fee)

                        profit_quote = trade_volume * (effective_sell - effective_buy)
                        if profit_quote <= 0:
                            continue
                        profit_percent = (profit_quote / (trade_volume * effective_buy)) * 100

                        if profit_percent >= min_profit_percent:
                            total_opportunities += 1
                            sum_profit_percent += profit_percent
                            total_profit_quote += profit_quote
                            max_profit = max(max_profit, profit_percent)
                            min_profit = min(min_profit, profit_percent)

            if total_opportunities == 0:
                continue

            avg_profit_percent = sum_profit_percent / total_opportunities
            if quote_currency == "IRT" and usdt_price:
                total_profit_usdt = total_profit_quote / usdt_price
            else:
                total_profit_usdt = total_profit_quote

            rankings.append({
                "symbol": symbol,
                "opportunities": total_opportunities,
                "avg_profit_percent": round(avg_profit_percent, 2),
                "max_profit_percent": round(max_profit, 2),
                "min_profit_percent": round(min_profit, 2),
                "total_profit_quote": round(total_profit_quote, 2),
                "total_profit_usdt": round(total_profit_usdt, 2),
                "quote_currency": quote_currency,
                "days_analyzed": days
            })

        rankings.sort(key=lambda x: x["total_profit_usdt"], reverse=True)

        return {
            "rankings": rankings,
            "summary": {
                "total_symbols_analyzed": len(rankings),
                "period_days": days,
                "min_profit_percent": min_profit_percent,
                "trade_notional_usdt": trade_notional_usdt,
                "analysis_timestamp": datetime.utcnow().isoformat()
            }
        }

    @staticmethod
    def _vwap_from_levels(levels: List[List[float]], requested_volume: float, side: str = 'ask') -> Tuple[float, float]:
        if not levels:
            return 0.0, 0.0
        cum_vol = 0.0
        cum_value = 0.0
        for price, vol in levels:
            if cum_vol >= requested_volume:
                break
            remaining = requested_volume - cum_vol
            take = min(vol, remaining)
            cum_vol += take
            cum_value += take * price
            if cum_vol >= requested_volume:
                break
        if cum_vol == 0:
            return 0.0, 0.0
        vwap = cum_value / cum_vol
        return vwap, cum_vol

    @staticmethod
    async def _get_usdt_price(db: AsyncSession) -> Optional[float]:
        stmt = (
            select(OrderbookSnapshot.best_bid_price)
            .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
            .where(ExchangeSymbol.common_symbol == "USDTIRT")
            .order_by(OrderbookSnapshot.created_at.desc())
            .limit(1)
        )
        result = await db.execute(stmt)
        price = result.scalar_one_or_none()
        return float(price) if price else None