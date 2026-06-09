import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from app.apps.arbitrage.models import OrderbookSnapshot, Exchange, ExchangeSymbol

logger = logging.getLogger(__name__)

def percentile(data, p):
    """Calculate p-th percentile of a list."""
    if not data:
        return 0
    data_sorted = sorted(data)
    n = len(data_sorted)
    idx = (p / 100) * (n - 1)
    if idx.is_integer():
        return data_sorted[int(idx)]
    lower = data_sorted[int(idx)]
    upper = data_sorted[int(idx) + 1]
    return lower + (upper - lower) * (idx - int(idx))


class AnalysisService:
    @staticmethod
    async def get_spread_history(db, common_symbol, exchange_name=None, hours=24, interval_minutes=10):
        # ... same as before (keep as is)
        stmt = (
            select(
                OrderbookSnapshot.created_at,
                OrderbookSnapshot.best_ask_price,
                OrderbookSnapshot.best_bid_price
            )
            .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
            .join(Exchange, OrderbookSnapshot.exchange_id == Exchange.id)
            .where(ExchangeSymbol.common_symbol == common_symbol)
            .where(OrderbookSnapshot.created_at >= datetime.utcnow() - timedelta(hours=hours))
            .order_by(OrderbookSnapshot.created_at)
        )
        if exchange_name:
            stmt = stmt.where(Exchange.name == exchange_name)
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            return []
        intervals = {}
        for ts, ask, bid in rows:
            if ask is None or bid is None:
                continue
            minutes = (ts.minute // interval_minutes) * interval_minutes
            interval_key = ts.replace(minute=minutes, second=0, microsecond=0)
            if interval_key not in intervals:
                intervals[interval_key] = {"spreads": [], "asks": [], "bids": []}
            spread = float(ask) - float(bid)
            intervals[interval_key]["spreads"].append(spread)
            intervals[interval_key]["asks"].append(float(ask))
            intervals[interval_key]["bids"].append(float(bid))
        result_data = []
        for ts, data in sorted(intervals.items()):
            avg_spread = sum(data["spreads"]) / len(data["spreads"])
            avg_price = (sum(data["asks"]) + sum(data["bids"])) / (2 * len(data["asks"]))
            spread_percent = (avg_spread / avg_price) * 100 if avg_price else 0
            result_data.append({
                "timestamp": ts.isoformat(),
                "spread_absolute": round(avg_spread, 2),
                "spread_percent": round(spread_percent, 4),
                "avg_ask": round(sum(data["asks"]) / len(data["asks"]), 2),
                "avg_bid": round(sum(data["bids"]) / len(data["bids"]), 2),
            })
        return result_data

    @staticmethod
    async def get_liquidity_depth(db, common_symbol, exchange_name=None, hours=24, depth_levels=5):
        # same as before (no numpy)
        subquery = (
            select(
                OrderbookSnapshot.exchange_id,
                func.max(OrderbookSnapshot.created_at).label("max_ts")
            )
            .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
            .where(ExchangeSymbol.common_symbol == common_symbol)
            .group_by(OrderbookSnapshot.exchange_id)
            .subquery()
        )
        stmt = (
            select(OrderbookSnapshot)
            .join(subquery, and_(
                OrderbookSnapshot.exchange_id == subquery.c.exchange_id,
                OrderbookSnapshot.created_at == subquery.c.max_ts
            ))
        )
        result = await db.execute(stmt)
        snapshots = result.scalars().all()
        depth_data = {}
        for snap in snapshots:
            exch = (await db.execute(select(Exchange.name).where(Exchange.id == snap.exchange_id))).scalar_one()
            asks = snap.asks or []
            bids = snap.bids or []
            ask_depth = []
            ask_cum = 0.0
            for i, (price, vol) in enumerate(asks[:depth_levels]):
                ask_cum += vol
                ask_depth.append({"level": i+1, "price": price, "volume_at_level": vol, "cumulative_volume": ask_cum})
            bid_depth = []
            bid_cum = 0.0
            for i, (price, vol) in enumerate(bids[:depth_levels]):
                bid_cum += vol
                bid_depth.append({"level": i+1, "price": price, "volume_at_level": vol, "cumulative_volume": bid_cum})
            depth_data[exch] = {"asks": ask_depth, "bids": bid_depth, "timestamp": snap.created_at.isoformat()}
        return depth_data

    @staticmethod
    async def get_price_volatility(db, common_symbol, exchange_name=None, hours=24):
        stmt = (
            select(OrderbookSnapshot.best_ask_price, OrderbookSnapshot.best_bid_price)
            .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
            .join(Exchange, OrderbookSnapshot.exchange_id == Exchange.id)
            .where(ExchangeSymbol.common_symbol == common_symbol)
            .where(OrderbookSnapshot.created_at >= datetime.utcnow() - timedelta(hours=hours))
        )
        if exchange_name:
            stmt = stmt.where(Exchange.name == exchange_name)
        result = await db.execute(stmt)
        rows = result.all()
        mid_prices = []
        for ask, bid in rows:
            if ask is not None and bid is not None:
                mid_prices.append((float(ask) + float(bid)) / 2)
        if len(mid_prices) < 2:
            return {}
        mean = sum(mid_prices) / len(mid_prices)
        variance = sum((p - mean) ** 2 for p in mid_prices) / len(mid_prices)
        volatility = variance ** 0.5
        volatility_percent = (volatility / mean) * 100 if mean else 0
        return {
            "volatility_absolute": round(volatility, 2),
            "volatility_percent": round(volatility_percent, 4),
            "mean_price": round(mean, 2),
            "sample_count": len(mid_prices),
            "time_range_hours": hours
        }

    @staticmethod
    async def get_cross_exchange_spread(db, common_symbol, hours=24):
        stmt = (
            select(
                OrderbookSnapshot.created_at,
                Exchange.name.label("exchange"),
                OrderbookSnapshot.best_ask_price,
                OrderbookSnapshot.best_bid_price
            )
            .join(Exchange, OrderbookSnapshot.exchange_id == Exchange.id)
            .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
            .where(ExchangeSymbol.common_symbol == common_symbol)
            .where(OrderbookSnapshot.created_at >= datetime.utcnow() - timedelta(hours=hours))
            .order_by(OrderbookSnapshot.created_at)
        )
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            return []
        interval_minutes = 5
        groups = {}
        for ts, exch, ask, bid in rows:
            if ask is None or bid is None:
                continue
            minutes = (ts.minute // interval_minutes) * interval_minutes
            interval_key = ts.replace(minute=minutes, second=0, microsecond=0)
            if interval_key not in groups:
                groups[interval_key] = {"best_bid": 0, "best_ask": float('inf')}
            groups[interval_key]["best_bid"] = max(groups[interval_key]["best_bid"], float(bid))
            groups[interval_key]["best_ask"] = min(groups[interval_key]["best_ask"], float(ask))
        results = []
        for ts, data in sorted(groups.items()):
            spread = data["best_bid"] - data["best_ask"]
            if spread > 0:
                profit_percent = (spread / data["best_ask"]) * 100
                results.append({
                    "timestamp": ts.isoformat(),
                    "best_bid": round(data["best_bid"], 2),
                    "best_ask": round(data["best_ask"], 2),
                    "max_spread": round(spread, 2),
                    "max_profit_percent": round(profit_percent, 4)
                })
        return results

    # Profit distribution (numpy-free)
    @staticmethod
    async def get_profit_distribution(db, common_symbol, hours=168):
        from app.apps.arbitrage.models import ArbitrageOpportunity
        stmt = (
            select(ArbitrageOpportunity.profit_percent)
            .where(ArbitrageOpportunity.common_symbol == common_symbol)
            .where(ArbitrageOpportunity.created_at >= datetime.utcnow() - timedelta(hours=hours))
        )
        result = await db.execute(stmt)
        profits = [float(p) for p in result.scalars().all() if p is not None]
        if not profits:
            return {"message": "No profit data", "bins": [], "counts": [], "percentiles": {}}
        min_p = min(profits)
        max_p = max(profits)
        bins = 10
        bin_width = (max_p - min_p) / bins if bins > 0 else 1
        hist = [0] * bins
        for p in profits:
            idx = min(int((p - min_p) / bin_width), bins-1)
            hist[idx] += 1
        bin_edges = [min_p + i * bin_width for i in range(bins+1)]
        percentiles = {
            "p50": round(percentile(profits, 50), 2),
            "p75": round(percentile(profits, 75), 2),
            "p90": round(percentile(profits, 90), 2),
            "p95": round(percentile(profits, 95), 2),
            "mean": round(sum(profits)/len(profits), 2),
            "max": round(max_p, 2),
            "min": round(min_p, 2)
        }
        return {"bins": [round(e, 2) for e in bin_edges], "counts": hist, "percentiles": percentiles, "sample_count": len(profits)}

    @staticmethod
    async def get_spread_distribution(db, common_symbol, exchange_name=None, hours=168):
        stmt = (
            select(OrderbookSnapshot.best_ask_price, OrderbookSnapshot.best_bid_price)
            .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
            .join(Exchange, OrderbookSnapshot.exchange_id == Exchange.id)
            .where(ExchangeSymbol.common_symbol == common_symbol)
            .where(OrderbookSnapshot.created_at >= datetime.utcnow() - timedelta(hours=hours))
            .where(OrderbookSnapshot.best_ask_price.isnot(None))
            .where(OrderbookSnapshot.best_bid_price.isnot(None))
        )
        if exchange_name:
            stmt = stmt.where(Exchange.name == exchange_name)
        result = await db.execute(stmt)
        rows = result.all()
        spreads = []
        for ask, bid in rows:
            if ask and bid and float(ask) > 0:
                spread_pct = (float(ask) - float(bid)) / float(ask) * 100
                spreads.append(spread_pct)
        if not spreads:
            return {"message": "No spread data", "percentiles": {}}
        percentiles = {
            "p50": round(percentile(spreads, 50), 3),
            "p75": round(percentile(spreads, 75), 3),
            "p90": round(percentile(spreads, 90), 3),
            "p95": round(percentile(spreads, 95), 3),
            "mean": round(sum(spreads)/len(spreads), 3),
            "max": round(max(spreads), 3)
        }
        return {"percentiles": percentiles, "sample_count": len(spreads)}

    @staticmethod
    async def get_imbalance_analysis(db, common_symbol, hours=168):
        from app.apps.arbitrage.models import BaseInventory, Exchange
        stmt = (
            select(Exchange.name, BaseInventory.balance)
            .join(Exchange, BaseInventory.exchange_id == Exchange.id)
            .where(BaseInventory.common_symbol == common_symbol)
        )
        result = await db.execute(stmt)
        rows = result.all()
        if not rows:
            return {"message": "No balance data"}
        balances = {name: float(bal) for name, bal in rows}
        avg_balance = sum(balances.values()) / len(balances)
        ratios = {name: bal / avg_balance for name, bal in balances.items()}
        return {
            "current_balances": balances,
            "average": avg_balance,
            "ratios_to_avg": ratios,
            "imbalance_suggestion": "Set imbalance_ratio to 0.2 if you want to rebalance when any exchange falls below 20% of avg."
        }

    @staticmethod
    async def get_trade_size_analysis(db, common_symbol, hours=168):
        from app.apps.arbitrage.models import ArbitrageOpportunity
        stmt = (
            select(ArbitrageOpportunity.traded_volume, ArbitrageOpportunity.profit_quote)
            .where(ArbitrageOpportunity.common_symbol == common_symbol)
            .where(ArbitrageOpportunity.created_at >= datetime.utcnow() - timedelta(hours=hours))
        )
        result = await db.execute(stmt)
        data = [(float(vol), float(profit)) for vol, profit in result.all() if vol and profit]
        if not data:
            return {"message": "No trade data"}
        volumes = [d[0] for d in data]
        median_vol = percentile(volumes, 50)
        return {
            "median_traded_volume": round(median_vol, 2),
            "min_trade_percent_suggestion": "Set min_trade_percent to 0.1-0.2 (10-20% of max available)",
            "valuability_factor_suggestion": "Keep at 1.0 unless profit per unit is very high",
        }

    @staticmethod
    async def get_rebalancing_loss_analysis(db, common_symbol, hours=168):
        from app.apps.arbitrage.models import RebalanceLog
        stmt = (
            select(RebalanceLog)
            .where(RebalanceLog.common_symbol == common_symbol)
            .where(RebalanceLog.created_at >= datetime.utcnow() - timedelta(hours=hours))
        )
        result = await db.execute(stmt)
        logs = result.scalars().all()
        if not logs:
            return {"message": "No rebalance logs in period"}
        losses = [log.profit_quote for log in logs if log.profit_quote < 0]
        if not losses:
            return {"message": "No loss-making rebalances"}
        avg_loss = sum(losses) / len(losses)
        total_loss = sum(losses)
        return {
            "total_loss_irt": round(total_loss, 2),
            "avg_loss_per_rebalance": round(avg_loss, 2),
            "num_rebalances": len(logs),
            "suggestion": "If avg_loss is high, reduce market_rebalance_amount_percent or tighten max_spread_percent."
        }