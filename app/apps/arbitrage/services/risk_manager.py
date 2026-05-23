from app.apps.arbitrage.models import SymbolArbitrageSettings

class RiskManager:
    @staticmethod
    def calculate_trade_percent(
        net_gain: float,
        network_commission_quote: float,
        params: SymbolArbitrageSettings
    ) -> float:
        cutoff = float(params.cutoff_threshold)
        min_trade_pct = float(params.min_trade_percent)
        min_trade_factor = float(params.min_trade_factor)
        valuability_factor = float(params.valuability_factor)

        min_threshold = min_trade_factor * network_commission_quote
        full_threshold = valuability_factor * network_commission_quote

        if net_gain <= 0:
            return 0.0
        if net_gain < cutoff:
            return 0.0
        if net_gain <= min_threshold:
            return min_trade_pct
        if net_gain >= full_threshold:
            return 1.0

        if full_threshold > min_threshold:
            slope = (1.0 - min_trade_pct) / (full_threshold - min_threshold)
            return min_trade_pct + slope * (net_gain - min_threshold)
        return min_trade_pct