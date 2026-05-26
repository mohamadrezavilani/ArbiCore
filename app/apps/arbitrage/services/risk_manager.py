import logging
from app.apps.arbitrage.models import SymbolArbitrageSettings

logger = logging.getLogger(__name__)


class RiskManager:
    @staticmethod
    def calculate_trade_percent(
            net_gain: float,
            network_commission_quote: float,
            params: SymbolArbitrageSettings,
            vol: float,
            weight: float,
            current_price: float,
            network_fee_base: float,
            max_base_pool: float  # <-- renamed: max base inventory (USDTIRT balance)
    ) -> float:
        """
        Dynamic cutoff: cutoff = (vol * current_price * network_fee_base) / (0.8 * max_base_pool)
        """
        target_base_pool = max_base_pool * 0.8
        if target_base_pool <= 0:
            logger.warning("target_base_pool <= 0, cutoff set to 0")
            base_cutoff = 0.0
        else:
            base_cutoff = (vol * current_price * network_fee_base) / target_base_pool

        cutoff = base_cutoff * weight
        logger.info(f"base_cutoff: {base_cutoff:.6f} = cutoff: {cutoff:.6f} * weight: {weight}")
        logger.info(
            f"Params: vol={vol:.4f}, price={current_price:.2f}, fee_base={network_fee_base:.4f}, max_base={max_base_pool:.2f}")

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