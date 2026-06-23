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
            max_base_pool: float
    ) -> float:

        # if max_base_pool <= 0:
        #     base_cutoff = 0.0
        # else:
        #     # More conservative: multiply by 1.5
        #     base_cutoff = (vol * current_price * network_fee_base * 1.5) / (max_base_pool * 0.8)
        #
        # logger.info(f"base_cutoff : {vol} *  {current_price} * {network_fee_base} * 1.5 / {max_base_pool} * 0.8 = {base_cutoff}")

        cutoff = 0
        # cutoff = base_cutoff * weight
        min_trade_pct = float(params.min_trade_percent)
        min_trade_factor = float(params.min_trade_factor)
        valuability_factor = float(params.valuability_factor)

        min_threshold = min_trade_factor * network_commission_quote
        full_threshold = valuability_factor * network_commission_quote

        logger.info(f"cutoff : {cutoff}, min_trade_pct: {min_trade_pct}, min_trade_factor: {min_trade_factor}, valuability_factor: {valuability_factor}, min_threshold: {min_threshold}, full_threshold: {full_threshold}")

        if net_gain <= 0 or net_gain < cutoff:
            return 0.0
        if net_gain <= min_threshold:
            return min_trade_pct
        if net_gain >= full_threshold:
            return 1.0

        if full_threshold > min_threshold:
            slope = (1.0 - min_trade_pct) / (full_threshold - min_threshold)
            logger.info(f"slope: {slope}, trade_pct:{min_trade_pct + slope * (net_gain - min_threshold)}")
            logger.info(f"return slope: {min_trade_pct + slope * (net_gain - min_threshold)}")
            return min_trade_pct + slope * (net_gain - min_threshold)
        logger.info(f"return min_trade_pct: {min_trade_pct}")
        return min_trade_pct