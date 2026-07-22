import asyncio

import aiohttp
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from app.apps.arbitrage.models import Exchange, ExchangeSymbol

logger = logging.getLogger(__name__)

EXCHANGE_TICKER_CONFIG = {
    "wallex": {
        "url": "https://api.wallex.ir/v1/markets",
    },
    "nobitex": {
        "url": "https://apiv2.nobitex.ir/v2/markets",
    },
    "bitpin": {
        "url": "https://api.bitpin.ir/api/v1/mkt/tickers/",
    }
}

# Orderbook test URLs for validation
EXCHANGE_ORDERBOOK_URL = {
    "wallex": "https://api.wallex.ir/v1/depth?symbol={symbol}",
    "nobitex": "https://apiv2.nobitex.ir/v3/orderbook/{symbol}",
    "bitpin": "https://api.bitpin.ir/api/v1/mth/orderbook/{symbol}/",
}


async def discover_and_seed_symbols(db: AsyncSession):
    """
    Discover symbols from exchanges, validate orderbook availability,
    and only activate those that exist on 2+ exchanges with working orderbooks.
    """
    # Step 1: Get active exchanges
    stmt = select(Exchange.id, Exchange.name).where(Exchange.is_active == True)
    result = await db.execute(stmt)
    exchanges = result.all()

    exchange_symbols_map = {}  # exchange_name -> {common_symbol: original_symbol}
    exchange_raw_symbols = {}  # exchange_name -> {common_symbol: original_symbol} (before validation)

    for exchange_id, exchange_name in exchanges:
        config = EXCHANGE_TICKER_CONFIG.get(exchange_name)
        if not config:
            continue

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(config["url"], timeout=10) as resp:
                    if resp.status != 200:
                        logger.error(f"Ticker fetch {exchange_name} failed: {resp.status}")
                        continue
                    data = await resp.json()
                    symbols = parse_symbols(exchange_name, data)
                    # Keep only IRT/USDT pairs
                    filtered = {k: v for k, v in symbols.items() if "IRT" in k or "USDT" in k}
                    exchange_raw_symbols[exchange_name] = filtered
                    logger.info(f"{exchange_name}: discovered {len(filtered)} tradable symbols from tickers")
        except Exception as e:
            logger.exception(f"Error discovering symbols for {exchange_name}: {e}")

    # Step 2: Validate orderbook availability for each symbol
    logger.info("Validating orderbook endpoints...")
    validated_symbols = {}  # exchange_name -> {common_symbol: original_symbol}

    for exchange_name, symbols in exchange_raw_symbols.items():
        validated = {}
        test_count = 0
        fail_count = 0

        async with aiohttp.ClientSession() as session:
            for common_symbol, original_symbol in symbols.items():
                url_template = EXCHANGE_ORDERBOOK_URL.get(exchange_name)
                if not url_template:
                    continue

                url = url_template.format(symbol=original_symbol)

                try:
                    async with session.get(url, timeout=10) as resp:
                        if resp.status == 200:
                            validated[common_symbol] = original_symbol
                            test_count += 1
                        elif resp.status == 404:
                            logger.debug(
                                f"Orderbook 404 for {exchange_name}/{original_symbol} "
                                f"(common: {common_symbol}) - skipping"
                            )
                            fail_count += 1
                        else:
                            logger.warning(
                                f"Orderbook {resp.status} for {exchange_name}/{original_symbol}"
                            )
                            fail_count += 1
                except Exception as e:
                    logger.debug(f"Error validating {exchange_name}/{original_symbol}: {e}")
                    fail_count += 1

                # Rate limit validation requests
                await asyncio.sleep(1.5 if exchange_name == "bitpin" else 0.1)

        validated_symbols[exchange_name] = validated
        logger.info(
            f"{exchange_name}: {len(validated)} valid orderbooks, "
            f"{fail_count} failed out of {len(symbols)} discovered"
        )

    # Step 3: Insert all mappings (inactive initially)
    for exchange_id, exchange_name in exchanges:
        symbols = validated_symbols.get(exchange_name, {})
        for common, original in symbols.items():
            existing = await db.execute(
                select(ExchangeSymbol).where(
                    ExchangeSymbol.exchange_id == exchange_id,
                    ExchangeSymbol.original_symbol == original
                )
            )
            if not existing.scalar_one_or_none():
                factor = 10.0 if exchange_name == "wallex" and "TMN" in original else 1.0
                mapping = ExchangeSymbol(
                    exchange_id=exchange_id,
                    original_symbol=original,
                    common_symbol=common,
                    price_conversion_factor=factor,
                    is_active=False  # Will activate later
                )
                db.add(mapping)
        await db.commit()

    # Step 4: Compute cross-exchange symbols (must exist on 2+ exchanges)
    common_symbol_counts = {}
    for exchange_name, symbols in validated_symbols.items():
        for sym in symbols:
            common_symbol_counts[sym] = common_symbol_counts.get(sym, 0) + 1

    active_symbols = {sym for sym, count in common_symbol_counts.items() if count >= 2}
    logger.info(f"Symbols with valid orderbooks on 2+ exchanges: {len(active_symbols)}")

    # Step 5: Activate only cross-exchange symbols with working orderbooks
    for exchange_id, exchange_name in exchanges:
        stmt = select(ExchangeSymbol).where(ExchangeSymbol.exchange_id == exchange_id)
        mappings = (await db.execute(stmt)).scalars().all()
        for mapping in mappings:
            # Only activate if:
            # 1. Symbol is in validated_symbols for this exchange (has working orderbook)
            # 2. Symbol exists on 2+ exchanges (arbitrage possible)
            is_valid = mapping.common_symbol in validated_symbols.get(exchange_name, {})
            is_cross_exchange = mapping.common_symbol in active_symbols
            mapping.is_active = is_valid and is_cross_exchange

            if mapping.is_active:
                logger.debug(f"Activated: {exchange_name}/{mapping.common_symbol}")

    await db.commit()
    logger.info(f"Activated {len(active_symbols)} cross-exchange symbols with working orderbooks")


def parse_symbols(exchange_name: str, data: dict) -> dict:
    """Parse exchange ticker response into common_symbol -> original_symbol mapping."""
    result = {}
    if exchange_name == "wallex":
        symbols_dict = data.get("result", {}).get("symbols", {})
        for orig, info in symbols_dict.items():
            if orig.endswith("TMN"):
                base = orig.replace("TMN", "")
                common = f"{base}IRT"
            elif orig.endswith("USDT"):
                base = orig.replace("USDT", "")
                common = f"{base}USDT"
            else:
                common = orig
            result[common] = orig
    elif exchange_name == "nobitex":
        if data.get("status") == "ok":
            for symbol in data.get("markets", {}).keys():
                result[symbol] = symbol
    elif exchange_name == "bitpin":
        for item in data:
            if isinstance(item, dict):
                orig = item.get("symbol")
                if orig:
                    common = orig.replace("_", "")
                    result[common] = orig
    return result