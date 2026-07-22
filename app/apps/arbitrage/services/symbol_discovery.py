import aiohttp
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
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
        "url": "https://api.bitpin.org/api/v1/mkt/tickers/",
    }
}

async def discover_and_seed_symbols(db: AsyncSession):
    # Step 1: Get active exchanges
    stmt = select(Exchange.id, Exchange.name).where(Exchange.is_active == True)
    result = await db.execute(stmt)
    exchanges = result.all()  # list of (id, name)

    exchange_symbols_map = {}  # exchange_name -> set(common_symbols)

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
                    exchange_symbols_map[exchange_name] = set(filtered.keys())
                    logger.info(f"{exchange_name}: discovered {len(filtered)} tradable symbols")

                    # Insert all mappings (active=False initially)
                    for common, original in filtered.items():
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
                                is_active=False  # will activate later
                            )
                            db.add(mapping)
                    await db.commit()
        except Exception as e:
            logger.exception(f"Error discovering symbols for {exchange_name}: {e}")
            await db.rollback()

    # Step 2: Compute cross-exchange symbols
    common_symbol_counts = {}
    for exchange_name, symbols in exchange_symbols_map.items():
        for sym in symbols:
            common_symbol_counts[sym] = common_symbol_counts.get(sym, 0) + 1

    active_symbols = {sym for sym, count in common_symbol_counts.items() if count >= 2}
    logger.info(f"Symbols appearing on 2+ exchanges: {len(active_symbols)}")

    # Step 3: Update all existing mappings: activate only those in active_symbols
    for exchange_id, exchange_name in exchanges:
        stmt = select(ExchangeSymbol).where(ExchangeSymbol.exchange_id == exchange_id)
        mappings = (await db.execute(stmt)).scalars().all()
        for mapping in mappings:
            mapping.is_active = mapping.common_symbol in active_symbols
    await db.commit()

    logger.info(f"Activated symbols on at least 2 exchanges. Total active: {len(active_symbols)}")

def parse_symbols(exchange_name: str, data: dict) -> dict:
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