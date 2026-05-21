import asyncio
from sqlalchemy import select
from app.core.config import settings
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.apps.arbitrage.models import Exchange, ExchangeSymbol, BaseInventory, QuoteInventory, ExchangeFee, \
    SymbolArbitrageSettings, Network


async def seed():
    engine = create_async_engine(str(settings.DATABASE_URL), echo=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        # Check if already seeded
        existing = await session.execute(select(Exchange).limit(1))
        if existing.first():
            print("✅ Exchanges already seeded – skipping.")
            return

        # ========== 1. Exchanges ==========
        wallex = Exchange(
            name="wallex", base_url="https://api.wallex.ir", orderbook_endpoint="/v1/depth",
            is_active=True, mode="simulator"
        )
        nobitex = Exchange(
            name="nobitex", base_url="https://apiv2.nobitex.ir", orderbook_endpoint="/v3/orderbook/{symbol}",
            is_active=True, mode="simulator"
        )
        bitpin = Exchange(
            name="bitpin", base_url="https://api.bitpin.org", orderbook_endpoint="/api/v1/mth/orderbook/{symbol}/",
            is_active=True, mode="simulator"
        )
        session.add_all([wallex, nobitex, bitpin])
        await session.commit()
        await session.refresh(wallex)
        await session.refresh(nobitex)
        await session.refresh(bitpin)
        print("✅ Exchanges created.")

        # ========== 2. Symbols ==========
        symbols = [
            ExchangeSymbol(exchange_id=wallex.id, original_symbol="TONTMN", common_symbol="TONIRT", price_conversion_factor=10.0),
            ExchangeSymbol(exchange_id=wallex.id, original_symbol="TONUSDT", common_symbol="TONUSDT", price_conversion_factor=1.0),
            ExchangeSymbol(exchange_id=wallex.id, original_symbol="USDTTMN", common_symbol="USDTIRT", price_conversion_factor=10.0),
            ExchangeSymbol(exchange_id=nobitex.id, original_symbol="TONIRT", common_symbol="TONIRT", price_conversion_factor=1.0),
            ExchangeSymbol(exchange_id=nobitex.id, original_symbol="TONUSDT", common_symbol="TONUSDT", price_conversion_factor=1.0),
            ExchangeSymbol(exchange_id=nobitex.id, original_symbol="USDTIRT", common_symbol="USDTIRT", price_conversion_factor=1.0),
            ExchangeSymbol(exchange_id=bitpin.id, original_symbol="TON_USDT", common_symbol="TONUSDT", price_conversion_factor=1.0),
            ExchangeSymbol(exchange_id=bitpin.id, original_symbol="TON_IRT", common_symbol="TONIRT", price_conversion_factor=10.0),
            ExchangeSymbol(exchange_id=bitpin.id, original_symbol="USDT_IRT", common_symbol="USDTIRT", price_conversion_factor=10.0),
        ]
        session.add_all(symbols)
        await session.commit()
        print("✅ Symbols created.")

        # ========== 3. Fees ==========
        fees = [
            ExchangeFee(exchange_id=wallex.id, quote_currency="IRT", taker_fee=0.003, maker_fee=0.0025),
            ExchangeFee(exchange_id=wallex.id, quote_currency="USDT", taker_fee=0.003, maker_fee=0.0025),
            ExchangeFee(exchange_id=nobitex.id, quote_currency="IRT", taker_fee=0.0025, maker_fee=0.0025),
            ExchangeFee(exchange_id=nobitex.id, quote_currency="USDT", taker_fee=0.0013, maker_fee=0.001),
            ExchangeFee(exchange_id=bitpin.id, quote_currency="IRT", taker_fee=0.0035, maker_fee=0.003),
            ExchangeFee(exchange_id=bitpin.id, quote_currency="USDT", taker_fee=0.0035, maker_fee=0.003),
        ]
        session.add_all(fees)
        await session.commit()
        print("✅ Fees created.")

        # ========== 4. Inventories ==========
        for exchange in [wallex, nobitex, bitpin]:
            for sym in ["TONIRT", "TONUSDT", "USDTIRT"]:
                session.add(BaseInventory(exchange_id=exchange.id, common_symbol=sym, balance=100.0))

        for exchange in [wallex, nobitex, bitpin]:
            session.add(QuoteInventory(exchange_id=exchange.id, currency="IRT", balance=10_000_000.0))
            session.add(QuoteInventory(exchange_id=exchange.id, currency="USDT", balance=10_000.0))

        await session.commit()
        print("✅ Inventories created.")

        # ========== 5. Networks ==========
        networks = [
            Network(symbol="TONIRT", network_name="TON", fee_per_transfer=0.1),
            Network(symbol="TONUSDT", network_name="TON", fee_per_transfer=0.1),
            Network(symbol="USDTIRT", network_name="TRC20", fee_per_transfer=0.7),
            Network(symbol="USDTIRT", network_name="BEP20", fee_per_transfer=0.7),
            Network(symbol="USDTIRT", network_name="Polygon", fee_per_transfer=0.7),
            Network(symbol="USDTIRT", network_name="ERC20", fee_per_transfer=5.0),
        ]
        session.add_all(networks)
        await session.commit()
        print("✅ Networks created.")

        # ========== 6. Symbol arbitrage settings (with default network IDs) ==========
        ton_network = (await session.execute(
            select(Network).where(Network.symbol == "TONIRT", Network.network_name == "TON"))).scalar_one()
        trc20_network = (await session.execute(
            select(Network).where(Network.symbol == "USDTIRT", Network.network_name == "TRC20"))).scalar_one()

        settings_rows = [
            SymbolArbitrageSettings(
                common_symbol="TONIRT",
                min_profit_percent=0.1,
                cutoff_threshold=0.1,
                min_trade_percent=0.20,
                min_trade_factor=0.3,
                valuability_factor=1.0,
                default_network_id=ton_network.id,
                is_active=True
            ),
            SymbolArbitrageSettings(
                common_symbol="TONUSDT",
                min_profit_percent=0.001,
                cutoff_threshold=0.005,
                min_trade_percent=0.20,
                min_trade_factor=0.3,
                valuability_factor=1.0,
                default_network_id=ton_network.id,
                is_active=True
            ),
            SymbolArbitrageSettings(
                common_symbol="USDTIRT",
                min_profit_percent=0.1,
                cutoff_threshold=0.1,
                min_trade_percent=0.20,
                min_trade_factor=0.3,
                valuability_factor=1.0,
                default_network_id=trc20_network.id,
                is_active=True
            ),
        ]
        session.add_all(settings_rows)
        await session.commit()
        print("✅ Database seeded successfully.")

if __name__ == "__main__":
    asyncio.run(seed())