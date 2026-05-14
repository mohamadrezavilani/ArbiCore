import asyncio
from sqlalchemy import select
from app.core.config import settings
from app.core.database import AsyncSessionLocal   # if you have a session maker
# If you don't have AsyncSessionLocal in database.py, use the engine directly:
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.apps.arbitrage.models import Exchange, ExchangeSymbol


async def seed():
    """
    Seed exchanges and symbols into the database.
    This function is idempotent – it checks for existing records before inserting.
    """
    # Use the same engine as your main app
    engine = create_async_engine(str(settings.DATABASE_URL), echo=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        # ========== 1. Check if any exchange already exists ==========
        existing_exchange = await session.execute(select(Exchange).limit(1))
        if existing_exchange.first():
            print("✅ Exchanges already seeded – skipping.")
            return

        # ========== 2. Create exchange records ==========
        wallex = Exchange(
            name="wallex",
            base_url="https://api.wallex.ir",
            orderbook_endpoint="/v1/depth",
            is_active=True,
            taker_fee=0.001,   # 0.1%
            maker_fee=0.001
        )
        nobitex = Exchange(
            name="nobitex",
            base_url="https://apiv2.nobitex.ir",
            orderbook_endpoint="/v3/orderbook/{symbol}",
            is_active=True,
            taker_fee=0.001,
            maker_fee=0.001
        )
        bitpin = Exchange(
            name="bitpin",
            base_url="https://api.bitpin.org",
            orderbook_endpoint="/api/v1/mth/orderbook/{symbol}/",
            is_active=True,
            taker_fee=0.0035,   # 0.35% acceptor fee
            maker_fee=0.003      # 0.3% placer fee
        )

        session.add_all([wallex, nobitex, bitpin])
        await session.commit()
        await session.refresh(wallex)
        await session.refresh(nobitex)
        await session.refresh(bitpin)

        # ========== 3. Create symbol records ==========
        # Wallex symbols
        wallex_symbols = [
            ExchangeSymbol(exchange_id=wallex.id, original_symbol="TONTMN", common_symbol="TONIRT", price_conversion_factor=10.0, is_active=True),
            ExchangeSymbol(exchange_id=wallex.id, original_symbol="TONUSDT", common_symbol="TONUSDT", price_conversion_factor=1.0, is_active=True),
            ExchangeSymbol(exchange_id=wallex.id, original_symbol="USDTTMN", common_symbol="USDTIRT", price_conversion_factor=10.0, is_active=True),
        ]

        # Nobitex symbols
        nobitex_symbols = [
            ExchangeSymbol(exchange_id=nobitex.id, original_symbol="TONIRT", common_symbol="TONIRT", price_conversion_factor=1.0, is_active=True),
            ExchangeSymbol(exchange_id=nobitex.id, original_symbol="TONUSDT", common_symbol="TONUSDT", price_conversion_factor=1.0, is_active=True),
            ExchangeSymbol(exchange_id=nobitex.id, original_symbol="USDTIRT", common_symbol="USDTIRT", price_conversion_factor=1.0, is_active=True),
        ]

        # Bitpin symbols
        bitpin_symbols = [
            ExchangeSymbol(exchange_id=bitpin.id, original_symbol="TON_USDT", common_symbol="TONUSDT", price_conversion_factor=1.0, is_active=True),
            ExchangeSymbol(exchange_id=bitpin.id, original_symbol="TON_IRT", common_symbol="TONIRT", price_conversion_factor=10.0, is_active=True),
            ExchangeSymbol(exchange_id=bitpin.id, original_symbol="USDT_IRT", common_symbol="USDTIRT", price_conversion_factor=10.0, is_active=True),
        ]

        session.add_all(wallex_symbols + nobitex_symbols + bitpin_symbols)
        await session.commit()
        print("✅ Database seeded successfully with exchanges and symbols (including Bitpin).")


if __name__ == "__main__":
    asyncio.run(seed())