import asyncio
import os
from dotenv import load_dotenv

# Load .env from the current directory (project root)
load_dotenv()

# Now import settings – it will find DATABASE_URL and SECRET_KEY
from app.core.config import settings
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.apps.arbitrage.models import Exchange, ExchangeSymbol


async def seed():
    engine = create_async_engine(str(settings.DATABASE_URL), echo=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        # Create exchanges
        wallex = Exchange(
            name="wallex",
            base_url="https://api.wallex.ir",
            orderbook_endpoint="/v1/depth",
            is_active=True
        )
        nobitex = Exchange(
            name="nobitex",
            base_url="https://apiv2.nobitex.ir",
            orderbook_endpoint="/v3/orderbook/{symbol}",
            is_active=True
        )
        session.add_all([wallex, nobitex])
        await session.commit()
        await session.refresh(wallex)
        await session.refresh(nobitex)

        # Wallex symbols
        wallex_symbols = [
            ExchangeSymbol(
                exchange_id=wallex.id,
                original_symbol="TONTMN",
                common_symbol="TONIRT",
                price_conversion_factor=10.0,
                is_active=True
            ),
            ExchangeSymbol(
                exchange_id=wallex.id,
                original_symbol="TONUSDT",
                common_symbol="TONUSDT",
                price_conversion_factor=1.0,
                is_active=True
            ),
            ExchangeSymbol(
                exchange_id=wallex.id,
                original_symbol="USDTTMN",
                common_symbol="USDTIRT",
                price_conversion_factor=10.0,
                is_active=True
            ),
        ]

        # Nobitex symbols
        nobitex_symbols = [
            ExchangeSymbol(
                exchange_id=nobitex.id,
                original_symbol="TONIRT",
                common_symbol="TONIRT",
                price_conversion_factor=1.0,
                is_active=True
            ),
            ExchangeSymbol(
                exchange_id=nobitex.id,
                original_symbol="TONUSDT",
                common_symbol="TONUSDT",
                price_conversion_factor=1.0,
                is_active=True
            ),
            ExchangeSymbol(
                exchange_id=nobitex.id,
                original_symbol="USDTIRT",
                common_symbol="USDTIRT",
                price_conversion_factor=1.0,
                is_active=True
            ),
        ]

        session.add_all(wallex_symbols + nobitex_symbols)
        await session.commit()
        print("✅ Database seeded successfully with exchanges and symbols.")


if __name__ == "__main__":
    asyncio.run(seed())