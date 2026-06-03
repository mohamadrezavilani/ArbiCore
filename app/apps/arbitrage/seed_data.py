import asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import settings
from app.models.base import Base  # Import your declarative base
# Import all models so that Base.metadata knows about them
from app.apps.arbitrage.models import (
    Exchange, ExchangeSymbol, BaseInventory, QuoteInventory, ExchangeFee,
    SymbolArbitrageSettings, Network, ExchangePairWeight
)

async def seed():
    engine = create_async_engine(str(settings.DATABASE_URL), echo=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    # ✅ Create tables if they don't exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Tables created (if not existed).")

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
            ExchangeSymbol(exchange_id=wallex.id, original_symbol="USDTTMN", common_symbol="USDTIRT", price_conversion_factor=10.0),
            ExchangeSymbol(exchange_id=nobitex.id, original_symbol="USDTIRT", common_symbol="USDTIRT", price_conversion_factor=1.0),
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
            for sym in ["USDTIRT"]:
                session.add(BaseInventory(exchange_id=exchange.id, common_symbol=sym, balance=1000.0))

        for exchange in [wallex, nobitex, bitpin]:
            session.add(QuoteInventory(exchange_id=exchange.id, currency="IRT", balance=1_000_000_000.0))
            # session.add(QuoteInventory(exchange_id=exchange.id, currency="USDT", balance=10.0))

        await session.commit()
        print("✅ Inventories created.")

        # ========== 5. Networks ==========
        networks = [
            Network(symbol="USDTIRT", network_name="TRC20", fee_per_transfer=0.7),
        ]
        session.add_all(networks)
        await session.commit()
        print("✅ Networks created.")

        # ========== 6. Symbol arbitrage settings ==========
        trc20_network = (await session.execute(
            select(Network).where(Network.symbol == "USDTIRT", Network.network_name == "TRC20")
        )).scalar_one()

        # ========== 7. Exchange Pair Weights ==========
        exchange_list = [wallex, nobitex, bitpin]
        for i in range(len(exchange_list)):
            for j in range(i + 1, len(exchange_list)):
                a = exchange_list[i]
                b = exchange_list[j]
                # Ensure canonical order: exchange_a_id < exchange_b_id
                if a.id < b.id:
                    pair = ExchangePairWeight(
                        exchange_a_id=a.id,
                        exchange_b_id=b.id,
                        weight=0.5,
                        last_buy_exchange_id=None,
                        last_sell_exchange_id=None
                    )
                else:
                    pair = ExchangePairWeight(
                        exchange_a_id=b.id,
                        exchange_b_id=a.id,
                        weight=0.5,
                        last_buy_exchange_id=None,
                        last_sell_exchange_id=None
                    )
                session.add(pair)
        await session.commit()
        print("✅ Exchange pair weights initialized (0.5 each).")

        settings_rows = [
            SymbolArbitrageSettings(
                common_symbol="USDTIRT",
                min_profit_percent=0.001,
                cutoff_threshold=0,
                min_trade_percent=0.20,
                min_trade_factor=0.3,
                valuability_factor=1.0,
                default_network_id=trc20_network.id,
                is_active=True,
                opportunistic_rebalance_enabled=False,
                opportunistic_rebalance_max_loss_percent=50.0,
                market_rebalance_enabled=True,
                market_rebalance_amount_percent=20.0,
                market_rebalance_max_spread_percent=0.6,
                market_rebalance_imbalance_ratio=0.25,
                market_rebalance_cooldown_seconds=60,
                last_rebalance_time=None,
                rebalance_pending=False,
                # NEW
                quote_rebalance_enabled=True,
                quote_rebalance_amount_percent=20.0,
                quote_rebalance_max_spread_percent=0.6,
                quote_rebalance_imbalance_ratio=0.25,
                quote_rebalance_cooldown_seconds=300,
                last_quote_rebalance_time=None,
                quote_rebalance_pending=False
            )
        ]
        session.add_all(settings_rows)
        await session.commit()
        print("✅ Database seeded successfully.")

if __name__ == "__main__":
    asyncio.run(seed())