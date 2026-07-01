import asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import settings
from app.models.base import Base
from app.apps.arbitrage.models import (
    Exchange, ExchangeSymbol, BaseInventory, QuoteInventory, ExchangeFee,
    SymbolArbitrageSettings, Network, ExchangePairWeight
)

async def seed():
    engine = create_async_engine(str(settings.DATABASE_URL), echo=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Tables created (if not existed).")

    async with async_session() as session:
        existing = await session.execute(select(Exchange).limit(1))
        if existing.first():
            print("✅ Exchanges already seeded – skipping.")
            return

        # ========== 1. Exchanges ==========
        wallex = Exchange(
            name="wallex", base_url="https://api.wallex.ir", orderbook_endpoint="/v1/depth",
            is_active=True, mode="live"
        )
        nobitex = Exchange(
            name="nobitex", base_url="https://apiv2.nobitex.ir", orderbook_endpoint="/v3/orderbook/{symbol}",
            is_active=False, mode="live"
        )
        bitpin = Exchange(
            name="bitpin", base_url="https://api.bitpin.org", orderbook_endpoint="/api/v1/mth/orderbook/{symbol}/",
            is_active=True, mode="live"
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
        # for exchange in [wallex, nobitex, bitpin]:
        #     for sym in ["USDTIRT"]:
        #         session.add(BaseInventory(exchange_id=exchange.id, common_symbol=sym, balance=1000.0))

        # for exchange in [wallex, nobitex, bitpin]:
        #     session.add(QuoteInventory(exchange_id=exchange.id, currency="IRT", balance=1_000_000_000.0))


        # ========== 4. Networks ==========
        networks = [
            Network(symbol="USDTIRT", network_name="TRC20", fee_per_transfer=0.7),
        ]
        session.add_all(networks)
        await session.commit()
        print("✅ Networks created.")

        # ========== 5. Symbol arbitrage settings ==========
        trc20_network = (await session.execute(
            select(Network).where(Network.symbol == "USDTIRT", Network.network_name == "TRC20")
        )).scalar_one()

        settings_rows = [
            SymbolArbitrageSettings(
                common_symbol="USDTIRT",

                # ---------- ARBITRAGE THRESHOLDS ----------
                # Minimum profit % required to execute a trade.
                # Set to 0.7% to ensure you only trade when the spread is large.
                # This compensates for using 100% of your volume.
                min_profit_percent=0.7,

                # Disables the net‑gain cutoff. You don't want a fixed cutoff
                # when you're using 100% of your available depth.
                cutoff_threshold=0,

                # ---------- VOLUME CONTROL (RISK MANAGER) ----------
                # Fraction of the max available volume to trade.
                # 1.0 = 100% → use all available funds on every qualifying trade.
                min_trade_percent=1.0,

                # Interpolation factor. 0 means the risk manager scales linearly
                # from 0 net gain upward. With min_trade_percent=1.0, it immediately
                # returns 1.0 for any positive net gain → full volume on every trade.
                min_trade_factor=0,

                # Multiplier for "full threshold". Kept at 1.0 (neutral).
                valuability_factor=1.0,

                # ---------- NETWORK (NOT USED FOR REBALANCING) ----------
                default_network_id=trc20_network.id,  # TRC20 for possible future withdrawals
                is_active=True,

                # ---------- OPPORTUNISTIC REBALANCE (DISABLED) ----------
                opportunistic_rebalance_enabled=False,
                opportunistic_rebalance_max_loss_percent=50.0,

                # ---------- BASE REBALANCING (USDT) ----------
                market_rebalance_enabled=True,

                # Amount to move, as % of the average balance.
                # 100.0% = move the full average → with 2 exchanges, this perfectly
                # equalizes them (e.g., 10 & 0 → avg=5 → move 5 → both become 5).
                market_rebalance_amount_percent=100.0,

                # Maximum allowable spread for rebalancing.
                # If rebalancing would cost more than 0.2%, postpone it.
                # This protects you from rebalancing when the loss is too high.
                market_rebalance_max_spread_percent=0.2,

                # Trigger rebalancing only when the poorest exchange has less than
                # 25% of the average balance. Prevents tiny, unnecessary rebalances.
                market_rebalance_imbalance_ratio=0.25,

                # Wait 5 minutes after a rebalance before allowing another one.
                # Prevents rapid oscillation between exchanges.
                market_rebalance_cooldown_seconds=300,

                last_rebalance_time=None,
                rebalance_pending=False,

                # ---------- QUOTE REBALANCING (IRT) ----------
                quote_rebalance_enabled=True,

                # Move 100% of the average IRT balance → perfectly equalises IRT
                # across exchanges (just like USDT above).
                quote_rebalance_amount_percent=100.0,

                # Same spread protection as base rebalancing.
                quote_rebalance_max_spread_percent=0.2,

                # Trigger when poorest IRT exchange < 25% of average IRT.
                quote_rebalance_imbalance_ratio=0.25,

                # Cooldown 5 minutes.
                quote_rebalance_cooldown_seconds=300,

                last_quote_rebalance_time=None,
                quote_rebalance_pending=False
            )
        ]
        session.add_all(settings_rows)
        await session.commit()
        print("✅ Settings created (rebalancing enabled).")

        # ========== 6. Exchange Pair Weights ==========
        exchange_list = [wallex, nobitex, bitpin]
        for i in range(len(exchange_list)):
            for j in range(i + 1, len(exchange_list)):
                a = exchange_list[i]
                b = exchange_list[j]
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
        print("✅ Exchange pair weights initialized.")

        print("✅ Database seeded successfully.")

if __name__ == "__main__":
    asyncio.run(seed())