#!/usr/bin/env python3
"""
Test script for exchange API clients – Live Order Placement.
Uses settings from .env (WALLEX_API_KEY, NOBITEX_API_KEY, NOBITEX_API_SECRET, BITPIN_API_KEY, BITPIN_API_SECRET)
Place small test orders to verify order placement, status, and cancellation.
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.exchanges.wallex import WallexClient
from app.exchanges.nobitex import NobitexClient
from app.exchanges.bitpin import BitpinClient
from app.exchanges.base import OrderResult

# ========== CONFIGURATION ==========
# Use very small amounts for testing!
TEST_AMOUNT = 1  # 0.001 of base currency (e.g., TON)

# Symbols that exist on each exchange (adjust if needed)
TEST_SYMBOLS = {
    "wallex": "USDTTMN",
    "nobitex": "USDTIRT",
    "bitpin": "USDT_IRT"
}


# ========== HELPER FUNCTIONS ==========
async def test_exchange(name, client, symbol, amount):
    print(f"\n{'=' * 50}\nTesting {name.upper()}\n{'=' * 50}")

    # 1. Check balances (to confirm client works)
    try:
        balances = await client.get_balances()
        print(f"✅ Balances: {balances}")
    except Exception as e:
        print(f"❌ Balance fetch error: {e}")
        return

    # 2. Place a market buy order
    client_order_id = f"{int(asyncio.get_event_loop().time())}"
    try:
        result = await client.place_market_order(
            symbol=symbol,
            side="buy",
            amount=amount,
            client_order_id=client_order_id
        )
        print(f"✅ Order placed: {result}")
    except Exception as e:
        print(f"❌ Place order error: {e}")
        return

    # 3. Check order status
    try:
        status = await client.order_status(result.client_order_id)
        print(f"✅ Order status: {status}")
    except Exception as e:
        print(f"❌ Status error: {e}")
        status = None

    # 4. Cancel if still open
    if status and status.status in ("pending", "partial"):
        try:
            cancelled = await client.cancel_order(result.client_order_id)
            print(f"✅ Cancel {'succeeded' if cancelled else 'failed'}")
        except Exception as e:
            print(f"❌ Cancel error: {e}")
    else:
        print("Order already filled or cancelled – skipping cancellation")

    print(f"\n✅ {name} test completed.\n")


async def main():
    print("⚠️  WARNING: This script will place REAL MARKET ORDERS on exchanges!")
    print("⚠️  Make sure you have set correct API keys in .env and use very small amounts.\n")

    # Test Wallex
    # await test_exchange("wallex", WallexClient(), TEST_SYMBOLS["wallex"], TEST_AMOUNT)

    # Test Nobitex
    await test_exchange("nobitex", NobitexClient(), TEST_SYMBOLS["nobitex"], TEST_AMOUNT)

    # Test Bitpin
    # await test_exchange("bitpin", BitpinClient(), TEST_SYMBOLS["bitpin"], TEST_AMOUNT)

    print("All tests finished.")


if __name__ == "__main__":
    asyncio.run(main())