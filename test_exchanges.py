#!/usr/bin/env python3
"""
Test script for exchange API clients.
Before running, set your API credentials in environment variables or directly in the script.
Use only for testing with small amounts!
"""

import asyncio
import os
import sys
from decimal import Decimal

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.exchanges.wallex import WallexClient
from app.exchanges.nobitex import NobitexClient
from app.exchanges.bitpin import BitpinClient
from app.exchanges.base import OrderResult

# ==================== CONFIGURATION ====================
# Replace with your actual API keys
WALLEX_API_KEY = os.getenv("WALLEX_API_KEY", "your_api_key_here")
NOBITEX_TOKEN = os.getenv("NOBITEX_TOKEN", "your_api_key_here")
BITPIN_TOKEN = os.getenv("BITPIN_TOKEN", "Alhn1v770loFYouF2fdt94KjilVIYrSin8NrBjuRUzXzYPYguK1g8a7kAEFsJwIVtZOrZIKl4tBzWOhMo40We9poCxMamI9baL5wx2yIPXoYPldSn7CPrPatDeJc3CbW")

# Test symbols and amounts (use very small amounts!)
TEST_SYMBOLS = {
    "wallex": "TONUSDT",      # original symbol
    "nobitex": "TONUSDT",     # original symbol (or TONIRT)
    "bitpin": "TON_USDT"      # original symbol
}
TEST_AMOUNT = 0.001  # Very small amount (e.g., 0.001 TON)

# ==================== Helper functions ====================
async def test_balances(client, name):
    print(f"\n--- Testing {name} balances ---")
    try:
        balances = await client.get_balances()
        print(f"✅ Balances fetched: {balances}")
        return True
    except Exception as e:
        print(f"❌ Error fetching balances: {e}")
        return False

async def test_place_order(client, name, symbol, amount):
    print(f"\n--- Testing {name} place order (buy {amount} {symbol}) ---")
    try:
        client_order_id = f"test_{name}_{int(asyncio.get_event_loop().time())}"
        result: OrderResult = await client.place_market_order(
            symbol=symbol,
            side="buy",
            amount=amount,
            client_order_id=client_order_id
        )
        print(f"✅ Order placed: {result}")
        return result
    except Exception as e:
        print(f"❌ Error placing order: {e}")
        return None

async def test_order_status(client, name, client_order_id):
    print(f"\n--- Testing {name} order status for {client_order_id} ---")
    try:
        status = await client.order_status(client_order_id)
        print(f"✅ Order status: {status}")
        return status
    except Exception as e:
        print(f"❌ Error getting status: {e}")
        return None

async def test_cancel_order(client, name, client_order_id):
    print(f"\n--- Testing {name} cancel order {client_order_id} ---")
    try:
        success = await client.cancel_order(client_order_id)
        print(f"✅ Cancel {'succeeded' if success else 'failed'}")
        return success
    except Exception as e:
        print(f"❌ Error cancelling: {e}")
        return False

# ==================== Main test suite ====================
async def run_exchange_tests(client_class, api_key, api_secret, exchange_name, symbol):
    if not api_key or api_key.startswith("your_"):
        print(f"\n⚠️ Skipping {exchange_name}: API key not set")
        return

    client = client_class(api_key, api_secret)
    print(f"\n{'='*50}\nTesting {exchange_name.upper()}\n{'='*50}")

    # 1. Test balances
    if not await test_balances(client, exchange_name):
        return

    # 2. Test order placement
    order = await test_place_order(client, exchange_name, symbol, TEST_AMOUNT)
    if not order:
        return

    # 3. Test order status
    status = await test_order_status(client, exchange_name, order.client_order_id)
    if status and status.status in ("pending", "partial"):
        print("Order is pending/partial – you may need to wait or check manually")
        # Optionally, wait a few seconds and check again
        await asyncio.sleep(2)
        status2 = await test_order_status(client, exchange_name, order.client_order_id)
        if status2 and status2.status == "filled":
            print("Order filled after waiting")

    # 4. Test cancel (only if order still open)
    if status and status.status in ("pending", "partial"):
        await test_cancel_order(client, exchange_name, order.client_order_id)
    else:
        print("Order already filled or cancelled – skipping cancellation test")

    print(f"\n✅ {exchange_name} tests completed.\n")

async def main():
    print("Starting exchange API tests...")
    print("WARNING: These tests will place REAL orders on exchanges!")
    print("Make sure you have set correct API keys and use very small amounts.\n")

    # Test Wallex
    # await run_exchange_tests(WallexClient, WALLEX_API_KEY, "", "wallex", TEST_SYMBOLS["wallex"])

    # Test Nobitex
    # await run_exchange_tests(NobitexClient, NOBITEX_TOKEN, "MWXXesvZbjj8qWe9bFGSxsIfDtjNZJgGr3Yl8PxHvak=", "nobitex", TEST_SYMBOLS["nobitex"])

    # Test Bitpin
    await run_exchange_tests(BitpinClient, BITPIN_TOKEN, "ED641RtDMMiM6cEddzpiiiOAYPr1MoYwAcfVl4eNBELePABLcGoXXPhakVjbOviHWUAYPjBuKz1hk9dfMaTZ7Y4haLV3W3ZRzRbGoVQ88ye4ArGiHVk2IziMvepqGROg", "bitpin", TEST_SYMBOLS["bitpin"])

    print("All tests finished.")

if __name__ == "__main__":
    asyncio.run(main())