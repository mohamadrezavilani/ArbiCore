#!/usr/bin/env python3
"""
Test script to check order status on Wallex and Bitpin.
Use the client_order_id from your logs (e.g., sell_b89d6ee9) to test.
"""

import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.exchanges.wallex import WallexClient
from app.exchanges.bitpin import BitpinClient
from app.exchanges.nobitex import NobitexClient


async def test_bitpin_status(client_order_id: str):
    """Test Bitpin order status."""
    print(f"\n=== Testing Bitpin status for order: {client_order_id} ===")
    client = BitpinClient()
    try:
        result = await client.order_status(client_order_id)
        print(f"✅ Status result: {result}")
        print(f"   - status: {result.status}")
        print(f"   - filled_volume: {result.filled_volume}")
        print(f"   - fee: {result.fee}")
        print(f"   - executions: {result.executions}")
    except Exception as e:
        print(f"❌ Error: {e}")


async def test_wallex_status(client_order_id: str):
    """Test Wallex order status."""
    print(f"\n=== Testing Wallex status for order: {client_order_id} ===")
    client = WallexClient()
    try:
        result = await client.order_status(client_order_id)
        print(f"✅ Status result: {result}")
    except Exception as e:
        print(f"❌ Error: {e}")


async def test_nobitex_status(client_order_id: str):
    """Test Nobitex order status."""
    print(f"\n=== Testing Nobitex status for order: {client_order_id} ===")
    client = NobitexClient()
    try:
        result = await client.order_status(client_order_id)
        print(f"✅ Status result: {result}")
    except Exception as e:
        print(f"❌ Error: {e}")


async def main():
    # Use the client_order_id from your logs
    # Example: sell_b89d6ee9 (Bitpin) or buy_a2ddc588 (Wallex)
    bitpin_order = "sell_b89d6ee9"   # replace with your actual ID
    wallex_order = "buy_a2ddc588"    # replace with your actual ID

    # Test Bitpin
    await test_bitpin_status(bitpin_order)

    # Test Wallex
    await test_wallex_status(wallex_order)

    # Optionally test Nobitex
    # await test_nobitex_status("some_id")


if __name__ == "__main__":
    asyncio.run(main())