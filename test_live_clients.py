import asyncio
import os
from app.exchanges.wallex import WallexClient
from app.exchanges.nobitex import NobitexClient
from app.exchanges.bitpin import BitpinClient

async def test_client(name, client):
    print(f"\n--- Testing {name} ---")
    try:
        balances = await client.get_balances()
        print(f"✅ Success: {balances}")
        return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

async def main():
    # Wallex
    wallex = WallexClient()   # reads API keys from settings
    await test_client("wallex", wallex)

    # Nobitex
    nobitex = NobitexClient()
    await test_client("nobitex", nobitex)

    # Bitpin
    bitpin = BitpinClient()
    await test_client("bitpin", bitpin)

if __name__ == "__main__":
    asyncio.run(main())