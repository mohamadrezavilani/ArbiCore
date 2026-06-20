import pytest
from unittest.mock import patch
from app.apps.arbitrage.services.balance_sync import BalanceSyncService
from app.apps.arbitrage.models import BaseInventory, QuoteInventory
from sqlalchemy import select


@pytest.mark.asyncio
async def test_sync_all_balances_with_db(db_session, live_exchange, mock_exchange_client):
    # Patch the factory to return our mock client
    with patch("app.apps.arbitrage.services.balance_sync.get_exchange_client") as mock_factory:
        mock_factory.return_value = mock_exchange_client

        # Run sync
        result = await BalanceSyncService.sync_all_balances(db_session)

        # Verify DB updates
        # Check quote inventory (IRT)
        quote_inv = await db_session.execute(
            select(QuoteInventory).where(
                QuoteInventory.exchange_id == live_exchange.id,
                QuoteInventory.currency == "IRT"
            )
        )
        quote = quote_inv.scalar_one_or_none()
        assert quote is not None
        assert quote.balance == 1000000.0

        # Check base inventory (USDTIRT)
        base_inv = await db_session.execute(
            select(BaseInventory).where(
                BaseInventory.exchange_id == live_exchange.id,
                BaseInventory.common_symbol == "USDTIRT"
            )
        )
        base = base_inv.scalar_one_or_none()
        assert base is not None
        assert base.balance == 500.0

        # Check result summary
        assert "test_exchange" in result
        assert result["test_exchange"]["IRT"] == 1000000.0