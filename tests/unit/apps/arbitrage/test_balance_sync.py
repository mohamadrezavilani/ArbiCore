import pytest
from unittest.mock import AsyncMock, patch
from app.apps.arbitrage.services.balance_sync import BalanceSyncService


@pytest.mark.asyncio
async def test_sync_all_balances_updates_inventory():
    # Mock the database session
    mock_session = AsyncMock()

    # Mock the exchange query to return a list of exchanges
    mock_exchange_result = AsyncMock()
    mock_exchange_result.scalars.return_value.all.return_value = [
        type('Exchange', (), {'id': 'ex1', 'name': 'wallex', 'mode': 'live'})()
    ]
    mock_session.execute.return_value = mock_exchange_result

    # Mock the get_exchange_client factory
    with patch("app.apps.arbitrage.services.balance_sync.get_exchange_client") as mock_factory:
        mock_client = AsyncMock()
        mock_client.get_balances = AsyncMock(return_value={"IRT": 2000000, "USDT": 1000})
        mock_factory.return_value = mock_client

        # Mock the symbol existence query
        mock_symbol_result = AsyncMock()
        mock_symbol_result.scalar_one_or_none.return_value = type('Symbol', (), {'id': 'sym1'})()
        mock_session.execute.return_value = mock_symbol_result

        # Run the service
        result = await BalanceSyncService.sync_all_balances(mock_session)

        # Verify that the session's add and commit were called appropriately
        assert mock_session.add.called
        assert mock_session.commit.called

        # Check result summary
        assert "wallex" in result
        assert result["wallex"]["IRT"] == 2000000