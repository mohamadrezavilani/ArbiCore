import pytest
from fastapi.testclient import TestClient
from app.main import app
from unittest.mock import patch, AsyncMock

client = TestClient(app)


@pytest.mark.asyncio
async def test_sync_balances_endpoint(db_session, live_exchange):
    # Patch the get_exchange_client at the service level
    with patch("app.apps.arbitrage.services.balance_sync.get_exchange_client") as mock_factory:
        mock_client = AsyncMock()
        mock_client.get_balances = AsyncMock(return_value={"IRT": 3000000, "USDT": 750})
        mock_factory.return_value = mock_client

        # Need to override the DB dependency to use our test session?
        # We'll use the app's dependency override in a more advanced setup.
        # For simplicity, we'll just test the endpoint with the real DB session
        # (but we must ensure the test DB is used).
        # We'll patch the get_db dependency to return our db_session.
        from app.core.database import get_db
        app.dependency_overrides[get_db] = lambda: db_session

        response = client.post("/api/v1/arbitrage/actions/sync-balances")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "test_exchange" in data["result"]

        # Clean up override
        app.dependency_overrides.clear()