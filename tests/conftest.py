import pytest
import asyncio
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.core.config import settings
from app.models.base import Base
from app.apps.arbitrage.models import Exchange, ExchangeSymbol, BaseInventory, QuoteInventory

# Use a test database (in-memory SQLite or separate PostgreSQL)
TEST_DATABASE_URL = "sqlite+aiosqlite:///./test.db"

@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for all async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def test_engine():
    """Create engine and create tables once for the test session."""
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest.fixture
async def db_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Provide a new async session per test, rolling back after each test."""
    async_session = async_sessionmaker(test_engine, expire_on_commit=False)
    async with async_session() as session:
        # Start a transaction and rollback after test to keep DB clean
        async with session.begin():
            yield session
        await session.rollback()  # Rollback after test (or close)

@pytest.fixture
async def live_exchange(db_session: AsyncSession):
    """Create a live exchange with a symbol mapping for testing."""
    exchange = Exchange(
        name="test_exchange",
        base_url="https://test.com",
        orderbook_endpoint="/depth",
        mode="live",
        is_active=True
    )
    db_session.add(exchange)
    await db_session.flush()
    # Add symbol mapping for USDTIRT
    symbol = ExchangeSymbol(
        exchange_id=exchange.id,
        original_symbol="USDTIRT",
        common_symbol="USDTIRT",
        price_conversion_factor=1.0
    )
    db_session.add(symbol)
    await db_session.commit()
    return exchange

@pytest.fixture
def mock_exchange_client(mocker):
    """Return a mocked exchange client that can be configured per test."""
    mock = mocker.AsyncMock()
    mock.get_balances = mocker.AsyncMock(return_value={"IRT": 1000000.0, "USDT": 500.0})
    return mock