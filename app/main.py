import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.core.config import settings
from app.core.database import engine
from app.core.handlers import register_exception_handlers
from app.core.logging import setup_logging
from app.apps.arbitrage.api import router as arbitrage_router
from app.apps.arbitrage.tasks import periodic_arbitrage_poll
from app.apps.arbitrage.seed_data import seed   # <-- import your seeding function


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    setup_logging()

    # 1. Create all tables (if they don't exist)
    async with engine.begin() as conn:
        from app.models.base import Base
        from app.apps.arbitrage import models   # registers models with Base
        await conn.run_sync(Base.metadata.create_all)

    # 2. Seed initial data (exchanges, symbols) – runs only once
    await seed()

    # 3. Start background arbitrage polling
    poll_task = asyncio.create_task(periodic_arbitrage_poll())

    yield

    # Shutdown
    poll_task.cancel()
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        debug=settings.DEBUG,
        version="0.1.0",
        lifespan=lifespan,
    )

    register_exception_handlers(app)

    app.include_router(
        arbitrage_router,
        prefix=f"{settings.API_V1_PREFIX}/arbitrage",
        tags=["arbitrage"]
    )

    return app


# Global FastAPI instance – Render imports this
app = create_app()