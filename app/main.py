import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.core.config import settings
from app.core.database import engine
from app.core.handlers import register_exception_handlers
from app.core.logging import setup_logging
from app.apps.arbitrage.api import router as arbitrage_router
from app.apps.arbitrage.tasks import periodic_arbitrage_poll


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    setup_logging()
    # Create tables (only for development – remove later and use Alembic)
    async with engine.begin() as conn:
        from app.models.base import Base
        from app.apps.arbitrage import models  # import to register models
        await conn.run_sync(Base.metadata.create_all)

    # Start background arbitrage polling
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

    # Register global exception handlers
    register_exception_handlers(app)

    # Include the arbitrage router directly – DO NOT mount a sub‑app
    app.include_router(
        arbitrage_router,
        prefix=f"{settings.API_V1_PREFIX}/arbitrage",
        tags=["arbitrage"]
    )

    return app


# Create the global FastAPI instance
app = create_app()