import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware          # <-- added import
from app.core.config import settings
from app.core.database import engine
from app.core.handlers import register_exception_handlers
from app.core.logging import setup_logging
from app.apps.arbitrage.api import router as arbitrage_router
from app.apps.arbitrage.tasks import periodic_arbitrage_poll
from app.apps.arbitrage.seed_data import seed

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    setup_logging()

    # 1. Create all tables (if they don't exist)
    async with engine.begin() as conn:
        from app.models.base import Base
        import app.apps.arbitrage.models   # registers models with Base
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Database tables ensured.")

    # 2. Seed initial data (exchanges, symbols, fees, etc.) – runs only once
    await seed()
    print("✅ Seeding completed (or skipped if already seeded).")

    # 3. Start background arbitrage polling
    poll_task = asyncio.create_task(periodic_arbitrage_poll())
    print("🚀 Arbitrage polling started.")

    yield

    # Shutdown
    poll_task.cancel()
    await engine.dispose()
    print("🛑 Application shutdown.")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        debug=settings.DEBUG,
        version="0.1.0",
        lifespan=lifespan,
    )

    # ========== CORS MIDDLEWARE – ALLOWS ANY ORIGIN ==========
    # Use this only for development. For production, replace "*" with your actual frontend domains.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],           # Allows requests from any origin
        allow_credentials=False,       # Must be False when allow_origins=["*"]
        allow_methods=["*"],           # Allows all HTTP methods (GET, POST, PUT, DELETE, OPTIONS, etc.)
        allow_headers=["*"],           # Allows all headers
    )
    # =========================================================

    register_exception_handlers(app)

    app.include_router(
        arbitrage_router,
        prefix=f"{settings.API_V1_PREFIX}/arbitrage",
        tags=["arbitrage"]
    )

    @app.get("/")
    async def root():
        return {"message": f"Welcome to {settings.APP_NAME}"}

    return app


# Global FastAPI instance
app = create_app()