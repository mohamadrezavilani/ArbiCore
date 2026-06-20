import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.core.database import engine, AsyncSessionLocal
from app.core.handlers import register_exception_handlers
from app.core.logging import setup_logging
from app.apps.arbitrage.api import router as arbitrage_router
from app.apps.arbitrage.api.analysis import router as analysis_router   # NEW
from app.apps.arbitrage.tasks import periodic_arbitrage_poll
from app.apps.arbitrage.seed_data import seed
from fastapi.responses import HTMLResponse
from app.apps.arbitrage.services.balance_sync import BalanceSyncService

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    setup_logging()
    async with engine.begin() as conn:
        from app.models.base import Base
        import app.apps.arbitrage.models
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Database tables ensured.")

    await seed()
    print("✅ Seeding completed (or skipped if already seeded).")

    # ---- NEW: Sync real balances immediately after startup ----
    async with AsyncSessionLocal() as db:
        try:
            await BalanceSyncService.sync_all_balances(db)
            print("✅ Initial balance sync completed.")
        except Exception as e:
            print(f"❌ Initial balance sync failed: {e}")

    # Start background arbitrage polling
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
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # =========================================================

    register_exception_handlers(app)

    # Include main arbitrage API router
    app.include_router(
        arbitrage_router,
        prefix=f"{settings.API_V1_PREFIX}/arbitrage",
        tags=["arbitrage"]
    )

    # Include analysis API router (under the same base path)
    app.include_router(
        analysis_router,
        prefix=f"{settings.API_V1_PREFIX}/arbitrage/analysis",
        tags=["analysis"]
    )

    # Serve main dashboard HTML
    @app.get("/", response_class=HTMLResponse)
    async def get_root():
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()

    # Serve analysis HTML page
    @app.get("/analysis", response_class=HTMLResponse)
    async def get_analysis():
        with open("analysis.html", "r", encoding="utf-8") as f:
            return f.read()

    return app


# Global FastAPI instance
app = create_app()