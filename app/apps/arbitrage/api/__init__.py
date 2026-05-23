from fastapi import APIRouter
from .dashboard import router as dashboard_router
from .exchanges import router as exchanges_router
from .symbols import router as symbols_router
from .snapshots import router as snapshots_router
from .opportunities import router as opportunities_router
from .balances import router as balances_router
from .settings import router as settings_router
from .rebalance_logs import router as rebalance_logs_router
from .actions import router as actions_router
from .pools import router as pools_router

router = APIRouter(prefix="/arbitrage", tags=["arbitrage"])

router.include_router(dashboard_router, prefix="/dashboard", tags=["dashboard"])
router.include_router(exchanges_router, prefix="/exchanges", tags=["exchanges"])
router.include_router(symbols_router, prefix="/exchange-symbols", tags=["exchange symbols"])
router.include_router(snapshots_router, prefix="/snapshots", tags=["snapshots"])
router.include_router(opportunities_router, prefix="/opportunities", tags=["opportunities"])
router.include_router(balances_router, prefix="/balances", tags=["balances"])
router.include_router(settings_router, prefix="/settings", tags=["settings"])
router.include_router(rebalance_logs_router, prefix="/rebalance-logs", tags=["rebalance logs"])
router.include_router(actions_router, prefix="/actions", tags=["actions"])
router.include_router(pools_router, prefix="/pools", tags=["pools"])