from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, union_all, func, cast, String
from sqlalchemy.orm import aliased
from app.core.database import get_db
from app.apps.arbitrage.models import ArbitrageOpportunity, RebalanceLog, RejectedOpportunity, Exchange, ExchangeFee, ExchangeSymbol, OrderbookSnapshot
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
from fastapi import HTTPException
from app.apps.arbitrage.inventory import get_quote_balance, get_base_balance, set_base_balance, set_quote_balance, update_quote_balance, update_base_balance
from app.apps.arbitrage.services.opportunity_logger import OpportunityLogger

router = APIRouter()

class ActionItem(BaseModel):
    id: str
    timestamp: datetime
    action_type: str
    details: dict

@router.get("/", response_model=List[ActionItem])
async def get_actions(
    limit: int = Query(50, ge=1, le=500),
    action_type: Optional[str] = Query(None, pattern="^(trade|rebalance|rejection)$"),
    db: AsyncSession = Depends(get_db)
):
    ExchangeA = aliased(Exchange)
    ExchangeB = aliased(Exchange)
    trades_query = select(
        ArbitrageOpportunity.id.label("id"),
        ArbitrageOpportunity.created_at.label("timestamp"),
        cast("trade", String).label("action_type"),
        func.json_build_object(
            "common_symbol", ArbitrageOpportunity.common_symbol,
            "exchange_a", ExchangeA.name,
            "exchange_b", ExchangeB.name,
            "trade_type", ArbitrageOpportunity.trade_type,
            "price_a", ArbitrageOpportunity.price_a,
            "price_b", ArbitrageOpportunity.price_b,
            "profit_percent", ArbitrageOpportunity.profit_percent,
            "traded_volume", ArbitrageOpportunity.traded_volume,
            "profit_quote", ArbitrageOpportunity.profit_quote
        ).label("details")
    ).join(
        ExchangeA, ArbitrageOpportunity.exchange_a_id == ExchangeA.id
    ).join(
        ExchangeB, ArbitrageOpportunity.exchange_b_id == ExchangeB.id
    )

    rebalances_query = select(
        RebalanceLog.id.label("id"),
        RebalanceLog.created_at.label("timestamp"),
        cast("rebalance", String).label("action_type"),
        func.json_build_object(
            "common_symbol", RebalanceLog.common_symbol,
            "currency", RebalanceLog.currency,
            "from_exchange", RebalanceLog.from_exchange,
            "to_exchange", RebalanceLog.to_exchange,
            "amount_sent", RebalanceLog.amount_sent,
            "network_fee", RebalanceLog.network_fee,
            "net_received", RebalanceLog.net_received,
            "reason", RebalanceLog.reason,
            "profit_quote", RebalanceLog.profit_quote
        ).label("details")
    )

    rejections_query = select(
        RejectedOpportunity.id.label("id"),
        RejectedOpportunity.created_at.label("timestamp"),
        cast("rejection", String).label("action_type"),
        func.json_build_object(
            "common_symbol", RejectedOpportunity.common_symbol,
            "exchange_a", RejectedOpportunity.exchange_a_name,
            "exchange_b", RejectedOpportunity.exchange_b_name,
            "trade_type", RejectedOpportunity.trade_type,
            "rejection_reason", RejectedOpportunity.rejection_reason,
            "details", RejectedOpportunity.details
        ).label("details")
    )

    combined = union_all(trades_query, rebalances_query, rejections_query).alias("combined")
    stmt = select(
        combined.c.id,
        combined.c.timestamp,
        combined.c.action_type,
        combined.c.details
    ).order_by(combined.c.timestamp.desc()).limit(limit)

    if action_type:
        stmt = stmt.where(combined.c.action_type == action_type)

    result = await db.execute(stmt)
    rows = result.all()
    return [
        ActionItem(
            id=str(row.id),
            timestamp=row.timestamp,
            action_type=row.action_type,
            details=row.details
        )
        for row in rows
    ]


# -------------------------------------------------------------------
# SMART REBALANCE: Move USDT to exchange with smallest USDT balance,
#                  Move IRT to exchange with smallest IRT balance.
#                  Deduct 200,000 IRT and 1.7 USDT (converted to IRT) as fees.
# -------------------------------------------------------------------
@router.post("/rebalance-smart")
async def rebalance_smart(db: AsyncSession = Depends(get_db)):
    """
    Moves all USDT to the exchange with the smallest USDT balance.
    Moves all IRT to the exchange with the smallest IRT balance.
    Deducts 200,000 IRT and 1.7 USDT (converted to IRT at cheapest ask) as network fees.
    Logs fees as rebalancing losses.
    """
    stmt = select(Exchange.name, Exchange.id).where(Exchange.is_active == True)
    result = await db.execute(stmt)
    exchanges = result.all()
    if len(exchanges) < 2:
        raise HTTPException(400, "Need at least 2 active exchanges")

    # 1. Find target for USDT (smallest USDT balance)
    usdt_balances = []
    for ex_name, ex_id in exchanges:
        bal = await get_base_balance(db, ex_name, "USDTIRT")
        usdt_balances.append((ex_name, bal))
    usdt_balances.sort(key=lambda x: x[1])
    target_usdt = usdt_balances[0][0]

    # Move all USDT to target_usdt
    total_usdt = 0.0
    for ex_name, _ in exchanges:
        bal = await get_base_balance(db, ex_name, "USDTIRT")
        total_usdt += bal
        if ex_name == target_usdt:
            continue
        await set_base_balance(db, ex_name, "USDTIRT", 0.0)
    await set_base_balance(db, target_usdt, "USDTIRT", total_usdt)

    # 2. Find target for IRT (smallest IRT balance)
    irt_balances = []
    for ex_name, ex_id in exchanges:
        bal = await get_quote_balance(db, ex_name, "IRT")
        irt_balances.append((ex_name, bal))
    irt_balances.sort(key=lambda x: x[1])
    target_irt = irt_balances[0][0]

    # Move all IRT to target_irt
    total_irt = 0.0
    for ex_name, _ in exchanges:
        bal = await get_quote_balance(db, ex_name, "IRT")
        total_irt += bal
        if ex_name == target_irt:
            continue
        await set_quote_balance(db, ex_name, "IRT", 0.0)
    await set_quote_balance(db, target_irt, "IRT", total_irt)

    # 3. Deduct fees from the receiving exchanges
    # IRT fee: 200,000 IRT from target_irt
    fee_irt = 200000.0
    target_irt_bal = await get_quote_balance(db, target_irt, "IRT")
    if target_irt_bal < fee_irt:
        raise HTTPException(400, f"Target IRT exchange {target_irt} has insufficient IRT ({target_irt_bal}) for fee {fee_irt}")
    await update_quote_balance(db, target_irt, "IRT", -fee_irt)

    # USDT fee: 1.7 USDT from target_usdt, convert to IRT at cheapest ask
    fee_usdt = 1.7
    target_usdt_bal = await get_base_balance(db, target_usdt, "USDTIRT")
    if target_usdt_bal < fee_usdt:
        raise HTTPException(400, f"Target USDT exchange {target_usdt} has insufficient USDT ({target_usdt_bal}) for fee {fee_usdt}")
    # Find cheapest USDT ask across exchanges
    best_price = None
    for ex_name, ex_id in exchanges:
        fee_stmt = select(ExchangeFee.taker_fee).where(
            ExchangeFee.exchange_id == ex_id,
            ExchangeFee.quote_currency == "IRT"
        )
        fee_res = await db.execute(fee_stmt)
        taker_fee = float(fee_res.scalar() or 0.0)
        price_stmt = (
            select(OrderbookSnapshot.best_ask_price)
            .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
            .where(ExchangeSymbol.common_symbol == "USDTIRT")
            .where(OrderbookSnapshot.exchange_id == ex_id)
            .order_by(OrderbookSnapshot.created_at.desc())
            .limit(1)
        )
        price_res = await db.execute(price_stmt)
        ask = price_res.scalar()
        if ask:
            effective = float(ask) * (1 + taker_fee)
            if best_price is None or effective < best_price:
                best_price = effective
    if best_price is None:
        raise HTTPException(503, "Could not determine USDT price for fee conversion")
    fee_irt_from_usdt = fee_usdt * best_price
    # Deduct the equivalent IRT from target_irt (since fee is paid in IRT)
    target_irt_bal = await get_quote_balance(db, target_irt, "IRT")
    if target_irt_bal < fee_irt_from_usdt:
        raise HTTPException(400, f"Target IRT exchange {target_irt} has insufficient IRT to cover USDT fee (need {fee_irt_from_usdt})")
    await update_quote_balance(db, target_irt, "IRT", -fee_irt_from_usdt)
    # Also deduct the USDT from target_usdt
    await update_base_balance(db, target_usdt, "USDTIRT", -fee_usdt)

    # 4. Log fees as rebalancing losses
    logger = OpportunityLogger()
    # IRT fee log
    await logger.log_rebalance(
        db,
        common_symbol=None,
        currency="IRT",
        from_exch="network_fee",
        to_exch="network_fee",
        amount_sent=fee_irt,
        fee=0,
        net=fee_irt,
        reason="smart_rebalance_irt_fee",
        profit_quote=-fee_irt
    )
    # USDT fee (converted) log
    await logger.log_rebalance(
        db,
        common_symbol=None,
        currency="IRT",
        from_exch="network_fee",
        to_exch="network_fee",
        amount_sent=fee_usdt,
        fee=0,
        net=fee_usdt,
        reason="smart_rebalance_usdt_fee",
        profit_quote=-fee_irt_from_usdt
    )
    await db.commit()

    return {
        "success": True,
        "target_usdt_exchange": target_usdt,
        "target_irt_exchange": target_irt,
        "final_usdt_on_target": target_usdt_bal - fee_usdt,
        "final_irt_on_target": target_irt_bal - fee_irt - fee_irt_from_usdt,
        "irt_fee_deducted": fee_irt,
        "usdt_fee_converted_to_irt": round(fee_irt_from_usdt, 2),
        "message": f"All USDT moved to {target_usdt} (needs USDT). All IRT moved to {target_irt} (needs IRT). Fees deducted."
    }