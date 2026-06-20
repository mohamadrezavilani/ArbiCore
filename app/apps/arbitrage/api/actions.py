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


@router.post("/rebalance-smart")
async def rebalance_smart(db: AsyncSession = Depends(get_db)):
    """
    - Moves ALL USDT to the exchange with the HIGHEST USDT bid price (best to sell).
    - Splits ALL IRT equally between the OTHER TWO exchanges (they will buy USDT cheaply).
    - Deducts 200,000 IRT and 1.4 USDT (converted to IRT at cheapest ask) as network fees.
    - Logs fees as rebalancing losses.
    """
    stmt = select(Exchange.name, Exchange.id).where(Exchange.is_active == True)
    result = await db.execute(stmt)
    exchanges = result.all()
    if len(exchanges) != 3:
        raise HTTPException(400, "This endpoint assumes exactly 3 active exchanges")
    exchange_names = [ex.name for ex in exchanges]

    # 1. Find exchange with highest USDT bid price (best to sell USDT)
    best_bid = None
    target_usdt = None
    for ex_name, ex_id in exchanges:
        # Get latest best_bid_price from orderbook
        price_stmt = (
            select(OrderbookSnapshot.best_bid_price)
            .join(ExchangeSymbol, OrderbookSnapshot.symbol_id == ExchangeSymbol.id)
            .where(ExchangeSymbol.common_symbol == "USDTIRT")
            .where(OrderbookSnapshot.exchange_id == ex_id)
            .order_by(OrderbookSnapshot.created_at.desc())
            .limit(1)
        )
        price_res = await db.execute(price_stmt)
        bid = price_res.scalar()
        if bid is None:
            # fallback: use current balance to decide
            bid = 0.0
        if best_bid is None or float(bid) > best_bid:
            best_bid = float(bid)
            target_usdt = ex_name

    if target_usdt is None:
        raise HTTPException(503, "Could not determine best exchange for USDT")

    # 2. Move all USDT to target_usdt
    total_usdt = 0.0
    for ex_name, _ in exchanges:
        bal = await get_base_balance(db, ex_name, "USDTIRT")
        total_usdt += bal
        if ex_name == target_usdt:
            continue
        await set_base_balance(db, ex_name, "USDTIRT", 0.0)
    await set_base_balance(db, target_usdt, "USDTIRT", total_usdt)

    # 3. Identify the two exchanges that are NOT the USDT target
    other_exchanges = [ex_name for ex_name in exchange_names if ex_name != target_usdt]
    if len(other_exchanges) != 2:
        raise HTTPException(500, "Could not find two other exchanges")

    # 4. Move all IRT to those two exchanges equally
    total_irt = 0.0
    for ex_name, _ in exchanges:
        bal = await get_quote_balance(db, ex_name, "IRT")
        total_irt += bal
        await set_quote_balance(db, ex_name, "IRT", 0.0)  # zero all first
    irt_per_target = total_irt / 2
    for ex_name in other_exchanges:
        await set_quote_balance(db, ex_name, "IRT", irt_per_target)

    # 5. Deduct fees (200,000 IRT and 1.4 USDT converted to IRT)
    fee_irt = 200000.0
    fee_usdt = 1.4

    # Find cheapest USDT ask price for conversion
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

    # Deduct IRT fee from the two IRT receivers equally
    irt_fee_per_receiver = fee_irt / 2
    for ex_name in other_exchanges:
        bal = await get_quote_balance(db, ex_name, "IRT")
        new_bal = bal - irt_fee_per_receiver
        if new_bal < 0:
            raise HTTPException(400, f"Insufficient IRT on {ex_name} for IRT fee deduction")
        await set_quote_balance(db, ex_name, "IRT", new_bal)

    # Deduct USDT fee (converted to IRT) from the two IRT receivers equally
    usdt_fee_irt_per_receiver = fee_irt_from_usdt / 2
    for ex_name in other_exchanges:
        bal = await get_quote_balance(db, ex_name, "IRT")
        new_bal = bal - usdt_fee_irt_per_receiver
        if new_bal < 0:
            raise HTTPException(400, f"Insufficient IRT on {ex_name} to cover USDT fee")
        await set_quote_balance(db, ex_name, "IRT", new_bal)

    # Deduct the USDT amount from target_usdt
    target_usdt_bal = await get_base_balance(db, target_usdt, "USDTIRT")
    if target_usdt_bal < fee_usdt:
        raise HTTPException(400, f"Target USDT exchange has insufficient USDT ({target_usdt_bal}) for fee {fee_usdt}")
    await update_base_balance(db, target_usdt, "USDTIRT", -fee_usdt)

    # 6. Log fees as rebalancing losses
    logger = OpportunityLogger()
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

    # Get final balances for response
    final_usdt = await get_base_balance(db, target_usdt, "USDTIRT")
    final_irt_receivers = {ex: await get_quote_balance(db, ex, "IRT") for ex in other_exchanges}

    return {
        "success": True,
        "target_usdt_exchange": target_usdt,
        "irt_receiver_exchanges": other_exchanges,
        "final_usdt_on_target": final_usdt,
        "final_irt_on_receivers": final_irt_receivers,
        "irt_fee_deducted": fee_irt,
        "usdt_fee_converted_to_irt": round(fee_irt_from_usdt, 2),
        "message": f"All USDT moved to {target_usdt} (highest bid price). IRT split equally between {other_exchanges}. Fees deducted."
    }

@router.post("/sync-balances")
async def sync_balances(db: AsyncSession = Depends(get_db)):
    from app.apps.arbitrage.services.balance_sync import BalanceSyncService
    result = await BalanceSyncService.sync_all_balances(db)
    return {"status": "success", "result": result}