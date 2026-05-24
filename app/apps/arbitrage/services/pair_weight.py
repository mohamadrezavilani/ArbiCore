from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.apps.arbitrage.models import Exchange, ExchangePairWeight

async def get_pair_weight(db: AsyncSession, buy_exchange: str, sell_exchange: str) -> float:
    """Return current weight (0.5-1.0) for the directed pair (buy → sell)."""
    # Get exchange IDs
    exch_stmt = select(Exchange.id, Exchange.name).where(Exchange.name.in_([buy_exchange, sell_exchange]))
    result = await db.execute(exch_stmt)
    rows = result.all()
    if len(rows) != 2:
        return 0.5
    exch_map = {name: id for id, name in rows}
    a_id, b_id = sorted([exch_map[buy_exchange], exch_map[sell_exchange]])
    stmt = select(ExchangePairWeight).where(
        ExchangePairWeight.exchange_a_id == a_id,
        ExchangePairWeight.exchange_b_id == b_id
    )
    pair = await db.execute(stmt)
    pair = pair.scalar_one_or_none()
    return float(pair.weight) if pair else 0.5

async def update_pair_weight(db: AsyncSession, buy_exchange: str, sell_exchange: str):
    """
    Update weight after a successful trade in direction buy_exchange → sell_exchange.
    Does NOT commit – caller must commit the transaction.
    """
    # Get exchange IDs
    exch_stmt = select(Exchange.id, Exchange.name).where(Exchange.name.in_([buy_exchange, sell_exchange]))
    result = await db.execute(exch_stmt)
    rows = result.all()
    if len(rows) != 2:
        return
    exch_map = {name: id for id, name in rows}
    buy_id = exch_map[buy_exchange]
    sell_id = exch_map[sell_exchange]
    a_id, b_id = sorted([buy_id, sell_id])

    # Fetch or create the pair record
    stmt = select(ExchangePairWeight).where(
        ExchangePairWeight.exchange_a_id == a_id,
        ExchangePairWeight.exchange_b_id == b_id
    )
    pair = await db.execute(stmt)
    pair = pair.scalar_one_or_none()

    if pair is None:
        # First trade ever for this pair: create with weight 0.5 and no previous direction
        pair = ExchangePairWeight(
            exchange_a_id=a_id,
            exchange_b_id=b_id,
            weight=0.5,
            last_buy_exchange_id=buy_id,
            last_sell_exchange_id=sell_id
        )
        db.add(pair)
        return  # weight stays 0.5

    # Compare direction with last trade
    last_buy = pair.last_buy_exchange_id
    last_sell = pair.last_sell_exchange_id
    if last_buy == buy_id and last_sell == sell_id:
        # Same direction as last trade → increase weight (max 1.0)
        new_weight = min(pair.weight + 0.1, 1.0)
    else:
        # Opposite direction → decrease weight (min 0.5)
        new_weight = max(pair.weight - 0.1, 0.5)

    pair.weight = new_weight
    pair.last_buy_exchange_id = buy_id
    pair.last_sell_exchange_id = sell_id
    # No commit here – the caller will commit the outer transaction