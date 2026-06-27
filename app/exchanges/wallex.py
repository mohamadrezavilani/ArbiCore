import uuid
import aiohttp
import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from decimal import Decimal
from app.exchanges.base import ExchangeClient, OrderResult
from app.core.config import settings

logger = logging.getLogger(__name__)

class WallexClient(ExchangeClient):
    def __init__(self):
        self.api_key = settings.WALLEX_API_KEY
        self.api_secret = settings.WALLEX_API_SECRET
        self.base_url = "https://api.wallex.ir"
        self.session = None

    async def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        url = f"{self.base_url}{path}"
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                if resp.status not in (200, 201):
                    # Read raw text first (already UTF-8)
                    text = await resp.text()
                    # Try to parse JSON to extract human-readable error message
                    try:
                        import json
                        error_data = json.loads(text)
                        # Build a readable message: include error_code and field-specific messages
                        msg_parts = []
                        if error_data.get("result"):
                            for field, msgs in error_data["result"].items():
                                if isinstance(msgs, list):
                                    msg_parts.append(f"{field}: {', '.join(msgs)}")
                                else:
                                    msg_parts.append(f"{field}: {msgs}")
                        if error_data.get("message"):
                            msg_parts.insert(0, error_data["message"])
                        readable_msg = " | ".join(msg_parts) if msg_parts else text
                    except:
                        readable_msg = text
                    # Log the full error for debugging
                    logger.error(f"Wallex API error {resp.status}: {readable_msg}")
                    raise Exception(f"Wallex API error {resp.status}: {readable_msg}")
                return await resp.json()

    async def get_balances(self) -> Dict[str, float]:
        data = await self._request("GET", "/v1/account/balances")
        result = data.get("result", {})
        balances = result.get("balances", {})
        mapped = {}
        for asset, info in balances.items():
            if asset == "TMN":
                mapped["IRT"] = float(info.get("value", 0)) * 10
            elif asset == "USDT":
                mapped["USDT"] = float(info.get("value", 0))
            else:
                mapped[asset] = float(info.get("value", 0))
        return mapped

    async def _get_tick_size(self, symbol: str) -> float:
        """Fetch orderbook and compute price tick size from asks."""
        ob = await self.fetch_orderbook(symbol)
        if not ob:
            return 1.0
        asks = ob.get("ask", [])
        if len(asks) < 2:
            return 1.0
        prices = sorted([float(a["price"]) for a in asks])
        diffs = [prices[i+1] - prices[i] for i in range(len(prices)-1)]
        if not diffs:
            return 1.0
        return min(diffs)

    async def place_market_order(self, symbol: str, side: str, amount: float, client_order_id: str,
                                 price: float = None, price_factor: float = 1.0) -> OrderResult:
        if price is None:
            ob_data = await self._request("GET", f"/v1/depth", params={"symbol": symbol})
            result = ob_data.get("result", {})
            if not result:
                raise Exception("Failed to fetch orderbook")
            if side.lower() == "buy":
                native_price = float(result["ask"][0]["price"])
            else:
                native_price = float(result["bid"][0]["price"])
        else:
            # Convert from common quote (IRT) to native (TMN) by dividing by factor
            native_price = price / price_factor
            # Round to nearest tick (price step)
            tick = await self._get_tick_size(symbol)
            native_price = round(native_price / tick) * tick
            native_price = int(native_price)

        payload = {
            "client_id": client_order_id,
            "price": str(native_price),  # integer, no decimals
            "quantity": str(amount),      # amount is in base currency (USDT)
            "side": side.upper(),
            "symbol": symbol,
            "type": "LIMIT"
        }
        response = await self._request("POST", "/v1/account/orders", json=payload)
        order_data = response.get("result", {})
        executions = []
        # Wallex uses "fills" field for executions
        raw_execs = order_data.get("fills", [])
        for exec_item in raw_execs:
            executions.append({
                "price": float(exec_item.get("price", 0)),
                "volume": float(exec_item.get("quantity", 0)),
                "fee": float(exec_item.get("fee", 0))
            })
        if not executions and order_data.get("status") == "filled":
            filled_qty = float(order_data.get("executedQty", 0))
            if filled_qty > 0:
                executions.append({
                    "price": float(order_data.get("executedPrice", native_price)),
                    "volume": filled_qty,
                    "fee": float(order_data.get("fee", 0))
                })
        total_vol = sum(e["volume"] for e in executions)
        status = "filled" if total_vol >= amount else "partial"
        return OrderResult(
            order_id=order_data.get("clientOrderId", client_order_id),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=0.0,
            fee=0.0,
            raw_response=response,
            executions=executions
        )

    async def order_status(self, client_order_id: str) -> OrderResult:
        try:
            response = await self._request("GET", f"/v1/account/orders/{client_order_id}")
        except Exception as e:
            if "404" in str(e):
                return OrderResult(
                    order_id=client_order_id,
                    client_order_id=client_order_id,
                    status="cancelled",
                    filled_price=0,
                    filled_volume=0,
                    fee=0,
                    raw_response=None,
                    executions=[]
                )
            raise
        order_data = response.get("result", {})
        status_raw = order_data.get("status", "").lower()
        if status_raw == "filled":
            status = "filled"
        elif status_raw in ("canceled", "cancelled"):
            status = "cancelled"
        elif status_raw == "active":
            status = "pending"
        else:
            status = "partial"
        executions = []
        raw_execs = order_data.get("fills", [])
        for exec_item in raw_execs:
            executions.append({
                "price": float(exec_item.get("price", 0)),
                "volume": float(exec_item.get("quantity", 0)),
                "fee": float(exec_item.get("fee", 0))
            })
        if not executions and order_data.get("status") == "filled":
            filled_qty = float(order_data.get("executedQty", 0))
            if filled_qty > 0:
                executions.append({
                    "price": float(order_data.get("executedPrice", 0)),
                    "volume": filled_qty,
                    "fee": float(order_data.get("fee", 0))
                })
        return OrderResult(
            order_id=order_data.get("clientOrderId", client_order_id),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=0.0,
            fee=0.0,
            raw_response=response,
            executions=executions
        )

    async def cancel_order(self, client_order_id: str) -> bool:
        try:
            await self._request("DELETE", f"/v1/account/orders/{client_order_id}")
            return True
        except Exception as e:
            if "404" in str(e):
                return True
            return False

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        raise NotImplementedError("Withdraw not implemented for Wallex in this version")

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/v1/depth"
        params = {"symbol": symbol}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    if data.get("success"):
                        return data["result"]
                    else:
                        return None
        except Exception as e:
            return None

    def extract_levels(self, raw_orderbook: Dict[str, Any]) -> Tuple[List[List[float]], List[List[float]]]:
        asks = raw_orderbook.get("ask", [])
        bids = raw_orderbook.get("bid", [])
        ask_levels = [[float(a["price"]), float(a["quantity"])] for a in asks] if asks else []
        bid_levels = [[float(b["price"]), float(b["quantity"])] for b in bids] if bids else []
        return ask_levels, bid_levels