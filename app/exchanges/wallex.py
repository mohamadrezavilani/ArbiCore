import uuid
import aiohttp
import logging
from typing import Dict, Any, Optional, List, Tuple
from app.exchanges.base import ExchangeClient, OrderResult
from app.core.config import settings

logger = logging.getLogger(__name__)

class WallexClient(ExchangeClient):
    def __init__(self):
        self.api_key = settings.WALLEX_API_KEY
        self.api_secret = settings.WALLEX_API_SECRET
        self.base_url = "https://api.wallex.ir"

    async def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        url = f"{self.base_url}{path}"
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                if 200 <= resp.status < 300:
                    return await resp.json()
                else:
                    text = await resp.text()
                    # Try to parse JSON for better error message
                    try:
                        import json
                        error_data = json.loads(text)
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
                    logger.error(f"Wallex API error {resp.status}: {readable_msg}")
                    raise Exception(f"Wallex API error {resp.status}: {readable_msg}")

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

    async def place_limit_order(self, symbol: str, side: str, amount: float, client_order_id: str, price: float) -> OrderResult:
        payload = {
            "client_id": client_order_id,
            "price": str(price),
            "quantity": str(amount),
            "side": side.upper(),
            "symbol": symbol,
            "type": "LIMIT"
        }
        response = await self._request("POST", "/v1/account/orders", json=payload)
        order_data = response.get("result", {})
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
                    "price": float(order_data.get("executedPrice", price)),
                    "volume": filled_qty,
                    "fee": float(order_data.get("fee", 0))
                })
        total_vol = sum(e["volume"] for e in executions)
        status = "filled" if total_vol >= amount else "pending" if order_data.get("status") == "active" else "partial"
        return OrderResult(
            order_id=order_data.get("clientOrderId", client_order_id),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=total_vol,
            fee=sum(e["fee"] for e in executions),
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
        total_vol = sum(e["volume"] for e in executions)
        return OrderResult(
            order_id=order_data.get("clientOrderId", client_order_id),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=total_vol,
            fee=sum(e["fee"] for e in executions),
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
        raise NotImplementedError("Withdraw not implemented for Wallex")

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
        except Exception:
            return None

    def extract_levels(self, raw_orderbook: Dict[str, Any]) -> Tuple[List[List[float]], List[List[float]]]:
        asks = raw_orderbook.get("ask", [])
        bids = raw_orderbook.get("bid", [])
        ask_levels = [[float(a["price"]), float(a["quantity"])] for a in asks] if asks else []
        bid_levels = [[float(b["price"]), float(b["quantity"])] for b in bids] if bids else []
        return ask_levels, bid_levels