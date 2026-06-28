import uuid
import aiohttp
import logging
import json
import asyncio
from typing import Dict, Any, Optional, List, Tuple
from app.exchanges.base import ExchangeClient, OrderResult
from app.core.config import settings

logger = logging.getLogger(__name__)
api_logger = logging.getLogger("exchange_api.wallex")
api_logger.setLevel(logging.DEBUG)
if not api_logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(message)s'))
    api_logger.addHandler(ch)

class WallexClient(ExchangeClient):
    def __init__(self):
        self.api_key = settings.WALLEX_API_KEY
        self.api_secret = settings.WALLEX_API_SECRET
        self.base_url = "https://api.wallex.ir"

    async def _request(self, method: str, path: str, retries: int = 2, **kwargs) -> Dict[str, Any]:
        headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        url = f"{self.base_url}{path}"
        api_logger.debug(f"Request: {method} {url} | params={kwargs.get('params', {})} | json={kwargs.get('json', {})}")
        last_exception = None
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method, url, headers=headers, timeout=aiohttp.ClientTimeout(total=30), **kwargs) as resp:
                        status = resp.status
                        if 200 <= status < 300:
                            data = await resp.json()
                            api_logger.debug(f"Response: {status} {json.dumps(data, ensure_ascii=False)[:500]}")
                            return data
                        else:
                            text = await resp.text()
                            api_logger.error(f"Error response: {status} {text}")
                            if 500 <= status < 600:
                                if attempt < retries - 1:
                                    wait = 2 ** attempt
                                    api_logger.warning(f"Received {status}, retrying in {wait}s (attempt {attempt+1}/{retries})")
                                    await asyncio.sleep(wait)
                                    continue
                            raise Exception(f"Wallex API error {status}: {text}")
            except asyncio.TimeoutError:
                api_logger.error(f"Timeout on request {method} {url}")
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    api_logger.warning(f"Timeout, retrying in {wait}s (attempt {attempt+1}/{retries})")
                    await asyncio.sleep(wait)
                    continue
                else:
                    raise Exception(f"Timeout after {retries} attempts")
            except Exception as e:
                api_logger.exception(f"Request failed: {e}")
                if attempt < retries - 1 and not isinstance(e, aiohttp.ClientError):
                    wait = 2 ** attempt
                    api_logger.warning(f"Error, retrying in {wait}s (attempt {attempt+1}/{retries})")
                    await asyncio.sleep(wait)
                    continue
                else:
                    raise
        raise Exception(f"Request failed after {retries} retries")

    async def get_balances(self) -> Dict[str, float]:
        try:
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
            api_logger.info(f"Balances fetched: {mapped}")
            return mapped
        except Exception:
            api_logger.exception("Failed to fetch balances")
            return {}

    async def place_limit_order(self, symbol: str, side: str, amount: float, client_order_id: str, price: float) -> OrderResult:
        api_logger.info(f"Placing limit order: symbol={symbol}, side={side}, amount={amount}, price={price}, cid={client_order_id}")
        payload = {
            "client_id": client_order_id,
            "price": str(price),
            "quantity": str(amount),
            "side": side.upper(),
            "symbol": symbol,
            "type": "LIMIT"
        }
        try:
            response = await self._request("POST", "/v1/account/orders", json=payload, retries=2)
        except Exception as e:
            api_logger.error(f"Order placement failed: {e}")
            raise
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
        result = OrderResult(
            order_id=order_data.get("clientOrderId", client_order_id),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=total_vol,
            fee=sum(e["fee"] for e in executions),
            raw_response=response,
            executions=executions
        )
        api_logger.info(f"Order placement result: {result}")
        return result

    async def order_status(self, client_order_id: str) -> OrderResult:
        api_logger.debug(f"Checking status for order {client_order_id}")
        try:
            response = await self._request("GET", f"/v1/account/orders/{client_order_id}", retries=1)
        except Exception as e:
            api_logger.warning(f"Order {client_order_id} status check failed: {e}, treating as pending")
            return OrderResult(
                order_id=client_order_id,
                client_order_id=client_order_id,
                status="pending",
                filled_price=0,
                filled_volume=0,
                fee=0,
                raw_response=None,
                executions=[]
            )
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
        result = OrderResult(
            order_id=order_data.get("clientOrderId", client_order_id),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=total_vol,
            fee=sum(e["fee"] for e in executions),
            raw_response=response,
            executions=executions
        )
        api_logger.debug(f"Order status result: {result}")
        return result

    async def cancel_order(self, client_order_id: str) -> bool:
        api_logger.info(f"Cancelling order {client_order_id}")
        try:
            await self._request("DELETE", f"/v1/account/orders/{client_order_id}", retries=1)
            api_logger.info(f"Order {client_order_id} cancelled successfully")
            return True
        except Exception as e:
            if "404" in str(e):
                api_logger.warning(f"Order {client_order_id} not found, treating as cancelled")
                return True
            api_logger.exception(f"Failed to cancel order {client_order_id}")
            return False

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        raise NotImplementedError("Withdraw not implemented for Wallex")

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/v1/depth"
        params = {"symbol": symbol}
        # api_logger.debug(f"Fetching orderbook for {symbol}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("success"):
                            # api_logger.debug(f"Orderbook fetched for {symbol}")
                            return data["result"]
                        else:
                            api_logger.error(f"Orderbook fetch failed: {data}")
                            return None
                    else:
                        api_logger.error(f"Orderbook fetch failed: {resp.status} {await resp.text()}")
                        return None
        except Exception as e:
            api_logger.exception(f"Orderbook fetch exception for {symbol}")
            return None

    def extract_levels(self, raw_orderbook: Dict[str, Any]) -> Tuple[List[List[float]], List[List[float]]]:
        asks = raw_orderbook.get("ask", [])
        bids = raw_orderbook.get("bid", [])
        ask_levels = [[float(a["price"]), float(a["quantity"])] for a in asks] if asks else []
        bid_levels = [[float(b["price"]), float(b["quantity"])] for b in bids] if bids else []
        return ask_levels, bid_levels