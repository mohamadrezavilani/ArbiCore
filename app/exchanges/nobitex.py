import asyncio

import aiohttp
import logging
import time
import json
import base64
from typing import Dict, Any, Optional, List, Tuple
from nacl.signing import SigningKey

from app.core.config import settings
from app.exchanges.base import ExchangeClient, OrderResult

logger = logging.getLogger(__name__)
api_logger = logging.getLogger("exchange_api.nobitex")
api_logger.setLevel(logging.DEBUG)
if not api_logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(message)s'))
    api_logger.addHandler(ch)

class NobitexClient(ExchangeClient):
    def __init__(self):
        self.token = settings.NOBITEX_API_TOKEN
        self.public_key = settings.NOBITEX_API_PUBLIC_KEY
        self.private_key_base64 = settings.NOBITEX_API_PRIVATE_KEY
        self.base_url = "https://apiv2.nobitex.ir"

    async def get_balances(self) -> Dict[str, float]:
        try:
            headers = {"Authorization": f"Token {self.token}"}
            url = f"{self.base_url}/v2/wallets"
            api_logger.debug(f"Fetching balances from {url}")
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=10) as resp:
                    if resp.status != 200:
                        api_logger.error(f"Balance fetch failed: {resp.status} {await resp.text()}")
                        raise Exception(f"Balance fetch failed: {resp.status}")
                    data = await resp.json()
                    if data.get("status") != "ok":
                        api_logger.error(f"Balance error: {data}")
                        raise Exception(f"Balance error: {data}")
                    wallets = data.get("wallets", {})
                    balances = {}
                    for currency, info in wallets.items():
                        balance = float(info.get("balance", 0))
                        if currency.upper() == "RLS":
                            balances["IRT"] = balance
                        elif currency.upper() == "USDT":
                            balances["USDT"] = balance
                        else:
                            balances[currency.upper()] = balance
                    api_logger.info(f"Balances fetched: {balances}")
                    return balances
        except Exception as e:
            api_logger.exception("Failed to fetch Nobitex balances")
            return {}

    def _sign(self, timestamp: int, method: str, path: str, body: str = "") -> str:
        private_key_bytes = base64.b64decode(self.private_key_base64)
        signing_key = SigningKey(private_key_bytes)
        message = f"{timestamp}{method}{path}{body}"
        signature = signing_key.sign(message.encode()).signature
        return base64.b64encode(signature).decode()

    async def _signed_request(self, method: str, path: str, json_data: Optional[Dict] = None, retries: int = 2) -> Dict[str, Any]:
        timestamp = int(time.time())
        body = json.dumps(json_data) if json_data else ""
        signature = self._sign(timestamp, method, path, body)
        headers = {
            "Nobitex-Key": self.public_key,
            "Nobitex-Signature": signature,
            "Nobitex-Timestamp": str(timestamp),
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}{path}"
        api_logger.debug(f"Signed request: {method} {url} | data={body[:200]}")
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method, url, headers=headers, json=json_data, timeout=30) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            api_logger.error(f"Signed request error: {resp.status} {text}")
                            if 500 <= resp.status < 600 and attempt < retries - 1:
                                wait = 2 ** attempt
                                api_logger.warning(f"Received {resp.status}, retrying in {wait}s (attempt {attempt+1}/{retries})")
                                await asyncio.sleep(wait)
                                continue
                            raise Exception(f"Nobitex API error {resp.status}: {text}")
                        data = await resp.json()
                        if data.get("status") != "ok":
                            api_logger.error(f"API error: {data}")
                            raise Exception(f"Nobitex API error: {data}")
                        api_logger.debug(f"Signed response: {json.dumps(data, ensure_ascii=False)[:500]}")
                        return data
            except asyncio.TimeoutError:
                api_logger.error(f"Timeout on signed request {method} {url}")
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    api_logger.warning(f"Timeout, retrying in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                else:
                    raise
            except Exception as e:
                api_logger.exception(f"Signed request failed: {e}")
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    api_logger.warning(f"Error, retrying in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                else:
                    raise
        raise Exception(f"Signed request failed after {retries} retries")

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/v3/orderbook/{symbol}"
        # api_logger.debug(f"Fetching orderbook for {symbol}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "ok":
                            # api_logger.debug(f"Orderbook fetched for {symbol}")
                            return data
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
        asks = raw_orderbook.get("asks", [])
        bids = raw_orderbook.get("bids", [])
        ask_levels = [[float(price), float(vol)] for price, vol in asks] if asks else []
        bid_levels = [[float(price), float(vol)] for price, vol in bids] if bids else []
        return ask_levels, bid_levels

    async def place_limit_order(self, symbol: str, side: str, amount: float, client_order_id: str, price: float) -> OrderResult:
        api_logger.info(f"Placing limit order: symbol={symbol}, side={side}, amount={amount}, price={price}, cid={client_order_id}")
        symbol_lower = symbol.lower()
        if symbol_lower.endswith("irt"):
            base = symbol_lower.replace("irt", "")
            src = base
            dst = "rls"
        elif symbol_lower.endswith("usdt"):
            base = symbol_lower.replace("usdt", "")
            src = base
            dst = "usdt"
        else:
            raise ValueError(f"Unsupported symbol: {symbol}")
        payload = {
            "type": side.lower(),
            "srcCurrency": src,
            "dstCurrency": dst,
            "amount": str(amount),
            "price": str(price),
            "clientOrderId": client_order_id
        }
        try:
            response = await self._signed_request("POST", "/market/orders/add", json_data=payload, retries=2)
        except Exception as e:
            api_logger.error(f"Order placement failed: {e}")
            raise
        order = response.get("order", {})
        matched_amount = float(order.get("matchedAmount", 0))
        status = "filled" if matched_amount >= amount else "pending" if order.get("status") == "active" else "partial"
        executions = []
        if matched_amount > 0:
            executions.append({
                "price": float(order.get("price", price)),
                "volume": matched_amount,
                "fee": float(order.get("fee", 0))
            })
        result = OrderResult(
            order_id=str(order.get("id")),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=matched_amount,
            fee=float(order.get("fee", 0)),
            raw_response=response,
            executions=executions
        )
        api_logger.info(f"Order placement result: {result}")
        return result

    async def order_status(self, client_order_id: str) -> OrderResult:
        api_logger.debug(f"Checking status for order {client_order_id}")
        try:
            payload = {"clientOrderId": client_order_id}
            response = await self._signed_request("POST", "/market/orders/status", json_data=payload, retries=1)
        except Exception as e:
            api_logger.warning(f"Order {client_order_id} status check failed: {e}, treating as pending")
            return OrderResult(
                order_id="",
                client_order_id=client_order_id,
                status="pending",
                filled_price=0,
                filled_volume=0,
                fee=0,
                raw_response=None,
                executions=[]
            )
        order = response.get("order", {})
        status_raw = order.get("status", "").lower()
        if status_raw == "active":
            status = "pending"
        elif status_raw == "canceled":
            status = "cancelled"
        elif status_raw == "filled":
            status = "filled"
        else:
            status = "partial"
        matched_amount = float(order.get("matchedAmount", 0))
        price = float(order.get("price", 0))
        fee = float(order.get("fee", 0))
        executions = []
        if matched_amount > 0 and price > 0:
            executions.append({
                "price": price,
                "volume": matched_amount,
                "fee": fee
            })
        result = OrderResult(
            order_id=str(order.get("id", "")),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=matched_amount,
            fee=fee,
            raw_response=response,
            executions=executions
        )
        api_logger.debug(f"Order status result: {result}")
        return result

    async def cancel_order(self, client_order_id: str) -> bool:
        api_logger.info(f"Cancelling order {client_order_id}")
        try:
            payload = {"clientOrderId": client_order_id, "status": "canceled"}
            await self._signed_request("POST", "/market/orders/update-status", json_data=payload, retries=1)
            api_logger.info(f"Order {client_order_id} cancelled successfully")
            return True
        except Exception as e:
            if "404" in str(e):
                api_logger.warning(f"Order {client_order_id} not found, treating as cancelled")
                return True
            api_logger.exception(f"Failed to cancel order {client_order_id}")
            return False

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        raise NotImplementedError("Withdraw not implemented")