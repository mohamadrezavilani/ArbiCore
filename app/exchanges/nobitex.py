# app/exchanges/nobitex.py

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

class NobitexClient(ExchangeClient):
    def __init__(self):
        self.token = settings.NOBITEX_API_TOKEN          # e.g., default_key
        self.public_key = settings.NOBITEX_API_PUBLIC_KEY  # base64 string
        self.private_key_base64 = settings.NOBITEX_API_PRIVATE_KEY  # base64 string
        self.base_url = "https://apiv2.nobitex.ir"

    # ---------- BALANCE (uses token) ----------
    async def get_balances(self) -> Dict[str, float]:
        try:
            headers = {"Authorization": f"Token {self.token}"}
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/v2/wallets"
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        raise Exception(f"Balance fetch failed: {resp.status}")
                    data = await resp.json()
                    if data.get("status") != "ok":
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
                    return balances
        except Exception as e:
            logger.exception("Failed to fetch Nobitex balances")
            return {}

    # ---------- SIGNED REQUESTS ----------
    def _sign(self, timestamp: int, method: str, path: str, body: str = "") -> str:
        """Generate Ed25519 signature using base64-decoded private key."""
        # Decode the base64 private key to raw bytes
        private_key_bytes = base64.b64decode(self.private_key_base64)
        signing_key = SigningKey(private_key_bytes)
        message = f"{timestamp}{method}{path}{body}"
        signature = signing_key.sign(message.encode()).signature
        return base64.b64encode(signature).decode()

    async def _signed_request(self, method: str, path: str, json_data: Optional[Dict] = None) -> Dict[str, Any]:
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
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, json=json_data) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Nobitex API error {resp.status}: {text}")
                data = await resp.json()
                if data.get("status") != "ok":
                    raise Exception(f"Nobitex API error: {data}")
                return data

    # ---------- ORDERBOOK (public) ----------
    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/v3/orderbook/{symbol}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    if data.get("status") == "ok":
                        return data
                    return None
        except Exception:
            return None

    def extract_levels(self, raw_orderbook: Dict[str, Any]) -> Tuple[List[List[float]], List[List[float]]]:
        asks = raw_orderbook.get("asks", [])
        bids = raw_orderbook.get("bids", [])
        ask_levels = [[float(price), float(vol)] for price, vol in asks] if asks else []
        bid_levels = [[float(price), float(vol)] for price, vol in bids] if bids else []
        return ask_levels, bid_levels

    # ---------- ORDER PLACEMENT ----------
    async def place_market_order(self, symbol: str, side: str, amount: float, client_order_id: str,
                                 price: float = None) -> OrderResult:
        if price is None:
            ob = await self.fetch_orderbook(symbol)
            if not ob:
                raise Exception("Failed to fetch orderbook")
            if side.lower() == "buy":
                price = float(ob["asks"][0][0])
            else:
                price = float(ob["bids"][0][0])
        # Determine src/dst based on symbol
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
        response = await self._signed_request("POST", "/market/orders/add", json_data=payload)
        order = response.get("order", {})
        order_id = str(order.get("id"))
        matched_amount = float(order.get("matchedAmount", 0))
        fee = float(order.get("fee", 0))
        status = "filled" if matched_amount >= amount else "partial"
        return OrderResult(
            order_id=order_id,
            client_order_id=client_order_id,
            status=status,
            filled_price=price,
            filled_volume=matched_amount,
            fee=fee,
            raw_response=response
        )
    async def order_status(self, client_order_id: str) -> OrderResult:
        payload = {"clientOrderId": client_order_id}
        response = await self._signed_request("POST", "/market/orders/status", json_data=payload)
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
        filled_price = float(order.get("price", 0))
        fee = float(order.get("fee", 0))
        return OrderResult(
            order_id=str(order.get("id", "")),
            client_order_id=client_order_id,
            status=status,
            filled_price=filled_price,
            filled_volume=matched_amount,
            fee=fee,
            raw_response=response
        )

    async def cancel_order(self, client_order_id: str) -> bool:
        try:
            payload = {"clientOrderId": client_order_id, "status": "canceled"}
            await self._signed_request("POST", "/market/orders/update-status", json_data=payload)
            return True
        except Exception as e:
            logger.exception(f"Cancel order failed: {e}")
            return False

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        raise NotImplementedError("Withdraw not implemented")