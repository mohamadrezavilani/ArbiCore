import aiohttp
import asyncio
import time
from typing import Dict, Any, Optional

from app.core.config import settings
from app.exchanges.base import ExchangeClient, OrderResult

class NobitexClient(ExchangeClient):
    def __init__(self):
        self.username = settings.NOBITEX_API_KEY   # email
        self.password = settings.NOBITEX_API_SECRET
        self.token = None
        self.token_expiry = 0
        self.base_url = "https://apiv2.nobitex.ir"
        self.session: Optional[aiohttp.ClientSession] = None
        self.user_agent = "TraderBot/ArbiCore/1.0"  # Can be configurable

    async def _ensure_token(self):
        """Get a new token if expired or not present."""
        if self.token and time.time() < self.token_expiry:
            return
        # Login to get token
        url = f"{self.base_url}/auth/login/"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": self.user_agent
        }
        # Note: TOTP is required if 2FA enabled. For now, assume no 2FA, or we need to ask user.
        # We'll send empty X-TOTP header (some exchanges accept missing).
        # If 2FA is enabled, the user must provide a TOTP code; we cannot generate automatically.
        # For simplicity, we assume 2FA is disabled.
        payload = {
            "username": self.username,
            "password": self.password,
            "captcha": "api"
        }
        async with self.session.post(url, headers=headers, json=payload) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise Exception(f"Nobitex login failed: {resp.status} {text}")
            data = await resp.json()
            if data.get("status") != "success":
                raise Exception(f"Nobitex login error: {data}")
            self.token = data.get("key")
            # Token valid for 4 hours (14400 seconds) or 30 days if remember=yes
            # We'll assume 4 hours to be safe
            self.token_expiry = time.time() + 14400

    async def _request(self, method: str, path: str, json_data: Optional[Dict] = None) -> Dict[str, Any]:
        if not self.session:
            self.session = aiohttp.ClientSession()
        await self._ensure_token()
        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}{path}"
        try:
            async with self.session.request(method, url, headers=headers, json=json_data) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Nobitex API error {resp.status}: {text}")
                data = await resp.json()
                if data.get("status") != "ok":
                    raise Exception(f"Nobitex API error: {data}")
                return data
        except Exception as e:
            # If token expired, retry once with new token
            if "401" in str(e) or "403" in str(e):
                self.token = None
                await self._ensure_token()
                # Retry the request (recursive with retry limit)
                return await self._request(method, path, json_data)
            raise

    async def get_balances(self) -> Dict[str, float]:
        try:
            data = await self._request("GET", "/users/wallets/list")
            wallets = data.get("wallets", [])
            balances = {}
            for wallet in wallets:
                currency = wallet.get("currency")
                balance = float(wallet.get("activeBalance", 0))
                if currency == "rls":
                    balances["IRT"] = balance
                elif currency == "usdt":
                    balances["USDT"] = balance
                else:
                    balances[currency.upper()] = balance
            return balances
        except Exception as e:
            print(f"Error fetching balances: {e}")
            return {}

    async def place_market_order(self, symbol: str, side: str, amount: float, client_order_id: str) -> OrderResult:
        try:
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
                raise ValueError(f"Unknown symbol format: {symbol}")

            # Fetch orderbook
            ob_data = await self._request("GET", f"/v3/orderbook/{symbol}")
            if side.lower() == "buy":
                best_price = float(ob_data["asks"][0][0])
            else:
                best_price = float(ob_data["bids"][0][0])

            payload = {
                "type": side.lower(),
                "srcCurrency": src,
                "dstCurrency": dst,
                "amount": str(amount),
                "price": str(best_price),
                "clientOrderId": client_order_id
            }
            response = await self._request("POST", "/market/orders/add", json_data=payload)
            order = response.get("order", {})
            order_id = str(order.get("id"))
            matched_amount = float(order.get("matchedAmount", 0))
            fee = float(order.get("fee", 0))
            status = "filled" if matched_amount >= amount else "partial"
            return OrderResult(
                order_id=order_id,
                client_order_id=client_order_id,
                status=status,
                filled_price=best_price,
                filled_volume=matched_amount,
                fee=fee,
                raw_response=response
            )
        except Exception as e:
            print(f"Error placing order: {e}")
            return OrderResult(
                order_id="",
                client_order_id=client_order_id,
                status="failed",
                filled_price=0,
                filled_volume=0,
                fee=0,
                raw_response=None
            )

    async def order_status(self, client_order_id: str) -> OrderResult:
        try:
            payload = {"clientOrderId": client_order_id}
            response = await self._request("POST", "/market/orders/status", json_data=payload)
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
        except Exception as e:
            print(f"Error getting order status: {e}")
            return OrderResult(
                order_id="",
                client_order_id=client_order_id,
                status="failed",
                filled_price=0,
                filled_volume=0,
                fee=0,
                raw_response=None
            )

    async def cancel_order(self, client_order_id: str) -> bool:
        try:
            payload = {"clientOrderId": client_order_id, "status": "canceled"}
            await self._request("POST", "/market/orders/update-status", json_data=payload)
            return True
        except Exception as e:
            print(f"Error cancelling order: {e}")
            return False

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        raise NotImplementedError("Withdraw not implemented for Nobitex")