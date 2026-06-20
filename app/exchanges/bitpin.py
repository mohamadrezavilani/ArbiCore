import logging
import aiohttp
import asyncio
import time
from typing import Dict, Any, Optional, List, Tuple

from app.core.config import settings
from app.exchanges.base import ExchangeClient, OrderResult


class BitpinClient(ExchangeClient):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.api_key = settings.BITPIN_API_KEY
        self.secret_key = settings.BITPIN_API_SECRET
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0
        self.base_url = "https://api.bitpin.ir"
        # No persistent session, no timeout

    async def _ensure_token(self):
        """Obtain or refresh access token. Creates its own session."""
        if self.access_token and time.time() < self.token_expiry:
            return

        async with aiohttp.ClientSession() as session:
            if self.refresh_token:
                try:
                    url = f"{self.base_url}/api/v1/usr/refresh_token/"
                    payload = {"refresh": self.refresh_token}
                    async with session.post(url, json=payload) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.access_token = data.get("access")
                            self.token_expiry = time.time() + 900
                            return
                        else:
                            self.logger.warning(f"Refresh token failed, clearing")
                            self.refresh_token = None
                except Exception:
                    self.logger.exception("Token refresh exception")
                    self.refresh_token = None

            # Authenticate from scratch
            url = f"{self.base_url}/api/v1/usr/authenticate/"
            payload = {"api_key": self.api_key, "secret_key": self.secret_key}
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Bitpin login failed: {resp.status} {text}")
                data = await resp.json()
                self.access_token = data.get("access")
                self.refresh_token = data.get("refresh")
                self.token_expiry = time.time() + 900

    async def _request(self, method: str, path: str, json_data: Optional[Dict] = None) -> Any:
        """Make an authenticated request. Creates its own session."""
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
        url = f"{self.base_url}{path}"

        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, json=json_data) as resp:
                if resp.status == 204:
                    return None
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Bitpin API error {resp.status}: {text}")
                return await resp.json()

    async def get_balances(self) -> Dict[str, float]:
        try:
            data = await self._request("GET", "/api/v1/wlt/wallets/")
            balances = {}
            for wallet in data:
                asset = wallet.get("asset")
                balance = float(wallet.get("balance", 0))
                if asset == "RIAL":
                    balances["IRT"] = balance
                elif asset == "USDT":
                    balances["USDT"] = balance
                else:
                    balances[asset] = balance
            return balances
        except Exception:
            self.logger.exception("Failed to fetch balances")
            return {}

    async def place_market_order(self, symbol: str, side: str, amount: float, client_order_id: str) -> OrderResult:
        try:
            ob_data = await self._request("GET", f"/api/v1/mth/orderbook/{symbol}/")
            if side.lower() == "buy":
                best_price = float(ob_data["asks"][0][0])
            else:
                best_price = float(ob_data["bids"][0][0])

            payload = {
                "symbol": symbol,
                "type": "limit",
                "side": side.lower(),
                "base_amount": str(amount),
                "price": str(best_price),
                "identifier": client_order_id
            }
            response = await self._request("POST", "/api/v1/odr/orders/", json_data=payload)
            order_id = str(response.get("id"))
            filled_vol = float(response.get("dealed_base_amount", 0))
            fee = float(response.get("commission", 0))
            status = "filled" if filled_vol >= amount else "pending"
            return OrderResult(
                order_id=order_id,
                client_order_id=client_order_id,
                status=status,
                filled_price=best_price,
                filled_volume=filled_vol,
                fee=fee,
                raw_response=response
            )
        except Exception:
            self.logger.exception(f"Failed to place {side} order")
            return OrderResult(
                order_id="",
                client_order_id=client_order_id,
                status="failed",
                filled_price=0,
                filled_volume=0,
                fee=0,
                raw_response=None
            )

    # app/exchanges/bitpin.py – update order_status and cancel_order

    async def order_status(self, client_order_id: str) -> OrderResult:
        try:
            response = await self._request("GET", f"/api/v1/odr/orders/identifier/{client_order_id}/")
            state = response.get("state", "").lower()
            if state == "active":
                status = "pending"
            elif state == "closed":
                status = "filled"
            else:
                status = "partial"
            filled_vol = float(response.get("dealed_base_amount", 0))
            filled_price = float(response.get("price", 0))
            fee = float(response.get("commission", 0))
            return OrderResult(
                order_id=str(response.get("id")),
                client_order_id=client_order_id,
                status=status,
                filled_price=filled_price,
                filled_volume=filled_vol,
                fee=fee,
                raw_response=response
            )
        except Exception as e:
            # If the order doesn't exist (404) or any other error, treat as cancelled/failed
            if "404" in str(e) or "500" in str(e):
                return OrderResult(
                    order_id="",
                    client_order_id=client_order_id,
                    status="cancelled",
                    filled_price=0,
                    filled_volume=0,
                    fee=0,
                    raw_response=None
                )
            self.logger.exception(f"Failed to get status for order {client_order_id}")
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
            await self._request("DELETE", f"/api/v1/odr/orders/identifier/{client_order_id}/")
            return True
        except Exception as e:
            if "404" in str(e) or "406" in str(e) or "500" in str(e):
                # Order not found or already cancelled – consider it successful
                return True
            self.logger.exception(f"Failed to cancel order {client_order_id}")
            return False

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        raise NotImplementedError("Withdraw not implemented for Bitpin")

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch raw orderbook – creates its own session, no timeout, no retry."""
        url = f"{self.base_url}/api/v1/mth/orderbook/{symbol}/"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        except aiohttp.ClientConnectorDNSError as e:
            self.logger.error(f"DNS error for {symbol}: {e}")
            return None
        except Exception:
            self.logger.exception(f"Failed to fetch orderbook for {symbol}")
            return None

    def extract_levels(self, raw_orderbook: Dict[str, Any]) -> Tuple[List[List[float]], List[List[float]]]:
        asks = raw_orderbook.get("asks", [])
        bids = raw_orderbook.get("bids", [])
        ask_levels = [[float(price), float(vol)] for price, vol in asks] if asks else []
        bid_levels = [[float(price), float(vol)] for price, vol in bids] if bids else []
        return ask_levels, bid_levels