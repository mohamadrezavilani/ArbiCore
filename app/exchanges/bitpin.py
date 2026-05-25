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
        self.session: Optional[aiohttp.ClientSession] = None
        self._timeout = aiohttp.ClientTimeout(total=30)

    async def _ensure_token(self):
        """Obtain or refresh access token. Logs errors on failure."""
        if self.access_token and time.time() < self.token_expiry:
            return
        if self.refresh_token:
            try:
                url = f"{self.base_url}/api/v1/usr/refresh_token/"
                payload = {"refresh": self.refresh_token}
                async with self.session.post(url, json=payload, timeout=self._timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.access_token = data.get("access")
                        self.token_expiry = time.time() + 900
                        return
                    else:
                        self.logger.warning(f"Refresh token failed with status {resp.status}, clearing refresh_token")
                        self.refresh_token = None
            except Exception as e:
                self.logger.exception("Exception during token refresh")
                self.refresh_token = None

        # Authenticate from scratch
        url = f"{self.base_url}/api/v1/usr/authenticate/"
        payload = {"api_key": self.api_key, "secret_key": self.secret_key}
        try:
            async with self.session.post(url, json=payload, timeout=self._timeout) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Bitpin login failed: {resp.status} {text}")
                data = await resp.json()
                self.access_token = data.get("access")
                self.refresh_token = data.get("refresh")
                self.token_expiry = time.time() + 900
        except Exception:
            self.logger.exception("Authentication failed")
            raise

    async def _request(self, method: str, path: str, json_data: Optional[Dict] = None) -> Any:
        """Make an authenticated request. Logs and retries token refresh on 401/403."""
        if not self.session:
            self.session = aiohttp.ClientSession()
        try:
            await self._ensure_token()
            headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
            url = f"{self.base_url}{path}"
            async with self.session.request(method, url, headers=headers, json=json_data,
                                            timeout=self._timeout) as resp:
                if resp.status == 204:
                    return None
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Bitpin API error {resp.status}: {text}")
                return await resp.json()
        except aiohttp.ClientError as e:
            self.logger.exception(f"Network error during {method} {path}")
            raise
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout during {method} {path}")
            raise
        except Exception as e:
            # If unauthorised, retry once after clearing token
            if "401" in str(e) or "403" in str(e):
                self.logger.warning(f"Auth error, retrying after token reset: {e}")
                self.access_token = None
                await self._ensure_token()
                return await self._request(method, path, json_data)
            self.logger.exception(f"Unexpected error in {method} {path}")
            raise

    async def get_balances(self) -> Dict[str, float]:
        """Fetch wallet balances. Returns dict on success, {} on error (after logging)."""
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
        """Place a market order (implemented as limit at best price)."""
        try:
            # Fetch orderbook to get best price
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
            self.logger.exception(f"Failed to place {side} order for {amount} {symbol}")
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
        """Check order status by client_order_id."""
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
            if "404" in str(e):
                self.logger.warning(f"Order {client_order_id} not found (assumed cancelled)")
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
        """Cancel an order by client_order_id. Returns True if cancelled or already gone."""
        try:
            await self._request("DELETE", f"/api/v1/odr/orders/identifier/{client_order_id}/")
            return True
        except Exception as e:
            if "404" in str(e) or "406" in str(e):
                self.logger.warning(f"Order {client_order_id} already cancelled or not found")
                return True
            self.logger.exception(f"Failed to cancel order {client_order_id}")
            return False

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        raise NotImplementedError("Withdraw not implemented for Bitpin")

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch raw orderbook for a symbol. Returns None on error (after logging)."""
        url = f"{self.base_url}/api/v1/mth/orderbook/{symbol}/"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=self._timeout) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        except Exception:
            self.logger.exception(f"Failed to fetch orderbook for {symbol}")
            return None

    def extract_levels(self, raw_orderbook: Dict[str, Any]) -> Tuple[List[List[float]], List[List[float]]]:
        asks = raw_orderbook.get("asks", [])
        bids = raw_orderbook.get("bids", [])
        ask_levels = [[float(price), float(vol)] for price, vol in asks] if asks else []
        bid_levels = [[float(price), float(vol)] for price, vol in bids] if bids else []
        return ask_levels, bid_levels