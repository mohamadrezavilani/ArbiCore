import asyncio
import logging
import aiohttp
import time
import json
from typing import Dict, Any, Optional, List, Tuple
from asyncio import sleep

from app.core.config import settings
from app.exchanges.base import ExchangeClient, OrderResult

logger = logging.getLogger(__name__)
api_logger = logging.getLogger("exchange_api.bitpin")
api_logger.setLevel(logging.DEBUG)
# Ensure the logger has a handler (will inherit root handler, but we set explicitly)
if not api_logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(message)s'))
    api_logger.addHandler(ch)

class BitpinClient(ExchangeClient):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.api_key = settings.BITPIN_API_KEY
        self.secret_key = settings.BITPIN_API_SECRET
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0
        self.base_url = "https://api.bitpin.ir"

    async def _ensure_token(self):
        if self.access_token and time.time() < self.token_expiry:
            return
        api_logger.debug("Acquiring new token")
        async with aiohttp.ClientSession() as session:
            if self.refresh_token:
                try:
                    url = f"{self.base_url}/api/v1/usr/refresh_token/"
                    payload = {"refresh": self.refresh_token}
                    api_logger.debug(f"Refreshing token at {url}")
                    async with session.post(url, json=payload) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.access_token = data.get("access")
                            self.token_expiry = time.time() + 900
                            api_logger.debug("Token refreshed successfully")
                            return
                        else:
                            api_logger.warning(f"Refresh token failed with status {resp.status}, clearing")
                            self.refresh_token = None
                except Exception as e:
                    api_logger.exception("Token refresh exception")
                    self.refresh_token = None

            url = f"{self.base_url}/api/v1/usr/authenticate/"
            payload = {"api_key": self.api_key, "secret_key": self.secret_key}
            api_logger.debug(f"Authenticating at {url}")
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    api_logger.error(f"Bitpin login failed: {resp.status} {text}")
                    raise Exception(f"Bitpin login failed: {resp.status} {text}")
                data = await resp.json()
                self.access_token = data.get("access")
                self.refresh_token = data.get("refresh")
                self.token_expiry = time.time() + 900
                api_logger.debug("Authentication successful")

    async def _request(self, method: str, path: str, json_data: Optional[Dict] = None, retries: int = 3) -> Any:
        """
        Send authenticated request with retry for 5xx errors.
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
        url = f"{self.base_url}{path}"
        api_logger.debug(f"Request: {method} {url} | data={json.dumps(json_data) if json_data else 'None'}")
        last_exception = None
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method, url, headers=headers, json=json_data, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        status = resp.status
                        if 200 <= status < 300:
                            if status == 204:
                                api_logger.debug(f"Response: {status} No Content")
                                return None
                            data = await resp.json()
                            api_logger.debug(f"Response: {status} {json.dumps(data, ensure_ascii=False)[:500]}")
                            return data
                        else:
                            text = await resp.text()
                            api_logger.error(f"Error response: {status} {text}")
                            # For 5xx, retry; for 4xx, raise immediately unless it's a 404 (which we handle separately in order_status)
                            if 500 <= status < 600:
                                if attempt < retries - 1:
                                    wait = 2 ** attempt
                                    api_logger.warning(f"Received {status}, retrying in {wait}s (attempt {attempt+1}/{retries})")
                                    await sleep(wait)
                                    continue
                            # For 400, 401, 403, 422, etc., raise
                            raise Exception(f"Bitpin API error {status}: {text}")
            except asyncio.TimeoutError:
                api_logger.error(f"Timeout on request {method} {url}")
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    api_logger.warning(f"Timeout, retrying in {wait}s (attempt {attempt+1}/{retries})")
                    await sleep(wait)
                    continue
                else:
                    raise Exception(f"Timeout after {retries} attempts")
            except Exception as e:
                api_logger.exception(f"Request failed: {e}")
                if attempt < retries - 1 and not isinstance(e, aiohttp.ClientError):
                    wait = 2 ** attempt
                    api_logger.warning(f"Error, retrying in {wait}s (attempt {attempt+1}/{retries})")
                    await sleep(wait)
                    continue
                else:
                    raise
        raise Exception(f"Request failed after {retries} retries")

    async def get_balances(self) -> Dict[str, float]:
        try:
            data = await self._request("GET", "/api/v1/wlt/wallets/")
            balances = {}
            for wallet in data:
                asset = wallet.get("asset")
                balance = float(wallet.get("balance", 0))
                if asset == "IRT":
                    balances["IRT"] = balance * 10
                elif asset == "USDT":
                    balances["USDT"] = balance
                else:
                    balances[asset] = balance
            api_logger.info(f"Balances fetched: {balances}")
            return balances
        except Exception:
            api_logger.exception("Failed to fetch balances")
            return {}

    async def place_limit_order(self, symbol: str, side: str, amount: float, client_order_id: str, price: float) -> OrderResult:
        api_logger.info(f"Placing limit order: symbol={symbol}, side={side}, amount={amount}, price={price}, cid={client_order_id}")
        payload = {
            "symbol": symbol,
            "type": "limit",
            "side": side.lower(),
            "base_amount": str(amount),
            "price": str(price),
            "identifier": client_order_id
        }
        try:
            response = await self._request("POST", "/api/v1/odr/orders/", json_data=payload, retries=2)
        except Exception as e:
            api_logger.error(f"Order placement failed: {e}")
            raise
        state = response.get("state", "").lower()
        if state == "active":
            status = "pending"
        elif state == "closed":
            status = "filled"
        else:
            status = "partial"
        filled_vol = float(response.get("dealed_base_amount", 0))
        fee = float(response.get("commission", 0))
        executions = []
        if filled_vol > 0 and price > 0:
            executions.append({
                "price": price,
                "volume": filled_vol,
                "fee": fee
            })
        result = OrderResult(
            order_id=str(response.get("id")),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=filled_vol,
            fee=fee,
            raw_response=response,
            executions=executions
        )
        api_logger.info(f"Order placement result: {result}")
        return result

    async def order_status(self, client_order_id: str) -> OrderResult:
        api_logger.debug(f"Checking status for order {client_order_id}")
        try:
            response = await self._request("GET", f"/api/v1/odr/orders/identifier/{client_order_id}/", retries=1)
        except Exception as e:
            # If it's a 404 or any error, treat as pending (do not cancel)
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
        state = response.get("state", "").lower()
        if state == "active":
            status = "pending"
        elif state == "closed":
            status = "filled"
        else:
            status = "partial"
        filled_vol = float(response.get("dealed_base_amount", 0))
        price = float(response.get("price", 0))
        fee = float(response.get("commission", 0))
        executions = []
        if filled_vol > 0 and price > 0:
            executions.append({
                "price": price,
                "volume": filled_vol,
                "fee": fee
            })
        result = OrderResult(
            order_id=str(response.get("id")),
            client_order_id=client_order_id,
            status=status,
            filled_price=0.0,
            filled_volume=filled_vol,
            fee=fee,
            raw_response=response,
            executions=executions
        )
        api_logger.debug(f"Order status result: {result}")
        return result

    async def cancel_order(self, client_order_id: str) -> bool:
        api_logger.info(f"Cancelling order {client_order_id}")
        try:
            await self._request("DELETE", f"/api/v1/odr/orders/identifier/{client_order_id}/", retries=1)
            api_logger.info(f"Order {client_order_id} cancelled successfully")
            return True
        except Exception as e:
            if "404" in str(e) or "406" in str(e) or "500" in str(e):
                api_logger.warning(f"Order {client_order_id} already cancelled or not found, treating as success")
                return True
            api_logger.exception(f"Failed to cancel order {client_order_id}")
            return False

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        raise NotImplementedError("Withdraw not implemented for Bitpin")

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/api/v1/mth/orderbook/{symbol}/"
        # api_logger.debug(f"Fetching orderbook for {symbol}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # api_logger.debug(f"Orderbook fetched for {symbol}")
                        return data
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