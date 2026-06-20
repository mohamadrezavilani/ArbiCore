import aiohttp
import logging
from typing import Dict, Any, Optional, List, Tuple

from app.core.config import settings
from app.exchanges.base import ExchangeClient, OrderResult

logger = logging.getLogger(__name__)


class NobitexClient(ExchangeClient):
    def __init__(self):
        self.token = settings.NOBITEX_API_KEY  # This is the token from your profile
        self.base_url = "https://apiv2.nobitex.ir"

    async def _request(self, method: str, path: str, headers: Optional[Dict] = None, json_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Make a request with token authentication."""
        if headers is None:
            headers = {}
        headers["Authorization"] = f"Token {self.token}"
        headers["Content-Type"] = "application/json"

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

    async def get_balances(self) -> Dict[str, float]:
        """Fetch balances from /users/wallets/list and extract IRT and USDT."""
        try:
            data = await self._request("GET", "/users/wallets/list")
            wallets = data.get("wallets", [])
            balances = {}
            for wallet in wallets:
                currency = wallet.get("currency")
                balance = float(wallet.get("balance", 0))
                if currency == "rls":
                    balances["IRT"] = balance
                elif currency == "usdt":
                    balances["USDT"] = balance
                # You can add other currencies if needed
            return balances
        except Exception as e:
            logger.exception(f"Failed to fetch Nobitex balances: {e}")
            return {}

    # ---- Order methods (to be implemented later with signature) ----
    async def place_market_order(self, symbol: str, side: str, amount: float, client_order_id: str) -> OrderResult:
        raise NotImplementedError("Market orders require signature authentication. Not implemented yet.")

    async def order_status(self, client_order_id: str) -> OrderResult:
        raise NotImplementedError("Order status requires signature authentication.")

    async def cancel_order(self, client_order_id: str) -> bool:
        raise NotImplementedError("Cancel order requires signature authentication.")

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        raise NotImplementedError("Withdraw not implemented for Nobitex")

    # ---- Orderbook fetching (public, no auth) ----
    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/v3/orderbook/{symbol}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    if data.get("status") == "ok":
                        return data
                    else:
                        return None
        except Exception as e:
            logger.exception(f"Failed to fetch orderbook for {symbol}: {e}")
            return None

    def extract_levels(self, raw_orderbook: Dict[str, Any]) -> Tuple[List[List[float]], List[List[float]]]:
        asks = raw_orderbook.get("asks", [])
        bids = raw_orderbook.get("bids", [])
        ask_levels = [[float(price), float(vol)] for price, vol in asks] if asks else []
        bid_levels = [[float(price), float(vol)] for price, vol in bids] if bids else []
        return ask_levels, bid_levels