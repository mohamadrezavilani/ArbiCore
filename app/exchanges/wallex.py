import uuid
import aiohttp
import asyncio
from typing import Dict, Any, Optional
from decimal import Decimal
from app.exchanges.base import ExchangeClient, OrderResult
from app.core.config import settings

class WallexClient(ExchangeClient):
    def __init__(self):
        self.api_key = settings.WALLEX_API_KEY
        self.api_secret = settings.WALLEX_API_SECRET
        self.base_url = "https://api.wallex.ir"
        self.session = None

    async def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        """Make authenticated request to Wallex API."""
        if not self.session:
            self.session = aiohttp.ClientSession()
        headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        url = f"{self.base_url}{path}"
        async with self.session.request(method, url, headers=headers, **kwargs) as resp:
            # Wallex returns 201 for order creation
            if resp.status not in (200, 201):
                text = await resp.text()
                raise Exception(f"Wallex API error {resp.status}: {text}")
            return await resp.json()

    async def get_balances(self) -> Dict[str, float]:
        data = await self._request("GET", "/v1/account/balances")
        result = data.get("result", {})
        balances = result.get("balances", {})
        mapped = {}
        for asset, info in balances.items():
            if asset == "TMN":  # Toman (1 TMN = 10 IRT, but we keep as TMN)
                mapped["IRT"] = float(info.get("value", 0)) * 10
            elif asset == "USDT":
                mapped["USDT"] = float(info.get("value", 0))
            else:
                # For base assets like TONTMN, we keep the asset name as is
                mapped[asset] = float(info.get("value", 0))
        return mapped

    async def place_market_order(self, symbol: str, side: str, amount: float, client_order_id: str) -> OrderResult:
        """
        Place a limit order at market price? Wallex requires a price for limit orders.
        For market orders, we need to use a very high/low price to simulate market.
        Alternatively, we can fetch orderbook and use best ask/bid price.
        For simplicity, we'll use best ask/bid price from orderbook.
        """
        # Fetch current orderbook to get best price
        ob_data = await self._request("GET", f"/v1/depth", params={"symbol": symbol})
        result = ob_data.get("result", {})
        if not result:
            raise Exception("Failed to fetch orderbook")
        if side.lower() == "buy":
            best_price = float(result["ask"][0]["price"])
        else:
            best_price = float(result["bid"][0]["price"])
        # Place limit order at best price (effectively market)
        payload = {
            "client_id": client_order_id,
            "price": str(best_price),
            "quantity": str(amount),
            "side": side.upper(),
            "symbol": symbol,
            "type": "LIMIT"
        }
        response = await self._request("POST", "/v1/account/orders", json=payload)
        # Response contains result with clientOrderId, executedQty, etc.
        order_data = response.get("result", {})
        executed_qty = float(order_data.get("executedQty", 0))
        executed_price = float(order_data.get("executedPrice", best_price))
        fee = float(order_data.get("fee", 0))
        status = "filled" if executed_qty >= amount else "partial"
        return OrderResult(
            order_id=order_data.get("clientOrderId", client_order_id),
            client_order_id=client_order_id,
            status=status,
            filled_price=executed_price,
            filled_volume=executed_qty,
            fee=fee,
            raw_response=response
        )

    async def order_status(self, client_order_id: str) -> OrderResult:
        """Get order status using clientOrderId."""
        try:
            response = await self._request("GET", f"/v1/account/orders/{client_order_id}")
        except Exception as e:
            # If order not found (e.g., cancelled and removed), treat as cancelled
            if "404" in str(e):
                return OrderResult(
                    order_id=client_order_id,
                    client_order_id=client_order_id,
                    status="cancelled",
                    filled_price=0,
                    filled_volume=0,
                    fee=0,
                    raw_response=None
                )
            raise
        order_data = response.get("result", {})
        status_raw = order_data.get("status", "").lower()
        # Map Wallex status to our internal status
        if status_raw == "filled":
            status = "filled"
        elif status_raw in ("canceled", "cancelled"):
            status = "cancelled"
        elif status_raw == "active":
            status = "pending"
        else:
            status = "partial"
        executed_qty = float(order_data.get("executedQty", 0))
        executed_price = float(order_data.get("executedPrice", 0))
        fee = float(order_data.get("fee", 0))
        return OrderResult(
            order_id=order_data.get("clientOrderId", client_order_id),
            client_order_id=client_order_id,
            status=status,
            filled_price=executed_price,
            filled_volume=executed_qty,
            fee=fee,
            raw_response=response
        )

    async def cancel_order(self, client_order_id: str) -> bool:
        """Cancel order by clientOrderId."""
        try:
            await self._request("DELETE", f"/v1/account/orders/{client_order_id}")
            return True
        except Exception as e:
            # If already cancelled or not found, we consider success
            if "404" in str(e):
                return True
            return False

    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        """Withdraw – not fully implemented; placeholder."""
        # Wallex has separate withdrawal endpoint; we can implement later if needed.
        raise NotImplementedError("Withdraw not implemented for Wallex in this version")