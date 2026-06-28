from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field

@dataclass
class OrderResult:
    """Result of placing an order."""
    order_id: str          # Exchange's unique order identifier
    client_order_id: str   # Our own unique ID (clientOrderId)
    status: str            # "filled", "partial", "pending", "cancelled", "failed"
    filled_price: float    # Average fill price (0 if none) – computed from executions
    filled_volume: float   # Amount actually filled (in base currency)
    fee: float             # Total fee in quote currency
    raw_response: Any      # The full response from the exchange (for debugging)
    executions: List[Dict[str, float]] = field(default_factory=list)  # [{"price": float, "volume": float, "fee": float}]

    def __post_init__(self):
        if self.executions:
            total_vol = sum(e.get('volume', 0) for e in self.executions)
            total_value = sum(e.get('price', 0) * e.get('volume', 0) for e in self.executions)
            self.filled_volume = total_vol
            self.filled_price = total_value / total_vol if total_vol > 0 else 0.0
            self.fee = sum(e.get('fee', 0) for e in self.executions)

class ExchangeClient(ABC):
    @abstractmethod
    async def get_balances(self) -> Dict[str, float]:
        """Returns a dictionary mapping asset names to balance."""
        pass

    @abstractmethod
    async def place_limit_order(self, symbol: str, side: str, amount: float, client_order_id: str, price: float) -> OrderResult:
        """
        Place a LIMIT order.
        price: the limit price in the exchange's native currency (not the common price).
        """
        pass

    @abstractmethod
    async def order_status(self, client_order_id: str) -> OrderResult:
        """Get current status of an order using our client_order_id."""
        pass

    @abstractmethod
    async def cancel_order(self, client_order_id: str) -> bool:
        """Cancel an order. Returns True if cancellation succeeded."""
        pass

    @abstractmethod
    async def withdraw(self, currency: str, amount: float, address: str, network: str) -> str:
        """Withdraw funds to an external address. Returns transaction ID."""
        pass

    @abstractmethod
    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch raw orderbook data from the exchange (public endpoint)."""
        pass

    @abstractmethod
    def extract_levels(self, raw_orderbook: Dict[str, Any]) -> Tuple[List[List[float]], List[List[float]]]:
        """Extract ask and bid levels from raw orderbook. Returns (asks, bids)."""
        pass