from typing import Optional
from app.exchanges.base import ExchangeClient
from app.exchanges.wallex import WallexClient
from app.exchanges.nobitex import NobitexClient
from app.exchanges.bitpin import BitpinClient

_exchange_client_map = {
    "wallex": WallexClient,
    "nobitex": NobitexClient,
    "bitpin": BitpinClient,
}

def get_exchange_client(exchange_name: str):
    if exchange_name == "wallex":
        return WallexClient()
    elif exchange_name == "nobitex":
        return NobitexClient()
    elif exchange_name == "bitpin":
        return BitpinClient()
    else:
        return None