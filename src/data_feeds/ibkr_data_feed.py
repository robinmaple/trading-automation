"""
Live data feed implementation using the Interactive Brokers API.
Adapts the existing IbkrClient and MarketDataManager to the AbstractDataFeed interface.
"""

import threading
import datetime
from typing import Dict, Any, Optional
from ibapi.contract import Contract

from src.core.abstract_data_feed import AbstractDataFeed
from src.core.market_data_manager import MarketDataManager
from src.core.ibkr_client import IbkrClient


class IBKRDataFeed(AbstractDataFeed):
    """Concrete data feed implementation for Interactive Brokers market data."""

    def __init__(self, ibkr_client: IbkrClient):
        """Initialize the data feed with an existing IbkrClient instance."""
        self.ibkr_client = ibkr_client
        self.market_data = MarketDataManager(ibkr_client)
        self._connected = False

    def connect(self, host='127.0.0.1', port=7497, client_id=0) -> bool:
        """Establish a connection to IB Gateway/TWS. Returns success status."""
        try:
            success = self.ibkr_client.connect(host, port, client_id)
            if success:
                self._connected = True
            return success
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def is_connected(self) -> bool:
        """Check if the data feed is connected to IBKR and market data is available."""
        return self._connected and self.ibkr_client.connected

    def subscribe(self, symbol: str, contract: Contract) -> bool:
        """Subscribe to market data for a specific symbol using the MarketDataManager."""
        try:
            self.market_data.subscribe(symbol, contract)
            return True
        except Exception as e:
            print(f"Failed to subscribe to {symbol}: {e}")
            return False

    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current price data for a symbol, formatted for backward compatibility."""
        price_data = self.market_data.get_current_price(symbol)
        if not price_data:
            return None

        result = {
            'price': price_data.get('price', 0.0),
            'timestamp': price_data.get('timestamp', datetime.datetime.now()),
            'data_type': price_data.get('type', 'UNKNOWN'),
            'updates': price_data.get('updates', 0)
        }

        for key, value in price_data.items():
            if key not in result:
                result[key] = value

        return result

    def disconnect(self) -> None:
        """Disconnect from the IBKR API and clean up resources."""
        if self.ibkr_client and hasattr(self.ibkr_client, 'disconnect'):
            self.ibkr_client.disconnect()
            self._connected = False
            print("✅ Disconnected from IBKR")