import threading
from src.core.abstract_data_feed import AbstractDataFeed
from src.core.market_data_manager import MarketDataManager
from src.core.ibkr_client import IbkrClient  # CHANGED: Import IbkrClient instead
from ibapi.contract import Contract
from typing import Dict, Any, Optional
import datetime

class IBKRDataFeed(AbstractDataFeed):
    """
    Live data feed implementation using Interactive Brokers API.
    This class adapts the existing OrderExecutor and MarketDataManager
    to the AbstractDataFeed interface.
    """
    
    def __init__(self, ibkr_client: IbkrClient):  # CHANGED: Parameter type
        """
        Initialize the IBKR data feed.
        
        Args:
            ibkr_client: The IbkrClient instance
        """
        self.ibkr_client = ibkr_client  # CHANGED: Store the client
        # MarketDataManager still needs the low-level OrderExecutor
        self.market_data = MarketDataManager(ibkr_client.order_executor)
        self._connected = False
        
    def connect(self, host='127.0.0.1', port=7497, client_id=0) -> bool:
        """Establish connection to IB Gateway/TWS. Returns success status."""
        try:
            # FIXED: Use the ibkr_client's connect method instead of recursive call
            success = self.ibkr_client.connect(host, port, client_id)
            if success:
                self._connected = True
            return success
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
                    
    def is_connected(self) -> bool:
        """
        Check if connected to IBKR and market data is available.
        """
        return self._connected and self.ibkr_client.connected  # CHANGED: Use client's connection state
    
    def subscribe(self, symbol: str, contract: Contract) -> bool:
        """
        Subscribe to market data for a symbol using the existing MarketDataManager.
        """
        try:
            # Use the existing market data manager's subscription logic
            self.market_data.subscribe(symbol, contract)
            return True
        except Exception as e:
            print(f"Failed to subscribe to {symbol}: {e}")
            return False
    
    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get current price data from the existing MarketDataManager.
        Maintains backward compatibility with the expected data format.
        """
        price_data = self.market_data.get_current_price(symbol)
        if not price_data:
            return None
            
        # Ensure the returned data has the expected structure
        result = {
            'price': price_data.get('price', 0.0),
            'timestamp': price_data.get('timestamp', datetime.datetime.now()),
            'data_type': price_data.get('type', 'UNKNOWN'),
            'updates': price_data.get('updates', 0)
        }
        
        # Preserve any additional fields for backward compatibility
        for key, value in price_data.items():
            if key not in result:
                result[key] = value
                
        return result