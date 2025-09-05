from src.core.abstract_data_feed import AbstractDataFeed
from src.core.market_data_manager import MarketDataManager
from src.core.order_executor import OrderExecutor
from ibapi.contract import Contract
from typing import Dict, Any, Optional
import datetime

class IBKRDataFeed(AbstractDataFeed):
    """
    Live data feed implementation using Interactive Brokers API.
    This class adapts the existing OrderExecutor and MarketDataManager
    to the AbstractDataFeed interface.
    """
    
    def __init__(self, order_executor: OrderExecutor):
        """
        Initialize the IBKR data feed.
        
        Args:
            order_executor: The existing OrderExecutor instance
        """
        self.executor = order_executor
        self.market_data = MarketDataManager(order_executor)
        self._connected = False
        
    def connect(self) -> bool:
        """
        Establish connection to IBKR. 
        Note: For IBKR, connection is handled by OrderExecutor in main.py.
        This method primarily checks and confirms connection status.
        """
        # For IBKR, the connection is established by the OrderExecutor
        # We just confirm that it's connected and ready
        self._connected = self.executor.connected and self.executor.connection_event.is_set()
        return self._connected
    
    def is_connected(self) -> bool:
        """
        Check if connected to IBKR and market data is available.
        """
        return self._connected and self.executor.connected
    
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