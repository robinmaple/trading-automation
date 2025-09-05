from abc import ABC, abstractmethod
from ibapi.contract import Contract
from typing import Dict, Any, Optional

class AbstractDataFeed(ABC):
    """
    Abstract base class defining the interface for all data feeds.
    Implementations can provide live data (e.g., IBKR) or historical/replay data.
    """
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data source.
        For live feeds: Connect to the broker API.
        For historical feeds: Load historical data.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if the data feed is connected and ready.
        
        Returns:
            bool: True if connected and ready to provide data
        """
        pass
    
    @abstractmethod
    def subscribe(self, symbol: str, contract: Contract) -> bool:
        """
        Subscribe to market data for a symbol.
        
        Args:
            symbol: The trading symbol (e.g., 'EUR', 'AAPL')
            contract: The IB contract object for the symbol
            
        Returns:
            bool: True if subscription successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the current price data for a symbol.
        
        Args:
            symbol: The trading symbol to get price for
            
        Returns:
            Optional[Dict]: Price data dictionary with keys:
                - 'price': float - current price
                - 'timestamp': datetime - time of last price update
                - 'data_type': str - type of data (e.g., 'LAST', 'BID', 'ASK')
                - 'updates': int - number of price updates received
                Returns None if no data available for symbol
        """
        pass


# REMOVED: The empty IBKRDataFeed and YFinanceHistoricalFeed class definitions
# These should only exist in their respective implementation files