from abstract_data_feed import AbstractDataFeed
from ibapi.contract import Contract
from typing import Dict, Any, Optional
import datetime

class MockFeed(AbstractDataFeed):
    """
    Mock data feed that generates a simple, deterministic price series
    starting from a provided anchor price for each symbol.
    Useful for testing order execution logic.
    """
    
    def __init__(self, anchor_price_str: str):
        """
        Initialize the mock data feed.
        
        Args:
            anchor_price_str: String in format "SYM1=PRICE1,SYM2=PRICE2" 
                            (e.g., "EUR=1.095,USD=1.2")
        """
        self._connected = False
        self.anchor_prices: Dict[str, float] = {}
        self.current_prices: Dict[str, float] = {}
        self.price_increment = 0.001  # Fixed increment per call (1 pip)
        
        # Parse the anchor price string
        if anchor_price_str:
            for pair in anchor_price_str.split(','):
                if '=' in pair:
                    symbol, price_str = pair.split('=')
                    self.anchor_prices[symbol.strip()] = float(price_str.strip())
                    self.current_prices[symbol.strip()] = float(price_str.strip())
        
    def connect(self) -> bool:
        """
        Initialize the mock data feed.
        """
        try:
            self._connected = True
            print(f"Mock data feed initialized with symbols: {list(self.anchor_prices.keys())}")
            for symbol, price in self.anchor_prices.items():
                print(f"  {symbol}: {price}")
            return True
        except Exception as e:
            print(f"Failed to initialize mock data feed: {e}")
            return False
    
    def is_connected(self) -> bool:
        return self._connected
    
    def subscribe(self, symbol: str, contract: Contract) -> bool:
        """
        Subscribe to a symbol. For mock feed, this just verifies we have an anchor price.
        """
        if symbol in self.anchor_prices:
            print(f"✅ Mock data subscribed: {symbol}")
            return True
        else:
            print(f"❌ No anchor price configured for symbol: {symbol}")
            return False
    
    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the next mock data point for the symbol.
        Increments the price by a fixed amount each call.
        """
        if symbol not in self.anchor_prices:
            return None
            
        # Increment the price for this symbol
        self.current_prices[symbol] += self.price_increment
        
        # Create the return data structure
        price_data = {
            'price': self.current_prices[symbol],
            'timestamp': datetime.datetime.now(),
            'data_type': 'MOCK',
            'updates': 0,  # This could be incremented if needed
            'history': []   # This could be populated if needed
        }
        
        return price_data