# mock_feed.py
from abstract_data_feed import AbstractDataFeed
from ibapi.contract import Contract
from typing import Dict, Any, Optional
import datetime
import random  # NEW IMPORT

class MockFeed(AbstractDataFeed):
    """
    Mock data feed that generates a simple, deterministic price series
    starting from a provided anchor price for each symbol.
    Useful for testing order execution logic.
    """
    
    def __init__(self, anchor_price_str: str, trend_direction: str = 'random'):
        """
        Initialize the mock data feed.
        
        Args:
            anchor_price_str: String in format "SYM1=PRICE1,SYM2=PRICE2" 
                            (e.g., "EUR=1.095,USD=1.2")
            trend_direction: Global bias for price movement ['up', 'down', 'random']
        """
        self._connected = False
        self.anchor_prices: Dict[str, float] = {}
        self.current_prices: Dict[str, float] = {}
        self.price_increment = 0.001  # Fixed base increment per call (1 pip)
        
        # NEW: Configure price movement based on trend direction
        self.trend_direction = trend_direction.lower()
        if self.trend_direction == 'up':
            self.price_delta = self.price_increment  # Always move up
        elif self.trend_direction == 'down':
            self.price_delta = -self.price_increment  # Always move down
        else:  # 'random' or any other value
            self.price_delta = self.price_increment  # Will be randomized in get_current_price
        
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
            print(f"Global trend direction: {self.trend_direction.upper()}")  # NEW: Print trend info
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
        Applies trend-based increment to the price each call.
        """
        if symbol not in self.anchor_prices:
            return None
            
        # NEW: Apply trend-based price movement
        if self.trend_direction == 'random':
            # Random walk: 50/50 chance of up/down movement
            current_delta = self.price_increment * random.choice([1, -1])
        else:
            # Deterministic trend: always use the configured delta
            current_delta = self.price_delta
            
        self.current_prices[symbol] += current_delta
        
        # Create the return data structure
        price_data = {
            'price': self.current_prices[symbol],
            'timestamp': datetime.datetime.now(),
            'data_type': 'MOCK',
            'updates': 0,  # This could be incremented if needed
            'history': []   # This could be populated if needed
        }
        
        return price_data