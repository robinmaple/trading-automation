# mock_feed.py
from src.core.abstract_data_feed import AbstractDataFeed
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
    
    # Phase 2 - Excel-based Mock Configuration - 2025-09-07 12:00 - Begin
    def __init__(self, planned_orders: list = None):
        """
        Initialize the mock data feed with configuration from planned orders.
        
        Args:
            planned_orders: List of PlannedOrder objects with mock configuration
        """
        self._connected = False
        self.current_prices: Dict[str, float] = {}
        self.mock_config: Dict[str, Dict[str, Any]] = {}  # symbol -> config
        
        if planned_orders:
            for order in planned_orders:
                if order.mock_anchor_price is not None:
                    symbol = order.symbol
                    self.current_prices[symbol] = order.mock_anchor_price
                    self.mock_config[symbol] = {
                        'trend': order.mock_trend,
                        'volatility': order.mock_volatility,
                        'anchor_price': order.mock_anchor_price
                    }
    # Phase 2 - Excel-based Mock Configuration - 2025-09-07 12:00 - End
        
    def connect(self) -> bool:
        """
        Initialize the mock data feed.
        """
        try:
            self._connected = True
            # Phase 2 - Updated Connection Message - 2025-09-07 12:00 - Begin
            print(f"Mock data feed initialized with {len(self.current_prices)} symbols:")
            for symbol, config in self.mock_config.items():
                print(f"  {symbol}: {config['anchor_price']} ({config['trend']}, vol: {config['volatility']})")
            # Phase 2 - Updated Connection Message - 2025-09-07 12:00 - End            
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
        # Phase 2 - Updated Subscription Check - 2025-09-07 12:00 - Begin
        if symbol in self.mock_config:
            print(f"✅ Mock data subscribed: {symbol}")
            return True
        else:
            print(f"❌ No mock configuration for symbol: {symbol}")
            return False
        # Phase 2 - Updated Subscription Check - 2025-09-07 12:00 - End
            
    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the next mock data point for the symbol.
        Applies trend-based increment to the price each call.
        """
        # Phase 2 - Per-Symbol Configuration - 2025-09-07 12:00 - Begin
        if symbol not in self.mock_config:
            return None
            
        config = self.mock_config.get(symbol, {})
        trend = config.get('trend', 'random')
        volatility = config.get('volatility', 0.001)
        
        # Calculate price movement based on per-symbol trend
        if trend == 'random':
            current_delta = volatility * random.choice([1, -1])
        elif trend == 'up':
            current_delta = volatility
        else:  # 'down'
            current_delta = -volatility
            
        self.current_prices[symbol] += current_delta
        
        # Prevent negative prices - Begin
        # Set reasonable minimum prices based on symbol type
        if symbol in ['EUR', 'AUD', 'GBP', 'JPY', 'CAD', 'USD']:  # Forex pairs
            min_price = 0.0001  # Reasonable minimum for forex
        else:  # Stocks, indices, etc.
            min_price = 0.01  # Reasonable minimum for equities
            
        if self.current_prices[symbol] < min_price:
            self.current_prices[symbol] = min_price
            # Reverse trend if we hit the floor to simulate bounce
            if trend == 'down':
                self.mock_config[symbol]['trend'] = 'up'
        # Prevent negative prices - End
        # Phase 2 - Per-Symbol Configuration - 2025-09-07 12:00 - End
                
        # Create the return data structure
        price_data = {
            'price': self.current_prices[symbol],
            'timestamp': datetime.datetime.now(),
            'data_type': 'MOCK',
            'updates': 0,  # This could be incremented if needed
            'history': []   # This could be populated if needed
        }
        
        return price_data