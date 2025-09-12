# Intelligent mock feed enhancement - Begin
from src.core.abstract_data_feed import AbstractDataFeed
from ibapi.contract import Contract
from typing import Dict, Any, Optional, List
import datetime
import random

class MockFeed(AbstractDataFeed):
    """
    Intelligent mock data feed that moves prices toward order entry points
    for effective testing while maintaining realistic behavior.
    """
    
    def __init__(self, planned_orders: list = None):
        """
        Initialize the intelligent mock data feed.
        
        Args:
            planned_orders: List of PlannedOrder objects with mock configuration
        """
        self._connected = False
        self.current_prices: Dict[str, float] = {}
        self.mock_config: Dict[str, Dict[str, Any]] = {}  # symbol -> config
        self.anchor_prices: Dict[str, List[float]] = {}   # symbol -> list of entry prices
        self.trend_strength = 0.7  # How strongly prices move toward anchors
        self.volatility_chance = 0.3  # Chance of random moves
        
        if planned_orders:
            self._initialize_from_orders(planned_orders)
            
    def _initialize_from_orders(self, planned_orders: list):
        """Initialize mock configuration from planned orders"""
        for order in planned_orders:
            symbol = order.symbol
            
            # Store anchor prices for intelligent movement
            if symbol not in self.anchor_prices:
                self.anchor_prices[symbol] = []
            if order.entry_price is not None:
                self.anchor_prices[symbol].append(order.entry_price)
            
            # Initialize current price near entry for realistic testing
            if symbol not in self.current_prices and order.entry_price is not None:
                # Start 2-5% away from entry for realistic movement
                deviation = random.uniform(0.02, 0.05) * (-1 if random.random() > 0.5 else 1)
                self.current_prices[symbol] = order.entry_price * (1 + deviation)
            
            # Store mock configuration
            self.mock_config[symbol] = {
                'trend': order.mock_trend if hasattr(order, 'mock_trend') else 'intelligent',
                'volatility': order.mock_volatility if hasattr(order, 'mock_volatility') else 0.001,
                'anchor_price': order.mock_anchor_price if hasattr(order, 'mock_anchor_price') else None
            }

    def connect(self) -> bool:
        """
        Initialize the intelligent mock data feed.
        """
        try:
            self._connected = True
            print(f"🤖 Intelligent mock feed initialized with {len(self.current_prices)} symbols:")
            for symbol, anchors in self.anchor_prices.items():
                print(f"  {symbol}: {len(anchors)} anchor prices around {anchors[0]:.4f}")
            return True
        except Exception as e:
            print(f"❌ Failed to initialize intelligent mock feed: {e}")
            return False

    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get intelligent mock price that moves toward order entry points.
        """
        if symbol not in self.current_prices:
            # Fallback for symbols not in Excel - random walk
            if symbol not in self.current_prices:
                self.current_prices[symbol] = random.uniform(100, 200)
            return self._create_price_data(symbol)
            
        current = self.current_prices[symbol]
        
        # Intelligent movement logic
        if symbol in self.anchor_prices and self.anchor_prices[symbol]:
            current = self._calculate_intelligent_price(symbol, current)
        else:
            # Fallback: simple random walk
            current += random.uniform(-current*0.001, current*0.001)
        
        self.current_prices[symbol] = current
        return self._create_price_data(symbol)
    
    def _calculate_intelligent_price(self, symbol: str, current_price: float) -> float:
        """Calculate price movement toward nearest anchor"""
        anchors = self.anchor_prices[symbol]
        
        # Find nearest anchor price
        nearest_anchor = min(anchors, key=lambda x: abs(x - current_price))
        
        # 70% chance: Move toward nearest anchor
        if random.random() < self.trend_strength:
            direction = 1 if nearest_anchor > current_price else -1
            distance = abs(nearest_anchor - current_price)
            
            # Adaptive step size: larger steps when far from target
            base_step = current_price * 0.001  # 0.1% base step
            if distance > current_price * 0.03:  # >3% away
                step_size = base_step * 2  # Double step when far
            else:
                step_size = base_step
                
            current_price += direction * step_size
        
        # 30% chance: Random movement
        else:
            if random.random() < self.volatility_chance:
                # Big random move (0.5% to 1.5%)
                move_pct = random.uniform(0.005, 0.015) * random.choice([-1, 1])
                current_price *= (1 + move_pct)
            else:
                # Small random walk (±0.1%)
                current_price += random.uniform(-current_price*0.001, current_price*0.001)
        
        return current_price
    
    def _create_price_data(self, symbol: str) -> Dict[str, Any]:
        """Create standardized price data structure"""
        return {
            'price': self.current_prices[symbol],
            'timestamp': datetime.datetime.now(),
            'data_type': 'MOCK',
            'symbol': symbol,
            'updates': 0,
            'history': []
        }
    
    def configure_intelligence(self, trend_strength: float = 0.7, volatility_chance: float = 0.3):
        """Configure how aggressively prices move toward anchors"""
        self.trend_strength = max(0.1, min(1.0, trend_strength))
        self.volatility_chance = max(0.0, min(1.0, volatility_chance))
        print(f"⚙️ Mock feed configured: trend_strength={self.trend_strength}, volatility={self.volatility_chance}")

    # Keep existing methods unchanged
    def is_connected(self) -> bool:
        return self._connected
    
    def subscribe(self, symbol: str, contract: Contract) -> bool:
        if symbol in self.current_prices:
            print(f"✅ Intelligent mock subscribed: {symbol}")
            return True
        else:
            print(f"⚠️ No intelligent config for {symbol}, using random walk")
            if symbol not in self.current_prices:
                self.current_prices[symbol] = random.uniform(100, 200)
            return True
# Intelligent mock feed enhancement - End