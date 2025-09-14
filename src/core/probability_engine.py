"""
Engine responsible for estimating the probability of order execution.
Uses current market data and simple logic to decide if an order should be placed.
This is a placeholder implementation for a future, more sophisticated model.
"""

from src.core.abstract_data_feed import AbstractDataFeed
import datetime


class FillProbabilityEngine:
    """Estimates fill probability for orders based on market conditions."""

    def __init__(self, data_feed: AbstractDataFeed):
        """Initialize the engine with a data feed and a configurable execution threshold."""
        self.data_feed = data_feed
        self.execution_threshold = 0.7

    def estimate_volatility(self, symbol, price_history, order) -> float:
        """Placeholder for volatility estimation. Returns a fixed low value for testing."""
        return 0.001

    def calculate_fill_probability(self, order, current_price, volatility) -> float:
        """Calculate fill probability for limit orders based on current vs. entry price."""
        if order.order_type.value == 'LMT':
            if order.action.value == 'BUY':
                if current_price <= order.entry_price:
                    return 0.95
                else:
                    return 0.1
            elif order.action.value == 'SELL':
                if current_price >= order.entry_price:
                    return 0.95
                else:
                    return 0.1

        return 0.5

    def extract_features(self, order, current_data) -> dict:
        """
        Extract comprehensive features for Phase B ML foundation.
        Returns a dictionary of features for persistence and analysis.
        """
        if not current_data:
            return {}
        
        current_price = current_data['price']
        current_time = datetime.datetime.now()
        
        features = {
            # Time-based features
            'timestamp': current_time.isoformat(),
            'time_of_day_seconds': current_time.hour * 3600 + current_time.minute * 60 + current_time.second,
            'day_of_week': current_time.weekday(),  # Monday=0, Sunday=6
            'seconds_since_midnight': current_time.hour * 3600 + current_time.minute * 60 + current_time.second,
            
            # Market data features
            'current_price': current_price,
            'bid': current_data.get('bid'),
            'ask': current_data.get('ask'),
            'bid_size': current_data.get('bid_size'),
            'ask_size': current_data.get('ask_size'),
            'last_price': current_data.get('last'),
            'volume': current_data.get('volume'),
            
            # Spread analysis
            'spread_absolute': current_data.get('ask', 0) - current_data.get('bid', 0) if current_data.get('ask') and current_data.get('bid') else None,
            'spread_relative': (current_data.get('ask', 0) - current_data.get('bid', 0)) / current_price if current_data.get('ask') and current_data.get('bid') and current_price else None,
            
            # Order context features
            'symbol': order.symbol,
            'order_side': order.action.value,
            'order_type': order.order_type.value,
            'entry_price': order.entry_price,
            'stop_loss': order.stop_loss,
            'priority_manual': getattr(order, 'priority', 3),
            'trading_setup': getattr(order, 'trading_setup', None),
            'core_timeframe': getattr(order, 'core_timeframe', None),
            
            # Price proximity features
            'price_diff_absolute': current_price - order.entry_price if order.entry_price else None,
            'price_diff_relative': (current_price - order.entry_price) / order.entry_price if order.entry_price else None,
            
            # Volatility placeholder (to be enhanced)
            'volatility_estimate': self.estimate_volatility(order.symbol, current_data.get('history', []), order)
        }
        
        return features
    
    def score_fill(self, order, return_features=False) -> float:
        """
        Compute fill probability score (0..1) based on rules and current market data.
        Phase B enhancement: optionally returns features dictionary for persistence.
        """
        current_data = self.data_feed.get_current_price(order.symbol)
        if not current_data:
            return 0.5 if not return_features else (0.5, {})  # neutral if no data

        current_price = current_data['price']
        price_history = current_data.get('history', [])
        volatility = self.estimate_volatility(order.symbol, price_history, order)

        # Heuristic scoring rules
        score = 0.5  # baseline
        if order.order_type.value == 'LMT':
            if order.action.value == 'BUY':
                # Closer current price to entry → higher score
                if current_price <= order.entry_price:
                    score = 0.9
                else:
                    score = max(0.1, 1.0 - (current_price - order.entry_price) / (order.entry_price * (1 + volatility)))
            elif order.action.value == 'SELL':
                if current_price >= order.entry_price:
                    score = 0.9
                else:
                    score = max(0.1, 1.0 - (order.entry_price - current_price) / (order.entry_price * (1 + volatility)))

        final_score = max(0.0, min(1.0, score))
        
        if return_features:
            features = self.extract_features(order, current_data)
            return final_score, features
        else:
            return final_score

    def should_execute_order(self, order) -> tuple[bool, float]:
        """Compatibility wrapper — decide based on threshold."""
        fill_prob = self.score_fill(order)
        execute = fill_prob >= self.execution_threshold

        print(f"🔍 {order.symbol}: Entry={order.entry_price:.5f}, "
              f"FillProb={fill_prob:.3f}, Threshold={self.execution_threshold:.3f}, "
              f"Execute={execute}")

        return execute, fill_prob

    def score_outcome_stub(self, order):
        """Stub for outcome probability scoring — to be implemented in Phase B."""
        return None