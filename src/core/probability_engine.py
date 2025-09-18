"""
Engine responsible for estimating the probability of order execution.
Uses current market data and simple logic to decide if an order should be placed.
This is a placeholder implementation for a future, more sophisticated model.
"""

from src.core.abstract_data_feed import AbstractDataFeed
import datetime


class FillProbabilityEngine:
    """Estimates fill probability for orders based on market conditions."""

    # Initialize engine with data feed and configurable execution threshold
    def __init__(self, data_feed: AbstractDataFeed):
        self.data_feed = data_feed
        self.execution_threshold = 0.7

    # Compute fill probability score with optional feature extraction
    def score_fill(self, order, return_features=False) -> float:
        current_data = self.data_feed.get_current_price(order.symbol)
        if not current_data:
            return 0.5 if not return_features else (0.5, {})

        current_price = current_data['price']
        price_history = current_data.get('history', [])
        volatility = self.estimate_volatility(order.symbol, price_history, order)

        score = 0.5  # baseline
        if order.order_type.value == 'LMT':
            if order.action.value == 'BUY':
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

    # Compatibility wrapper for execution decisions
    def should_execute_order(self, order) -> tuple[bool, float]:
        fill_prob = self.score_fill(order)
        execute = fill_prob >= self.execution_threshold

        print(f"🔍 {order.symbol}: Entry={order.entry_price:.5f}, "
            f"FillProb={fill_prob:.3f}, Threshold={self.execution_threshold:.3f}, "
            f"Execute={execute}")

        return execute, fill_prob

    # Calculate fill probability for limit orders
    def calculate_fill_probability(self, order, current_price, volatility) -> float:
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

    # Extract comprehensive features for ML foundation
    def extract_features(self, order, current_data) -> dict:
        if not current_data:
            return {}
        
        current_price = current_data['price']
        current_time = datetime.datetime.now()
        
        features = {
            'timestamp': current_time.isoformat(),
            'time_of_day_seconds': current_time.hour * 3600 + current_time.minute * 60 + current_time.second,
            'day_of_week': current_time.weekday(),
            'seconds_since_midnight': current_time.hour * 3600 + current_time.minute * 60 + current_time.second,
            'current_price': current_price,
            'bid': current_data.get('bid'),
            'ask': current_data.get('ask'),
            'bid_size': current_data.get('bid_size'),
            'ask_size': current_data.get('ask_size'),
            'last_price': current_data.get('last'),
            'volume': current_data.get('volume'),
            'spread_absolute': current_data.get('ask', 0) - current_data.get('bid', 0) if current_data.get('ask') and current_data.get('bid') else None,
            'spread_relative': (current_data.get('ask', 0) - current_data.get('bid', 0)) / current_price if current_data.get('ask') and current_data.get('bid') and current_price else None,
            'symbol': order.symbol,
            'order_side': order.action.value,
            'order_type': order.order_type.value,
            'entry_price': order.entry_price,
            'stop_loss': order.stop_loss,
            'priority_manual': getattr(order, 'priority', 3),
            'trading_setup': getattr(order, 'trading_setup', None),
            'core_timeframe': getattr(order, 'core_timeframe', None),
            'price_diff_absolute': current_price - order.entry_price if order.entry_price else None,
            'price_diff_relative': (current_price - order.entry_price) / order.entry_price if order.entry_price else None,
            'volatility_estimate': self.estimate_volatility(order.symbol, current_data.get('history', []), order)
        }
        
        return features

    # Placeholder for volatility estimation
    def estimate_volatility(self, symbol, price_history, order) -> float:
        return 0.001

    # Placeholder for outcome probability scoring
    def score_outcome_stub(self, order):
        return None