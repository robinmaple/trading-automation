"""
Engine responsible for estimating the probability of order execution.
Uses current market data and simple logic to decide if an order should be placed.
This is a placeholder implementation for a future, more sophisticated model.
"""

from src.core.abstract_data_feed import AbstractDataFeed


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
    
    def score_fill(self, order) -> float:
        """Compute fill probability score (0..1) based on rules and current market data."""
        current_data = self.data_feed.get_current_price(order.symbol)
        if not current_data:
            return 0.5  # neutral if no data

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

        return max(0.0, min(1.0, score))

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
