from abstract_data_feed import AbstractDataFeed  # NEW IMPORT

class FillProbabilityEngine:
    def __init__(self, data_feed: AbstractDataFeed):  # CHANGED: Parameter type
        self.data_feed = data_feed  # CHANGED: Now holds abstract data feed
        self.execution_threshold = 0.7  # Configurable
    
    def should_execute_order(self, order):
        """Determine if an order should be executed"""
        # CHANGED: Use data_feed instead of market_data.prices
        current_data = self.data_feed.get_current_price(order.symbol)
        if not current_data:
            return False, 0.0
        
        current_price = current_data['price']
        
        # CHANGED: Get history from the current_data dict
        price_history = current_data.get('history', [])
        volatility = self.estimate_volatility(order.symbol, price_history, order)
        fill_prob = self.calculate_fill_probability(order, current_price, volatility)
        
        return fill_prob >= self.execution_threshold, fill_prob