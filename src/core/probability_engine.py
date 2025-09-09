from src.core.abstract_data_feed import AbstractDataFeed

class FillProbabilityEngine:
    def __init__(self, data_feed: AbstractDataFeed):
        self.data_feed = data_feed
        self.execution_threshold = 0.7  # Configurable
    
    def should_execute_order(self, order):
        """Determine if an order should be executed"""
        current_data = self.data_feed.get_current_price(order.symbol)
        if not current_data:
            return False, 0.0
        
        current_price = current_data['price']
        
        # Get history from the current_data dict
        price_history = current_data.get('history', [])
        volatility = self.estimate_volatility(order.symbol, price_history, order)
        fill_prob = self.calculate_fill_probability(order, current_price, volatility)
        
        # Debug output - Begin
        print(f"🔍 {order.symbol}: Current={current_price:.5f}, Entry={order.entry_price:.5f}, "
              f"FillProb={fill_prob:.3f}, Threshold={self.execution_threshold:.3f}, "
              f"Execute={fill_prob >= self.execution_threshold}")
        # Debug output - End

        return fill_prob >= self.execution_threshold, fill_prob

    def estimate_volatility(self, symbol, price_history, order):
        """
        Minimal volatility estimation for testing.
        In production, this would use statistical measures.
        """
        # For testing, return a fixed low volatility value
        return 0.001  # Represents low volatility environment

    def calculate_fill_probability(self, order, current_price, volatility):
        """
        Simple fill probability calculation for testing.
        For LIMIT orders: high probability if price is favorable
        """
        if order.order_type.value == 'LMT':
            if order.action.value == 'BUY':
                # For BUY LIMIT: higher probability if current price <= entry price
                if current_price <= order.entry_price:
                    return 0.95  # Very high probability of fill
                else:
                    return 0.1   # Low probability if price above entry
            elif order.action.value == 'SELL':
                # For SELL LIMIT: higher probability if current price >= entry price  
                if current_price >= order.entry_price:
                    return 0.95  # Very high probability of fill
                else:
                    return 0.1   # Low probability if price below entry
        
        # Default probability for other order types
        return 0.5