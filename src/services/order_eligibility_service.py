import datetime


class OrderEligibilityService:
    """
    Service responsible for determining if a PlannedOrder is eligible for execution.
    Checks for duplicates, expiration, and (in the future) risk limits.
    """

    def __init__(self, trading_manager):
        # Phase 1: We need references to the data this service requires
        self._trading_manager = trading_manager
        # These will be set after TradingManager initialization
        self.planned_orders = []
        self.probability_engine = None

    def set_dependencies(self, planned_orders, probability_engine):
        """Set the dependencies required for finding executable orders"""
        self.planned_orders = planned_orders
        self.probability_engine = probability_engine

    def can_trade(self, planned_order):
        """
        Main method to check if an order is eligible to be traded.
        Args:
            planned_order (PlannedOrder): The order to check.

        Returns:
            bool: True if the order is eligible, False otherwise.
        """
        # Phase 1: Delegate to the existing logic in TradingManager.
        # This method will be filled with extracted logic in subsequent steps.
        return self._trading_manager._check_order_eligibility(planned_order)

    def find_executable_orders(self):
        """
        Finds all orders that meet execution criteria based on market conditions.
        Returns: List of executable orders with their fill probability.
        """
        # Phase 1: Implement the actual logic instead of delegating
        executable = []
        
        for order in self.planned_orders:
            # Check basic constraints
            if not self._trading_manager._can_place_order(order):
                print(f"   ⚠️  {order.symbol}: Cannot place order (basic constraints failed)")
                continue
            
            # Check intelligent execution criteria
            should_execute, fill_prob = self.probability_engine.should_execute_order(order)
            
            print(f"   Checking {order.action.value} {order.symbol}: should_execute={should_execute}, fill_prob={fill_prob:.3f}")

            if should_execute:
                executable.append({
                    'order': order,
                    'fill_probability': fill_prob,
                    'timestamp': datetime.datetime.now()
                })
        
        return executable