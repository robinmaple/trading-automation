class OrderEligibilityService:
    """
    Service responsible for determining if a PlannedOrder is eligible for execution.
    Checks for duplicates, expiration, and (in the future) risk limits.
    """

    def __init__(self, trading_manager):
        # Phase 1: Hold a reference to the TradingManager to delegate calls
        # during the initial refactoring phase. This will be removed later.
        self._trading_manager = trading_manager

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
        # Phase 0: Delegate to the existing logic in TradingManager.
        # This method will be refactored to implement the logic internally later.
        return self._trading_manager._find_executable_orders()