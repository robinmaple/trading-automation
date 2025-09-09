class PositionSizingService:
    """
    Service responsible for calculating position size based on risk management rules.
    """

    def __init__(self, trading_manager):
        # Phase 1: Hold a reference to the TradingManager to delegate calls
        # during the initial refactoring phase. This will be removed later.
        self._trading_manager = trading_manager

    def calculate_order_quantity(self, planned_order, total_capital):
        """
        Calculates the number of shares to trade for a given PlannedOrder.
        Args:
            planned_order (PlannedOrder): The order for which to calculate quantity.
            total_capital (float): The total account capital.

        Returns:
            int: The calculated quantity (number of shares).
        """
        # Phase 0: Delegate to the existing logic in TradingManager.
        # Extract parameters from the PlannedOrder object for the legacy method call.
        return self._trading_manager._calculate_position_size(
            planned_order,
            total_capital,
            planned_order.risk_per_trade
        )