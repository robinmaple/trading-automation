class PositionSizingService:
    """
    Service responsible for calculating position size based on risk management rules.
    """

    def __init__(self, trading_manager):
        # Phase 1: We may not need this reference soon, but keeping it for now.
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
        # Phase 1: Use our own internal implementation instead of delegating back.
        return self.calculate_quantity(
            planned_order.security_type.value,
            planned_order.entry_price,
            planned_order.stop_loss,
            total_capital,
            planned_order.risk_per_trade
        )

    def calculate_quantity(self, security_type, entry_price, stop_loss, total_capital, risk_per_trade):
        """
        Core position sizing logic - extracted from TradingManager.
        Calculate position size based on security type and risk management.
        """
        if entry_price is None or stop_loss is None:
            raise ValueError("Entry price and stop loss are required for quantity calculation")
            
        # Different risk calculation based on security type
        if security_type == "OPT":
            # For options, risk is the premium difference per contract
            # Options are typically 100 shares per contract, so multiply by 100
            risk_per_unit = abs(entry_price - stop_loss) * 100
        else:
            # For stocks, forex, futures, etc. - risk is price difference per share/unit
            risk_per_unit = abs(entry_price - stop_loss)
        
        if risk_per_unit == 0:
            raise ValueError("Entry price and stop loss cannot be the same")
            
        risk_amount = total_capital * risk_per_trade
        base_quantity = risk_amount / risk_per_unit
        
        # Different rounding and minimums based on security type
        if security_type == "CASH":
            # Forex typically trades in standard lots (100,000) or mini lots (10,000)
            # Round to the nearest 10,000 units for reasonable position sizing
            quantity = round(base_quantity / 10000) * 10000
            quantity = max(quantity, 10000)  # Minimum 10,000 units
        elif security_type == "STK":
            # Stocks trade in whole shares
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 share
        elif security_type == "OPT":
            # Options trade in whole contracts (each typically represents 100 shares)
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
            print(f"Options position: {quantity} contracts (each = 100 shares)")
        elif security_type == "FUT":
            # Futures trade in whole contracts
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
        else:
            # Default to whole units for other security types
            quantity = round(base_quantity)
            quantity = max(quantity, 1)
            
        return quantity