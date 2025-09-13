"""
Service responsible for calculating position size based on risk management rules.
Handles different calculation and rounding logic for various security types
(stocks, options, futures, forex) to ensure appropriate risk per trade.
"""

class PositionSizingService:
    """Calculates trade quantity based on account capital and risk parameters."""

    def __init__(self, trading_manager):
        """Initialize the service with a reference to the trading manager."""
        self._trading_manager = trading_manager

    def calculate_order_quantity(self, planned_order, total_capital) -> int:
        """Calculate the number of shares/units to trade for a given PlannedOrder."""
        return self.calculate_quantity(
            planned_order.security_type.value,
            planned_order.entry_price,
            planned_order.stop_loss,
            total_capital,
            planned_order.risk_per_trade
        )

    def calculate_quantity(self, security_type, entry_price, stop_loss, total_capital, risk_per_trade) -> int:
        """
        Core logic to calculate position size based on security type and risk management.
        Applies different rounding and minimums for stocks, options, futures, and forex.
        """
        if entry_price is None or stop_loss is None:
            raise ValueError("Entry price and stop loss are required for quantity calculation")

        # Calculate risk per unit based on security type
        if security_type == "OPT":
            # For options, risk is the premium difference per contract (x100 shares)
            risk_per_unit = abs(entry_price - stop_loss) * 100
        else:
            # For stocks, forex, futures, etc. - risk is price difference per share/unit
            risk_per_unit = abs(entry_price - stop_loss)

        if risk_per_unit == 0:
            raise ValueError("Entry price and stop loss cannot be the same")

        risk_amount = total_capital * risk_per_trade
        base_quantity = risk_amount / risk_per_unit

        # Apply security-type specific rounding and minimums
        if security_type == "CASH":
            # Forex: round to nearest 10,000 units (mini lots)
            quantity = round(base_quantity / 10000) * 10000
            quantity = max(quantity, 10000)  # Minimum 10,000 units
        elif security_type == "STK":
            # Stocks: round to whole shares
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 share
        elif security_type == "OPT":
            # Options: round to whole contracts (each represents 100 shares)
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
            print(f"Options position: {quantity} contracts (each = 100 shares)")
        elif security_type == "FUT":
            # Futures: round to whole contracts
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
        else:
            # Default: round to whole units for other security types
            quantity = round(base_quantity)
            quantity = max(quantity, 1)

        return quantity