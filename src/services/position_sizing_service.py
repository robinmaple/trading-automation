"""
Service responsible for calculating position size based on risk management rules.
Handles different calculation and rounding logic for various security types
(stocks, options, futures, forex) to ensure appropriate risk per trade.
"""

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)

# Minimal safe logging import for fallback
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)


class PositionSizingService:
    """Calculates trade quantity based on account capital and risk parameters."""

    def __init__(self, trading_manager):
        """Initialize the service with a reference to the trading manager."""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing PositionSizingService",
            context_provider={
                "trading_manager_provided": trading_manager is not None,
                "trading_manager_type": type(trading_manager).__name__
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self._trading_manager = trading_manager

    def calculate_order_quantity(self, planned_order, total_capital) -> int:
        """Calculate the number of shares/units to trade for a given PlannedOrder."""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Calculating order quantity for planned order",
            context_provider={
                "planned_order_id": getattr(planned_order, 'id', 'Unknown'),
                "symbol": getattr(planned_order, 'symbol', 'Unknown'),
                "security_type": getattr(planned_order, 'security_type', 'Unknown'),
                "entry_price": getattr(planned_order, 'entry_price', None),
                "stop_loss": getattr(planned_order, 'stop_loss', None),
                "risk_per_trade": getattr(planned_order, 'risk_per_trade', None),
                "total_capital": total_capital
            }
        )
        # <Context-Aware Logging Integration - End>
        
        quantity = self.calculate_quantity(
            planned_order.security_type.value,
            planned_order.entry_price,
            planned_order.stop_loss,
            total_capital,
            planned_order.risk_per_trade
        )
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            f"Order quantity calculation completed",
            context_provider={
                "planned_order_id": getattr(planned_order, 'id', 'Unknown'),
                "symbol": getattr(planned_order, 'symbol', 'Unknown'),
                "calculated_quantity": quantity,
                "security_type": planned_order.security_type.value
            },
            decision_reason="Position sizing calculation successful"
        )
        # <Context-Aware Logging Integration - End>
        
        return quantity

    def calculate_quantity(self, security_type, entry_price, stop_loss, total_capital, risk_per_trade) -> int:
        """
        Core logic to calculate position size based on security type and risk management.
        Applies different rounding and minimums for stocks, options, futures, and forex.
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            f"Starting quantity calculation for {security_type}",
            context_provider={
                "security_type": security_type,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "total_capital": total_capital,
                "risk_per_trade": risk_per_trade
            }
        )
        # <Context-Aware Logging Integration - End>
        
        if entry_price is None or stop_loss is None:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Missing required price data for quantity calculation",
                context_provider={
                    "security_type": security_type,
                    "entry_price": entry_price,
                    "stop_loss": stop_loss
                },
                decision_reason="Quantity calculation failed - missing price data"
            )
            # <Context-Aware Logging Integration - End>
            raise ValueError("Entry price and stop loss are required for quantity calculation")

        # Calculate risk per unit based on security type
        if security_type == "OPT":
            # For options, risk is the premium difference per contract (x100 shares)
            risk_per_unit = abs(entry_price - stop_loss) * 100
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Calculated options risk per unit",
                context_provider={
                    "security_type": security_type,
                    "risk_per_unit": risk_per_unit,
                    "premium_difference": abs(entry_price - stop_loss),
                    "multiplier": 100
                }
            )
            # <Context-Aware Logging Integration - End>
        else:
            # For stocks, forex, futures, etc. - risk is price difference per share/unit
            risk_per_unit = abs(entry_price - stop_loss)
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Calculated standard risk per unit",
                context_provider={
                    "security_type": security_type,
                    "risk_per_unit": risk_per_unit,
                    "price_difference": abs(entry_price - stop_loss)
                }
            )
            # <Context-Aware Logging Integration - End>

        if risk_per_unit == 0:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Zero risk per unit detected",
                context_provider={
                    "security_type": security_type,
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "risk_per_unit": risk_per_unit
                },
                decision_reason="Quantity calculation failed - zero risk per unit"
            )
            # <Context-Aware Logging Integration - End>
            raise ValueError("Entry price and stop loss cannot be the same")

        risk_amount = total_capital * risk_per_trade
        base_quantity = risk_amount / risk_per_unit
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Calculated base quantity before rounding",
            context_provider={
                "security_type": security_type,
                "risk_amount": risk_amount,
                "base_quantity": base_quantity,
                "risk_per_unit": risk_per_unit
            }
        )
        # <Context-Aware Logging Integration - End>

        # Apply security-type specific rounding and minimums
        if security_type == "CASH":
            # Forex: round to nearest 10,000 units (mini lots)
            quantity = round(base_quantity / 10000) * 10000
            quantity = max(quantity, 10000)  # Minimum 10,000 units
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Applied forex rounding and minimum",
                context_provider={
                    "security_type": security_type,
                    "base_quantity": base_quantity,
                    "rounded_quantity": quantity,
                    "rounding_unit": 10000,
                    "minimum_quantity": 10000
                }
            )
            # <Context-Aware Logging Integration - End>
        elif security_type == "STK":
            # Stocks: round to whole shares
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 share
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Applied stock rounding and minimum",
                context_provider={
                    "security_type": security_type,
                    "base_quantity": base_quantity,
                    "rounded_quantity": quantity,
                    "rounding_unit": 1,
                    "minimum_quantity": 1
                }
            )
            # <Context-Aware Logging Integration - End>
        elif security_type == "OPT":
            # Options: round to whole contracts (each represents 100 shares)
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Applied options rounding and minimum",
                context_provider={
                    "security_type": security_type,
                    "base_quantity": base_quantity,
                    "rounded_quantity": quantity,
                    "rounding_unit": 1,
                    "minimum_quantity": 1,
                    "contracts_equivalent_shares": quantity * 100
                }
            )
            # <Context-Aware Logging Integration - End>
        elif security_type == "FUT":
            # Futures: round to whole contracts
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Applied futures rounding and minimum",
                context_provider={
                    "security_type": security_type,
                    "base_quantity": base_quantity,
                    "rounded_quantity": quantity,
                    "rounding_unit": 1,
                    "minimum_quantity": 1
                }
            )
            # <Context-Aware Logging Integration - End>
        else:
            # Default: round to whole units for other security types
            quantity = round(base_quantity)
            quantity = max(quantity, 1)
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Applied default rounding and minimum",
                context_provider={
                    "security_type": security_type,
                    "base_quantity": base_quantity,
                    "rounded_quantity": quantity,
                    "rounding_unit": 1,
                    "minimum_quantity": 1
                }
            )
            # <Context-Aware Logging Integration - End>

        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            f"Quantity calculation completed for {security_type}",
            context_provider={
                "security_type": security_type,
                "final_quantity": quantity,
                "risk_amount": risk_amount,
                "risk_per_unit": risk_per_unit,
                "base_quantity": base_quantity,
                "total_capital": total_capital,
                "risk_per_trade": risk_per_trade
            },
            decision_reason="Position sizing calculation successful"
        )
        # <Context-Aware Logging Integration - End>
        
        return quantity