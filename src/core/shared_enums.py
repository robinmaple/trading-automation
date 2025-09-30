"""
Shared enumerations for the trading system.
Provides single source of truth for enum values used across database and application layers.
"""

from enum import Enum


class OrderState(Enum):
    """Represents the state of a PlannedOrder in the system."""
    PENDING = "PENDING"              # Waiting to be executed
    LIVE = "LIVE"                    # Order sent to broker, general
    LIVE_WORKING = "LIVE_WORKING"    # Sent to broker, working
    FILLED = "FILLED"                # Successfully executed (position open)
    CANCELLED = "CANCELLED"          # Cancelled by our system
    EXPIRED = "EXPIRED"              # Expired due to time-based strategy
    LIQUIDATED = "LIQUIDATED"        # Position fully liquidated (manually or via stops/targets)
    LIQUIDATED_EXTERNALLY = "LIQUIDATED_EXTERNALLY"  # Manual intervention via broker
    REPLACED = "REPLACED"            # Replaced by better order
    AON_REJECTED = "AON_REJECTED"    # AON order rejected by broker or system validation