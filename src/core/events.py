from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


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


@dataclass
class OrderEvent:
    """A data class representing a state change event for an order."""
    order_id: int
    symbol: str
    old_state: OrderState
    new_state: OrderState
    timestamp: datetime
    source: str  # e.g., 'ExecutionService', 'ExpirationService', 'ReconciliationEngine'
    details: Optional[dict] = None  # Additional context like fill price, quantity, etc.