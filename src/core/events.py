from dataclasses import dataclass
from datetime import datetime
from typing import Optional

# <Shared Enum Integration - Begin>
from src.core.shared_enums import OrderState
# <Shared Enum Integration - End>


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