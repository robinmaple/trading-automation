from datetime import datetime
from typing import Optional
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

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

"""
Event system for trading platform.
Provides event type definitions and base event classes for pub/sub architecture.
"""

class EventType(Enum):
    """All event types in the trading system."""
    PRICE_UPDATE = "price_update"
    ORDER_EXECUTED = "order_executed"
    ORDER_REJECTED = "order_rejected" 
    MARKET_HOURS_CHANGE = "market_hours_change"
    RISK_LIMIT_BREACH = "risk_limit_breach"
    DATA_FEED_STATUS = "data_feed_status"
    POSITION_UPDATE = "position_update"
    SYSTEM_HEALTH = "system_health"
    TRADING_SIGNAL = "trading_signal"

@dataclass
class TradingEvent:
    """Base event class for all trading events."""
    event_type: EventType
    timestamp: datetime = field(default_factory=datetime.now)
    source: str = "unknown"
    data: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for serialization."""
        return {
            'event_type': self.event_type.value,
            'timestamp': self.timestamp.isoformat(),
            'source': self.source,
            'data': self.data
        }

@dataclass
class PriceUpdateEvent(TradingEvent):
    """Event for market price updates."""
    symbol: str = ""
    price: float = 0.0
    price_type: str = ""  # BID, ASK, LAST
    
    def __post_init__(self):
        self.event_type = EventType.PRICE_UPDATE
        self.data.update({
            'symbol': self.symbol,
            'price': self.price,
            'price_type': self.price_type
        })

@dataclass 
class OrderExecutedEvent(TradingEvent):
    """Event for order execution."""
    symbol: str = ""
    order_id: str = ""
    quantity: float = 0.0
    price: float = 0.0
    
    def __post_init__(self):
        self.event_type = EventType.ORDER_EXECUTED
        self.data.update({
            'symbol': self.symbol,
            'order_id': self.order_id,
            'quantity': self.quantity,
            'price': self.price
        })