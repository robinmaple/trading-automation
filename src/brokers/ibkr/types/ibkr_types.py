from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime


@dataclass
class IbkrOrder:
    """Represents an order from IBKR API"""
    order_id: int
    client_id: int
    perm_id: int
    action: str  # BUY, SELL
    order_type: str  # LMT, MKT, STP, etc.
    total_quantity: float
    filled_quantity: float
    remaining_quantity: float
    avg_fill_price: float
    status: str  # PendingSubmit, Submitted, Filled, Cancelled, etc.
    lmt_price: Optional[float] = None
    aux_price: Optional[float] = None  # For stop orders
    parent_id: Optional[int] = None
    why_held: Optional[str] = None
    last_update_time: Optional[datetime] = None
    # <Enhanced IBKR Order Fields - Begin>
    symbol: Optional[str] = None  # Symbol for the order
    security_type: Optional[str] = None  # STK, OPT, FUT, CASH, etc.
    exchange: Optional[str] = None  # Exchange where order is placed
    currency: Optional[str] = None  # Currency of the order
    time_in_force: Optional[str] = None  # GTC, DAY, IOC, etc.
    # <Enhanced IBKR Order Fields - End>


@dataclass
class IbkrPosition:
    """Represents a position from IBKR API"""
    account: str
    contract_id: int
    symbol: str
    security_type: str  # STK, CASH, OPT, FUT
    currency: str
    position: float  # Positive for long, negative for short
    avg_cost: float
    market_price: Optional[float] = None
    market_value: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    realized_pnl: Optional[float] = None


@dataclass
class ReconciliationResult:
    """Result of a reconciliation operation"""
    success: bool
    operation_type: str  # 'orders', 'positions'
    discrepancies: List[Dict[str, Any]]
    timestamp: datetime
    error: Optional[str] = None


@dataclass
class OrderDiscrepancy:
    """Represents a discrepancy between internal and external order state"""
    order_id: int
    internal_status: str
    external_status: str
    discrepancy_type: str  # 'status_mismatch', 'missing_internal', 'missing_external'
    details: Dict[str, Any]


@dataclass
class PositionDiscrepancy:
    """Represents a discrepancy between internal and external position state"""
    symbol: str
    internal_position: float
    external_position: float
    discrepancy_type: str  # 'quantity_mismatch', 'missing_internal', 'missing_external'
    details: Dict[str, Any]