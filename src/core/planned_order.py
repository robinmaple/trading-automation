"""
Core domain models for the automated trading system.
Defines the fundamental data structures (Enums, Dataclasses) representing:
- Trading concepts (SecurityType, Action, OrderType, PositionStrategy)
- A PlannedOrder, which is the central entity read from Excel and prepared for execution.
- An ActiveOrder, which tracks the state of an order submitted to the broker.
Also provides a static PlannedOrderManager for loading orders from an Excel template.
"""

from decimal import Decimal
from ibapi.client import *
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import pandas as pd
from pandas.tseries.offsets import BDay  # Business day offset
import datetime
from config.trading_core_config import get_config

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)

# Market Hours Utility Functions - Begin
def is_market_hours(check_time: Optional[datetime.datetime] = None) -> bool:
    """
    Check if a given time is within market hours (9:30 AM - 4:00 PM ET, Mon-Fri).
    Simple implementation - adjust for timezones as needed.
    """
    if check_time is None:
        check_time = datetime.datetime.now()
    
    # Check if weekday (0=Monday, 4=Friday)
    is_weekday = 0 <= check_time.weekday() <= 4
    
    # Check if within market hours (9:30 AM - 4:00 PM)
    market_open = check_time.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = check_time.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return is_weekday and (market_open <= check_time <= market_close)

def get_next_trading_day(reference_time: Optional[datetime.datetime] = None) -> datetime.datetime:
    """
    Get the next trading day from reference time.
    If reference time is after market close, returns next business day.
    """
    if reference_time is None:
        reference_time = datetime.datetime.now()
    
    # Use pandas Business Day offset
    next_business_day = reference_time + BDay(1)
    
    # Set to market open time (9:30 AM)
    next_trading_day = next_business_day.replace(hour=9, minute=30, second=0, microsecond=0)
    
    return next_trading_day
# Market Hours Utility Functions - End

class SecurityType(Enum):
    """Supported security types for trading."""
    STK = "STK"
    OPT = "OPT"
    FUT = "FUT"
    IND = "IND"
    FOP = "FOP"
    CASH = "CASH"
    BAG = "BAG"
    WAR = "WAR"
    BOND = "BOND"
    CMDTY = "CMDTY"
    NEWS = "NEWS"
    FUND = "FUND"

class Action(Enum):
    """Supported trade actions."""
    BUY = "BUY"
    SELL = "SELL"
    SSHORT = "SSHORT"  # Short sell

class OrderType(Enum):
    """Supported order types."""
    LMT = "LMT"
    MKT = "MKT"
    STP = "STP"
    STP_LMT = "STP LMT"
    TRAIL = "TRAIL"

class PositionStrategy(Enum):
    """Defines the strategy for holding and managing a position."""
    DAY = "DAY"           # Close before market close
    CORE = "CORE"         # Good till cancel
    HYBRID = "HYBRID"     # 10-day expiration

    @classmethod
    def _missing_(cls, value):
        """Handle case-insensitive and flexible matching for enum creation."""
        value_str = str(value).upper().strip()
        for member in cls:
            if member.value.upper() == value_str:
                return member
        for member in cls:
            if value_str.startswith(member.value.upper()):
                return member
        raise ValueError(f"Invalid PositionStrategy: {value}")

    # Modified Expiration Logic - Begin
    def get_expiration_days(self, import_time: Optional[datetime.datetime] = None) -> Optional[int]:
        """Return the number of days until expiration for this strategy."""
        if import_time is None:
            import_time = datetime.datetime.now()
        
        if self == PositionStrategy.DAY:
            # For DAY strategy: if importing during market hours, expire today
            # if after hours, expire next trading day
            if is_market_hours(import_time):
                return 0  # Expire same day
            else:
                # Calculate days until next trading day
                next_trading_day = get_next_trading_day(import_time)
                days_until_expiration = (next_trading_day.date() - import_time.date()).days
                return max(0, days_until_expiration)
                
        elif self == PositionStrategy.CORE:
            return None  # No expiration for CORE strategy
            
        elif self == PositionStrategy.HYBRID:
            return 10  # Fixed 10 calendar days for HYBRID
            
        return None
    # Modified Expiration Logic - End

    def requires_market_close_action(self) -> bool:
        """Check if this strategy requires closing at market close."""
        return self in [PositionStrategy.DAY, PositionStrategy.HYBRID]

@dataclass
class PlannedOrder:
    """
    The primary domain model representing a trading order planned from an Excel sheet.
    Contains all parameters, validation logic, and methods for conversion to broker-specific objects.
    """
    # Required fields from Excel
    security_type: SecurityType
    exchange: str
    currency: str
    action: Action
    symbol: str
    order_type: OrderType = OrderType.LMT
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    position_strategy: PositionStrategy = PositionStrategy.CORE
    # Phase B Additions - Begin
    trading_setup: Optional[str] = None  # Trading strategy/setup type
    core_timeframe: Optional[str] = None  # Core trading timeframe
    # Phase B Additions - End
    # Phase 1 Additions - Begin
    overall_trend: str = field(default='Neutral')  # Mandatory: 'Bull', 'Bear', 'Neutral'
    brief_analysis: Optional[str] = None  # Optional free-text analysis
    # Phase 1 Additions - End
    expiration_date: Optional[datetime.datetime] = None
    # Calculated fields (no need to store in Excel)
    _quantity: Optional[float] = None
    _profit_target: Optional[float] = None
    # Import time tracking for expiration calculations
    _import_time: Optional[datetime.datetime] = field(default=None, init=False)

    risk_per_trade: Decimal = field(default=Decimal('0.005'))
    risk_reward_ratio: Decimal = field(default=Decimal('2.0'))
    priority: int = field(default=3)

    @property
    def trend_alignment(self) -> bool:
        """
        Return True if action aligns with overall_trend:
        - BUY in Bull or SELL in Bear
        - Sideways/Neutral trend considered misaligned for strict scoring
        """
        if not self.overall_trend:
            return False
        trend = self.overall_trend.lower()
        if (self.action == Action.BUY and trend == 'bull') or \
        (self.action == Action.SELL and trend == 'bear'):
            return True
        return False

    def __post_init__(self) -> None:
        """Dataclass hook to validate and set expiration after initialization."""
        # Minimal logging
        if logger:
            logger.debug(f"Initializing PlannedOrder: {self.symbol}")
        
        self.validate()
        self._set_expiration_date()

    def validate(self) -> None:
        """Enforce business rules and data integrity on order parameters."""
        # <Essential Field Validation - Begin>
        # These fields are required for any meaningful trading order
        if self.entry_price is None:
            raise ValueError("Entry price is required")
        if self.stop_loss is None:
            raise ValueError("Stop loss is required")
        if not self.symbol or not isinstance(self.symbol, str) or self.symbol.strip() == "":
            raise ValueError("Symbol is required and must be a non-empty string")
        if self.action is None:
            raise ValueError("Action is required")
        if self.security_type is None:
            raise ValueError("Security type is required")
        if not self.exchange or self.exchange.strip() == "":
            raise ValueError("Exchange is required")
        if not self.currency or self.currency.strip() == "":
            raise ValueError("Currency is required")
        # <Essential Field Validation - End>

        # <Business Logic Validation - Begin>
        # Risk management validation
        if self.risk_per_trade is None:
            raise ValueError("Risk per trade cannot be None")
        if self.risk_reward_ratio is None:
            raise ValueError("Risk reward ratio cannot be None")  
        if self.priority is None:
            raise ValueError("Priority cannot be None")

        if not 1 <= self.priority <= 5:
            raise ValueError("Priority must be between 1 and 5")
            
        if self.risk_per_trade <= 0:
            raise ValueError("Risk per trade must be positive")
        if self.risk_per_trade > 0.02:  # 2% max risk
            raise ValueError("Risk per trade cannot exceed 2%")
        if self.risk_reward_ratio < 1.0:
            raise ValueError("Risk/reward ratio must be at least 1.0")

        # Stop loss positioning logic
        if (self.action == Action.BUY and self.stop_loss >= self.entry_price) or \
        (self.action == Action.SELL and self.stop_loss <= self.entry_price):
            raise ValueError("Stop loss must be on the correct side of the entry price for a protective order")
        # <Business Logic Validation - End>

        # <Phase 1 Validation - Begin>
        # Phase 1 validation for overall_trend
        allowed_trends = ['Bull', 'Bear', 'Neutral']
        if self.overall_trend is None:
            self.overall_trend = "Neutral"  # Provide default
        elif self.overall_trend not in allowed_trends:
            raise ValueError(f"overall_trend must be one of {allowed_trends}, got '{self.overall_trend}'")
        # <Phase 1 Validation - End>

        # <Phase B Validation - Begin>
        # Phase B additions
        if self.trading_setup and len(self.trading_setup) > 100:
            raise ValueError("Trading setup description too long")
        if self.core_timeframe and len(self.core_timeframe) > 50:
            raise ValueError("Core timeframe description too long")
        # <Phase B Validation - End>

        # <Data Quality Validation - Begin>
        # Additional data quality checks
        if self.entry_price <= 0:
            raise ValueError("Entry price must be positive")
        if self.stop_loss <= 0:
            raise ValueError("Stop loss must be positive")
        if self.entry_price == self.stop_loss:
            raise ValueError("Entry price and stop loss cannot be equal")
        # <Data Quality Validation - End>

        # Minimal logging
        if logger:
            logger.debug(f"PlannedOrder validation passed: {self.symbol}")

    # Modified Expiration Date Calculation - Begin
    def _set_expiration_date(self) -> None:
        """Set expiration date based on position strategy and import time."""
        # Use provided import time or current time
        calculation_time = self._import_time or datetime.datetime.now()
        
        expiration_days = self.position_strategy.get_expiration_days(calculation_time)
        if expiration_days is not None:
            self.expiration_date = calculation_time + datetime.timedelta(days=expiration_days)
            
            # Set expiration to market close time (4 PM)
            self.expiration_date = self.expiration_date.replace(
                hour=16, minute=0, second=0, microsecond=0
            )
    # Modified Expiration Date Calculation - End

    def calculate_quantity(self, total_capital: float) -> float:
        """Calculate position size based on risk management. Rounds appropriately for the security type."""
        if self.entry_price is None or self.stop_loss is None:
            raise ValueError("Entry price and stop loss required for quantity calculation")

        risk_per_share = abs(self.entry_price - self.stop_loss)
        risk_amount = total_capital * self.risk_per_trade
        base_quantity = risk_amount / risk_per_share

        MINIMUM_CASH_UNITS = 10000
        if self.security_type == SecurityType.CASH:
            calculated_quantity = round(base_quantity / MINIMUM_CASH_UNITS) * MINIMUM_CASH_UNITS
            calculated_quantity = max(calculated_quantity, MINIMUM_CASH_UNITS)
        else:
            calculated_quantity = round(base_quantity)
            calculated_quantity = max(calculated_quantity, 1)

        self._quantity = calculated_quantity
        
        # Minimal logging
        if logger:
            logger.debug(f"Calculated quantity for {self.symbol}: {calculated_quantity}")
        
        return self._quantity

    def calculate_profit_target(self) -> float:
        """Calculate profit target price based on risk/reward ratio."""
        if self.entry_price is None or self.stop_loss is None:
            raise ValueError("Entry price and stop loss required for profit target")

        risk_amount = abs(self.entry_price - self.stop_loss)
        if self.action == Action.BUY:
            self._profit_target = self.entry_price + (risk_amount * self.risk_reward_ratio)
        else:
            self._profit_target = self.entry_price - (risk_amount * self.risk_reward_ratio)
        return self._profit_target

    def to_ib_contract(self) -> Contract:
        """Convert the planned order to an IBKR Contract object."""
        contract = Contract()
        contract.symbol = self.symbol
        contract.secType = self.security_type.value
        contract.exchange = self.exchange
        contract.currency = self.currency
        return contract

    def to_ib_order(self, total_capital: float) -> Order:
        """Convert the planned order to an IBKR Order object."""
        order = Order()
        order.action = self.action.value
        order.orderType = self.order_type.value
        order.totalQuantity = self.calculate_quantity(total_capital)

        if self.order_type == OrderType.LMT and self.entry_price:
            order.lmtPrice = self.entry_price
        elif self.order_type == OrderType.STP and self.stop_loss:
            order.auxPrice = self.stop_loss

        if self.position_strategy == PositionStrategy.DAY:
            order.tif = "DAY"
        else:
            order.tif = "GTC"

        return order

class PlannedOrderManager:
    """Static class providing utilities for loading and managing PlannedOrders."""

    @staticmethod
    def from_excel(file_path: str, config: Optional[Dict[str, Any]] = None) -> List[PlannedOrder]:
        """Load and parse planned orders from an Excel template with configurable defaults."""
        # Use provided config or get default (live environment)
        if config is None:
            from config.trading_core_config import get_config
            config = get_config("live")
        
        order_defaults = config.get('order_defaults', {})
        
        # Capture import time for expiration calculations
        import_time = datetime.datetime.now()

        orders = []
        try:
            df = pd.read_excel(file_path)
            
            # Minimal logging
            if logger:
                logger.info(f"Loading planned orders from: {file_path}")

            for index, row in df.iterrows():
                try:
                    # Parse enums / strings
                    security_type = SecurityType[str(row['Security Type']).strip()]
                    action = Action[str(row['Action']).strip().upper()]
                    order_type_str = str(row.get('Order Type', 'LMT')).strip()
                    order_type = OrderType[order_type_str] if order_type_str and order_type_str != 'nan' else OrderType.LMT

                    # Convert position strategy string to PositionStrategy enum
                    position_strategy_str = str(row.get('Position Management Strategy', 'CORE')).strip()
                    position_strategy = PositionStrategy(position_strategy_str)

                    # Risk and other fields
                    risk_per_trade = float(row["Risk Per Trade"]) if pd.notna(row.get("Risk Per Trade")) else float(order_defaults.get("risk_per_trade", 0.005))
                    entry_price = float(row["Entry Price"]) if pd.notna(row.get("Entry Price")) else None
                    stop_loss = float(row["Stop Loss"]) if pd.notna(row.get("Stop Loss")) else None
                    risk_reward_ratio = float(row["Risk Reward Ratio"]) if pd.notna(row.get("Risk Reward Ratio")) else float(order_defaults.get("risk_reward_ratio", 2.0))
                    priority = int(row["Priority"]) if pd.notna(row.get("Priority")) else int(order_defaults.get("priority", 3))

                    # Phase B additions
                    trading_setup = str(row.get("Trading Setup", "")).strip() or None
                    core_timeframe = str(row.get("Core Timeframe", "")).strip() or None

                    # Phase 1 additions
                    overall_trend = str(row.get("Overall Trend", "Neutral")).strip()
                    brief_analysis = str(row.get("Brief Analysis", "")).strip() or "No analysis provided"

                    # Build PlannedOrder
                    order = PlannedOrder(
                        security_type=security_type,
                        exchange=str(row['Exchange']).strip(),
                        currency=str(row['Currency']).strip(),
                        action=action,
                        symbol=str(row['Symbol']).strip(),
                        order_type=order_type,
                        position_strategy=position_strategy,
                        risk_per_trade=risk_per_trade,
                        entry_price=entry_price,
                        stop_loss=stop_loss,
                        risk_reward_ratio=risk_reward_ratio,
                        priority=priority,
                        trading_setup=trading_setup,
                        core_timeframe=core_timeframe,
                        overall_trend=overall_trend,
                        brief_analysis=brief_analysis,
                    )
                    
                    # Set import time for proper expiration calculation
                    order._import_time = import_time
                    
                    orders.append(order)

                except Exception as row_error:
                    if logger:
                        logger.error(f"Error processing row {index + 2}: {row_error}")
                    continue

            # Minimal logging
            if logger:
                logger.info(f"Successfully loaded {len(orders)} orders from Excel")
            
            return orders

        except FileNotFoundError:
            if logger:
                logger.error(f"Excel file not found: {file_path}")
            return []
        except Exception as e:
            if logger:
                logger.error(f"Error loading Excel file: {e}")
            return []

class ActiveOrder:
    """Tracks the state and metadata of an order that has been submitted to the broker."""
    planned_order: PlannedOrder
    order_ids: List[int]
    db_id: int
    status: str  # 'SUBMITTED', 'WORKING', 'FILLED', 'CANCELLING'
    capital_commitment: float
    timestamp: datetime.datetime
    is_live_trading: bool
    fill_probability: float  # Probability at time of submission

    @property
    def symbol(self) -> str:
        """Convenience property to access the symbol from the planned order."""
        return self.planned_order.symbol

    def is_working(self) -> bool:
        """Check if the order is still active (not filled or cancelled)."""
        return self.status in ['SUBMITTED', 'WORKING']

    def age_seconds(self) -> float:
        """Return how long this order has been active in seconds."""
        return (datetime.datetime.now() - self.timestamp).total_seconds()

    def __str__(self) -> str:
        return (f"ActiveOrder({self.planned_order.symbol}, status={self.status}, "
                f"capital=${self.capital_commitment:,.2f}, age={self.age_seconds():.1f}s)")

    def update_status(self, new_status: str) -> None:
        """Update the status of this order."""
        # Minimal logging
        if logger and self.status != new_status:
            logger.debug(f"ActiveOrder status change: {self.symbol} {self.status} -> {new_status}")
        self.status = new_status