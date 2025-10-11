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

# <Context-Aware Logger Integration - Begin>
from src.core.context_aware_logger import get_context_logger, TradingEventType
# <Context-Aware Logger Integration - End>

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

    def __post_init__(self) -> None:
        """Dataclass hook to validate and set expiration after initialization."""
        # <Context-Aware Logger Initialization - Begin>
        self.context_logger = get_context_logger()
        # <Context-Aware Logger Initialization - End>
        
        # <Context-Aware Logging - PlannedOrder Initialization Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "PlannedOrder initialization starting",
            symbol=self.symbol,
            context_provider={
                "security_type": self.security_type.value,
                "action": self.action.value,
                "order_type": self.order_type.value,
                "position_strategy": self.position_strategy.value,
                "entry_price": self.entry_price,
                "stop_loss": self.stop_loss
            }
        )
        # <Context-Aware Logging - PlannedOrder Initialization Start - End>
        
        # Minimal logging
        if logger:
            logger.debug(f"Initializing PlannedOrder: {self.symbol}")
        
        self.validate()
        self._set_expiration_date()
        
        # <Context-Aware Logging - PlannedOrder Initialization Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "PlannedOrder initialization completed successfully",
            symbol=self.symbol,
            context_provider={
                "expiration_date": self.expiration_date.isoformat() if self.expiration_date else None,
                "trend_alignment": self.trend_alignment,
                "risk_per_trade": float(self.risk_per_trade),
                "risk_reward_ratio": float(self.risk_reward_ratio),
                "priority": self.priority
            },
            decision_reason="PlannedOrder validated and ready for execution"
        )
        # <Context-Aware Logging - PlannedOrder Initialization Complete - End>

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

    def validate(self) -> None:
        """Enforce business rules and data integrity on order parameters."""
        # <Context-Aware Logging - Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting PlannedOrder validation",
            symbol=self.symbol,
            context_provider={
                "validation_phase": "essential_fields"
            }
        )
        # <Context-Aware Logging - Validation Start - End>
        
        # <Essential Field Validation - Begin>
        # These fields are required for any meaningful trading order
        if self.entry_price is None:
            validation_error = "Entry price is required"
            self._log_validation_error(validation_error, "essential_fields")
            raise ValueError(validation_error)
        if self.stop_loss is None:
            validation_error = "Stop loss is required"
            self._log_validation_error(validation_error, "essential_fields")
            raise ValueError(validation_error)
        if not self.symbol or not isinstance(self.symbol, str) or self.symbol.strip() == "":
            validation_error = "Symbol is required and must be a non-empty string"
            self._log_validation_error(validation_error, "essential_fields")
            raise ValueError(validation_error)
        if self.action is None:
            validation_error = "Action is required"
            self._log_validation_error(validation_error, "essential_fields")
            raise ValueError(validation_error)
        if self.security_type is None:
            validation_error = "Security type is required"
            self._log_validation_error(validation_error, "essential_fields")
            raise ValueError(validation_error)
        if not self.exchange or self.exchange.strip() == "":
            validation_error = "Exchange is required"
            self._log_validation_error(validation_error, "essential_fields")
            raise ValueError(validation_error)
        if not self.currency or self.currency.strip() == "":
            validation_error = "Currency is required"
            self._log_validation_error(validation_error, "essential_fields")
            raise ValueError(validation_error)
        # <Essential Field Validation - End>

        # <Context-Aware Logging - Business Logic Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting business logic validation",
            symbol=self.symbol,
            context_provider={
                "validation_phase": "business_logic"
            }
        )
        # <Context-Aware Logging - Business Logic Validation Start - End>
        
        # <Business Logic Validation - Begin>
        # Risk management validation
        if self.risk_per_trade is None:
            validation_error = "Risk per trade cannot be None"
            self._log_validation_error(validation_error, "business_logic")
            raise ValueError(validation_error)
        if self.risk_reward_ratio is None:
            validation_error = "Risk reward ratio cannot be None"  
            self._log_validation_error(validation_error, "business_logic")
            raise ValueError(validation_error)
        if self.priority is None:
            validation_error = "Priority cannot be None"
            self._log_validation_error(validation_error, "business_logic")
            raise ValueError(validation_error)

        if not 1 <= self.priority <= 5:
            validation_error = "Priority must be between 1 and 5"
            self._log_validation_error(validation_error, "business_logic")
            raise ValueError(validation_error)
            
        if self.risk_per_trade <= 0:
            validation_error = "Risk per trade must be positive"
            self._log_validation_error(validation_error, "business_logic")
            raise ValueError(validation_error)
        if self.risk_per_trade > 0.02:  # 2% max risk
            validation_error = "Risk per trade cannot exceed 2%"
            self._log_validation_error(validation_error, "business_logic")
            raise ValueError(validation_error)
        if self.risk_reward_ratio < 1.0:
            validation_error = "Risk/reward ratio must be at least 1.0"
            self._log_validation_error(validation_error, "business_logic")
            raise ValueError(validation_error)

        # Stop loss positioning logic
        if (self.action == Action.BUY and self.stop_loss >= self.entry_price) or \
        (self.action == Action.SELL and self.stop_loss <= self.entry_price):
            validation_error = "Stop loss must be on the correct side of the entry price for a protective order"
            self._log_validation_error(validation_error, "business_logic")
            raise ValueError(validation_error)
        # <Business Logic Validation - End>

        # <Context-Aware Logging - Phase Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting phase-specific validation",
            symbol=self.symbol,
            context_provider={
                "validation_phase": "phase_validation"
            }
        )
        # <Context-Aware Logging - Phase Validation Start - End>
        
        # <Phase 1 Validation - Begin>
        # Phase 1 validation for overall_trend
        allowed_trends = ['Bull', 'Bear', 'Neutral']
        if self.overall_trend is None:
            self.overall_trend = "Neutral"  # Provide default
        elif self.overall_trend not in allowed_trends:
            validation_error = f"overall_trend must be one of {allowed_trends}, got '{self.overall_trend}'"
            self._log_validation_error(validation_error, "phase_validation")
            raise ValueError(validation_error)
        # <Phase 1 Validation - End>

        # <Phase B Validation - Begin>
        # Phase B additions
        if self.trading_setup and len(self.trading_setup) > 100:
            validation_error = "Trading setup description too long"
            self._log_validation_error(validation_error, "phase_validation")
            raise ValueError(validation_error)
        if self.core_timeframe and len(self.core_timeframe) > 50:
            validation_error = "Core timeframe description too long"
            self._log_validation_error(validation_error, "phase_validation")
            raise ValueError(validation_error)
        # <Phase B Validation - End>

        # <Context-Aware Logging - Data Quality Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting data quality validation",
            symbol=self.symbol,
            context_provider={
                "validation_phase": "data_quality"
            }
        )
        # <Context-Aware Logging - Data Quality Validation Start - End>
        
        # <Data Quality Validation - Begin>
        # Additional data quality checks
        if self.entry_price <= 0:
            validation_error = "Entry price must be positive"
            self._log_validation_error(validation_error, "data_quality")
            raise ValueError(validation_error)
        if self.stop_loss <= 0:
            validation_error = "Stop loss must be positive"
            self._log_validation_error(validation_error, "data_quality")
            raise ValueError(validation_error)
        if self.entry_price == self.stop_loss:
            validation_error = "Entry price and stop loss cannot be equal"
            self._log_validation_error(validation_error, "data_quality")
            raise ValueError(validation_error)
        # <Data Quality Validation - End>

        # <Context-Aware Logging - Validation Success - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "PlannedOrder validation completed successfully",
            symbol=self.symbol,
            context_provider={
                "validation_phases_passed": ["essential_fields", "business_logic", "phase_validation", "data_quality"],
                "overall_trend": self.overall_trend,
                "trend_alignment": self.trend_alignment
            },
            decision_reason="All validation checks passed"
        )
        # <Context-Aware Logging - Validation Success - End>
        
        # Minimal logging
        if logger:
            logger.debug(f"PlannedOrder validation passed: {self.symbol}")

    def _log_validation_error(self, error_message: str, validation_phase: str) -> None:
        """Helper method to log validation errors with structured context."""
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            f"PlannedOrder validation failed in {validation_phase}",
            symbol=self.symbol,
            context_provider={
                "error_message": error_message,
                "validation_phase": validation_phase,
                "entry_price": self.entry_price,
                "stop_loss": self.stop_loss,
                "action": self.action.value if self.action else None,
                "security_type": self.security_type.value if self.security_type else None
            },
            decision_reason=f"Validation error: {error_message}"
        )

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
            
            # <Context-Aware Logging - Expiration Calculation - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Expiration date calculated for order",
                symbol=self.symbol,
                context_provider={
                    "position_strategy": self.position_strategy.value,
                    "expiration_days": expiration_days,
                    "expiration_date": self.expiration_date.isoformat(),
                    "calculation_time": calculation_time.isoformat(),
                    "market_hours": is_market_hours(calculation_time)
                },
                decision_reason=f"Expiration set to {expiration_days} days for {self.position_strategy.value} strategy"
            )
            # <Context-Aware Logging - Expiration Calculation - End>
    # Modified Expiration Date Calculation - End

    def calculate_quantity(self, total_capital: float) -> float:
        """Calculate position size based on risk management. Rounds appropriately for the security type."""
        # <Context-Aware Logging - Quantity Calculation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Starting position quantity calculation",
            symbol=self.symbol,
            context_provider={
                "total_capital": total_capital,
                "entry_price": self.entry_price,
                "stop_loss": self.stop_loss,
                "risk_per_trade": float(self.risk_per_trade),
                "security_type": self.security_type.value
            }
        )
        # <Context-Aware Logging - Quantity Calculation Start - End>
        
        if self.entry_price is None or self.stop_loss is None:
            calculation_error = "Entry price and stop loss required for quantity calculation"
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Quantity calculation failed - missing required data",
                symbol=self.symbol,
                context_provider={
                    "entry_price_available": self.entry_price is not None,
                    "stop_loss_available": self.stop_loss is not None
                },
                decision_reason=calculation_error
            )
            raise ValueError(calculation_error)

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
        
        # <Context-Aware Logging - Quantity Calculation Success - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Position quantity calculated successfully",
            symbol=self.symbol,
            context_provider={
                "calculated_quantity": calculated_quantity,
                "risk_per_share": risk_per_share,
                "risk_amount": risk_amount,
                "base_quantity": base_quantity,
                "minimum_units": MINIMUM_CASH_UNITS if self.security_type == SecurityType.CASH else 1
            },
            decision_reason=f"Position size calculated: {calculated_quantity} units"
        )
        # <Context-Aware Logging - Quantity Calculation Success - End>
        
        # Minimal logging
        if logger:
            logger.debug(f"Calculated quantity for {self.symbol}: {calculated_quantity}")
        
        return self._quantity

    def calculate_profit_target(self) -> float:
        """Calculate profit target price based on risk/reward ratio."""
        # <Context-Aware Logging - Profit Target Calculation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Starting profit target calculation",
            symbol=self.symbol,
            context_provider={
                "entry_price": self.entry_price,
                "stop_loss": self.stop_loss,
                "risk_reward_ratio": float(self.risk_reward_ratio),
                "action": self.action.value
            }
        )
        # <Context-Aware Logging - Profit Target Calculation Start - End>
        
        if self.entry_price is None or self.stop_loss is None:
            calculation_error = "Entry price and stop loss required for profit target"
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Profit target calculation failed - missing required data",
                symbol=self.symbol,
                context_provider={
                    "entry_price_available": self.entry_price is not None,
                    "stop_loss_available": self.stop_loss is not None
                },
                decision_reason=calculation_error
            )
            raise ValueError(calculation_error)

        risk_amount = abs(self.entry_price - self.stop_loss)
        if self.action == Action.BUY:
            self._profit_target = self.entry_price + (risk_amount * self.risk_reward_ratio)
        else:
            self._profit_target = self.entry_price - (risk_amount * self.risk_reward_ratio)
            
        # <Context-Aware Logging - Profit Target Calculation Success - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Profit target calculated successfully",
            symbol=self.symbol,
            context_provider={
                "profit_target": self._profit_target,
                "risk_amount": risk_amount,
                "calculation_method": "BUY_ADD" if self.action == Action.BUY else "SELL_SUBTRACT"
            },
            decision_reason=f"Profit target set to ${self._profit_target:.2f}"
        )
        # <Context-Aware Logging - Profit Target Calculation Success - End>
        
        return self._profit_target

    def to_ib_contract(self) -> Contract:
        """Convert the planned order to an IBKR Contract object."""
        contract = Contract()
        contract.symbol = self.symbol
        contract.secType = self.security_type.value
        contract.exchange = self.exchange
        contract.currency = self.currency
        
        # <Context-Aware Logging - IB Contract Creation - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "IBKR Contract created from PlannedOrder",
            symbol=self.symbol,
            context_provider={
                "contract_symbol": contract.symbol,
                "security_type": contract.secType,
                "exchange": contract.exchange,
                "currency": contract.currency
            }
        )
        # <Context-Aware Logging - IB Contract Creation - End>
        
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

        # <Context-Aware Logging - IB Order Creation - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "IBKR Order created from PlannedOrder",
            symbol=self.symbol,
            context_provider={
                "order_action": order.action,
                "order_type": order.orderType,
                "total_quantity": order.totalQuantity,
                "limit_price": getattr(order, 'lmtPrice', None),
                "stop_price": getattr(order, 'auxPrice', None),
                "time_in_force": order.tif,
                "position_strategy": self.position_strategy.value
            },
            decision_reason="PlannedOrder converted to IBKR Order object"
        )
        # <Context-Aware Logging - IB Order Creation - End>

        return order

class PlannedOrderManager:
    """Static class providing utilities for loading and managing PlannedOrders."""

    @staticmethod
    def from_excel(file_path: str, config: Optional[Dict[str, Any]] = None) -> List[PlannedOrder]:
        """Load and parse planned orders from an Excel template - ORIGINAL WORKING VERSION with zero-row filtering"""
        # <Context-Aware Logger Initialization - Begin>
        context_logger = get_context_logger()
        # <Context-Aware Logger Initialization - End>
        
        # <Context-Aware Logging - Excel Loading Start - Begin>
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting Excel file loading for planned orders",
            context_provider={
                "file_path": file_path,
                "config_provided": config is not None
            }
        )
        # <Context-Aware Logging - Excel Loading Start - End>
        
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
            
            # <Context-Aware Logging - Excel File Loaded - Begin>
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Excel file loaded successfully",
                context_provider={
                    "total_rows": len(df),
                    "file_path": file_path,
                    "columns": list(df.columns)
                }
            )
            # <Context-Aware Logging - Excel File Loaded - End>
            
            # Minimal logging
            if logger:
                logger.info(f"Loading planned orders from: {file_path}")

            valid_rows = 0
            skipped_rows = 0
            error_rows = 0
            
            for index, row in df.iterrows():
                try:
                    # NEW: Skip all-zero rows (the only enhancement needed)
                    if PlannedOrderManager._is_all_zero_row(row):
                        skipped_rows += 1
                        continue
                    
                    # ORIGINAL WORKING CODE BELOW - don't change this
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
                    valid_rows += 1
                    
                    # <Context-Aware Logging - Row Processing Success - Begin>
                    context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "Excel row processed successfully",
                        symbol=order.symbol,
                        context_provider={
                            "row_index": index + 2,  # +2 for Excel row numbers (header + 1-based)
                            "security_type": security_type.value,
                            "action": action.value,
                            "position_strategy": position_strategy.value,
                            "entry_price": entry_price,
                            "stop_loss": stop_loss
                        },
                        decision_reason="Row successfully converted to PlannedOrder"
                    )
                    # <Context-Aware Logging - Row Processing Success - End>

                except Exception as row_error:
                    error_rows += 1
                    # <Context-Aware Logging - Row Processing Error - Begin>
                    context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Error processing Excel row",
                        context_provider={
                            "row_index": index + 2,
                            "error_type": type(row_error).__name__,
                            "error_message": str(row_error),
                            "symbol": str(row.get('Symbol', 'UNKNOWN')).strip() if 'Symbol' in row else 'UNKNOWN'
                        },
                        decision_reason=f"Row processing failed: {row_error}"
                    )
                    # <Context-Aware Logging - Row Processing Error - End>
                    if logger:
                        logger.error(f"Error processing row {index + 2}: {row_error}")
                    continue

            # <Context-Aware Logging - Excel Loading Summary - Begin>
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Excel file processing completed",
                context_provider={
                    "total_rows_processed": len(df),
                    "valid_orders_created": valid_rows,
                    "skipped_zero_rows": skipped_rows,
                    "error_rows": error_rows,
                    "success_rate_percent": round((valid_rows / len(df)) * 100, 2) if len(df) > 0 else 0
                },
                decision_reason=f"Successfully loaded {valid_rows} orders from {len(df)} total rows"
            )
            # <Context-Aware Logging - Excel Loading Summary - End>
            
            # Minimal logging
            if logger:
                logger.info(f"Successfully loaded {len(orders)} orders from Excel")
            
            return orders

        except FileNotFoundError:
            # <Context-Aware Logging - File Not Found - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Excel file not found",
                context_provider={
                    "file_path": file_path
                },
                decision_reason="FileNotFoundError - Excel file does not exist"
            )
            # <Context-Aware Logging - File Not Found - End>
            if logger:
                logger.error(f"Excel file not found: {file_path}")
            return []
        except Exception as e:
            # <Context-Aware Logging - Excel Loading Error - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error loading Excel file",
                context_provider={
                    "file_path": file_path,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason=f"Excel file loading failed: {e}"
            )
            # <Context-Aware Logging - Excel Loading Error - End>
            if logger:
                logger.error(f"Error loading Excel file: {e}")
            return []

    @staticmethod
    def _is_all_zero_row(row: pd.Series) -> bool:
        """Check if a row contains all zeros or empty values - minimal enhancement only"""
        # Only check key fields that should never be zero
        symbol = str(row.get('Symbol', '')).strip()
        if symbol and symbol not in ['0', '0.0']:
            return False
            
        # If symbol is zero/empty, check if other key fields are also zero
        entry_price = row.get('Entry Price')
        if entry_price and float(entry_price) != 0:
            return False
            
        return True

    @staticmethod
    def _is_invalid_row(row: pd.Series) -> bool:
        """Check if a row is invalid (all zeros, empty, or meaningless values)."""
        required_fields = ['Symbol', 'Security Type', 'Exchange', 'Currency', 'Action']
        
        for field in required_fields:
            if field in row:
                value = str(row[field]).strip()
                if value and value not in ['0', '0.0', 'nan', '']:
                    return False
        
        # Additional check: if all numeric values are zero
        numeric_fields = ['Entry Price', 'Stop Loss', 'Risk Per Trade', 'Risk Reward Ratio', 'Priority']
        all_numeric_zero = True
        for field in numeric_fields:
            if field in row and pd.notna(row[field]):
                try:
                    if float(row[field]) != 0:
                        all_numeric_zero = False
                        break
                except (ValueError, TypeError):
                    all_numeric_zero = False
                    break
        
        return all_numeric_zero

    @staticmethod
    def _parse_numeric_field(row: pd.Series, field_name: str, data_type, 
                            default=None, required=False, min_value=None, max_value=None):
        """Safely parse numeric fields with validation."""
        if field_name not in row or pd.isna(row[field_name]):
            if required:
                raise ValueError(f"Required field '{field_name}' is missing")
            return default
        
        try:
            value = data_type(row[field_name])
            
            # Check for zero values in required fields
            if required and value == 0:
                raise ValueError(f"Required field '{field_name}' cannot be zero")
            
            # Range validation
            if min_value is not None and value < min_value:
                raise ValueError(f"Field '{field_name}' must be at least {min_value}")
            if max_value is not None and value > max_value:
                raise ValueError(f"Field '{field_name}' cannot exceed {max_value}")
                
            return value
        except (ValueError, TypeError) as e:
            if required:
                raise ValueError(f"Invalid value for required field '{field_name}': {row[field_name]}")
            return default

    @staticmethod
    def _parse_string_field(row: pd.Series, field_name: str, default: Optional[str] = None):
        """Safely parse string fields, handling zeros and empty values."""
        if field_name not in row or pd.isna(row[field_name]):
            return default
        
        value = str(row[field_name]).strip()
        if not value or value in ['0', '0.0', 'nan']:
            return default
        
        return value

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

    def __post_init__(self):
        """Initialize context logger for ActiveOrder."""
        self.context_logger = get_context_logger()

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
        # <Context-Aware Logging - ActiveOrder Status Change - Begin>
        if self.status != new_status:
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "ActiveOrder status change",
                symbol=self.symbol,
                context_provider={
                    "old_status": self.status,
                    "new_status": new_status,
                    "order_ids": self.order_ids,
                    "age_seconds": self.age_seconds(),
                    "capital_commitment": self.capital_commitment,
                    "fill_probability": self.fill_probability
                },
                decision_reason=f"Order status changed from {self.status} to {new_status}"
            )
        # <Context-Aware Logging - ActiveOrder Status Change - End>
        
        # Minimal logging
        if logger and self.status != new_status:
            logger.debug(f"ActiveOrder status change: {self.symbol} {self.status} -> {new_status}")
        self.status = new_status