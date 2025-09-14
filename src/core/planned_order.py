"""
Core domain models for the automated trading system.
Defines the fundamental data structures (Enums, Dataclasses) representing:
- Trading concepts (SecurityType, Action, OrderType, PositionStrategy)
- A PlannedOrder, which is the central entity read from Excel and prepared for execution.
- An ActiveOrder, which tracks the state of an order submitted to the broker.
Also provides a static PlannedOrderManager for loading orders from an Excel template.
"""

from ibapi.client import *
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import pandas as pd
import datetime

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

    def get_expiration_days(self) -> Optional[int]:
        """Return the number of days until expiration for this strategy."""
        if self == PositionStrategy.DAY:
            return 0
        elif self == PositionStrategy.CORE:
            return None
        elif self == PositionStrategy.HYBRID:
            return 10
        return None

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
    risk_per_trade: float = 0.005  # 0.5% default
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    risk_reward_ratio: float = 2.0
    position_strategy: PositionStrategy = PositionStrategy.CORE
    priority: int = 3  # Default to medium priority (scale 1-5)
    # Phase B Additions - Begin
    trading_setup: Optional[str] = None  # Trading strategy/setup type
    core_timeframe: Optional[str] = None  # Core trading timeframe
    # Phase B Additions - End
    expiration_date: Optional[datetime.datetime] = None
    # Calculated fields (no need to store in Excel)
    _quantity: Optional[float] = None
    _profit_target: Optional[float] = None

    def __post_init__(self) -> None:
        """Dataclass hook to validate and set expiration after initialization."""
        self.validate()
        self._set_expiration_date()

    def validate(self) -> None:
        """Enforce business rules on order parameters."""
        if self.risk_per_trade > 0.02:
            raise ValueError("Risk per trade cannot exceed 2%")
        if not 1 <= self.priority <= 5:
            raise ValueError("Priority must be between 1 and 5")
        # Phase B Additions - Begin
        if self.trading_setup and len(self.trading_setup) > 100:
            raise ValueError("Trading setup description too long")
        if self.core_timeframe and len(self.core_timeframe) > 50:
            raise ValueError("Core timeframe description too long")
        # Phase B Additions - End
        if self.entry_price is not None and self.stop_loss is not None:
            if (self.action == Action.BUY and self.stop_loss >= self.entry_price) or \
               (self.action == Action.SELL and self.stop_loss <= self.entry_price):
                raise ValueError("Stop loss must be on the correct side of the entry price for a protective order")

    def _set_expiration_date(self) -> None:
        """Set expiration date based on position strategy."""
        expiration_days = self.position_strategy.get_expiration_days()
        if expiration_days is not None:
            self.expiration_date = datetime.datetime.now() + datetime.timedelta(days=expiration_days)

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

@dataclass
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
        self.status = new_status

class PlannedOrderManager:
    """Static class providing utilities for loading and managing PlannedOrders."""

    @staticmethod
    def from_excel(file_path: str) -> list[PlannedOrder]:
        """Load and parse planned orders from an Excel template."""
        orders = []
        try:
            print(f"Loading Excel file: {file_path}")
            df = pd.read_excel(file_path)
            print(f"Excel loaded successfully. Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print("\nFirst 3 rows of data:")
            print(df.head(3).to_string())

            for index, row in df.iterrows():
                try:
                    print(f"\n--- Processing row {index + 2} (Excel row {index + 2}) ---")
                    print("Raw row data:")
                    for col, value in row.items():
                        print(f"  {col}: {value} (type: {type(value)})")

                    security_type_str = str(row['Security Type']).strip()
                    print(f"Security Type: '{security_type_str}'")
                    security_type = SecurityType[security_type_str]

                    action_str = str(row['Action']).strip().upper()
                    print(f"Action: '{action_str}'")
                    action = Action[action_str]

                    order_type_str = str(row.get('Order Type', 'LMT')).strip()
                    print(f"Order Type: '{order_type_str}'")
                    order_type = OrderType[order_type_str] if order_type_str and order_type_str != 'nan' else OrderType.LMT

                    position_strategy_str = str(row.get('Position Management Strategy', 'CORE')).strip()
                    print(f"Position Strategy: '{position_strategy_str}'")
                    try:
                        position_strategy = PositionStrategy[position_strategy_str]
                    except KeyError as e:
                        print(f"ERROR: Invalid Position Management Strategy: '{position_strategy_str}'")
                        print(f"Valid options: {[e.value for e in PositionStrategy]}")
                        raise

                    risk_per_trade = float(row.get('Risk Per Trade', 0.005))
                    print(f"Risk Per Trade: {risk_per_trade}")

                    entry_price = None
                    if pd.notna(row.get('Entry Price')):
                        entry_price = float(row['Entry Price'])
                    print(f"Entry Price: {entry_price}")

                    stop_loss = None
                    if pd.notna(row.get('Stop Loss')):
                        stop_loss = float(row['Stop Loss'])
                    print(f"Stop Loss: {stop_loss}")

                    risk_reward_ratio = float(row.get('Risk Reward Ratio', 2.0))
                    print(f"Risk Reward Ratio: {risk_reward_ratio}")

                    priority = int(row.get('Priority', 3))
                    print(f"Priority: {priority}")

                    # Phase B Additions - Begin
                    trading_setup = None
                    if pd.notna(row.get('Trading Setup')):
                        trading_setup = str(row['Trading Setup']).strip()
                    print(f"Trading Setup: '{trading_setup}'")

                    core_timeframe = None
                    if pd.notna(row.get('Core Timeframe')):
                        core_timeframe = str(row['Core Timeframe']).strip()
                    print(f"Core Timeframe: '{core_timeframe}'")
                    # Phase B Additions - End

                    order = PlannedOrder(
                        security_type=security_type,
                        exchange=str(row['Exchange']).strip(),
                        currency=str(row['Currency']).strip(),
                        action=action,
                        symbol=str(row['Symbol']).strip(),
                        order_type=order_type,
                        risk_per_trade=risk_per_trade,
                        entry_price=entry_price,
                        stop_loss=stop_loss,
                        risk_reward_ratio=risk_reward_ratio,
                        position_strategy=position_strategy,
                        priority=priority,
                        # Phase B Additions - Begin
                        trading_setup=trading_setup,
                        core_timeframe=core_timeframe
                        # Phase B Additions - End
                    )
                    orders.append(order)
                    print(f"✅ Successfully created order for {order.symbol}")

                except Exception as row_error:
                    print(f"❌ ERROR processing row {index + 2}: {row_error}")
                    print(f"Row data: {dict(row)}")
                    import traceback
                    traceback.print_exc()

            print(f"\n✅ Successfully loaded {len(orders)} orders from Excel")
            return orders

        except FileNotFoundError:
            print(f"❌ Excel file not found: {file_path}")
            return []
        except Exception as e:
            print(f"❌ Error loading Excel file: {e}")
            import traceback
            traceback.print_exc()
            return []

    @staticmethod
    def display_valid_values():
        """Display all valid values for enums for debugging Excel templates."""
        print("\n=== VALID VALUES FOR EXCEL COLUMNS ===")
        print("Security Type options:", [e.name for e in SecurityType])
        print("Action options:", [e.name for e in Action])
        print("Order Type options:", [e.name for e in OrderType])
        print("Position Management Strategy options:", [e.name for e in PositionStrategy])
        print("=== END VALID VALUES ===\n")