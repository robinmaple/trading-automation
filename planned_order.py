from ibapi.client import *
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any
import pandas as pd

class SecurityType(Enum):
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
    BUY = "BUY"
    SELL = "SELL"
    SSHORT = "SSHORT"  # Short sell

class OrderType(Enum):
    LMT = "LMT"
    MKT = "MKT"
    STP = "STP"
    STP_LMT = "STP LMT"
    TRAIL = "TRAIL"

# In planned_order.py - enhance the PositionStrategy enum
class PositionStrategy(Enum):
    DAY = "DAY"           # Close before market close
    CORE = "CORE"         # Good till cancel  
    HYBRID = "HYBRID"     # Partial day, partial core
    
    @classmethod
    def _missing_(cls, value):
        """Handle case-insensitive and flexible matching"""
        value_str = str(value).upper().strip()
        for member in cls:
            if member.value.upper() == value_str:
                return member
        # Try partial matching
        for member in cls:
            if value_str.startswith(member.value.upper()):
                return member
        raise ValueError(f"Invalid PositionStrategy: {value}")
    
@dataclass
class PlannedOrder:
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
    
    # Calculated fields (no need to store in Excel)
    _quantity: Optional[float] = None
    _profit_target: Optional[float] = None
    
    def __post_init__(self):
        self.validate()
        
    def validate(self):
        """Validate the order parameters"""
        if self.risk_per_trade > 0.02:  # Max 2% risk
            raise ValueError("Risk per trade cannot exceed 2%")
        if self.entry_price is not None and self.stop_loss is not None:
            if (self.action == Action.BUY and self.stop_loss >= self.entry_price) or \
               (self.action == Action.SELL and self.stop_loss <= self.entry_price):
                raise ValueError("Stop loss must be protective")
    
    def calculate_quantity(self, total_capital: float) -> float:
        """Calculate position size based on risk management"""
        if self.entry_price is None or self.stop_loss is None:
            raise ValueError("Entry price and stop loss required for quantity calculation")
            
        risk_per_share = abs(self.entry_price - self.stop_loss)
        risk_amount = total_capital * self.risk_per_trade
        self._quantity = risk_amount / risk_per_share

        # NEW: Handle Forex (CASH) contracts - round to appropriate lot sizes
        if self.security_type == SecurityType.CASH:
            # Forex typically trades in standard lots (100,000) or mini lots (10,000)
            # Round to the nearest 10,000 units for reasonable position sizing
            calculated_quantity = round(calculated_quantity / 10000) * 10000
            calculated_quantity = max(calculated_quantity, 10000)  # Minimum 10,000 units
        else:
            # All other security types (STK, OPT, FUT, etc.) trade in whole units
            calculated_quantity = round(calculated_quantity)
            calculated_quantity = max(calculated_quantity, 1)  # Minimum 1 unit
                        
        self._quantity = calculated_quantity
        return self._quantity
        
    def calculate_profit_target(self) -> float:
        """Calculate profit target price"""
        if self.entry_price is None or self.stop_loss is None:
            raise ValueError("Entry price and stop loss required for profit target")
            
        risk_amount = abs(self.entry_price - self.stop_loss)
        if self.action == Action.BUY:
            self._profit_target = self.entry_price + (risk_amount * self.risk_reward_ratio)
        else:  # SELL or SSHORT
            self._profit_target = self.entry_price - (risk_amount * self.risk_reward_ratio)
        return self._profit_target
    
    def to_ib_contract(self) -> Contract:
        """Convert to IB Contract object"""
        contract = Contract()
        contract.symbol = self.symbol
        contract.secType = self.security_type.value
        contract.exchange = self.exchange
        contract.currency = self.currency
        return contract
    
    def to_ib_order(self, total_capital: float) -> Order:
        """Convert to IB Order object"""
        order = Order()
        order.action = self.action.value
        order.orderType = self.order_type.value
        order.totalQuantity = self.calculate_quantity(total_capital)
        
        if self.order_type == OrderType.LMT and self.entry_price:
            order.lmtPrice = self.entry_price
        elif self.order_type == OrderType.STP and self.stop_loss:
            order.auxPrice = self.stop_loss
            
        # Set time in force based on position strategy
        if self.position_strategy == PositionStrategy.DAY:
            order.tif = "DAY"
        else:
            order.tif = "GTC"
            
        return order

class PlannedOrderManager:
    @staticmethod
    def from_excel(file_path: str) -> list[PlannedOrder]:
        """Load planned orders from Excel template with detailed debugging"""
        orders = []
        
        try:
            print(f"Loading Excel file: {file_path}")
            
            # Read the Excel file
            df = pd.read_excel(file_path)
            print(f"Excel loaded successfully. Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            
            # Display first few rows for debugging
            print("\nFirst 3 rows of data:")
            print(df.head(3).to_string())
            
            for index, row in df.iterrows():
                try:
                    print(f"\n--- Processing row {index + 2} (Excel row {index + 2}) ---")
                    
                    # Debug: Print the entire row
                    print("Raw row data:")
                    for col, value in row.items():
                        print(f"  {col}: {value} (type: {type(value)})")
                    
                    # Convert string values to appropriate enum types with detailed error handling
                    security_type_str = str(row['Security Type']).strip()
                    print(f"Security Type: '{security_type_str}'")
                    security_type = SecurityType[security_type_str]
                    
                    action_str = str(row['Action']).strip().upper()
                    print(f"Action: '{action_str}'")
                    action = Action[action_str]
                    
                    # Handle optional fields with defaults
                    order_type_str = str(row.get('Order Type', 'LMT')).strip()
                    print(f"Order Type: '{order_type_str}'")
                    order_type = OrderType[order_type_str] if order_type_str and order_type_str != 'nan' else OrderType.LMT
                    
                    position_strategy_str = str(row.get('Position Management Strategy', 'CORE')).strip()
                    print(f"Position Strategy: '{position_strategy_str}'")
                    
                    # This is where the error is occurring - let's debug this specifically
                    try:
                        position_strategy = PositionStrategy[position_strategy_str]
                    except KeyError as e:
                        print(f"ERROR: Invalid Position Management Strategy: '{position_strategy_str}'")
                        print(f"Valid options: {[e.value for e in PositionStrategy]}")
                        raise
                    
                    # Handle numeric fields with validation
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
                    
                    # Create the order
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
                        position_strategy=position_strategy
                    )
                    
                    orders.append(order)
                    print(f"✅ Successfully created order for {order.symbol}")
                    
                except Exception as row_error:
                    print(f"❌ ERROR processing row {index + 2}: {row_error}")
                    print(f"Row data: {dict(row)}")
                    import traceback
                    traceback.print_exc()
                    # Decide whether to continue or stop on first error
                    # raise row_error  # Uncomment to stop on first error
            
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
        """Display all valid values for enums for debugging"""
        print("\n=== VALID VALUES FOR EXCEL COLUMNS ===")
        print("Security Type options:", [e.name for e in SecurityType])
        print("Action options:", [e.name for e in Action])
        print("Order Type options:", [e.name for e in OrderType])
        print("Position Management Strategy options:", [e.name for e in PositionStrategy])
        print("=== END VALID VALUES ===\n")