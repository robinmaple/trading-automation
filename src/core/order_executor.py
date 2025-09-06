from decimal import Decimal
import time
import datetime
from typing import Optional, TYPE_CHECKING

# Database Integration - Begin
from sqlalchemy.orm import Session
from src.core.database import get_db_session
from src.core.models import ExecutedOrderDB, PlannedOrderDB
# Database Integration - End

# Type checking import - Begin
if TYPE_CHECKING:
    from src.core.planned_order import PlannedOrder
# Type checking import - End

class OrderExecutor:
    """
    Pure business logic for order construction and risk calculation.
    Decoupled from IBKR API communication.
    """
    
    def __init__(self, db_session: Optional[Session] = None):
        # Database Integration - Begin
        # Accept optional session, fall back to global session
        self.db_session = db_session or get_db_session()
        # Database Integration - End
    
    def create_forex_contract(self, symbol='EUR', currency='USD'):
        """Create a Forex contract"""
        from ibapi.contract import Contract
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CASH"
        contract.exchange = "IDEALPRO"
        contract.currency = currency
        return contract
    
    def create_native_bracket_order(self, action, order_type, security_type, entry_price, stop_loss,
                                  risk_per_trade, risk_reward_ratio, total_capital, starting_order_id):
        """
        Create a native bracket order for Forex
        Returns: list of [parent_order, take_profit_order, stop_loss_order]
        """
        from ibapi.order import Order
        
        parent_id = starting_order_id
        
        # Calculate quantity based on security type
        quantity = self._calculate_quantity(security_type, entry_price, stop_loss, 
                                          total_capital, risk_per_trade)
        
        # Calculate profit target based on risk reward ratio
        profit_target = self._calculate_profit_target(action, entry_price, stop_loss, risk_reward_ratio)
        
        # 1. PARENT ORDER (Entry)
        parent = Order()
        parent.orderId = parent_id
        parent.action = action
        parent.orderType = order_type
        parent.totalQuantity = quantity
        parent.lmtPrice = round(entry_price, 5)
        parent.transmit = False
        
        # Determine opposite action for closing orders
        closing_action = "SELL" if action == "BUY" else "BUY"
        
        # 2. TAKE-PROFIT ORDER
        take_profit = Order()
        take_profit.orderId = parent_id + 1
        take_profit.action = closing_action
        take_profit.orderType = "LMT"
        take_profit.totalQuantity = quantity
        
        # Use calculated profit target
        take_profit.lmtPrice = round(profit_target, 5)
        take_profit.parentId = parent_id
        take_profit.transmit = False
        take_profit.openClose = "C"
        take_profit.origin = 0
        
        # 3. STOP-LOSS ORDER
        stop_loss_order = Order()
        stop_loss_order.orderId = parent_id + 2
        stop_loss_order.action = closing_action
        stop_loss_order.orderType = "STP"
        stop_loss_order.totalQuantity = quantity
        
        # Use provided stop loss price
        stop_loss_order.auxPrice = round(stop_loss, 5)
            
        stop_loss_order.parentId = parent_id
        stop_loss_order.transmit = True
        stop_loss_order.openClose = "C"
        stop_loss_order.origin = 0
        
        return [parent, take_profit, stop_loss_order]

    def _calculate_quantity(self, security_type, entry_price, stop_loss, total_capital, risk_per_trade):
        """Calculate position size based on security type and risk management"""
        if entry_price is None or stop_loss is None:
            raise ValueError("Entry price and stop loss are required for quantity calculation")
            
        # Different risk calculation based on security type
        if security_type == "OPT":
            # For options, risk is the premium difference per contract
            # Options are typically 100 shares per contract, so multiply by 100
            risk_per_unit = abs(entry_price - stop_loss) * 100
        else:
            # For stocks, forex, futures, etc. - risk is price difference per share/unit
            risk_per_unit = abs(entry_price - stop_loss)
        
        if risk_per_unit == 0:
            raise ValueError("Entry price and stop loss cannot be the same")
            
        risk_amount = total_capital * risk_per_trade
        base_quantity = risk_amount / risk_per_unit
        
        # Different rounding and minimums based on security type
        if security_type == "CASH":
            # Forex typically trades in standard lots (100,000) or mini lots (10,000)
            # Round to the nearest 10,000 units for reasonable position sizing
            quantity = round(base_quantity / 10000) * 10000
            quantity = max(quantity, 10000)  # Minimum 10,000 units
        elif security_type == "STK":
            # Stocks trade in whole shares
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 share
        elif security_type == "OPT":
            # Options trade in whole contracts (each typically represents 100 shares)
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
            print(f"Options position: {quantity} contracts (each = 100 shares)")
        elif security_type == "FUT":
            # Futures trade in whole contracts
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
        else:
            # Default to whole units for other security types
            quantity = round(base_quantity)
            quantity = max(quantity, 1)
            
        return quantity

    def _calculate_profit_target(self, action, entry_price, stop_loss, risk_reward_ratio):
        """Calculate profit target based on risk reward ratio"""
        risk_amount = abs(entry_price - stop_loss)
        if action == "BUY":
            profit_target = entry_price + (risk_amount * risk_reward_ratio)
        else:  # SELL
            profit_target = entry_price - (risk_amount * risk_reward_ratio)
        return profit_target

    # Database Integration - Begin
    def record_order_execution(self, planned_order: 'PlannedOrder', filled_price: float, 
                             filled_quantity: float, commission: float = 0.0, 
                             status: str = 'FILLED') -> Optional[int]:
        """
        Record an order execution in the database.
        Returns the ID of the created ExecutedOrderDB record, or None on failure.
        """
        try:
            # Find the corresponding planned order in database
            planned_order_id = self._find_planned_order_id(planned_order)
            
            if planned_order_id is None:
                print(f"❌ Cannot record execution: Planned order not found in database for {planned_order.symbol}")
                return None
            
            # Create executed order record
            executed_order = ExecutedOrderDB(
                planned_order_id=planned_order_id,
                filled_price=filled_price,
                filled_quantity=filled_quantity,
                commission=commission,
                status=status,
                executed_at=datetime.datetime.now()
            )
            
            # Add to database and commit
            self.db_session.add(executed_order)
            self.db_session.commit()
            
            print(f"✅ Execution recorded for {planned_order.symbol}: "
                  f"{filled_quantity} @ {filled_price}, Status: {status}")
            
            return executed_order.id
            
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to record order execution: {e}")
            return None

    def _find_planned_order_id(self, planned_order: 'PlannedOrder') -> Optional[int]:
        """Find the database ID for a matching planned order"""
        try:
            db_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=planned_order.symbol,
                entry_price=planned_order.entry_price,
                stop_loss=planned_order.stop_loss,
                action=planned_order.action.value,
                order_type=planned_order.order_type.value
            ).first()
            
            return db_order.id if db_order else None
            
        except Exception as e:
            print(f"❌ Error finding planned order in database: {e}")
            return None
    # Database Integration - End