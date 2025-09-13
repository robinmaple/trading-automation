from decimal import Decimal
import datetime
from typing import Optional
from sqlalchemy.orm import Session

from src.core.events import OrderState
from src.core.database import get_db_session
from src.core.models import ExecutedOrderDB, PlannedOrderDB, PositionStrategy

import typing
if typing.TYPE_CHECKING:
    from typing import Tuple

# Order Persistence Service Consolidation - Begin
class OrderPersistenceService:
    """
    Service for handling ALL order-related database persistence operations.
    Consolidated from OrderStateService to eliminate duplication.
    """
    
    def __init__(self, db_session: Optional[Session] = None):
        """Initialize with optional database session"""
        self.db_session = db_session or get_db_session()
    
    def record_order_execution(self, planned_order, filled_price: float, 
                             filled_quantity: float, commission: float = 0.0, 
                             status: str = 'FILLED', is_live_trading: bool = False) -> Optional[int]:
        """
        Record an order execution in the database.
        Returns the ID of the created ExecutedOrderDB record, or None on failure.
        """
        try:
            # Find the corresponding planned order in database
            planned_order_id = self._find_planned_order_id(planned_order)
            
            if planned_order_id is None:
                print(f"❌ Cannot record execution: Planned order not found in database for {planned_order.symbol}")
                print(f"   Searching for: {planned_order.symbol}, {planned_order.entry_price}, {planned_order.stop_loss}")
                # Debug: Show what's actually in the database
                existing_orders = self.db_session.query(PlannedOrderDB).filter_by(symbol=planned_order.symbol).all()
                print(f"   Existing orders for {planned_order.symbol}: {len(existing_orders)}")
                for order in existing_orders:
                    print(f"     - {order.symbol}: entry={order.entry_price}, stop={order.stop_loss}, status={order.status}")
                return None
            
            # Create executed order record
            executed_order = ExecutedOrderDB(
                planned_order_id=planned_order_id,
                filled_price=filled_price,
                filled_quantity=filled_quantity,
                commission=commission,
                status=status,
                executed_at=datetime.datetime.now(),
                is_live_trading=is_live_trading
            )
            
            # Add expiration date for HYBRID strategy orders - Begin
            if planned_order.position_strategy.value == 'HYBRID':
                expiration_date = datetime.datetime.now() + datetime.timedelta(days=10)
                executed_order.expiration_date = expiration_date
                print(f"📅 HYBRID order expiration set: {expiration_date.strftime('%Y-%m-%d %H:%M')}")
            # Add expiration date for HYBRID strategy orders - End

            # Add to database and commit
            self.db_session.add(executed_order)
            self.db_session.commit()
            
            print(f"✅ Execution recorded for {planned_order.symbol}: "
                  f"{filled_quantity} @ {filled_price}, Status: {status}")
            
            return executed_order.id
            
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to record order execution: {e}")
            import traceback
            traceback.print_exc()
            return None
        
    def _find_planned_order_id(self, planned_order) -> Optional[int]:
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

    def convert_to_db_model(self, planned_order):
        """
        Convert PlannedOrder to PlannedOrderDB for database persistence.
        Migrated from OrderStateService.
        """
        try:
            # Find position strategy in database
            position_strategy = self.db_session.query(PositionStrategy).filter_by(
                name=planned_order.position_strategy.value
            ).first()
            
            if not position_strategy:
                raise ValueError(f"Position strategy {planned_order.position_strategy.value} not found in database")
            
            # Create database model
            db_model = PlannedOrderDB(
                symbol=planned_order.symbol,
                security_type=planned_order.security_type.value,
                action=planned_order.action.value,
                order_type=planned_order.order_type.value,
                entry_price=planned_order.entry_price,
                stop_loss=planned_order.stop_loss,
                risk_per_trade=planned_order.risk_per_trade,
                risk_reward_ratio=planned_order.risk_reward_ratio,
                priority=planned_order.priority,
                position_strategy_id=position_strategy.id,
                status='PENDING'
            )

            return db_model
            
        except Exception as e:
            print(f"❌ Failed to convert planned order to DB model: {e}")
            raise

    def create_executed_order(self, planned_order, fill_info):
        """
        Creates a new ExecutedOrder record from a PlannedOrder and fill information.
        Migrated from OrderStateService.
        """
        try:
            # Find the planned order in database
            planned_order_id = self._find_planned_order_id(planned_order)
            if not planned_order_id:
                print(f"❌ Cannot create executed order: Planned order not found for {planned_order.symbol}")
                return None
            
            # Create executed order
            executed_order = ExecutedOrderDB(
                planned_order_id=planned_order_id,
                filled_price=fill_info.get('price', 0),
                filled_quantity=fill_info.get('quantity', 0),
                commission=fill_info.get('commission', 0),
                pnl=fill_info.get('pnl', 0),
                status=fill_info.get('status', 'FILLED'),
                executed_at=datetime.datetime.now()
            )
            
            self.db_session.add(executed_order)
            self.db_session.commit()
            
            print(f"✅ Created executed order for {planned_order.symbol}")
            return executed_order
            
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to create executed order: {e}")
            return None

    def handle_order_rejection(self, planned_order_id: int, rejection_reason: str) -> bool:
        """
        Mark an order as CANCELED with rejection reason when broker rejects it.
        Prevents repeated attempts to execute the same rejected order.
        """
        try:
            order = self.db_session.query(PlannedOrderDB).filter_by(id=planned_order_id).first()
            if order:
                order.status = 'CANCELLED'
                order.rejection_reason = rejection_reason
                order.updated_at = datetime.datetime.now()
                self.db_session.commit()
                print(f"✅ Order {planned_order_id} canceled due to rejection: {rejection_reason}")
                return True
            else:
                print(f"❌ Order {planned_order_id} not found for rejection handling")
                return False
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to cancel rejected order {planned_order_id}: {e}")
            return False

    def validate_sufficient_margin(self, symbol: str, quantity: float, entry_price: float, 
                                currency: str = 'USD') -> 'Tuple[bool, str]':  # Use string annotation
        """
        Check if account has sufficient margin for the proposed trade.
        Returns (is_valid, message) tuple.
        """
        try:
            account_value = self.get_account_value()
            
            # Calculate trade value and margin requirement
            trade_value = quantity * entry_price
            
            # Different margin requirements based on security type
            if symbol in ['EUR', 'AUD', 'GBP', 'JPY', 'CAD']:  # Forex pairs
                margin_requirement = trade_value * 0.02  # 2% margin for forex
            else:  # Stocks, options, etc.
                margin_requirement = trade_value * 0.5  # 50% margin for equities
            
            # Don't use more than 80% of account value for margin
            max_allowed_margin = account_value * 0.8
            
            if margin_requirement > max_allowed_margin:
                message = (f"Insufficient margin. Required: ${margin_requirement:,.2f}, "
                        f"Available: ${max_allowed_margin:,.2f}, "
                        f"Account Value: ${account_value:,.2f}")
                return False, message
            
            return True, "Sufficient margin available"
            
        except Exception as e:
            return False, f"Margin validation error: {e}"

    def get_account_value(self, account_id: str = None) -> float:
        """
        Get current account value from IBKR or return mock value for testing.
        In production, this should connect to IBKR API to get real account value.
        """
        try:
            # TODO: Replace with actual IBKR account value query
            # For now, return a mock value that's reasonable for testing
            mock_account_value = 100000.0  # $100,000 mock account value
            print(f"📊 Current account value (mock): ${mock_account_value:,.2f}")
            return mock_account_value
            
        except Exception as e:
            print(f"❌ Failed to get account value: {e}")
            return 50000.0  # Fallback value        

    # Enhanced update_order_status with reason parameter - Begin
    def update_order_status(self, order, status: str, reason: str = "", order_ids=None) -> bool:
        """Update order status in database with optional reason"""
        try:
            # Validate status against known enum values
            valid_statuses = ['PENDING', 'LIVE', 'LIVE_WORKING', 'FILLED', 'CANCELLED', 
                             'EXPIRED', 'LIQUIDATED', 'REPLACED']
            
            if status not in valid_statuses:
                print(f"❌ Invalid order status: '{status}'. Valid values: {valid_statuses}")
                return False
            
            # Find the order using exact matching criteria
            db_order = self._find_planned_order_db_record(order)
            
            if db_order:
                db_order.status = status
                if order_ids:
                    db_order.ibkr_order_ids = str(order_ids)
                if reason:
                    db_order.status_reason = reason[:255]  # Truncate if too long
                db_order.updated_at = datetime.datetime.now()
                
                self.db_session.commit()
                print(f"✅ Updated {order.symbol} status to {status}: {reason}")
                return True
            else:
                print(f"❌ Order not found in database: {order.symbol}")
                # Try to create the order if it doesn't exist
                try:
                    db_model = self.convert_to_db_model(order)
                    db_model.status = status
                    if reason:
                        db_model.status_reason = reason[:255]
                    self.db_session.add(db_model)
                    self.db_session.commit()
                    print(f"✅ Created new order record for {order.symbol} with status {status}")
                    return True
                except Exception as create_error:
                    print(f"❌ Failed to create order record: {create_error}")
                    return False
                
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to update order status: {e}")
            return False

    def _find_planned_order_db_record(self, order) -> Optional[PlannedOrderDB]:
        """Find the exact database record for a planned order"""
        try:
            return self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value,
                order_type=order.order_type.value
            ).first()
        except Exception as e:
            print(f"❌ Error finding planned order in database: {e}")
            return None
    # Enhanced update_order_status with reason parameter - End

# Order Persistence Service Consolidation - End