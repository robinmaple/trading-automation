from decimal import Decimal
import datetime
from typing import Optional
from sqlalchemy.orm import Session

from src.core.database import get_db_session
from src.core.models import ExecutedOrderDB, PlannedOrderDB, PositionStrategy

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

    def update_order_status(self, order, status: str, order_ids=None) -> bool:
        """Update order status in database - enhanced with order_ids support"""
        try:
            # Find the order using multiple criteria for better matching
            query = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss
            )
            
            # Additional filters for better precision
            if hasattr(order, 'action') and hasattr(order.action, 'value'):
                query = query.filter_by(action=order.action.value)
            if hasattr(order, 'order_type') and hasattr(order.order_type, 'value'):
                query = query.filter_by(order_type=order.order_type.value)
            
            db_order = query.first()
            
            if db_order:
                db_order.status = status
                if order_ids:
                    db_order.ibkr_order_ids = str(order_ids)
                self.db_session.commit()
                print(f"✅ Updated order status to {status} in database")
                return True
            else:
                print(f"❌ Order not found in database: {order.symbol}")
                return False
                
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to update order status: {e}")
            return False

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
# Order Persistence Service Consolidation - End