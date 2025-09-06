from decimal import Decimal
import datetime
from typing import Optional
from sqlalchemy.orm import Session

from src.core.database import get_db_session
from src.core.models import ExecutedOrderDB, PlannedOrderDB

class OrderPersistenceService:
    """
    Service for handling order-related database persistence operations.
    Extracted from OrderExecutor to follow single responsibility principle.
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
        """Update order status in database"""
        try:
            db_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss
            ).first()
            
            if db_order:
                db_order.status = status
                if order_ids:
                    db_order.ibkr_order_ids = str(order_ids)
                self.db_session.commit()
                print(f"✅ Updated order status to {status} in database")
                return True
            return False
            
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to update order status: {e}")
            return False