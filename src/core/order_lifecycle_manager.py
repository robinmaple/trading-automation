# src/core/order_lifecycle_manager.py
"""
Manages the complete order lifecycle from loading to execution.
Handles order validation, persistence, state transitions, and duplicate detection.
Provides comprehensive order management with database integration.
"""

import datetime
from typing import List, Optional, Dict, Tuple
from sqlalchemy.orm import Session
from src.core.planned_order import PlannedOrder
from src.core.models import PlannedOrderDB
from src.core.events import OrderState
from src.services.order_loading_service import OrderLoadingService
from src.services.order_persistence_service import OrderPersistenceService
from src.services.state_service import StateService


class OrderLifecycleManager:
    """Manages the complete lifecycle of orders from loading to completion."""
    
    def __init__(self, loading_service: OrderLoadingService,
                 persistence_service: OrderPersistenceService,
                 state_service: StateService,
                 db_session: Session):
        """Initialize the order lifecycle manager with required services."""
        self.loading_service = loading_service
        self.persistence_service = persistence_service
        self.state_service = state_service
        self.db_session = db_session
        
    def load_and_persist_orders(self, excel_path: str) -> List[PlannedOrder]:
        """Load orders from Excel, validate, and persist valid ones to database."""
        print(f"📥 Loading orders from: {excel_path}")
        
        try:
            # Load and validate orders from Excel
            valid_orders = self.loading_service.load_and_validate_orders(excel_path)
            print(f"✅ Found {len(valid_orders)} valid orders in Excel")
            
            if not valid_orders:
                return []
                
            # Persist each valid order to database
            persisted_count = 0
            for order in valid_orders:
                if self._persist_single_order(order):
                    persisted_count += 1
            
            self.db_session.commit()
            print(f"💾 Persisted {persisted_count}/{len(valid_orders)} orders to database")
            
            return valid_orders
            
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to load and persist orders: {e}")
            raise
            
    def _persist_single_order(self, order: PlannedOrder) -> bool:
        """Persist a single order to database with duplicate checking."""
        try:
            # Check for existing order with same parameters
            existing_order = self.find_existing_order(order)
            if existing_order and self._is_duplicate_order(order, existing_order):
                print(f"⏩ Skipping duplicate order: {order.symbol} {order.action.value} @ {order.entry_price:.4f}")
                return False
                
            # Convert to database model and persist
            db_order = self.persistence_service.convert_to_db_model(order)
            self.db_session.add(db_order)
            print(f"✅ Persisted order: {order.symbol} {order.action.value} @ {order.entry_price:.4f}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to persist order {order.symbol}: {e}")
            return False
            
    def validate_order(self, order: PlannedOrder) -> Tuple[bool, Optional[str]]:
        """Validate an order with detailed error messages."""
        # Basic parameter validation
        if order.entry_price is None:
            return False, "Entry price is required"
        if order.stop_loss is None:
            return False, "Stop loss is required"
        if order.risk_per_trade is None:
            return False, "Risk per trade is required"
            
        # Price relationship validation
        if order.action.value == 'BUY':
            if order.stop_loss >= order.entry_price:
                return False, "Stop loss must be below entry price for BUY orders"
        elif order.action.value == 'SELL':
            if order.stop_loss <= order.entry_price:
                return False, "Stop loss must be above entry price for SELL orders"
        else:
            return False, f"Invalid order action: {order.action.value}"
            
        # Risk management validation
        if order.risk_per_trade <= 0:
            return False, "Risk per trade must be positive"
        if order.risk_per_trade > 0.02:  # 2% max risk
            return False, "Risk per trade cannot exceed 2%"
        if order.risk_reward_ratio < 1.0:
            return False, "Risk/reward ratio must be at least 1.0"
        if not 1 <= order.priority <= 5:
            return False, "Priority must be between 1 and 5"
            
        return True, None
        
    def find_existing_order(self, order: PlannedOrder) -> Optional[PlannedOrderDB]:
        """Find an existing order in database with matching parameters."""
        try:
            return self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value,
                order_type=order.order_type.value
            ).first()
        except Exception as e:
            print(f"❌ Error querying for existing order {order.symbol}: {e}")
            return None
            
    def _is_duplicate_order(self, new_order: PlannedOrder, existing_order: PlannedOrderDB) -> bool:
        """Check if new order is a duplicate of existing database order."""
        # Consider orders with same symbol, action, entry, and stop as duplicates
        same_action = existing_order.action == new_order.action.value
        same_entry = abs(existing_order.entry_price - new_order.entry_price) < 0.0001
        same_stop = abs(existing_order.stop_loss - new_order.stop_loss) < 0.0001
        
        return same_action and same_entry and same_stop
        
    def get_order_status(self, order: PlannedOrder) -> Optional[OrderState]:
        """Get the current status of an order from database."""
        existing_order = self.find_existing_order(order)
        return existing_order.status if existing_order else None
        
    def is_order_executable(self, order: PlannedOrder) -> Tuple[bool, Optional[str]]:
        """Check if an order can be executed based on current state."""
        # Basic validation first
        is_valid, error_msg = self.validate_order(order)
        if not is_valid:
            return False, error_msg
            
        # Check if order already exists in certain states
        existing_order = self.find_existing_order(order)
        if existing_order:
            if existing_order.status in ['LIVE', 'LIVE_WORKING', 'FILLED']:
                return False, f"Order already in state: {existing_order.status}"
            if existing_order.status in ['CANCELLED', 'REJECTED', 'FAILED']:
                # Allow re-execution of failed/cancelled orders
                pass
                
        # Check for open positions
        if self.state_service.has_open_position(order.symbol):
            return False, f"Open position exists for {order.symbol}"
            
        return True, None
        
    def update_order_status(self, order: PlannedOrder, status: OrderState, 
                          message: Optional[str] = None) -> bool:
        """Update the status of an order in the database."""
        try:
            existing_order = self.find_existing_order(order)
            if not existing_order:
                print(f"⚠️  Order not found in database for status update: {order.symbol}")
                return False
                
            old_status = existing_order.status
            existing_order.status = status
            existing_order.updated_at = datetime.datetime.now()
            
            if message:
                existing_order.status_message = message
                
            self.db_session.commit()
            print(f"📋 Status update: {order.symbol} {old_status} → {status}")
            if message:
                print(f"   Message: {message}")
                
            return True
            
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to update order status for {order.symbol}: {e}")
            return False
            
    def bulk_update_status(self, status_updates: List[Tuple[PlannedOrder, OrderState, Optional[str]]]) -> Dict[str, bool]:
        """Update status for multiple orders in a single transaction."""
        results = {}
        
        try:
            for order, status, message in status_updates:
                success = self.update_order_status(order, status, message)
                results[order.symbol] = success
                
            return results
            
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Bulk status update failed: {e}")
            return {order.symbol: False for order, _, _ in status_updates}
            
    def get_orders_by_status(self, status: OrderState) -> List[PlannedOrderDB]:
        """Get all orders with a specific status from database."""
        try:
            return self.db_session.query(PlannedOrderDB).filter_by(status=status).all()
        except Exception as e:
            print(f"❌ Error querying orders by status {status}: {e}")
            return []
            
    def cleanup_old_orders(self, days_old: int = 30) -> int:
        """Clean up orders older than specified days from database."""
        try:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_old)
            old_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.created_at < cutoff_date,
                PlannedOrderDB.status.in_(['FILLED', 'CANCELLED', 'REJECTED'])
            ).all()
            
            deleted_count = 0
            for order in old_orders:
                self.db_session.delete(order)
                deleted_count += 1
                
            self.db_session.commit()
            print(f"🧹 Cleaned up {deleted_count} orders older than {days_old} days")
            return deleted_count
            
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to clean up old orders: {e}")
            return 0
            
    def get_order_statistics(self) -> Dict[str, any]:
        """Get statistics about orders in the system."""
        try:
            total_orders = self.db_session.query(PlannedOrderDB).count()
            status_counts = {}
            
            for status in OrderState:
                count = self.db_session.query(PlannedOrderDB).filter_by(status=status).count()
                status_counts[status] = count
                
            return {
                'total_orders': total_orders,
                'status_counts': status_counts,
                'oldest_order': self._get_oldest_order_date(),
                'newest_order': self._get_newest_order_date()
            }
            
        except Exception as e:
            print(f"❌ Error getting order statistics: {e}")
            return {}
            
    def _get_oldest_order_date(self) -> Optional[datetime.datetime]:
        """Get the creation date of the oldest order."""
        try:
            oldest = self.db_session.query(PlannedOrderDB).order_by(PlannedOrderDB.created_at.asc()).first()
            return oldest.created_at if oldest else None
        except Exception:
            return None
            
    def _get_newest_order_date(self) -> Optional[datetime.datetime]:
        """Get the creation date of the newest order."""
        try:
            newest = self.db_session.query(PlannedOrderDB).order_by(PlannedOrderDB.created_at.desc()).first()
            return newest.created_at if newest else None
        except Exception:
            return None
            
    def find_orders_needing_attention(self) -> List[PlannedOrderDB]:
        """Find orders that may need manual attention."""
        try:
            # Orders stuck in executing state for too long
            stuck_time = datetime.datetime.now() - datetime.timedelta(hours=2)
            stuck_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.status == 'EXECUTING',
                PlannedOrderDB.updated_at < stuck_time
            ).all()
            
            # Orders with multiple failures
            failed_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.status == 'FAILED'
            ).all()
            
            return stuck_orders + failed_orders
            
        except Exception as e:
            print(f"❌ Error finding orders needing attention: {e}")
            return []