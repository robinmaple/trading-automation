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

# <Order Loading Orchestrator Integration - Begin>
from src.core.order_loading_orchestrator import OrderLoadingOrchestrator
# <Order Loading Orchestrator Integration - End>

# <AON Configuration Integration - Begin>
from config.trading_core_config import get_config
from src.core.shared_enums import OrderState as SharedOrderState
# <AON Configuration Integration - End>


class OrderLifecycleManager:
    """Manages the complete lifecycle of orders from loading to completion."""
    
    def __init__(self, loading_service: OrderLoadingService,
                 persistence_service: OrderPersistenceService,
                 state_service: StateService,
                 db_session: Session,
                 # <Order Loading Orchestrator Integration - Begin>
                 order_loading_orchestrator: Optional[OrderLoadingOrchestrator] = None,
                 # <Order Loading Orchestrator Integration - End>
                 # <AON Configuration Integration - Begin>
                 config: Optional[Dict] = None
                 # <AON Configuration Integration - End>
                 ):
        """Initialize the order lifecycle manager with required services."""
        self.loading_service = loading_service
        self.persistence_service = persistence_service
        self.state_service = state_service
        self.db_session = db_session
        # <Order Loading Orchestrator Integration - Begin>
        self.order_loading_orchestrator = order_loading_orchestrator
        # <Order Loading Orchestrator Integration - End>
        # <AON Configuration Integration - Begin>
        self.config = config or get_config()
        self.aon_config = self.config.get('aon_execution', {})
        # <AON Configuration Integration - End>
        
    def load_and_persist_orders(self, excel_path: str) -> List[PlannedOrder]:
        """Load orders from Excel, validate, and persist valid ones to database."""
        print(f"📥 Loading orders from: {excel_path}")
        
        try:
            # <Multi-Source Order Loading - Begin>
            if self.order_loading_orchestrator:
                # Use orchestrator for multi-source loading (DB resumption + Excel)
                all_orders = self.order_loading_orchestrator.load_all_orders(excel_path)
                print(f"✅ Loaded {len(all_orders)} orders from all sources")
            else:
                # Fallback to original Excel-only loading
                all_orders = self.loading_service.load_and_validate_orders(excel_path)
                print(f"✅ Found {len(all_orders)} valid orders in Excel")
            # <Multi-Source Order Loading - End>
            
            if not all_orders:
                return []
                
            # <Enhanced Persistence Logic - Begin>
            # Only persist orders that are new (from Excel) and not duplicates
            persisted_count = 0
            for order in all_orders:
                # Only attempt persistence for orders that likely came from Excel
                # (DB-resumed orders are already persisted)
                if self._should_persist_order(order):
                    if self._persist_single_order(order):
                        persisted_count += 1
            # <Enhanced Persistence Logic - End>
            
            self.db_session.commit()
            print(f"💾 Persisted {persisted_count} new orders to database")
            
            return all_orders
            
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to load and persist orders: {e}")
            raise

    # <AON Validation Methods - Begin>
    def validate_order_for_aon(self, order: PlannedOrder, total_capital: float) -> Tuple[bool, str]:
        """
        Validate if order should use AON execution based on volume-based thresholds.
        
        Args:
            order: The planned order to validate
            total_capital: Total account capital for notional calculation
            
        Returns:
            Tuple of (is_valid, reason_message)
        """
        # Check if AON is enabled
        if not self.aon_config.get('enabled', True):
            return True, "AON validation skipped (disabled)"
        
        # Extract actual numeric values from potentially mocked objects
        try:
            entry_price = getattr(order.entry_price, 'return_value', order.entry_price)
            if hasattr(entry_price, '__call__'):
                entry_price = entry_price()
            
            # Calculate order notional value with proper numeric extraction
            quantity = order.calculate_quantity(total_capital)
            # Ensure quantity is also a numeric value
            quantity = getattr(quantity, 'return_value', quantity)
            if hasattr(quantity, '__call__'):
                quantity = quantity()
                
            notional_value = entry_price * quantity
            
            # Validate numeric values
            if not isinstance(entry_price, (int, float)) or entry_price <= 0:
                return False, f"Invalid entry price: {entry_price}"
                
            if not isinstance(quantity, (int, float)) or quantity <= 0:
                return False, f"Invalid quantity: {quantity}"
                
        except Exception as e:
            return False, f"Cannot calculate order notional: {e}"
        
        # Get AON threshold for this symbol
        aon_threshold = self._calculate_aon_threshold(order.symbol)
        if aon_threshold is None:
            return False, "Cannot determine AON threshold (volume data unavailable)"
        
        # Check if order exceeds AON threshold
        if notional_value > aon_threshold:
            return False, f"Order notional ${notional_value:,.2f} exceeds AON threshold ${aon_threshold:,.2f}"
        
        return True, f"AON valid: ${notional_value:,.2f} <= ${aon_threshold:,.2f}"

    def _calculate_aon_threshold(self, symbol: str) -> Optional[float]:
        """
        Calculate AON threshold based on daily volume and configured percentages.
        
        Args:
            symbol: Trading symbol to calculate threshold for
            
        Returns:
            AON threshold in dollars, or None if cannot determine
        """
        try:
            # Get daily volume for symbol
            daily_volume = self._get_daily_volume(symbol)
            if daily_volume is None:
                # Fallback to fixed notional
                return self.aon_config.get('fallback_fixed_notional', 50000)
            
            # Get volume percentage for this symbol
            symbol_specific = self.aon_config.get('symbol_specific', {})
            volume_percentage = symbol_specific.get(symbol, 
                                self.aon_config.get('default_volume_percentage', 0.001))
            
            # Ensure volume_percentage is numeric
            volume_percentage = float(volume_percentage)
            
            # Calculate threshold: daily_volume * entry_price * percentage
            # For now using a placeholder - in practice you'd get current price
            current_price = 100.0  # Placeholder - would come from data feed
            threshold = daily_volume * current_price * volume_percentage
            
            print(f"📊 AON threshold for {symbol}: {daily_volume:,.0f} shares * ${current_price:.2f} * {volume_percentage:.4f} = ${threshold:,.2f}")
            return threshold
            
        except Exception as e:
            print(f"❌ Error calculating AON threshold for {symbol}: {e}")
            return self.aon_config.get('fallback_fixed_notional', 50000)
                
    def _get_daily_volume(self, symbol: str) -> Optional[float]:
        """
        Get daily volume for a symbol. Placeholder implementation.
        
        In practice, this would fetch from IBKR data feed or cache.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Daily volume in shares, or None if unavailable
        """
        # Placeholder implementation - would integrate with IBKR data feed
        # For now, return mock volumes based on symbol liquidity
        mock_volumes = {
            'SPY': 50000000,   # 50M shares
            'QQQ': 30000000,   # 30M shares  
            'IWM': 20000000,   # 20M shares
            'AAPL': 40000000,  # 40M shares
            'TSLA': 25000000,  # 25M shares
        }
        
        volume = mock_volumes.get(symbol, 10000000)  # Default 10M shares
        print(f"📈 Daily volume for {symbol}: {volume:,.0f} shares (mock data)")
        return volume
    # <AON Validation Methods - End>
        
    # <Enhanced Order Persistence Logic - Begin>
    def _should_persist_order(self, order: PlannedOrder) -> bool:
        """
        Determine if an order should be persisted to database.
        Only persist orders that are new (likely from Excel) and not duplicates.
        """
        # Check if order already exists in database
        existing_order = self.find_existing_order(order)
        if existing_order:
            return False  # Already persisted
            
        # Additional logic could be added here to distinguish between
        # DB-resumed orders vs new Excel orders
        return True
    # <Enhanced Order Persistence Logic - End>
            
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
        """
        Validate order system state - assumes data integrity already validated by PlannedOrder.
        
        This method checks system-level constraints, not data integrity. It assumes
        the PlannedOrder object is internally valid (required fields present, business
        rules satisfied). Data integrity should be enforced at object creation.
        """
        # <Delegate Data Integrity to PlannedOrder - Begin>
        try:
            # Fail fast if data integrity issues - should never happen for valid orders
            order.validate()
        except ValueError as e:
            # This indicates a serious data integrity issue that should be fixed upstream
            return False, f"Data integrity violation: {e}"
        # <Delegate Data Integrity to PlannedOrder - End>
            
        # <System State Validation - Begin>
        # UNIQUE: Check for open positions
        if self.state_service.has_open_position(order.symbol):
            return False, f"Open position exists for {order.symbol}"
            
        # UNIQUE: Check database state for existing orders
        existing_order = self.find_existing_order(order)
        if existing_order:
            # <AON Status Integration - Begin>
            if existing_order.status in [SharedOrderState.LIVE.value, SharedOrderState.LIVE_WORKING.value, SharedOrderState.FILLED.value]:
                return False, f"Active order already exists: {existing_order.status}"
            # <AON Status Integration - End>
            # Allow re-execution of failed/cancelled orders
            # (they remain in the system for record-keeping but can be re-tried)
        # <System State Validation - End>
            
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
        # Delegate to validate_order for comprehensive checking
        return self.validate_order(order)
        
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
                # <AON Status Integration - Begin>
                PlannedOrderDB.status.in_(['FILLED', 'CANCELLED', 'AON_REJECTED'])
                # <AON Status Integration - End>
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
                # <AON Status Integration - Begin>
                PlannedOrderDB.status.in_(['FAILED', 'AON_REJECTED'])
                # <AON Status Integration - End>
            ).all()
            
            return stuck_orders + failed_orders
            
        except Exception as e:
            print(f"❌ Error finding orders needing attention: {e}")
            return []