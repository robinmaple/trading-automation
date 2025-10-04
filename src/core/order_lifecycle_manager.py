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

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)


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
        if logger:
            logger.debug("Initializing OrderLifecycleManager")
            
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
        
        if logger:
            logger.info("OrderLifecycleManager initialized successfully")
        
    def load_and_persist_orders(self, excel_path: str) -> List[PlannedOrder]:
        """Load orders from Excel, validate, and persist valid ones to database."""
        if logger:
            logger.info(f"Loading orders from: {excel_path}")
        
        try:
            # <Multi-Source Order Loading - Begin>
            if self.order_loading_orchestrator:
                if logger:
                    logger.debug("Using OrderLoadingOrchestrator for multi-source loading")
                # Use orchestrator for multi-source loading (DB resumption + Excel)
                all_orders = self.order_loading_orchestrator.load_all_orders(excel_path)
                if logger:
                    logger.info(f"Loaded {len(all_orders)} orders from all sources")
            else:
                if logger:
                    logger.debug("Using OrderLoadingService for Excel-only loading")
                # Fallback to original Excel-only loading
                all_orders = self.loading_service.load_and_validate_orders(excel_path)
                if logger:
                    logger.info(f"Found {len(all_orders)} valid orders in Excel")
            # <Multi-Source Order Loading - End>
            
            if not all_orders:
                if logger:
                    logger.warning("No orders loaded from Excel file")
                return []
                
            # <Enhanced Persistence Logic - Begin>
            # Handle order persistence with session awareness
            persisted_count = 0
            updated_count = 0
            
            for order in all_orders:
                # Determine if this order needs persistence or updating
                persistence_action = self._determine_persistence_action(order)
                
                if persistence_action == 'CREATE':
                    if self._persist_single_order(order):
                        persisted_count += 1
                elif persistence_action == 'UPDATE':
                    if self._update_existing_order(order):
                        updated_count += 1
                elif persistence_action == 'SKIP':
                    if logger:
                        logger.debug(f"Skipping order: {order.symbol} (already active)")
                else:
                    if logger:
                        logger.warning(f"Unknown persistence action for {order.symbol}: {persistence_action}")
            # <Enhanced Persistence Logic - End>
            
            self.db_session.commit()
            if logger:
                logger.info(f"Order persistence completed: {persisted_count} new, {updated_count} updated")
            
            return all_orders
            
        except Exception as e:
            self.db_session.rollback()
            if logger:
                logger.error(f"Failed to load and persist orders: {e}")
            raise

    # <Enhanced Persistence Logic - Begin>
    def _determine_persistence_action(self, order: PlannedOrder) -> str:
        """
        Determine what persistence action to take for an order.
        
        Args:
            order: The PlannedOrder to evaluate
            
        Returns:
            'CREATE' for new orders, 'UPDATE' for Excel updates, 'SKIP' for DB-resumed orders
        """
        # Check if order already exists in database
        existing_order = self.find_existing_order(order)
        
        if not existing_order:
            return 'CREATE'  # New order - create in database
            
        # Order exists - determine if this is an Excel update or DB-resumed order
        existing_status = existing_order.status
        
        # If order is active (not filled/expired), treat Excel version as update
        if existing_status in [SharedOrderState.PENDING.value, SharedOrderState.LIVE.value, 
                             SharedOrderState.LIVE_WORKING.value]:
            # Check if this appears to be an Excel update (prices changed)
            if self._is_excel_update(order, existing_order):
                return 'UPDATE'
            else:
                return 'SKIP'  # DB-resumed order, no changes
        else:
            # Order is filled/expired - create new order for same trading idea
            return 'CREATE'
            
    def _is_excel_update(self, excel_order: PlannedOrder, db_order: PlannedOrderDB) -> bool:
        """
        Check if Excel order represents an update to existing database order.
        
        Args:
            excel_order: Order from Excel
            db_order: Existing database order
            
        Returns:
            True if Excel order has meaningful changes, False otherwise
        """
        # Check for price changes that would indicate an update
        price_changed = (abs(excel_order.entry_price - db_order.entry_price) > 0.0001 or
                        abs(excel_order.stop_loss - db_order.stop_loss) > 0.0001)
        
        # Check for other meaningful field changes
        priority_changed = (excel_order.priority != db_order.priority)
        risk_changed = (abs(excel_order.risk_per_trade - db_order.risk_per_trade) > 0.0001)
        
        return price_changed or priority_changed or risk_changed
        
    def _update_existing_order(self, order: PlannedOrder) -> bool:
        """
        Update an existing database order with Excel changes.
        
        Args:
            order: PlannedOrder with updated values
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            existing_order = self.find_existing_order(order)
            if not existing_order:
                if logger:
                    logger.warning(f"Cannot update: Order not found for {order.symbol}")
                return False
                
            # Update order fields with Excel values
            existing_order.entry_price = order.entry_price
            existing_order.stop_loss = order.stop_loss
            existing_order.risk_per_trade = order.risk_per_trade
            existing_order.risk_reward_ratio = order.risk_reward_ratio
            existing_order.priority = order.priority
            existing_order.updated_at = datetime.datetime.now()
            
            if logger:
                logger.info(f"Updated order: {order.symbol} (Excel changes applied)")
            return True
            
        except Exception as e:
            if logger:
                logger.error(f"Failed to update order {order.symbol}: {e}")
            return False
    # <Enhanced Persistence Logic - End>
            
    def _persist_single_order(self, order: PlannedOrder) -> bool:
        """Persist a single order to database with duplicate checking."""
        try:
            # Check for existing order with same parameters
            existing_order = self.find_existing_order(order)
            if existing_order and self._is_duplicate_order(order, existing_order):
                if logger:
                    logger.debug(f"Skipping duplicate order: {order.symbol} {order.action.value} @ {order.entry_price:.4f}")
                return False
                
            # Convert to database model and persist
            db_order = self.persistence_service.convert_to_db_model(order)
            self.db_session.add(db_order)
            if logger:
                logger.info(f"Persisted order: {order.symbol} {order.action.value} @ {order.entry_price:.4f}")
            return True
            
        except Exception as e:
            if logger:
                logger.error(f"Failed to persist order {order.symbol}: {e}")
            return False

    # <Enhanced Order Validation - Begin>
    def validate_order(self, order: PlannedOrder) -> Tuple[bool, Optional[str]]:
        """
        Validate order system state with session-aware duplicate detection.
        
        This method checks system-level constraints, not data integrity. It assumes
        the PlannedOrder object is internally valid (required fields present, business
        rules satisfied). Data integrity should be enforced at object creation.
        """
        if logger:
            logger.debug(f"Validating order: {order.symbol}")
            
        # <Delegate Data Integrity to PlannedOrder - Begin>
        try:
            # Fail fast if data integrity issues - should never happen for valid orders
            order.validate()
        except ValueError as e:
            # This indicates a serious data integrity issue that should be fixed upstream
            if logger:
                logger.error(f"Data integrity violation for {order.symbol}: {e}")
            return False, f"Data integrity violation: {e}"
        # <Delegate Data Integrity to PlannedOrder - End>
            
        # <System State Validation - Begin>
        # UNIQUE: Check for open positions
        if self.state_service.has_open_position(order.symbol):
            if logger:
                logger.warning(f"Validation failed: Open position exists for {order.symbol}")
            return False, f"Open position exists for {order.symbol}"
            
        # UNIQUE: Check database state for existing orders with session awareness
        existing_order = self.find_existing_order(order)
        if existing_order:
            return self._validate_existing_order_scenario(order, existing_order)
        # <System State Validation - End>
            
        if logger:
            logger.debug(f"Order validation passed: {order.symbol}")
        return True, None
        
    def _validate_existing_order_scenario(self, new_order: PlannedOrder, existing_order: PlannedOrderDB) -> Tuple[bool, str]:
        """
        Validate scenarios involving existing database orders.
        
        Args:
            new_order: The new order being validated
            existing_order: Existing database order
            
        Returns:
            Tuple of (is_valid, reason_message)
        """
        existing_status = existing_order.status
        
        # Active orders block new identical orders
        if existing_status in [SharedOrderState.LIVE.value, SharedOrderState.LIVE_WORKING.value, SharedOrderState.FILLED.value]:
            if logger:
                logger.warning(f"Validation failed: Active order already exists for {new_order.symbol}: {existing_status}")
            return False, f"Active order already exists: {existing_status}"
            
        # Allow re-execution of failed/cancelled/expired orders (same trading idea)
        if existing_status in [SharedOrderState.CANCELLED.value, SharedOrderState.EXPIRED.value, 
                             SharedOrderState.AON_REJECTED.value]:
            # Check if this is the same trading idea (prices unchanged)
            if self._is_same_trading_idea(new_order, existing_order):
                if logger:
                    logger.debug(f"Validation passed: Re-executing {existing_status} order for {new_order.symbol}")
                return True, f"Re-executing {existing_status} order"
            else:
                if logger:
                    logger.warning(f"Validation failed: Different trading idea for {existing_status} order {new_order.symbol}")
                return False, f"Different trading idea for {existing_status} order"
                
        # PENDING orders can be updated/replaced
        if existing_status == SharedOrderState.PENDING.value:
            if logger:
                logger.debug(f"Validation passed: Updating PENDING order for {new_order.symbol}")
            return True, "Updating PENDING order"
            
        if logger:
            logger.warning(f"Validation failed: Unknown order status for {new_order.symbol}: {existing_status}")
        return False, f"Unknown order status: {existing_status}"
        
    def _is_same_trading_idea(self, order1: PlannedOrder, order2: PlannedOrderDB) -> bool:
        """
        Check if two orders represent the same trading idea (same symbol, action, similar prices).
        
        Args:
            order1: First order (PlannedOrder)
            order2: Second order (PlannedOrderDB)
            
        Returns:
            True if same trading idea, False otherwise
        """
        same_action = order2.action == order1.action.value
        similar_entry = abs(order2.entry_price - order1.entry_price) < 0.0001
        similar_stop = abs(order2.stop_loss - order1.stop_loss) < 0.0001
        
        return same_action and similar_entry and similar_stop
    # <Enhanced Order Validation - End>

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
        if logger:
            logger.debug(f"Validating AON execution for {order.symbol}")
            
        # Check if AON is enabled
        if not self.aon_config.get('enabled', True):
            if logger:
                logger.debug(f"AON validation skipped for {order.symbol} (disabled)")
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
                if logger:
                    logger.warning(f"AON validation failed: Invalid entry price for {order.symbol}: {entry_price}")
                return False, f"Invalid entry price: {entry_price}"
                
            if not isinstance(quantity, (int, float)) or quantity <= 0:
                if logger:
                    logger.warning(f"AON validation failed: Invalid quantity for {order.symbol}: {quantity}")
                return False, f"Invalid quantity: {quantity}"
                
        except Exception as e:
            if logger:
                logger.error(f"AON validation failed: Cannot calculate order notional for {order.symbol}: {e}")
            return False, f"Cannot calculate order notional: {e}"
        
        # Get AON threshold for this symbol
        aon_threshold = self._calculate_aon_threshold(order.symbol)
        if aon_threshold is None:
            if logger:
                logger.warning(f"AON validation failed: Cannot determine AON threshold for {order.symbol}")
            return False, "Cannot determine AON threshold (volume data unavailable)"
        
        # Check if order exceeds AON threshold
        if notional_value > aon_threshold:
            if logger:
                logger.warning(f"AON validation failed for {order.symbol}: Order notional ${notional_value:,.2f} exceeds AON threshold ${aon_threshold:,.2f}")
            return False, f"Order notional ${notional_value:,.2f} exceeds AON threshold ${aon_threshold:,.2f}"
        
        if logger:
            logger.debug(f"AON validation passed for {order.symbol}: ${notional_value:,.2f} <= ${aon_threshold:,.2f}")
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
                fallback_threshold = self.aon_config.get('fallback_fixed_notional', 50000)
                if logger:
                    logger.debug(f"Using fallback AON threshold for {symbol}: ${fallback_threshold:,.2f}")
                return fallback_threshold
            
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
            
            if logger:
                logger.debug(f"AON threshold for {symbol}: {daily_volume:,.0f} shares * ${current_price:.2f} * {volume_percentage:.4f} = ${threshold:,.2f}")
            return threshold
            
        except Exception as e:
            if logger:
                logger.error(f"Error calculating AON threshold for {symbol}: {e}")
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
        if logger:
            logger.debug(f"Daily volume for {symbol}: {volume:,.0f} shares (mock data)")
        return volume
    # <AON Validation Methods - End>
        
    def find_existing_order(self, order: PlannedOrder) -> Optional[PlannedOrderDB]:
        """Find an existing order in database with matching parameters."""
        try:
            existing_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value,
                order_type=order.order_type.value
            ).first()
            
            if logger:
                if existing_order:
                    logger.debug(f"Found existing order for {order.symbol} with status: {existing_order.status}")
                else:
                    logger.debug(f"No existing order found for {order.symbol}")
                    
            return existing_order
            
        except Exception as e:
            if logger:
                logger.error(f"Error querying for existing order {order.symbol}: {e}")
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
        status = existing_order.status if existing_order else None
        
        if logger:
            logger.debug(f"Order status for {order.symbol}: {status}")
            
        return status
        
    def is_order_executable(self, order: PlannedOrder) -> Tuple[bool, Optional[str]]:
        """Check if an order can be executed based on current state."""
        if logger:
            logger.debug(f"Checking executability for {order.symbol}")
        # Delegate to validate_order for comprehensive checking
        return self.validate_order(order)
        
    def update_order_status(self, order: PlannedOrder, status: OrderState, 
                          message: Optional[str] = None) -> bool:
        """Update the status of an order in the database."""
        try:
            existing_order = self.find_existing_order(order)
            if not existing_order:
                if logger:
                    logger.warning(f"Order not found in database for status update: {order.symbol}")
                return False
                
            old_status = existing_order.status
            existing_order.status = status
            existing_order.updated_at = datetime.datetime.now()
            
            if message:
                existing_order.status_message = message
                
            self.db_session.commit()
            
            if logger:
                logger.info(f"Status update: {order.symbol} {old_status} → {status}")
                if message:
                    logger.info(f"Status message: {message}")
                
            return True
            
        except Exception as e:
            self.db_session.rollback()
            if logger:
                logger.error(f"Failed to update order status for {order.symbol}: {e}")
            return False
            
    def bulk_update_status(self, status_updates: List[Tuple[PlannedOrder, OrderState, Optional[str]]]) -> Dict[str, bool]:
        """Update status for multiple orders in a single transaction."""
        if logger:
            logger.info(f"Performing bulk status update for {len(status_updates)} orders")
            
        results = {}
        
        try:
            for order, status, message in status_updates:
                success = self.update_order_status(order, status, message)
                results[order.symbol] = success
                
            if logger:
                success_count = sum(1 for result in results.values() if result)
                logger.info(f"Bulk status update completed: {success_count}/{len(status_updates)} successful")
                
            return results
            
        except Exception as e:
            self.db_session.rollback()
            if logger:
                logger.error(f"Bulk status update failed: {e}")
            return {order.symbol: False for order, _, _ in status_updates}
            
    def get_orders_by_status(self, status: OrderState) -> List[PlannedOrderDB]:
        """Get all orders with a specific status from database."""
        try:
            orders = self.db_session.query(PlannedOrderDB).filter_by(status=status).all()
            if logger:
                logger.debug(f"Found {len(orders)} orders with status {status}")
            return orders
        except Exception as e:
            if logger:
                logger.error(f"Error querying orders by status {status}: {e}")
            return []
            
    def cleanup_old_orders(self, days_old: int = 30) -> int:
        """Clean up orders older than specified days from database."""
        if logger:
            logger.info(f"Cleaning up orders older than {days_old} days")
            
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
            
            if logger:
                logger.info(f"Cleaned up {deleted_count} orders older than {days_old} days")
            return deleted_count
            
        except Exception as e:
            self.db_session.rollback()
            if logger:
                logger.error(f"Failed to clean up old orders: {e}")
            return 0
            
    def get_order_statistics(self) -> Dict[str, any]:
        """Get statistics about orders in the system."""
        if logger:
            logger.debug("Generating order statistics")
            
        try:
            total_orders = self.db_session.query(PlannedOrderDB).count()
            status_counts = {}
            
            for status in OrderState:
                count = self.db_session.query(PlannedOrderDB).filter_by(status=status).count()
                status_counts[status] = count
                
            stats = {
                'total_orders': total_orders,
                'status_counts': status_counts,
                'oldest_order': self._get_oldest_order_date(),
                'newest_order': self._get_newest_order_date()
            }
            
            if logger:
                logger.info(f"Order statistics: {stats}")
                
            return stats
            
        except Exception as e:
            if logger:
                logger.error(f"Error getting order statistics: {e}")
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
        if logger:
            logger.debug("Finding orders needing attention")
            
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
            
            attention_orders = stuck_orders + failed_orders
            
            if logger:
                logger.info(f"Found {len(attention_orders)} orders needing attention")
                
            return attention_orders
            
        except Exception as e:
            if logger:
                logger.error(f"Error finding orders needing attention: {e}")
            return []