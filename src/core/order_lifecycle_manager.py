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

# Context-aware logging import - Begin
from src.core.context_aware_logger import get_context_logger, TradingEventType
# Context-aware logging import - End


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
        # Context-aware logging initialization - Begin
        self.context_logger = get_context_logger()
        # Context-aware logging initialization - End
        
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
        
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="OrderLifecycleManager initialized",
            context_provider={
                'has_loading_orchestrator': lambda: order_loading_orchestrator is not None,
                'aon_enabled': lambda: self.aon_config.get('enabled', True)
            },
            decision_reason="Service startup"
        )
        
    def load_and_persist_orders(self, excel_path: str) -> List[PlannedOrder]:
        """Load orders from Excel, validate, and persist valid ones to database."""
        self.context_logger.log_event(
            event_type=TradingEventType.ORDER_VALIDATION,
            message="Starting order loading and persistence",
            context_provider={
                'excel_path': lambda: excel_path,
                'loading_method': lambda: 'orchestrator' if self.order_loading_orchestrator else 'excel_only'
            },
            decision_reason="Begin order loading process"
        )
        
        try:
            # <Multi-Source Order Loading - Begin>
            if self.order_loading_orchestrator:
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Using OrderLoadingOrchestrator for multi-source loading",
                    decision_reason="Multi-source loading selected"
                )
                all_orders = self.order_loading_orchestrator.load_all_orders(excel_path)
            else:
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Using OrderLoadingService for Excel-only loading",
                    decision_reason="Excel-only loading selected"
                )
                all_orders = self.loading_service.load_and_validate_orders(excel_path)
            # <Multi-Source Order Loading - End>
            
            if not all_orders:
                self.context_logger.log_event(
                    event_type=TradingEventType.ORDER_VALIDATION,
                    message="No orders loaded from Excel file",
                    context_provider={
                        'excel_path': lambda: excel_path
                    },
                    decision_reason="Empty order set"
                )
                return []
                
            # <Enhanced Persistence Logic - Begin>
            persisted_count = 0
            updated_count = 0
            skipped_count = 0
            
            for order in all_orders:
                persistence_action = self._determine_persistence_action(order)
                
                if persistence_action == 'CREATE':
                    if self._persist_single_order(order):
                        persisted_count += 1
                elif persistence_action == 'UPDATE':
                    if self._update_existing_order(order):
                        updated_count += 1
                elif persistence_action == 'SKIP':
                    skipped_count += 1
                    self.context_logger.log_event(
                        event_type=TradingEventType.ORDER_VALIDATION,
                        message="Skipping existing order",
                        symbol=order.symbol,
                        context_provider={
                            'action': lambda: persistence_action
                        },
                        decision_reason="Order already active in database"
                    )
                else:
                    self.context_logger.log_event(
                        event_type=TradingEventType.SYSTEM_HEALTH,
                        message="Unknown persistence action",
                        symbol=order.symbol,
                        context_provider={
                            'action': lambda: persistence_action
                        },
                        decision_reason="Persistence action resolution failure"
                    )
            # <Enhanced Persistence Logic - End>
            
            self.db_session.commit()
            self.context_logger.log_event(
                event_type=TradingEventType.ORDER_VALIDATION,
                message="Order persistence batch completed",
                context_provider={
                    'total_orders': lambda: len(all_orders),
                    'persisted_count': lambda: persisted_count,
                    'updated_count': lambda: updated_count,
                    'skipped_count': lambda: skipped_count
                },
                decision_reason="Order loading and persistence finished"
            )
            
            return all_orders
            
        except Exception as e:
            self.db_session.rollback()
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Order loading and persistence failed",
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Order loading exception"
            )
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
        existing_order = self.find_existing_order(order)
        
        if not existing_order:
            return 'CREATE'  # New order - create in database
            
        existing_status = existing_order.status
        
        if existing_status in [SharedOrderState.PENDING.value, SharedOrderState.LIVE.value, 
                             SharedOrderState.LIVE_WORKING.value]:
            if self._is_excel_update(order, existing_order):
                return 'UPDATE'
            else:
                return 'SKIP'
        else:
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
        price_changed = (abs(excel_order.entry_price - db_order.entry_price) > 0.0001 or
                        abs(excel_order.stop_loss - db_order.stop_loss) > 0.0001)
        
        priority_changed = (excel_order.priority != db_order.priority)
        risk_changed = (abs(excel_order.risk_per_trade - db_order.risk_per_trade) > 0.0001)
        
        return price_changed or priority_changed or risk_changed
        
    # <Enhanced Persistence Logic - End>

# OrderLifecycleManager expiration date persistence fix - Begin
    def _persist_single_order(self, order: PlannedOrder) -> bool:
        """Persist a single order to database with duplicate checking and expiration date handling."""
        try:
            existing_order = self.find_existing_order(order)
            if existing_order and self._is_duplicate_order(order, existing_order):
                self.context_logger.log_event(
                    event_type=TradingEventType.ORDER_VALIDATION,
                    message="Skipping duplicate order",
                    symbol=order.symbol,
                    context_provider={
                        'entry_price': lambda: order.entry_price,
                        'action': lambda: order.action.value
                    },
                    decision_reason="Duplicate order detection"
                )
                return False
            
            # Ensure import time is set for proper expiration date calculation
            if not hasattr(order, '_import_time') or order._import_time is None:
                order._import_time = datetime.datetime.now()
                self.context_logger.log_event(
                    event_type=TradingEventType.ORDER_VALIDATION,
                    message="Set import time for expiration calculation",
                    symbol=order.symbol,
                    context_provider={
                        'import_time': lambda: order._import_time.isoformat()
                    },
                    decision_reason="Import time initialized for new order"
                )
            
            # Recalculate expiration date with proper import time
            order._set_expiration_date()
            
            db_order = self.persistence_service.convert_to_db_model(order)
            self.db_session.add(db_order)
            
            self.context_logger.log_event(
                event_type=TradingEventType.ORDER_VALIDATION,
                message="Order persisted to database with expiration date",
                symbol=order.symbol,
                context_provider={
                    'entry_price': lambda: order.entry_price,
                    'action': lambda: order.action.value,
                    'order_type': lambda: order.order_type.value,
                    'expiration_date': lambda: order.expiration_date.isoformat() if order.expiration_date else None
                },
                decision_reason="New order creation with expiration date"
            )
            return True
            
        except Exception as e:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Order persistence failed",
                symbol=order.symbol,
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Order persistence exception"
            )
            return False

    def _update_existing_order(self, order: PlannedOrder) -> bool:
        """
        Update an existing database order with Excel changes including expiration date refresh.
        
        Args:
            order: PlannedOrder with updated values
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            existing_order = self.find_existing_order(order)
            if not existing_order:
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Cannot update - order not found",
                    symbol=order.symbol,
                    decision_reason="Order lookup failure during update"
                )
                return False
            
            # Update core fields
            existing_order.entry_price = order.entry_price
            existing_order.stop_loss = order.stop_loss
            existing_order.risk_per_trade = order.risk_per_trade
            existing_order.risk_reward_ratio = order.risk_reward_ratio
            existing_order.priority = order.priority
            existing_order.updated_at = datetime.datetime.now()
            
            # Refresh expiration date for the updated order
            if not hasattr(order, '_import_time') or order._import_time is None:
                order._import_time = datetime.datetime.now()
            
            order._set_expiration_date()
            existing_order.expiration_date = order.expiration_date
            
            self.context_logger.log_event(
                event_type=TradingEventType.ORDER_VALIDATION,
                message="Order updated with Excel changes and refreshed expiration",
                symbol=order.symbol,
                context_provider={
                    'entry_price': lambda: order.entry_price,
                    'stop_loss': lambda: order.stop_loss,
                    'priority': lambda: order.priority,
                    'expiration_date': lambda: order.expiration_date.isoformat() if order.expiration_date else None
                },
                decision_reason="Excel update applied to database order with expiration refresh"
            )
            return True
            
        except Exception as e:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Order update failed",
                symbol=order.symbol,
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Order update exception"
            )
            return False
# OrderLifecycleManager expiration date persistence fix - End

    # <Enhanced Order Validation - Begin>
    def validate_order(self, order: PlannedOrder) -> Tuple[bool, Optional[str]]:
        """
        Validate order system state with session-aware duplicate detection.
        
        This method checks system-level constraints, not data integrity. It assumes
        the PlannedOrder object is internally valid (required fields present, business
        rules satisfied). Data integrity should be enforced at object creation.
        """
        self.context_logger.log_event(
            event_type=TradingEventType.ORDER_VALIDATION,
            message="Starting order validation",
            symbol=order.symbol,
            context_provider={
                'action': lambda: order.action.value,
                'order_type': lambda: order.order_type.value
            },
            decision_reason="Begin system-level validation"
        )
            
        try:
            order.validate()
        except ValueError as e:
            self.context_logger.log_event(
                event_type=TradingEventType.ORDER_VALIDATION,
                message="Data integrity validation failed",
                symbol=order.symbol,
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Data integrity violation"
            )
            return False, f"Data integrity violation: {e}"
            
        if self.state_service.has_open_position(order.symbol):
            self.context_logger.log_event(
                event_type=TradingEventType.ORDER_VALIDATION,
                message="Validation failed - open position exists",
                symbol=order.symbol,
                decision_reason="Open position conflict"
            )
            return False, f"Open position exists for {order.symbol}"
            
        existing_order = self.find_existing_order(order)
        if existing_order:
            return self._validate_existing_order_scenario(order, existing_order)
            
        self.context_logger.log_event(
            event_type=TradingEventType.ORDER_VALIDATION,
            message="Order validation passed",
            symbol=order.symbol,
            decision_reason="All validation checks passed"
        )
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
        
        if existing_status in [SharedOrderState.LIVE.value, SharedOrderState.LIVE_WORKING.value, SharedOrderState.FILLED.value]:
            self.context_logger.log_event(
                event_type=TradingEventType.ORDER_VALIDATION,
                message="Validation failed - active order exists",
                symbol=new_order.symbol,
                context_provider={
                    'existing_status': lambda: existing_status
                },
                decision_reason="Active order conflict"
            )
            return False, f"Active order already exists: {existing_status}"
            
        if existing_status in [SharedOrderState.CANCELLED.value, SharedOrderState.EXPIRED.value, 
                             SharedOrderState.AON_REJECTED.value]:
            if self._is_same_trading_idea(new_order, existing_order):
                self.context_logger.log_event(
                    event_type=TradingEventType.ORDER_VALIDATION,
                    message="Validation passed - re-executing order",
                    symbol=new_order.symbol,
                    context_provider={
                        'previous_status': lambda: existing_status
                    },
                    decision_reason="Re-execution of terminated order"
                )
                return True, f"Re-executing {existing_status} order"
            else:
                self.context_logger.log_event(
                    event_type=TradingEventType.ORDER_VALIDATION,
                    message="Validation failed - different trading idea",
                    symbol=new_order.symbol,
                    context_provider={
                        'previous_status': lambda: existing_status
                    },
                    decision_reason="Different trading idea detected"
                )
                return False, f"Different trading idea for {existing_status} order"
                
        if existing_status == SharedOrderState.PENDING.value:
            self.context_logger.log_event(
                event_type=TradingEventType.ORDER_VALIDATION,
                message="Validation passed - updating pending order",
                symbol=new_order.symbol,
                decision_reason="Pending order update allowed"
            )
            return True, "Updating PENDING order"
            
        self.context_logger.log_event(
            event_type=TradingEventType.ORDER_VALIDATION,
            message="Validation failed - unknown order status",
            symbol=new_order.symbol,
            context_provider={
                'existing_status': lambda: existing_status
            },
            decision_reason="Unknown order status encountered"
        )
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
        self.context_logger.log_event(
            event_type=TradingEventType.RISK_EVALUATION,
            message="Starting AON validation",
            symbol=order.symbol,
            context_provider={
                'total_capital': lambda: total_capital,
                'aon_enabled': lambda: self.aon_config.get('enabled', True)
            },
            decision_reason="Begin AON eligibility check"
        )
            
        if not self.aon_config.get('enabled', True):
            self.context_logger.log_event(
                event_type=TradingEventType.RISK_EVALUATION,
                message="AON validation skipped - disabled",
                symbol=order.symbol,
                decision_reason="AON feature disabled"
            )
            return True, "AON validation skipped (disabled)"
        
        try:
            entry_price = getattr(order.entry_price, 'return_value', order.entry_price)
            if hasattr(entry_price, '__call__'):
                entry_price = entry_price()
            
            quantity = order.calculate_quantity(total_capital)
            quantity = getattr(quantity, 'return_value', quantity)
            if hasattr(quantity, '__call__'):
                quantity = quantity()
                
            notional_value = entry_price * quantity
            
            if not isinstance(entry_price, (int, float)) or entry_price <= 0:
                self.context_logger.log_event(
                    event_type=TradingEventType.RISK_EVALUATION,
                    message="AON validation failed - invalid entry price",
                    symbol=order.symbol,
                    context_provider={
                        'entry_price': lambda: entry_price
                    },
                    decision_reason="Invalid entry price format"
                )
                return False, f"Invalid entry price: {entry_price}"
                
            if not isinstance(quantity, (int, float)) or quantity <= 0:
                self.context_logger.log_event(
                    event_type=TradingEventType.RISK_EVALUATION,
                    message="AON validation failed - invalid quantity",
                    symbol=order.symbol,
                    context_provider={
                        'quantity': lambda: quantity
                    },
                    decision_reason="Invalid quantity format"
                )
                return False, f"Invalid quantity: {quantity}"
                
        except Exception as e:
            self.context_logger.log_event(
                event_type=TradingEventType.RISK_EVALUATION,
                message="AON validation failed - calculation error",
                symbol=order.symbol,
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Notional calculation exception"
            )
            return False, f"Cannot calculate order notional: {e}"
        
        aon_threshold = self._calculate_aon_threshold(order.symbol)
        if aon_threshold is None:
            self.context_logger.log_event(
                event_type=TradingEventType.RISK_EVALUATION,
                message="AON validation failed - threshold unavailable",
                symbol=order.symbol,
                decision_reason="AON threshold calculation failure"
            )
            return False, "Cannot determine AON threshold (volume data unavailable)"
        
        if notional_value > aon_threshold:
            self.context_logger.log_event(
                event_type=TradingEventType.RISK_EVALUATION,
                message="AON validation failed - exceeds threshold",
                symbol=order.symbol,
                context_provider={
                    'notional_value': lambda: round(notional_value, 2),
                    'aon_threshold': lambda: round(aon_threshold, 2),
                    'exceeds_by': lambda: round(notional_value - aon_threshold, 2)
                },
                decision_reason="Order notional exceeds AON threshold"
            )
            return False, f"Order notional ${notional_value:,.2f} exceeds AON threshold ${aon_threshold:,.2f}"
        
        self.context_logger.log_event(
            event_type=TradingEventType.RISK_EVALUATION,
            message="AON validation passed",
            symbol=order.symbol,
            context_provider={
                'notional_value': lambda: round(notional_value, 2),
                'aon_threshold': lambda: round(aon_threshold, 2)
            },
            decision_reason="Order eligible for AON execution"
        )
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
            daily_volume = self._get_daily_volume(symbol)
            if daily_volume is None:
                fallback_threshold = self.aon_config.get('fallback_fixed_notional', 50000)
                self.context_logger.log_event(
                    event_type=TradingEventType.RISK_EVALUATION,
                    message="Using fallback AON threshold",
                    symbol=symbol,
                    context_provider={
                        'fallback_threshold': lambda: fallback_threshold
                    },
                    decision_reason="Daily volume unavailable"
                )
                return fallback_threshold
            
            symbol_specific = self.aon_config.get('symbol_specific', {})
            volume_percentage = symbol_specific.get(symbol, 
                                self.aon_config.get('default_volume_percentage', 0.001))
            
            volume_percentage = float(volume_percentage)
            current_price = 100.0  # Placeholder - would come from data feed
            threshold = daily_volume * current_price * volume_percentage
            
            self.context_logger.log_event(
                event_type=TradingEventType.RISK_EVALUATION,
                message="AON threshold calculated",
                symbol=symbol,
                context_provider={
                    'daily_volume': lambda: daily_volume,
                    'current_price': lambda: current_price,
                    'volume_percentage': lambda: volume_percentage,
                    'calculated_threshold': lambda: round(threshold, 2)
                },
                decision_reason="AON threshold calculation completed"
            )
            return threshold
            
        except Exception as e:
            fallback_threshold = self.aon_config.get('fallback_fixed_notional', 50000)
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="AON threshold calculation failed - using fallback",
                symbol=symbol,
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e),
                    'fallback_threshold': lambda: fallback_threshold
                },
                decision_reason="AON threshold calculation exception"
            )
            return fallback_threshold
                
    def _get_daily_volume(self, symbol: str) -> Optional[float]:
        """
        Get daily volume for a symbol. Placeholder implementation.
        
        In practice, this would fetch from IBKR data feed or cache.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Daily volume in shares, or None if unavailable
        """
        mock_volumes = {
            'SPY': 50000000,
            'QQQ': 30000000,
            'IWM': 20000000,
            'AAPL': 40000000,
            'TSLA': 25000000,
        }
        
        volume = mock_volumes.get(symbol, 10000000)
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
            
            return existing_order
            
        except Exception as e:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Existing order query failed",
                symbol=order.symbol,
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Database query exception"
            )
            return None
            
    def _is_duplicate_order(self, new_order: PlannedOrder, existing_order: PlannedOrderDB) -> bool:
        """Check if new order is a duplicate of existing database order."""
        same_action = existing_order.action == new_order.action.value
        same_entry = abs(existing_order.entry_price - new_order.entry_price) < 0.0001
        same_stop = abs(existing_order.stop_loss - new_order.stop_loss) < 0.0001
        
        return same_action and same_entry and same_stop
        
    def get_order_status(self, order: PlannedOrder) -> Optional[OrderState]:
        """Get the current status of an order from database."""
        existing_order = self.find_existing_order(order)
        status = existing_order.status if existing_order else None
            
        return status
        
    def is_order_executable(self, order: PlannedOrder) -> Tuple[bool, Optional[str]]:
        """Check if an order can be executed based on current state."""
        return self.validate_order(order)
        
    def update_order_status(self, order: PlannedOrder, status: OrderState, 
                          message: Optional[str] = None) -> bool:
        """Update the status of an order in the database."""
        try:
            existing_order = self.find_existing_order(order)
            if not existing_order:
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Order not found for status update",
                    symbol=order.symbol,
                    decision_reason="Order lookup failure"
                )
                return False
                
            old_status = existing_order.status
            existing_order.status = status
            existing_order.updated_at = datetime.datetime.now()
            
            if message:
                existing_order.status_message = message
                
            self.db_session.commit()
            
            self.context_logger.log_event(
                event_type=TradingEventType.STATE_TRANSITION,
                message="Order status updated",
                symbol=order.symbol,
                context_provider={
                    'old_status': lambda: old_status,
                    'new_status': lambda: status,
                    'has_message': lambda: message is not None
                },
                decision_reason="Order status transition"
            )
            return True
            
        except Exception as e:
            self.db_session.rollback()
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Order status update failed",
                symbol=order.symbol,
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Status update exception"
            )
            return False
            
    def bulk_update_status(self, status_updates: List[Tuple[PlannedOrder, OrderState, Optional[str]]]) -> Dict[str, bool]:
        """Update status for multiple orders in a single transaction."""
        self.context_logger.log_event(
            event_type=TradingEventType.STATE_TRANSITION,
            message="Starting bulk status update",
            context_provider={
                'order_count': lambda: len(status_updates)
            },
            decision_reason="Begin batch status update"
        )
            
        results = {}
        
        try:
            for order, status, message in status_updates:
                success = self.update_order_status(order, status, message)
                results[order.symbol] = success
                
            success_count = sum(1 for result in results.values() if result)
            self.context_logger.log_event(
                event_type=TradingEventType.STATE_TRANSITION,
                message="Bulk status update completed",
                context_provider={
                    'success_count': lambda: success_count,
                    'total_count': lambda: len(status_updates),
                    'success_rate': lambda: round(success_count / len(status_updates) * 100, 2) if status_updates else 0
                },
                decision_reason="Batch status update finished"
            )
            return results
            
        except Exception as e:
            self.db_session.rollback()
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Bulk status update failed",
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Batch status update exception"
            )
            return {order.symbol: False for order, _, _ in status_updates}
            
    def get_orders_by_status(self, status: OrderState) -> List[PlannedOrderDB]:
        """Get all orders with a specific status from database."""
        try:
            orders = self.db_session.query(PlannedOrderDB).filter_by(status=status).all()
            return orders
        except Exception as e:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Order status query failed",
                context_provider={
                    'status': lambda: status,
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Status-based query exception"
            )
            return []
            
    def cleanup_old_orders(self, days_old: int = 30) -> int:
        """Clean up orders older than specified days from database."""
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Starting old order cleanup",
            context_provider={
                'days_old': lambda: days_old
            },
            decision_reason="Begin order cleanup process"
        )
            
        try:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_old)
            old_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.created_at < cutoff_date,
                PlannedOrderDB.status.in_(['FILLED', 'CANCELLED', 'AON_REJECTED'])
            ).all()
            
            deleted_count = 0
            for order in old_orders:
                self.db_session.delete(order)
                deleted_count += 1
                
            self.db_session.commit()
            
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Old order cleanup completed",
                context_provider={
                    'deleted_count': lambda: deleted_count,
                    'cutoff_date': lambda: cutoff_date.isoformat()
                },
                decision_reason="Order cleanup finished"
            )
            return deleted_count
            
        except Exception as e:
            self.db_session.rollback()
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Old order cleanup failed",
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Order cleanup exception"
            )
            return 0
            
    def get_order_statistics(self) -> Dict[str, any]:
        """Get statistics about orders in the system."""
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Generating order statistics",
            decision_reason="Begin statistics collection"
        )
            
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
            
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Order statistics generated",
                context_provider={
                    'total_orders': lambda: total_orders,
                    'status_count': lambda: len(status_counts)
                },
                decision_reason="Statistics collection completed"
            )
            return stats
            
        except Exception as e:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Order statistics generation failed",
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Statistics collection exception"
            )
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
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Finding orders needing attention",
            decision_reason="Begin attention order scan"
        )
            
        try:
            stuck_time = datetime.datetime.now() - datetime.timedelta(hours=2)
            stuck_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.status == 'EXECUTING',
                PlannedOrderDB.updated_at < stuck_time
            ).all()
            
            failed_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.status.in_(['FAILED', 'AON_REJECTED'])
            ).all()
            
            attention_orders = stuck_orders + failed_orders
            
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Attention order scan completed",
                context_provider={
                    'stuck_orders': lambda: len(stuck_orders),
                    'failed_orders': lambda: len(failed_orders),
                    'total_attention_orders': lambda: len(attention_orders)
                },
                decision_reason="Attention order scan finished"
            )
            return attention_orders
            
        except Exception as e:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Attention order scan failed",
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Attention order scan exception"
            )
            return []