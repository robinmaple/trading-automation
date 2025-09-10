import datetime
from typing import List, Dict, Optional
import threading
import time
import pandas as pd
from src.core.ibkr_client import IbkrClient
from src.core.planned_order import PlannedOrder, ActiveOrder
from src.core.probability_engine import FillProbabilityEngine
from src.core.abstract_data_feed import AbstractDataFeed

# ==================== DATABASE INTEGRATION - BEGIN ====================
from src.core.database import get_db_session
from src.core.models import PlannedOrderDB, PositionStrategy
# ==================== DATABASE INTEGRATION - END ====================

# ==================== SERVICE LAYER INTEGRATION - BEGIN ====================
from src.services.order_eligibility_service import OrderEligibilityService
from src.services.order_execution_service import OrderExecutionService
from src.services.position_sizing_service import PositionSizingService
from src.services.order_loading_service import OrderLoadingService
from src.services.order_persistence_service import OrderPersistenceService
# ==================== SERVICE LAYER INTEGRATION - END ====================

# ==================== STATE SERVICE INTEGRATION - BEGIN ====================
from src.services.state_service import StateService
from src.core.events import OrderEvent, OrderState
# ==================== STATE SERVICE INTEGRATION - END ====================

# Reconciliation Engine Integration - Begin
from src.core.reconciliation_engine import ReconciliationEngine
# Reconciliation Engine Integration - End

class TradingManager:
    def __init__(self, data_feed: AbstractDataFeed, excel_path: str = "plan.xlsx", 
                 ibkr_client: Optional[IbkrClient] = None,
                 order_persistence_service: Optional[OrderPersistenceService] = None):
        self.data_feed = data_feed
        self.excel_path = excel_path
        self.ibkr_client = ibkr_client
        self.planned_orders: List[PlannedOrder] = []
        # Phase 2 - ActiveOrder Tracking - Begin
        self.active_orders: Dict[int, ActiveOrder] = {}  # Changed: Now uses ActiveOrder class
        # Phase 2 - ActiveOrder Tracking - End
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.total_capital = 100000
        
        self.max_open_orders = 5
        self.execution_threshold = 0.7

        # Track market data subscriptions
        self.subscribed_symbols = set()
        self.market_data_updates = {}
        self._initialized = False

        # ==================== DATABASE INTEGRATION - BEGIN ====================
        # Initialize database session for order persistence
        self.db_session = get_db_session()
        # ==================== DATABASE INTEGRATION - END ====================

        # ==================== ORDER PERSISTENCE SERVICE - BEGIN ====================
        # Initialize order persistence service
        self.order_persistence_service = order_persistence_service or OrderPersistenceService(self.db_session)
        # ==================== ORDER PERSISTENCE SERVICE - END ====================

        # ==================== STATE SERVICE INTEGRATION - BEGIN ====================
        # Initialize the State Service as the single source of truth for order state
        self.state_service = StateService(self.db_session)
        # Subscribe to state change events
        self.state_service.subscribe('order_state_change', self._handle_order_state_change)
        # ==================== STATE SERVICE INTEGRATION - END ====================

        # ==================== RECONCILIATION ENGINE INTEGRATION - BEGIN ====================
        # Initialize reconciliation engine for state synchronization
        self.reconciliation_engine = ReconciliationEngine(ibkr_client, self.state_service)
        # ==================== RECONCILIATION ENGINE INTEGRATION - END ====================
        
        # ==================== SERVICE LAYER INTEGRATION - BEGIN ====================
        # Initialize the new service classes for the refactored architecture.
        # Phase 0: These services will initially delegate back to TradingManager methods.
        self.execution_service = OrderExecutionService(self, self.ibkr_client)
        self.sizing_service = PositionSizingService(self)
        self.loading_service = OrderLoadingService(self, self.db_session)
        self.eligibility_service = None
        # ==================== SERVICE LAYER INTEGRATION - END ====================

    def _initialize(self):
        """Internal initialization - called automatically when needed"""
        if self._initialized:
            return True
            
        if not self.data_feed.is_connected():
            print("Cannot initialize - data feed not connected")
            return False
            
        self.probability_engine = FillProbabilityEngine(self.data_feed)
      
        # ==================== SERVICE DEPENDENCY SETUP - BEGIN ====================
        # Initialize complex services that need probability_engine
        self.eligibility_service = OrderEligibilityService(self.planned_orders, self.probability_engine)
        # Set dependencies for execution service
        self.execution_service.set_dependencies(self.order_persistence_service, self.active_orders)
        # ==================== SERVICE DEPENDENCY SETUP - END ====================

        self._initialized = True
        print("✅ Trading manager initialized")
        
        if self.ibkr_client and self.ibkr_client.connected:
            print("✅ Real order execution enabled (IBKR connected)")
        elif self.ibkr_client:
            print("⚠️  Order executor provided but not connected to IBKR - will use simulation")
        else:
            print("ℹ️  No order executor provided - using simulation mode")
            
        return True
    
    # ==================== STATE EVENT HANDLER - BEGIN ====================
    def _handle_order_state_change(self, event: OrderEvent) -> None:
        """
        Handle order state change events from the StateService.
        This enables event-driven architecture for state changes.
        """
        print(f"📢 State Event: {event.symbol} {event.old_state} -> {event.new_state} via {event.source}")
        
        # Add custom logic here for specific state transitions
        if event.new_state == OrderState.FILLED:
            print(f"🎉 Order {event.order_id} filled! Details: {event.details}")
        elif event.new_state == OrderState.CANCELLED:
            print(f"❌ Order {event.order_id} was cancelled")
    # ==================== STATE EVENT HANDLER - END ====================

    def _execute_order(self, order, fill_probability):
        """
        Execute a single order - now creates ActiveOrder objects
        """
        try:
            # Get actual account value from IBKR
            if self.ibkr_client and self.ibkr_client.connected:
                total_capital = self.ibkr_client.get_account_value()
            else:
                total_capital = self.total_capital
            
            # Live/Paper trading tracking
            is_live_trading = self._get_trading_mode()
            mode_str = "LIVE" if is_live_trading else "PAPER"
            
            print(f"🎯 EXECUTING ({mode_str}): {order.action.value} {order.symbol} {order.order_type.value} @ {order.entry_price}")
            print(f"   Account Value: ${total_capital:,.2f}")
            print(f"   Fill Probability: {fill_probability:.2%}")
            print(f"   Stop Loss: {order.stop_loss}, Profit Target: {order.calculate_profit_target()}")
            
            # ==================== SERVICE INTEGRATION - BEGIN ====================
            # Phase 0: Delegate quantity calculation to the sizing service.
            quantity = self.sizing_service.calculate_order_quantity(
                order,
                total_capital
            )
            # ==================== SERVICE INTEGRATION - END ====================
            capital_commitment = order.entry_price * quantity
            print(f"   Quantity: {quantity}, Capital Commitment: ${capital_commitment:,.2f}")
            
            # ==================== EXECUTION PATH DEBUGGING - BEGIN ====================
            # Debug: Show which execution path we're taking
            ibkr_connected = self.ibkr_client and self.ibkr_client.connected
            print(f"   IBKR Connected: {ibkr_connected}, Live Trading: {is_live_trading}")
            # ==================== EXECUTION PATH DEBUGGING - END ====================

            # ==================== STATE UPDATE - BEGIN ====================
            # Update order status to LIVE_WORKING before execution
            self._update_order_status(order, OrderState.LIVE_WORKING)
            # ==================== STATE UPDATE - END ====================

            # ==================== SERVICE INTEGRATION - BEGIN ====================
            # Phase 0: Delegate order execution to the dedicated service.
            # The service will call the original _execute_order logic internally.
            self.execution_service.place_order(order, fill_probability, total_capital, quantity, capital_commitment, is_live_trading)
            # ==================== SERVICE INTEGRATION - END ====================
            
        except Exception as e:
            print(f"❌ Failed to execute order for {order.symbol}: {e}")
            import traceback
            traceback.print_exc()

    def _execute_single_order(self, order, fill_probability, total_capital, quantity, capital_commitment, is_live_trading):
        """
        Core order execution logic - now delegated to the execution service.
        """
        # Phase 1: Delegate to the service which now contains the actual logic.
        return self.execution_service.execute_single_order(
            order, fill_probability, total_capital, quantity, capital_commitment, is_live_trading
        )

    def cancel_active_order(self, active_order: ActiveOrder) -> bool:
        """
        Cancel an active order through IBKR API
        Returns: Success status
        """
        if not self.ibkr_client or not self.ibkr_client.connected:
            print("❌ Cannot cancel order - not connected to IBKR")
            return False
            
        try:
            # Cancel all orders in the bracket (parent, take-profit, stop-loss)
            for order_id in active_order.order_ids:
                success = self.ibkr_client.cancel_order(order_id)
                if not success:
                    print(f"❌ Failed to cancel order {order_id}")
                    return False
            
            # Update order status
            active_order.update_status('CANCELLED')
            print(f"✅ Cancelled order {active_order.symbol} (IDs: {active_order.order_ids})")
            return True
            
        except Exception as e:
            print(f"❌ Error cancelling order {active_order.symbol}: {e}")
            return False

    def replace_active_order(self, old_order: ActiveOrder, new_planned_order: PlannedOrder, 
                           new_fill_probability: float) -> bool:
        """
        Replace a stale active order with a new order
        Returns: Success status
        """
        print(f"🔄 Replacing stale order {old_order.symbol} with new order")
        
        # 1. Cancel the old order
        if not self.cancel_active_order(old_order):
            print("❌ Replacement failed - could not cancel old order")
            return False
        
        # 2. Execute the new order
        self._execute_order(new_planned_order, new_fill_probability)
        
        # 3. Update old order status
        old_order.update_status('REPLACED')
        
        print(f"✅ Successfully replaced order {old_order.symbol}")
        return True

    def cleanup_completed_orders(self):
        """Remove filled/cancelled/replaced orders from active tracking"""
        orders_to_remove = []
        
        for order_id, active_order in self.active_orders.items():
            if not active_order.is_working():
                orders_to_remove.append(order_id)
        
        for order_id in orders_to_remove:
            del self.active_orders[order_id]
            
        if orders_to_remove:
            print(f"🧹 Cleaned up {len(orders_to_remove)} completed orders")

    def get_active_orders_summary(self) -> List[Dict]:
        """Get summary of all active orders for monitoring"""
        return [active_order.to_dict() for active_order in self.active_orders.values()]
    # ==================== ACTIVE ORDER MANAGEMENT - END ====================

    # ==================== QUANTITY CALCULATION - BEGIN ====================
    def _calculate_quantity(self, security_type, entry_price, stop_loss, total_capital, risk_per_trade):
        """Calculate position size based on security type and risk management"""
        # Phase 1: Delegate to the service which now contains the actual logic.
        return self.sizing_service.calculate_quantity(
            security_type, entry_price, stop_loss, total_capital, risk_per_trade
        )
    # ==================== QUANTITY CALCULATION - END ====================

    def start_monitoring(self, interval_seconds: int = 5):
        """Start monitoring with automatic initialization"""
        if not self._initialize():
            print("❌ Failed to initialize trading manager")
            return False

        # Start reconciliation engine if IBKR client is connected
        if self.ibkr_client and self.ibkr_client.connected:
            self.reconciliation_engine.start()
        """Start continuous monitoring"""
        if not self.data_feed.is_connected():
            raise Exception("Data feed not connected")
            
        self.monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(interval_seconds,),
            daemon=True
        )
        self.monitor_thread.start()
        print("Monitoring started")
        return True
    
    def _monitoring_loop(self, interval_seconds: int):
        """Main monitoring loop with better error handling"""
        error_count = 0
        max_errors = 10
        
        self._subscribe_to_all_symbols()

        while self.monitoring and error_count < max_errors:
            try:
                self._check_and_execute_orders()
                error_count = 0  # Reset error count on success
                time.sleep(interval_seconds)
                
            except Exception as e:
                error_count += 1
                print(f"Monitoring error ({error_count}/{max_errors}): {e}")
                import traceback
                traceback.print_exc()
                
                # Exponential backoff on errors
                backoff_time = min(60 * error_count, 300)  # Max 5 minutes
                time.sleep(backoff_time)
                
        if error_count >= max_errors:
            print("Too many errors, stopping monitoring")
            self.monitoring = False

    def _track_market_data(self):
        """Enhanced market data tracking with data type awareness"""
        print("Market data tracking is now handled by the data feed implementation.")

    def _subscribe_to_all_symbols(self):
        """Enhanced subscription with retry logic"""
        if not self.planned_orders:
            return
            
        print("\n" + "="*60)
        print("SUBSCRIBING TO MARKET DATA")
        print("="*60)
        
        for order in self.planned_orders:
            if order.symbol not in self.subscribed_symbols:
                try:
                    contract = order.to_ib_contract()
                    success = self.data_feed.subscribe(order.symbol, contract)
                    
                    if success:
                        self.subscribed_symbols.add(order.symbol)
                        self.market_data_updates[order.symbol] = 0
                        print(f"✅ Subscription successful: {order.symbol}")
                    else:
                        print(f"❌ Subscription failed: {order.symbol}")
                        
                except Exception as e:
                    print(f"❌ Failed to subscribe to {order.symbol}: {e}")
        
        print(f"Total successful subscriptions: {len(self.subscribed_symbols)}/{len(self.planned_orders)}")
        print("="*60)

    def _check_and_execute_orders(self):
        """Check market conditions and execute orders if conditions are met"""
        if not self.planned_orders:
            print("No planned orders to monitor")
            return
            
        # Display order summary
        print(f"\n📊 PLANNED ORDERS SUMMARY ({len(self.planned_orders)} orders)")
        print("-" * 50)
        
        for i, order in enumerate(self.planned_orders):
            # Get current market price for this symbol
            price_data = self.data_feed.get_current_price(order.symbol)
            current_price = price_data['price'] if price_data and price_data.get('price') else None
            
            # FIX: Handle None entry_price gracefully
            entry_display = f"${order.entry_price:8.4f}" if order.entry_price is not None else "None"

            if current_price and order.entry_price is not None:
                # Calculate basic metrics for display
                price_diff = current_price - order.entry_price
                percent_diff = (price_diff / order.entry_price * 100)
                
                print(f"{i+1:2}. {order.action.value:4} {order.symbol:6} | "
                      f"Current: ${current_price:8.4f} | Entry: {entry_display} | "
                      f"Diff: {percent_diff:6.2f}%")
            else:
                print(f"{i+1:2}. {order.action.value:4} {order.symbol:6} | "
                      f"No market data yet | Entry: {entry_display}")
                        
        # ==================== SERVICE INTEGRATION - BEGIN ====================                        
        # Phase 0: Use the new service to find executable orders.
        # The service currently delegates to the old _find_executable_orders method.
        executable_orders = self.eligibility_service.find_executable_orders()
        # ==================== SERVICE INTEGRATION - END ====================                        
                        
        if executable_orders:
            print(f"🎯 Found {len(executable_orders)} executable orders")
            for executable in executable_orders:
                order = executable['order']
                fill_prob = executable['fill_probability']
                
                # ==================== DUPLICATE CHECK - BEGIN ====================
                # Check for existing open position before execution
                if self.state_service.has_open_position(order.symbol):
                    print(f"⏩ Skipping {order.symbol} - open position exists")
                    continue
                # ==================== DUPLICATE CHECK - END ====================
                
                self._execute_order(order, fill_prob)        
        else:
            print("💡 No executable orders found at this time")
        
        print("-" * 50)
        
    def load_planned_orders(self) -> List[PlannedOrder]:
        """Load and validate planned orders from Excel, persist only valid ones to database"""
        valid_orders = self.loading_service.load_and_validate_orders(self.excel_path)
        
        # Persist valid orders to database
        for order in valid_orders:
            try:
                db_order = self.order_persistence_service.convert_to_db_model(order)
                self.db_session.add(db_order)
                print(f"✅ Persisted order: {order.symbol}")
            except Exception as e:
                print(f"❌ Failed to persist order {order.symbol}: {e}")
        
        self.db_session.commit()
        self.planned_orders = valid_orders
        return valid_orders

    # ==================== DUPLICATE DETECTION HELPER - BEGIN ====================
    def _find_existing_planned_order(self, order: PlannedOrder) -> Optional[PlannedOrderDB]:
        """
        Check if an order already exists in the database.
        Returns: The existing PlannedOrderDB if found, None otherwise.
        """
        try:
            existing_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value
            ).first()
            
            return existing_order
            
        except Exception as e:
            print(f"❌ Error checking for existing order {order.symbol}: {e}")
            return None
    # ==================== DUPLICATE DETECTION HELPER - END ====================

    # Phase 2 - Order Validation Helper - Begin
    def _validate_order_basic(self, order: PlannedOrder) -> bool:
        """
        Basic validation for orders (without max_orders check).
        Used for filtering during order loading.
        """
        # Check for required price data
        if order.entry_price is None or order.stop_loss is None:
            return False
            
        # Check stop loss is protective
        if (order.action.value == 'BUY' and order.stop_loss >= order.entry_price) or \
           (order.action.value == 'SELL' and order.stop_loss <= order.entry_price):
            return False
            
        # Check risk management
        if order.risk_per_trade <= 0 or order.risk_per_trade > 0.02:  # Max 2% risk
            return False
            
        if order.risk_reward_ratio < 1.0:  # At least 1:1 risk reward
            return False
            
        # Check priority is valid
        if not 1 <= order.priority <= 5:
            return False
            
        return True
    # Phase 2 - Order Validation Helper - End        

    def _find_executable_orders(self):
        """Find orders that meet execution criteria"""
        # Phase 1: Delegate to the service which now contains the actual logic.
        return self.eligibility_service.find_executable_orders()
    
    def stop_monitoring(self):
        """Stop the monitoring loop and clean up database connection"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        
        # Stop reconciliation engine
        self.reconciliation_engine.stop()

        # ==================== DATABASE INTEGRATION - BEGIN ====================
        # Close database session on shutdown
        if self.db_session:
            self.db_session.close()
        # ==================== DATABASE INTEGRATION - END ====================
        
        print("Monitoring stopped")

    # ==================== DATABASE INTEGRATION - BEGIN ====================
    # New methods for database operations
    
    def _update_order_status(self, order, status, order_ids=None):
        """Update order status in database"""
        try:
            # Find the order in database
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
                
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to update order status: {e}")

    def _get_trading_mode(self) -> bool:
        """Determine if we're in live trading using existing IbkrClient detection"""
        if self.ibkr_client and self.ibkr_client.connected:
            # Use the existing paper account detection from IbkrClient
            is_live_trading = not self.ibkr_client.is_paper_account
            print(f"📊 Trading mode detected: {'LIVE' if is_live_trading else 'PAPER'} (Account: {self.ibkr_client.account_number})")
            return is_live_trading
        else:
            # Simulation mode - treat as paper trading
            print("📊 Trading mode: PAPER (Simulation/No IBKR connection)")
            return False
        
    # ==================== DATABASE INTEGRATION - END ====================

    # Phase 2 - Database ID Lookup Method - Begin
    def _find_planned_order_db_id(self, order) -> Optional[int]:
        """Find the database ID for a planned order"""
        try:
            db_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value,
                order_type=order.order_type.value
            ).first()
            
            return db_order.id if db_order else None
            
        except Exception as e:
            print(f"❌ Error finding planned order in database: {e}")
            return None
    # Phase 2 - Database ID Lookup Method - End

    # Phase 2 - Capital Commitment Calculation - Begin
    def _calculate_capital_commitment(self, order, total_capital: float) -> float:
        """Calculate the capital commitment for an order"""
        try:
            quantity = order.calculate_quantity(total_capital)
            capital_commitment = order.entry_price * quantity
            
            # For non-stock instruments, this would be more complex
            # (margin requirements, option premiums, etc.)
            # For now, we use simple calculation for stocks/ETFs
            return capital_commitment
            
        except Exception as e:
            print(f"❌ Error calculating capital commitment for {order.symbol}: {e}")
            return 0.0
    # Phase 2 - Capital Commitment Calculation - End

    def _can_place_order(self, order):
        """Basic validation for order placement"""
        # Check if we already have max open orders
        # Phase 2 - ActiveOrder Structure Update - Begin
        working_orders = sum(1 for ao in self.active_orders.values() if ao.is_working())
        if working_orders >= self.max_open_orders:
            return False
        # Phase 2 - ActiveOrder Structure Update - End
            
        # Check for required price data
        if order.entry_price is None:
            return False
            
        # Check if this specific order is already active
        order_key = f"{order.symbol}_{order.action.value}_{order.entry_price}_{order.stop_loss}"
        
        # Phase 2 - ActiveOrder Structure Update - Begin
        for active_order in self.active_orders.values():
            if not active_order.is_working():
                continue
            active_order_obj = active_order.planned_order
            active_key = f"{active_order_obj.symbol}_{active_order_obj.action.value}_{active_order_obj.entry_price}_{active_order_obj.stop_loss}"
            if order_key == active_key:
                print(f"⚠️  Order already active: {order.symbol} {order.action.value} @ {order.entry_price}")
                return False
        # Phase 2 - ActiveOrder Structure Update - End
                
        return True
    
    # Phase 2 - Order Scoring Helper - Begin
    def _calculate_order_score(self, order: PlannedOrder, fill_probability: float) -> float:
        """
        Calculate a combined score for order prioritization.
        Uses: score = priority * fill_probability
        """
        return order.priority * fill_probability
    # Phase 2 - Order Scoring Helper - End

        # Phase 2 - Eligible Orders Method - Begin
    def _get_eligible_orders(self) -> List[Dict]:
        """
        Find all eligible orders that meet execution criteria, sorted by score.
        Returns: List of dicts with order, fill_probability, and score
        """
        eligible_orders = []
        
        for order in self.planned_orders:
            # Skip orders that can't be placed due to basic constraints
            if not self._can_place_order(order):
                continue
            
            # Check intelligent execution criteria
            should_execute, fill_prob = self.probability_engine.should_execute_order(order)
            
            if should_execute:
                score = self._calculate_order_score(order, fill_prob)
                eligible_orders.append({
                    'order': order,
                    'fill_probability': fill_prob,
                    'score': score,
                    'timestamp': datetime.datetime.now()
                })
        
        # Sort by score descending (highest score first)
        eligible_orders.sort(key=lambda x: x['score'], reverse=True)
        return eligible_orders
    # Phase 2 - Eligible Orders Method - End

    # Phase 2 - Committed Capital Method - Begin
    def _get_committed_capital(self) -> float:
        """
        Calculate total capital currently committed to working orders.
        """
        committed_capital = 0.0
        
        for active_order in self.active_orders.values():
            if active_order.is_working():
                committed_capital += active_order.capital_commitment
                
        return committed_capital
    # Phase 2 - Committed Capital Method - End

    # Phase 2 - Worst Active Order Method - Begin
    def _find_worst_active_order(self, min_score_threshold: float = 0.0) -> Optional[ActiveOrder]:
        """
        Find the worst active order that is stale and eligible for replacement.
        Returns: The ActiveOrder with lowest score that's stale, or None
        """
        worst_order = None
        worst_score = float('inf')
        current_time = datetime.datetime.now()
        
        for active_order in self.active_orders.values():
            if not active_order.is_working():
                continue
                
            # Check if order is stale (>30 minutes)
            age_minutes = (current_time - active_order.timestamp).total_seconds() / 60
            if age_minutes < 30:
                continue
                
            # Calculate current score for this active order
            current_score = self._calculate_order_score(active_order.planned_order, active_order.fill_probability)
            
            # Apply minimum score threshold (for replacement logic)
            if current_score < min_score_threshold:
                continue
                
            # Find the order with the lowest score
            if current_score < worst_score:
                worst_score = current_score
                worst_order = active_order
        
        return worst_order
    # Phase 2 - Worst Active Order Method - End