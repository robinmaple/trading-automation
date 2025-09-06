import datetime
from typing import List, Dict, Optional
import threading
import time
import pandas as pd
from src.core.ibkr_client import IbkrClient
from src.core.planned_order import PlannedOrder, PlannedOrderManager
from src.core.probability_engine import FillProbabilityEngine
from src.core.abstract_data_feed import AbstractDataFeed

# ==================== DATABASE INTEGRATION - BEGIN ====================
from src.core.database import get_db_session
from src.core.models import PlannedOrderDB, PositionStrategy
# ==================== DATABASE INTEGRATION - END ====================

class TradingManager:
    def __init__(self, data_feed: AbstractDataFeed, excel_path: str = "plan.xlsx", ibkr_client: Optional[IbkrClient] = None):
        self.data_feed = data_feed
        self.excel_path = excel_path
        self.ibkr_client = ibkr_client
        self.planned_orders: List[PlannedOrder] = []
        self.active_orders: Dict[int, Dict] = {}
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

    def _initialize(self):
        """Internal initialization - called automatically when needed"""
        if self._initialized:
            return True
            
        if not self.data_feed.is_connected():
            print("Cannot initialize - data feed not connected")
            return False
            
        self.probability_engine = FillProbabilityEngine(self.data_feed)
        
        self._initialized = True
        print("✅ Trading manager initialized")
        
        if self.ibkr_client and self.ibkr_client.connected:
            print("✅ Real order execution enabled (IBKR connected)")
        elif self.ibkr_client:
            print("⚠️  Order executor provided but not connected to IBKR - will use simulation")
        else:
            print("ℹ️  No order executor provided - using simulation mode")
            
        return True
    
    def start_monitoring(self, interval_seconds: int = 5):
        """Start monitoring with automatic initialization"""
        if not self._initialize():
            print("❌ Failed to initialize trading manager")
            return False

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
            
            if current_price:
                # Calculate basic metrics for display
                price_diff = current_price - order.entry_price if order.entry_price else 0
                percent_diff = (price_diff / order.entry_price * 100) if order.entry_price else 0
                
                print(f"{i+1:2}. {order.action.value:4} {order.symbol:6} | "
                      f"Current: ${current_price:8.4f} | Entry: ${order.entry_price:8.4f} | "
                      f"Diff: {percent_diff:6.2f}%")
            else:
                print(f"{i+1:2}. {order.action.value:4} {order.symbol:6} | "
                      f"No market data yet | Entry: ${order.entry_price:8.4f}")
        
        executable_orders = self._find_executable_orders()
        if executable_orders:
            print(f"🎯 Found {len(executable_orders)} executable orders")
            for executable in executable_orders:
                order = executable['order']
                fill_prob = executable['fill_probability']
                self._execute_order(order, fill_prob)        
        else:
            print("💡 No executable orders found at this time")
        
        print("-" * 50)

    def _execute_order(self, order, fill_probability):
        """
        Execute a single order - now with database persistence
        """
        try:
            # Get actual account value from IBKR
            if self.ibkr_client and self.ibkr_client.connected:
                total_capital = self.ibkr_client.get_account_value()
            else:
                total_capital = self.total_capital
            
            print(f"🎯 EXECUTING: {order.action.value} {order.symbol} {order.order_type.value} @ {order.entry_price}")
            print(f"   Account Value: ${total_capital:,.2f}")
            print(f"   Fill Probability: {fill_probability:.2%}")
            print(f"   Stop Loss: {order.stop_loss}, Profit Target: {order.calculate_profit_target()}")
            
            # Real order execution if executor is available and connected
            if self.ibkr_client and self.ibkr_client.connected:
                
                contract = order.to_ib_contract()
                
                # Place real order through IBKR
                order_ids = self.ibkr_client.place_bracket_order(
                    contract,
                    order.action.value,
                    order.order_type.value,
                    order.security_type.value,
                    order.entry_price,
                    order.stop_loss,
                    order.risk_per_trade,
                    order.risk_reward_ratio
                )
                
                if order_ids:
                    print(f"✅ REAL ORDER PLACED: Order IDs {order_ids} sent to IBKR")
                    
                    # ==================== DATABASE INTEGRATION - BEGIN ====================
                    # Update order status to LIVE in database
                    self._update_order_status(order, 'LIVE', order_ids)
                    # ==================== DATABASE INTEGRATION - END ====================
                    
                    # Track the active order
                    self.active_orders[order_ids[0]] = {
                        'order': order,
                        'order_ids': order_ids,
                        'timestamp': datetime.datetime.now(),
                        'status': 'Submitted'
                    }
                else:
                    print("❌ Failed to place real order through IBKR")
                    
            else:
                # Fall back to simulation
                print(f"✅ SIMULATION: Order for {order.symbol} executed successfully")
                
                # ==================== DATABASE INTEGRATION - BEGIN ====================
                # Mark as FILLED in database for simulation
                self._update_order_status(order, 'FILLED')
                # ==================== DATABASE INTEGRATION - END ====================
            
        except Exception as e:
            print(f"❌ Failed to execute order for {order.symbol}: {e}")
            import traceback
            traceback.print_exc()
            
    def load_planned_orders(self) -> List[PlannedOrder]:
        """Load and validate planned orders from Excel and persist to database"""
        try:
            # Load from Excel first
            excel_orders = PlannedOrderManager.from_excel(self.excel_path)
            print(f"Loaded {len(excel_orders)} planned orders from Excel")
            
            # ==================== DATABASE INTEGRATION - BEGIN ====================
            # Persist all orders to database
            db_orders = []
            for excel_order in excel_orders:
                db_order = self._convert_to_db_model(excel_order)
                self.db_session.add(db_order)
                db_orders.append(db_order)
            
            self.db_session.commit()
            print(f"✅ Persisted {len(db_orders)} orders to database")
            # ==================== DATABASE INTEGRATION - END ====================
            
            # Keep in memory for processing
            self.planned_orders = excel_orders
            return excel_orders
            
        except Exception as e:
            # ==================== DATABASE INTEGRATION - BEGIN ====================
            self.db_session.rollback()
            # ==================== DATABASE INTEGRATION - END ====================
            print(f"Error loading planned orders: {e}")
            return []

    def _find_executable_orders(self):
        """Find orders that meet execution criteria"""
        executable = []
        
        for order in self.planned_orders:
            # Check basic constraints
            if not self._can_place_order(order):
                continue
            
            # Check intelligent execution criteria
            should_execute, fill_prob = self.probability_engine.should_execute_order(order)
            
            print(f"   Checking {order.action.value} {order.symbol}: should_execute={should_execute}, fill_prob={fill_prob:.3f}")

            if should_execute:
                executable.append({
                    'order': order,
                    'fill_probability': fill_prob,
                    'timestamp': datetime.datetime.now()
                })
        
        return executable

    def _can_place_order(self, order):
        """Basic validation for order placement"""
        # Check if we already have max open orders
        if len(self.active_orders) >= self.max_open_orders:
            return False
            
        # Check for required price data
        if order.entry_price is None:
            return False
            
        # Check if this specific order is already active
        order_key = f"{order.symbol}_{order.action.value}_{order.entry_price}_{order.stop_loss}"
        
        for active_order in self.active_orders.values():
            active_order_obj = active_order['order']
            active_key = f"{active_order_obj.symbol}_{active_order_obj.action.value}_{active_order_obj.entry_price}_{active_order_obj.stop_loss}"
            if order_key == active_key:
                print(f"⚠️  Order already active: {order.symbol} {order.action.value} @ {order.entry_price}")
                return False
                
        return True
    
    def stop_monitoring(self):
        """Stop the monitoring loop and clean up database connection"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        
        # ==================== DATABASE INTEGRATION - BEGIN ====================
        # Close database session on shutdown
        if self.db_session:
            self.db_session.close()
        # ==================== DATABASE INTEGRATION - END ====================
        
        print("Monitoring stopped")

    # ==================== DATABASE INTEGRATION - BEGIN ====================
    # New methods for database operations
    
    def _convert_to_db_model(self, planned_order: PlannedOrder) -> PlannedOrderDB:
        """Convert PlannedOrder to PlannedOrderDB for database persistence"""
        # Find position strategy in database
        position_strategy = self.db_session.query(PositionStrategy).filter_by(
            name=planned_order.position_strategy.value
        ).first()
        
        if not position_strategy:
            raise ValueError(f"Position strategy {planned_order.position_strategy.value} not found in database")
        
        return PlannedOrderDB(
            symbol=planned_order.symbol,
            security_type=planned_order.security_type.value,
            action=planned_order.action.value,
            order_type=planned_order.order_type.value,
            entry_price=planned_order.entry_price,
            stop_loss=planned_order.stop_loss,
            risk_per_trade=planned_order.risk_per_trade,
            risk_reward_ratio=planned_order.risk_reward_ratio,
            position_strategy_id=position_strategy.id,
            status='PENDING'
        )

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
    # ==================== DATABASE INTEGRATION - END ====================