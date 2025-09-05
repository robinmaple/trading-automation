import datetime
from typing import List, Dict, Optional
import threading
import time
import pandas as pd
from order_executor import OrderExecutor
from planned_order import PlannedOrder, PlannedOrderManager
# REMOVED: from market_data_manager import MarketDataManager  (no longer used directly)
from probability_engine import FillProbabilityEngine
from abstract_data_feed import AbstractDataFeed

class TradingManager:
    def __init__(self, data_feed: AbstractDataFeed, excel_path: str = "plan.xlsx"):
        self.data_feed = data_feed
        self.excel_path = excel_path
        self.planned_orders: List[PlannedOrder] = []
        self.active_orders: Dict[int, Dict] = {}  # order_id -> order info
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.total_capital = 100000  # Default, should be configurable
        
        self.max_open_orders = 5
        self.execution_threshold = 0.7

        # Track market data subscriptions
        self.subscribed_symbols = set()
        self.market_data_updates = {}  # symbol -> update count
        self._initialized = False  # Track initialization state

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
        return True

    def start_monitoring(self, interval_seconds: int = 5):
        """Start monitoring with automatic initialization"""
        if not self._initialize():
            print("❌ Failed to initialize trading manager")
            return False

        """Start continuous monitoring"""
        # CHANGED: Check data_feed connection instead of executor connection
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
                # CHANGED: Removed call to _track_market_data()
                # Data tracking is now the responsibility of the data feed
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
        # CHANGED: This method is now largely obsolete.
        # The data feed implementation should handle its own status reporting.
        # Keeping the method signature for now to avoid breaking anything unexpectedly.
        print("Market data tracking is now handled by the data feed implementation.")

    def _subscribe_to_all_symbols(self):
        """Enhanced subscription with retry logic"""
        # CHANGED: self.market_data -> self.data_feed
        if not self.planned_orders:
            return
            
        print("\n" + "="*60)
        print("SUBSCRIBING TO MARKET DATA")
        print("="*60)
        
        for order in self.planned_orders:
            if order.symbol not in self.subscribed_symbols:
                try:
                    contract = order.to_ib_contract()
                    # CHANGED: Use the data_feed's subscribe method
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
        Execute a single order (simulation for now)
        """
        try:
            print(f"🎯 EXECUTING: {order.action.value} {order.symbol} {order.order_type.value} @ {order.entry_price}")
            print(f"   Quantity: {order.calculate_quantity(self.total_capital):.0f}")
            print(f"   Fill Probability: {fill_probability:.2%}")
            print(f"   Stop Loss: {order.stop_loss}, Profit Target: {order.calculate_profit_target()}")
            
            # In a real implementation, you would call:
            # self.order_executor.place_bracket_order(contract, entry_price, quantity, ...)
            
            # For now, simulate execution
            print(f"✅ SIMULATION: Order for {order.symbol} executed successfully")
            
        except Exception as e:
            print(f"❌ Failed to execute order for {order.symbol}: {e}")


    def load_planned_orders(self) -> List[PlannedOrder]:
        """Load and validate planned orders"""
        try:
            self.planned_orders = PlannedOrderManager.from_excel(self.excel_path)
            print(f"Loaded {len(self.planned_orders)} planned orders")
            return self.planned_orders
        except Exception as e:
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
            
            # NEW: Debug output
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
        # Simple validation for now - will be enhanced in Phase 2
        if len(self.active_orders) >= self.max_open_orders:
            return False
        if order.entry_price is None:
            return False
        return True

    def stop_monitoring(self):
        """Stop the monitoring loop"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        print("Monitoring stopped")