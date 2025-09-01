# trading_manager.py
from typing import List, Dict, Optional
import threading
import time
import pandas as pd
from order_executor import OrderExecutor
from planned_order import PlannedOrder, PlannedOrderManager

class TradingManager:
    def __init__(self, order_executor: OrderExecutor, excel_path: str = "plan.xlsx"):
        self.executor = order_executor
        self.excel_path = excel_path
        self.planned_orders: List[PlannedOrder] = []
        self.active_orders: Dict[int, Dict] = {}  # order_id -> order info
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.total_capital = 100000  # Default, should be configurable
        
    def load_planned_orders(self) -> List[PlannedOrder]:
        """Load and validate planned orders"""
        try:
            self.planned_orders = PlannedOrderManager.from_excel(self.excel_path)
            print(f"Loaded {len(self.planned_orders)} planned orders")
            return self.planned_orders
        except Exception as e:
            print(f"Error loading planned orders: {e}")
            return []
    
    def start_monitoring(self, interval_seconds: int = 30):
        """Start continuous monitoring"""
        if not self.executor.connected:
            raise Exception("Not connected to IB")
            
        self.monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(interval_seconds,),
            daemon=True
        )
        self.monitor_thread.start()
        print("Monitoring started")
    
    def _monitoring_loop(self, interval_seconds: int):
        """Main monitoring loop with better error handling"""
        error_count = 0
        max_errors = 10
        
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
            
    def _check_and_execute_orders(self):
        """Check market conditions and execute orders if conditions are met"""
        if not self.planned_orders:
            print("No planned orders to monitor")
            return
            
        print(f"Monitoring {len(self.planned_orders)} planned orders...")
        
        # For now, just log that we're monitoring
        # In Phase 2, this will contain the actual market data checking and execution logic
        for i, order in enumerate(self.planned_orders):
            print(f"Order {i+1}: {order.action.value} {order.symbol} @ {order.entry_price}")
            
        # TODO: Phase 2 - Implement market data subscription and execution logic
        # This will involve:
        # 1. Subscribing to market data for each symbol
        # 2. Calculating fill probability
        # 3. Executing orders when conditions are met
        
    def stop_monitoring(self):
        """Stop the monitoring loop"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        print("Monitoring stopped")