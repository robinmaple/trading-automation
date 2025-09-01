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
        """Main monitoring loop"""
        while self.monitoring:
            try:
                # self._check_and_execute_orders()
                time.sleep(interval_seconds)
            except Exception as e:
                print(f"Monitoring error: {e}")
                time.sleep(60)  # Wait longer on error