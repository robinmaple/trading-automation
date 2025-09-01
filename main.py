# main.py
from order_executor import OrderExecutor
from trading_manager import TradingManager
import time
import sys

def main():
    try:
        # Initialize
        executor = OrderExecutor()
        trading_mgr = TradingManager(executor, "plan.xlsx")
        
        # Connect to IB
        if not executor.connect_to_ib('127.0.0.1', 7497, 0):
            print("Failed to connect to IB")
            return
        
        if not executor.wait_for_connection(timeout=10):
            print("Connection timeout")
            return
        
        # Load planned orders
        try:
            # Display valid values for debugging
            from planned_order import PlannedOrderManager
            PlannedOrderManager.display_valid_values()

            trading_mgr.load_planned_orders()
        except Exception as e:
            print(f"Warning: Could not load planned orders: {e}")
            print("Continuing without planned orders...")
        
        # Start monitoring
        trading_mgr.start_monitoring(interval_seconds=30)
        
        # Keep running
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print("\nShutting down...")
            
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        try:
            trading_mgr.stop_monitoring()
            executor.disconnect()
        except:
            pass

if __name__ == "__main__":
    main()