from order_executor import OrderExecutor
from trading_manager import TradingManager
from abstract_data_feed import AbstractDataFeed
from ibkr_data_feed import IBKRDataFeed
from yfinance_historical_feed import YFinanceHistoricalFeed
from mock_feed import MockFeed
import time
import sys
import argparse

def main():
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Trading System')
        parser.add_argument('--mode', choices=['live', 'historical', 'mock'], required=True,
                          help='Select data feed mode: live (IBKR), historical (yfinance), or mock (generated data)')
        
        # Add historical mode specific options
        parser.add_argument('--start-date', help='Start date for historical data (YYYY-MM-DD)')
        parser.add_argument('--end-date', help='End date for historical data (YYYY-MM-DD)')
        parser.add_argument('--interval', choices=['1m', '5m', '15m', '30m', '1h', '1d'],
                          default='1m', help='Data interval for historical mode')
        # Add mock mode specific options
        parser.add_argument('--anchor-price', required='--mode mock' in sys.argv,
                          help='Starting price for mock data feed (e.g., "EUR=1.095")')
        # NEW: Add trend argument for mock mode
        parser.add_argument('--mock-trend', choices=['up', 'down', 'random'], default='random',
                          help='Global price trend direction for mock data (default: random)')
        
        args = parser.parse_args()
        
        # Initialize based on mode
        if args.mode == 'live':
            # Initialize IB-connected components
            executor = OrderExecutor()
            data_feed = IBKRDataFeed(executor)
            trading_mgr = TradingManager(data_feed, "plan.xlsx")
            
            # Connect to IB
            if not executor.connect_to_ib('127.0.0.1', 7497, 0):
                print("Failed to connect to IB")
                return
        
            if not executor.wait_for_connection(timeout=10):
                print("Connection timeout")
                return
                
        elif args.mode == 'historical':
            # Initialize historical/replay components with configurable parameters
            data_feed = YFinanceHistoricalFeed(
                start_date=args.start_date,
                end_date=args.end_date,
                interval=args.interval
            )
            trading_mgr = TradingManager(data_feed, "plan.xlsx")
            data_feed.connect()  # Load historical data (replaces IB connection)
        
        else:  # args.mode == 'mock'
            # Initialize mock data feed with anchor price and trend
            data_feed = MockFeed(args.anchor_price, args.mock_trend)  # MODIFIED: Added trend argument
            trading_mgr = TradingManager(data_feed, "plan.xlsx")
            data_feed.connect()  # Initialize the mock data generator
        
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
        trading_mgr.start_monitoring(interval_seconds=5)
        
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
            if args.mode == 'live':  # Only disconnect if we connected to IB
                executor.disconnect()
        except:
            pass

if __name__ == "__main__":
    main()