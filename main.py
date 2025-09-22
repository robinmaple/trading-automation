from src.core.planned_order import Action, OrderType, PositionStrategy, SecurityType
from src.core.ibkr_client import IbkrClient
from src.core.trading_manager import TradingManager
from src.core.abstract_data_feed import AbstractDataFeed
from src.data_feeds.ibkr_data_feed import IBKRDataFeed
from src.data_feeds.yfinance_historical_feed import YFinanceHistoricalFeed
from src.data_feeds.mock_feed import MockFeed
# Database Initialization Import - 2025-09-07 19:36 - Begin
from src.core.database import init_database
# Database Initialization Import - 2025-09-07 19:36 - End

# Market Data Debug Tool Import - Begin
from src.core.market_data_debug import MarketDataDebugger
import datetime
# Market Data Debug Tool Import - End

import time
import sys
import argparse

def main():
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Trading System')
        parser.add_argument('--mode', choices=['live', 'historical', 'mock', 'hybrid'], required=True,
                          help='Select data feed mode: live (IBKR), historical (yfinance), mock (generated data), or hybrid (mock data + real IBKR orders)')
        
        # Add historical mode specific options
        parser.add_argument('--start-date', help='Start date for historical data (YYYY-MM-DD)')
        parser.add_argument('--end-date', help='End date for historical data (YYYY-MM-DD)')
        parser.add_argument('--interval', choices=['1m', '5m', '15m', '30m', '1h', '1d'],
                          default='1m', help='Data interval for historical mode')
        
        # Add debug option
        parser.add_argument('--debug-market-data', action='store_true',
                          help='Run market data diagnostic before starting trading')
        
        # Phase 2 - Remove Mock Command-line Parameters - 2025-09-07 13:26 - Begin
        # Removed: --anchor-price and --mock-trend parameters
        # Phase 2 - Remove Mock Command-line Parameters - 2025-09-07 13:26 - End        

        args = parser.parse_args()

        # Database Initialization Call - 2025-09-07 19:36 - Begin
        init_database()  # Initialize the database before creating any managers
        # Database Initialization Call - 2025-09-07 19:36 - End
                
        # Load planned orders first to get mock configuration
        # Phase 2 - Load Orders Before Feed Initialization - 2025-09-07 13:26 - Begin
        from src.core.planned_order import PlannedOrderManager

        # Replace with this if you want to display valid values:
        print("📋 Valid Security Types:", [st.value for st in SecurityType])
        print("📋 Valid Actions:", [a.value for a in Action])
        print("📋 Valid Order Types:", [ot.value for ot in OrderType])
        print("📋 Valid Position Strategies:", [ps.value for ps in PositionStrategy])
        # Remove or replace the invalid method call - End

        planned_orders = []
        try:
            planned_orders = PlannedOrderManager.from_excel("plan.xlsx")
            print(f"✅ Loaded {len(planned_orders)} planned orders")
        except Exception as e:
            print(f"❌ Failed to load planned orders: {e}")
            if args.mode in ['mock', 'hybrid']:
                print("❌ Mock/hybrid mode requires valid planned orders with mock configuration")
                return
        # Phase 2 - Load Orders Before Feed Initialization - 2025-09-07 13:26 - End

        # Initialize based on mode
        if args.mode == 'live':
            # Initialize IB-connected components
            ibkr_client = IbkrClient()
            data_feed = IBKRDataFeed(ibkr_client.order_executor)  # Data feed still needs low-level executor
            trading_mgr = TradingManager(data_feed, "plan.xlsx", ibkr_client)  # Pass the facade to TradingManager
            
            # Connect to IB
            if not ibkr_client.connect('127.0.0.1', 7497, 0):
                print("Failed to connect to IB")
                return
            
            # ==================== MARKET DATA DIAGNOSTIC - BEGIN ====================
            if args.debug_market_data:
                print("\n" + "="*60)
                print("RUNNING MARKET DATA DIAGNOSTIC FOR IBKR SUPPORT")
                print("="*60)

                # Create and run debugger
                debugger = MarketDataDebugger(ibkr_client)
                summary = debugger.run_comprehensive_diagnostic([
                    "EUR", "AAPL", "TSLA", "GLD", "ES", 
                    "GBP", "NQ", "CL", "GC", "IBM"
                ])

                # Save detailed report
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                report_file = f"ibkr_market_data_diagnostic_{timestamp}.log"
                debugger.save_diagnostic_report(report_file)

                print(f"\n✅ Diagnostic complete! Report saved to: {report_file}")
                print("📋 Please share this file with IBKR support for investigation")
                print("="*60 + "\n")

                # Optional: Add a pause to review the diagnostic before continuing
                input("Press Enter to continue with trading, or Ctrl+C to exit and review the diagnostic...")
            # ==================== MARKET DATA DIAGNOSTIC - END ====================
                
        elif args.mode == 'historical':
            # Initialize historical/replay components with configurable parameters
            data_feed = YFinanceHistoricalFeed(
                start_date=args.start_date,
                end_date=args.end_date,
                interval=args.interval
            )
            trading_mgr = TradingManager(data_feed, "plan.xlsx")
            data_feed.connect()  # Load historical data (replaces IB connection)
        
        # Phase 2 - Updated Mock Mode Initialization - 2025-09-07 13:26 - Begin
        elif args.mode == 'mock':
            # Initialize mock data feed with configuration from planned orders
            data_feed = MockFeed(planned_orders)
            trading_mgr = TradingManager(data_feed, "plan.xlsx")
            data_feed.connect()  # Initialize the mock data generator
        
        else:  # args.mode == 'hybrid'
            # Initialize both mock data feed AND IBKR order executor
            ibkr_client = IbkrClient()
            data_feed = MockFeed(planned_orders)
            trading_mgr = TradingManager(data_feed, "plan.xlsx", ibkr_client)  # Pass the facade to TradingManager
            
            # Connect to IB for order execution
            if not ibkr_client.connect('127.0.0.1', 7497, 0):
                print("Failed to connect to IB")
                return
            
            # ==================== MARKET DATA DIAGNOSTIC - BEGIN ====================
            if args.debug_market_data:
                print("\n" + "="*60)
                print("RUNNING MARKET DATA DIAGNOSTIC FOR IBKR SUPPORT (Hybrid Mode)")
                print("="*60)

                # Create and run debugger
                debugger = MarketDataDebugger(ibkr_client)
                summary = debugger.run_comprehensive_diagnostic([
                    "EUR", "AAPL", "TSLA", "GLD", "ES", 
                    "GBP", "NQ", "CL", "GC", "IBM"
                ])

                # Save detailed report
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                report_file = f"ibkr_market_data_diagnostic_{timestamp}.log"
                debugger.save_diagnostic_report(report_file)

                print(f"\n✅ Diagnostic complete! Report saved to: {report_file}")
                print("📋 Please share this file with IBKR support for investigation")
                print("="*60 + "\n")

                # Optional: Add a pause to review the diagnostic before continuing
                input("Press Enter to continue with trading, or Ctrl+C to exit and review the diagnostic...")
            # ==================== MARKET DATA DIAGNOSTIC - END ====================
            
            # Initialize mock data feed
            data_feed.connect()
                   
        # Phase 2 - Moved Order Loading Earlier - 2025-09-07 13:26 - Begin
        # Load planned orders (already loaded above, just register with trading manager)
        try:
            trading_mgr.load_planned_orders()
        except Exception as e:
            print(f"Warning: Could not register planned orders with trading manager: {e}")
            print("Continuing without planned orders...")
        # Phase 2 - Moved Order Loading Earlier - 2025-09-07 13:26 - End
                
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
            if args.mode in ['live', 'hybrid']:
                ibkr_client.disconnect()
        except:
            pass

if __name__ == "__main__":
    main()