import time
import sys
import argparse
import datetime

from src.core.planned_order import (
    Action,
    OrderType,
    PositionStrategy,
    SecurityType,
    PlannedOrderManager,
)
from src.core.ibkr_client import IbkrClient
from src.core.trading_manager import TradingManager
from src.data_feeds.ibkr_data_feed import IBKRDataFeed
from src.core.database import init_database
# <Event Bus Integration - Begin>
from src.core.event_bus import EventBus
# <Event Bus Integration - End>
# <Session Management Integration - Begin>
from src.core.simple_logger import start_trading_session, end_trading_session, get_current_session_file
# <Session Management Integration - End>

def run_scanner_from_main(ibkr_client, data_feed, config=None):
    try:
        from src.scanner.scan_manager import ScanManager
        from config.scanner_config import ScannerConfig
        from src.scanner.integration.ibkr_data_adapter import IBKRDataAdapter
        
        print("🚀 Starting Tiered Scanner with REAL Market Data...")
        print("=" * 50)
        
        # TEST: Try Tier 1 only mode first
        print("🧪 TESTING: Tier 1 Only Mode (no strategies)")
        scanner_config_tier1 = ScannerConfig(
            enabled_strategies=[],  # Empty = Tier 1 only
            min_confidence_score=60,
            max_candidates=25
        )
        
        data_adapter = IBKRDataAdapter(data_feed)
        scan_manager_tier1 = ScanManager(data_adapter, scanner_config_tier1)
        
        tier1_results = scan_manager_tier1.generate_all_candidates()
        print(f"📋 TIER 1 RESULTS: {len(tier1_results)} candidates")
        
        if tier1_results:
            filepath = scan_manager_tier1.tiered_scanner.save_results_to_excel(tier1_results)
            print(f"💾 Tier 1 results saved to: {filepath}")
            
            # Show Tier 1 candidates
            print("\n📊 Tier 1 Candidates:")
            for i, candidate in enumerate(tier1_results[:10]):
                print(f"   {i+1}. {candidate['symbol']} - Price: ${candidate['current_price']:.2f}")
        
        # Now try with strategy
        print("\n🧪 TESTING: With Bull Trend Pullback Strategy")
        scanner_config = ScannerConfig(
            enabled_strategies=['bull_trend_pullback'],
            min_confidence_score=60,
            max_candidates=25
        )
        
        scan_manager = ScanManager(data_adapter, scanner_config)
        candidates = scan_manager.generate_all_candidates()
        
        if candidates:
            filepath = scan_manager.tiered_scanner.save_results_to_excel(candidates)
            print(f"💾 Strategy results saved to: {filepath}")
            print(f"✅ Found {len(candidates)} strategy candidates")
        else:
            print("⚠️  No strategy candidates found - strategy is being selective")
            
        return candidates if candidates else tier1_results
            
    except Exception as e:
        print(f"❌ Scanner failed: {e}")
        import traceback
        traceback.print_exc()
        return []
        
def main():
    # <Session Management - Begin>
    session_file = None
    trading_mgr = None
    ibkr_client = None
    # <Session Management - End>
    
    try:
        # <Session Management - Begin>
        # Start session explicitly at the beginning of main execution
        session_file = start_trading_session()
        print(f"📝 Trading session started: {session_file}")
        print(f"⏰ Session start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        # <Session Management - End>

        parser = argparse.ArgumentParser(description="Trading System")
        parser.add_argument(
            "--mode",
            choices=["live", "paper"],
            required=True,
            help="Select connection mode: live (port 7496) or paper (port 7497)",
        )
        parser.add_argument(
            "--debug-market-data",
            action="store_true",
            help="Run market data diagnostic before starting trading",
        )
        parser.add_argument('--scanner', action='store_true', 
                   help='Run bull trend pullback scanner')
        parser.add_argument('--scanner-only', action='store_true',
                        help='Run scanner and exit (no trading)')

        args = parser.parse_args()

        # Initialize DB
        init_database()

        # Print valid values
        print("📋 Valid Security Types:", [st.value for st in SecurityType])
        print("📋 Valid Actions:", [a.value for a in Action])
        print("📋 Valid Order Types:", [ot.value for ot in OrderType])
        print("📋 Valid Position Strategies:", [ps.value for ps in PositionStrategy])

        # Load planned orders
        try:
            planned_orders = PlannedOrderManager.from_excel("plan.xlsx")
            print(f"✅ Loaded {len(planned_orders)} planned orders")
        except Exception as e:
            print(f"❌ Failed to load planned orders: {e}")
            # Don't return yet - scanner might still work without planned orders

        # <Event Bus Creation - Begin>
        # Create the central event bus for system communication
        event_bus = EventBus()
        print("✅ EventBus created - enabling real-time price notifications")
        # <Event Bus Creation - End>

        # Setup IBKR client
        ibkr_client = IbkrClient()
        port = 7496 if args.mode == "live" else 7497
        print(f"🔌 Connecting to IB API at 127.0.0.1:{port} ({args.mode.upper()} mode)...")

        if not ibkr_client.connect("127.0.0.1", port, 0):
            print("❌ Failed to connect to IB")
            # <Session Management - Begin>
            end_trading_session()
            print("✅ Trading session ended due to connection failure")
            # <Session Management - End>
            return

        # Create data feed with already-connected client
        data_feed = IBKRDataFeed(ibkr_client, event_bus)
        print("✅ IBKRDataFeed connected to EventBus for price publishing")        

        # Verify the data feed is properly initialized
        print(f"✅ Data feed status: {data_feed.is_connected()}")
        print(f"✅ IBKR client connected: {ibkr_client.connected}")

        # SCANNER INTEGRATION - Run before normal trading
        scanner_candidates = None
        if args.scanner or args.scanner_only:
            print("\n" + "="*60)
            print("🔍 SCANNER MODE ACTIVATED")
            print("="*60)
            
            scanner_candidates = run_scanner_from_main(ibkr_client, data_feed)
            
            if args.scanner_only:
                print("\n🎯 Scanner-only mode complete. Exiting.")
                print("💡 Use --scanner (without --scanner-only) to run scanner + trading")
                # <Session Management - Begin>
                end_trading_session()
                print("✅ Trading session ended after scanner-only execution")
                # <Session Management - End>
                return  # Exit after scanner if scanner-only mode
            
            # If --scanner (without --scanner-only), continue to normal trading
            print("\n📈 Continuing to normal trading with scanner results...")
            print("="*60)

        # Exit here if we only wanted to run the scanner
        if args.scanner_only:
            # <Session Management - Begin>
            end_trading_session()
            print("✅ Trading session ended after scanner-only execution")
            # <Session Management - End>
            return

        # <Event-Driven Trading Manager - Begin>
        # Create TradingManager with EventBus dependency
        trading_mgr = TradingManager(
            data_feed=data_feed, 
            excel_path="plan.xlsx", 
            ibkr_client=ibkr_client,
            event_bus=event_bus  # Pass EventBus to TradingManager
        )
        print("✅ TradingManager connected to EventBus for price notifications")
        # <Event-Driven Trading Manager - End>

        # <Event-Driven Market Data Manager - Begin>
        # Get the MarketDataManager from data feed and connect it to EventBus
        if hasattr(data_feed, 'market_data_manager') and data_feed.market_data_manager:
            data_feed.market_data_manager.event_bus = event_bus
            print("✅ MarketDataManager connected to EventBus for price publishing")
        else:
            print("⚠️  MarketDataManager not found in data feed - event publishing may not work")
        # <Event-Driven Market Data Manager - End>

        # Register planned orders
        try:
            trading_mgr.load_planned_orders()
            print(f"✅ Registered {len(trading_mgr.planned_orders)} planned orders")
        except Exception as e:
            print(f"⚠️  Could not register planned orders: {e}")
            print("Continuing without planned orders...")

        # If we have scanner candidates, you could integrate them here
        if scanner_candidates:
            print(f"💡 Scanner provided {len(scanner_candidates)} candidates for trading consideration")
            # You could add logic here to use scanner candidates in your trading strategy
            # For example: trading_mgr.integrate_scanner_candidates(scanner_candidates)

        # Start monitoring with debug output
        print("\n🚀 Starting trading monitoring...")

        # Temporary debug - check system state
        print(f"🔍 Data feed connected: {data_feed.is_connected()}")
        print(f"🔍 IBKR client connected: {ibkr_client.connected}")
        print(f"🔍 Planned orders count: {len(trading_mgr.planned_orders)}")

        # Try to get a test price
        try:
            test_price = data_feed.get_current_price("AAPL")
            print(f"🔍 Test AAPL price: {test_price}")
        except Exception as e:
            print(f"🔍 Price check failed: {e}")

        # Start monitoring with result check
        success = trading_mgr.start_monitoring(interval_seconds=30)
        if success:
            print("✅ Monitoring started successfully")
            print("📡 Now listening for market data updates...")
            print("🔔 Event-driven system ACTIVE - orders will execute on price changes")
            # <Session Management - Begin>
            print(f"📝 Session logging to: {session_file}")
            # <Session Management - End>
        else:
            print("❌ Failed to start monitoring - check logs above")
            print("💡 Possible issues: data feed not connected, no planned orders, or initialization failed")
            # <Session Management - Begin>
            end_trading_session()
            print("✅ Trading session ended due to monitoring failure")
            # <Session Management - End>
            return  # Exit if monitoring failed

        try:
            # <Session Management - Begin>
            print(f"🔄 Trading session ACTIVE - Monitoring every 30 seconds...")
            print(f"📊 Session logs being written to: {session_file}")
            # <Session Management - End>
            
            while True:
                # <Session Management - Begin>
                # Reduced frequency for status messages since detailed logs go to session file
                print("💤 Monitoring... (Ctrl+C to stop)")
                # <Session Management - End>
                time.sleep(60)
                
        except KeyboardInterrupt:
            print("\n⏹️  Shutting down gracefully...")
        except Exception as e:
            print(f"❌ Fatal error in monitoring loop: {e}")
            import traceback
            traceback.print_exc()

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        print("🧹 Cleaning up resources...")
        try:
            # <Session Management - Begin>
            # Stop trading manager first
            if trading_mgr:
                trading_mgr.stop_monitoring()  # This will also call end_trading_session() internally
            else:
                # If trading manager wasn't created, end session manually
                end_trading_session()
                print("✅ Trading session ended")
            # <Session Management - End>
            
            # Disconnect IBKR client
            if ibkr_client:
                ibkr_client.disconnect()
                
            print("✅ Cleanup completed")
            # <Session Management - Begin>
            print(f"📁 Session log saved: {session_file}")
            # <Session Management - End>
            
        except Exception as e:
            print(f"⚠️  Cleanup warning: {e}")
            # <Session Management - Begin>
            # Ensure session is ended even if cleanup fails
            end_trading_session()
            print("✅ Trading session ended (with cleanup warnings)")
            # <Session Management - End>

if __name__ == "__main__":
    main()