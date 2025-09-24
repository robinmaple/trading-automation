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
from src.core.market_data_debug import MarketDataDebugger


def run_market_data_diagnostic(ibkr_client: IbkrClient, hybrid: bool = False):
    """Run a market data diagnostic test for IBKR support."""
    print("\n" + "=" * 60)
    if hybrid:
        print("RUNNING MARKET DATA DIAGNOSTIC FOR IBKR SUPPORT (Paper Mode)")
    else:
        print("RUNNING MARKET DATA DIAGNOSTIC FOR IBKR SUPPORT")
    print("=" * 60)

    debugger = MarketDataDebugger(ibkr_client)
    debugger.run_comprehensive_diagnostic(
        ["EUR", "AAPL", "TSLA", "GLD", "ES", "GBP", "NQ", "CL", "GC", "IBM"]
    )

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"ibkr_market_data_diagnostic_{timestamp}.log"
    debugger.save_diagnostic_report(report_file)

    print(f"\n✅ Diagnostic complete! Report saved to: {report_file}")
    print("📋 Please share this file with IBKR support for investigation")
    print("=" * 60 + "\n")
    input(
        "Press Enter to continue with trading, or Ctrl+C to exit and review the diagnostic..."
    )


def main():
    try:
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
            return

        # Setup IBKR client
        ibkr_client = IbkrClient()
        port = 7496 if args.mode == "live" else 7497
        print(f"Connecting to IB API at 127.0.0.1:{port}...")

        if not ibkr_client.connect("127.0.0.1", port, 0):
            print("Failed to connect to IB")
            return

        data_feed = IBKRDataFeed(ibkr_client)
        trading_mgr = TradingManager(data_feed, "plan.xlsx", ibkr_client)

        # Run diagnostic if requested
        if args.debug_market_data:
            run_market_data_diagnostic(
                ibkr_client, hybrid=(args.mode == "paper")
            )

        # Register planned orders
        try:
            trading_mgr.load_planned_orders()
        except Exception as e:
            print(f"Warning: Could not register planned orders: {e}")
            print("Continuing without planned orders...")

        # Start monitoring
        trading_mgr.start_monitoring(interval_seconds=30)

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
            ibkr_client.disconnect()
        except:
            pass


if __name__ == "__main__":
    main()
