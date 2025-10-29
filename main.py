import time
import sys
import argparse
import datetime

from src.trading.orders.planned_order import (
    Action,
    OrderType,
    PositionStrategy,
    SecurityType,
    PlannedOrderManager,
)
from src.brokers.ibkr.ibkr_client import IbkrClient
from src.trading.execution.trading_manager import TradingManager
from src.market_data.feeds.ibkr_data_feed import IBKRDataFeed
from src.core.database import init_database
# <Event Bus Integration - Begin>
from src.core.event_bus import EventBus
# <Event Bus Integration - End>
# <Context-Aware Logger Integration - Begin>
from src.core.context_aware_logger import get_context_logger, start_trading_session, end_trading_session, TradingEventType
# <Context-Aware Logger Integration - End>
        
def main():
    # <Session Management - Begin>
    session_file = None
    trading_mgr = None
    ibkr_client = None
    # <Session Management - End>
    
    # <Context-Aware Logger Initialization - Begin>
    context_logger = get_context_logger()
    # <Context-Aware Logger Initialization - End>
    
    try:
        # <Session Management - Begin>
        # Start session explicitly at the beginning of main execution
        session_file = start_trading_session()
        print(f"📝 Trading session started: {session_file}")
        print(f"⏰ Session start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        # <Session Management - End>
        
        # <System Startup Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Trading system starting",
            context_provider={
                "start_time": lambda: datetime.datetime.now().isoformat(),
                "session_file": lambda: session_file
            }
        )
        # <System Startup Logging - End>

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
        parser.add_argument(
            "--import-excel",
            action="store_true",
            help="Load planned orders from Excel template (default: load from database only)",
        )

        args = parser.parse_args()

        # <Argument Parsing Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Command line arguments parsed",
            context_provider={
                "mode": lambda: args.mode,
                "debug_market_data": lambda: args.debug_market_data,
            }
        )
        # <Argument Parsing Logging - End>

        # Initialize DB
        init_database()
        
        # <Database Initialization Logging - Begin>
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            "Database initialized successfully",
            context_provider={
                "operation": "init_database"
            }
        )
        # <Database Initialization Logging - End>

        # Print valid values
        print("📋 Valid Security Types:", [st.value for st in SecurityType])
        print("📋 Valid Actions:", [a.value for a in Action])
        print("📋 Valid Order Types:", [ot.value for ot in OrderType])
        print("📋 Valid Position Strategies:", [ps.value for ps in PositionStrategy])

        # Load planned orders
        # Order Loading Logic - Begin (UPDATED)
        # Load planned orders conditionally based on --import-excel flag
        planned_orders = []
        
        if args.import_excel:
            try:
                planned_orders = PlannedOrderManager.from_excel("plan.xlsx")
                print(f"✅ Loaded {len(planned_orders)} planned orders from Excel")
                # <Order Loading Logging - Begin>
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Planned orders loaded from Excel via CLI flag",
                    context_provider={
                        "order_count": lambda: len(planned_orders),
                        "file_path": "plan.xlsx",
                        "operation": "from_excel",
                        "source": "excel_import"
                    }
                )
                # <Order Loading Logging - End>
            except Exception as e:
                print(f"❌ Failed to load planned orders from Excel: {e}")
                # <Order Loading Error Logging - Begin>
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Failed to load planned orders from Excel via CLI flag",
                    context_provider={
                        "error": lambda: str(e),
                        "file_path": "plan.xlsx",
                        "source": "excel_import"
                    },
                    decision_reason="File may be missing or corrupted"
                )
                # <Order Loading Error Logging - End>
        else:
            print("ℹ️  Skipping Excel import (default: loading from database)")
            # <Database Loading Logging - Begin>
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Skipping Excel import - will load from database",
                context_provider={
                    "source": "database",
                    "operation": "skip_excel_import"
                }
            )
            # <Database Loading Logging - End>
        # Order Loading Logic - End

        # <Event Bus Creation - Begin>
        # Create the central event bus for system communication
        event_bus = EventBus()
        print("✅ EventBus created - enabling real-time price notifications")
        # <Event Bus Creation - End>
        
        # <Event Bus Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Event bus initialized",
            context_provider={
                "component": "EventBus"
            }
        )
        # <Event Bus Logging - End>

        # Setup IBKR client
        ibkr_client = IbkrClient()
        port = 7496 if args.mode == "live" else 7497
        print(f"🔌 Connecting to IB API at 127.0.0.1:{port} ({args.mode.upper()} mode)...")
        
        # <Connection Attempt Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Attempting IBKR connection",
            context_provider={
                "host": "127.0.0.1",
                "port": lambda: port,
                "mode": lambda: args.mode,
                "client_id": 0
            }
        )
        # <Connection Attempt Logging - End>

        if not ibkr_client.connect("127.0.0.1", port, 0):
            print("❌ Failed to connect to IB")
            # <Connection Failure Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR connection failed",
                context_provider={
                    "host": "127.0.0.1",
                    "port": lambda: port,
                    "mode": lambda: args.mode
                },
                decision_reason="Connection timeout or IB Gateway not running"
            )
            # <Connection Failure Logging - End>
            # <Session Management - Begin>
            end_trading_session()
            print("✅ Trading session ended due to connection failure")
            # <Session Management - End>
            return

        # <Connection Success Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKR connection established successfully",
            context_provider={
                "host": "127.0.0.1",
                "port": lambda: port,
                "mode": lambda: args.mode,
                "connected": lambda: ibkr_client.connected
            }
        )
        # <Connection Success Logging - End>

        # Create data feed with already-connected client
        data_feed = IBKRDataFeed(ibkr_client, event_bus)
        print("✅ IBKRDataFeed connected to EventBus for price publishing")        

        # Verify the data feed is properly initialized
        print(f"✅ Data feed status: {data_feed.is_connected()}")
        print(f"✅ IBKR client connected: {ibkr_client.connected}")
        
        # <Data Feed Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Data feed initialized",
            context_provider={
                "component": "IBKRDataFeed",
                "data_feed_connected": lambda: data_feed.is_connected(),
                "ibkr_client_connected": lambda: ibkr_client.connected,
                "event_bus_connected": True
            }
        )
        # <Data Feed Logging - End>

        # <Event-Driven Trading Manager - Begin>
        # Create TradingManager with EventBus dependency
        trading_mgr = TradingManager(
            data_feed=data_feed, 
            excel_path="plan.xlsx" if args.import_excel else None,  # Pass Excel path only when importing
            ibkr_client=ibkr_client,
            enable_advanced_features=False,  # ← CRITICAL: Set to False
            event_bus=event_bus  # Pass EventBus to TradingManager
        )
        print("✅ TradingManager connected to EventBus for price notifications")
        # <Event-Driven Trading Manager - End>

        # <Trading Manager Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Trading manager initialized",
            context_provider={
                "component": "TradingManager",
                "excel_path": "plan.xlsx",
                "event_bus_connected": True
            }
        )
        # <Trading Manager Logging - End>

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
            # <Order Registration Logging - Begin>
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Planned orders registered in trading manager",
                context_provider={
                    "registered_orders": lambda: len(trading_mgr.planned_orders),
                    "operation": "load_planned_orders"
                }
            )
            # <Order Registration Logging - End>
        except Exception as e:
            print(f"⚠️  Could not register planned orders: {e}")
            print("Continuing without planned orders...")
            # <Order Registration Error Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Failed to register planned orders in trading manager",
                context_provider={
                    "error": lambda: str(e)
                },
                decision_reason="Continue without planned orders"
            )
            # <Order Registration Error Logging - End>

        # Start monitoring with debug output
        print("\n🚀 Starting trading monitoring...")

        # Temporary debug - check system state
        print(f"🔍 Data feed connected: {data_feed.is_connected()}")
        print(f"🔍 IBKR client connected: {ibkr_client.connected}")
        print(f"🔍 Planned orders count: {len(trading_mgr.planned_orders)}")
        
        # <System State Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Pre-monitoring system state check",
            context_provider={
                "data_feed_connected": lambda: data_feed.is_connected(),
                "ibkr_client_connected": lambda: ibkr_client.connected,
                "planned_orders_count": lambda: len(trading_mgr.planned_orders),
            }
        )
        # <System State Logging - End>

        # Try to get a test price
        try:
            test_price = data_feed.get_current_price("AAPL")
            print(f"🔍 Test AAPL price: {test_price}")
            # <Market Data Test Logging - Begin>
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Market data connectivity test",
                symbol="AAPL",
                context_provider={
                    "test_symbol": "AAPL",
                    "test_price": lambda: test_price,
                    "operation": "get_current_price"
                }
            )
            # <Market Data Test Logging - End>
        except Exception as e:
            print(f"🔍 Price check failed: {e}")
            # <Market Data Test Error Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Market data test failed",
                context_provider={
                    "test_symbol": "AAPL",
                    "error": lambda: str(e)
                },
                decision_reason="Market data feed may have issues"
            )
            # <Market Data Test Error Logging - End>

        # Start monitoring with result check
        success = trading_mgr.start_monitoring(interval_seconds=30)
        if success:
            print("✅ Monitoring started successfully")
            print("📡 Now listening for market data updates...")
            print("🔔 Event-driven system ACTIVE - orders will execute on price changes")
            # <Session Management - Begin>
            print(f"📝 Session logging to: {session_file}")
            # <Session Management - End>
            # <Monitoring Start Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Trading monitoring started successfully",
                context_provider={
                    "monitoring_interval": 30,
                    "session_file": lambda: session_file,
                    "system_state": "ACTIVE"
                }
            )
            # <Monitoring Start Logging - End>
        else:
            print("❌ Failed to start monitoring - check logs above")
            print("💡 Possible issues: data feed not connected, no planned orders, or initialization failed")
            # <Monitoring Failure Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Failed to start trading monitoring",
                context_provider={
                    "monitoring_interval": 30,
                    "data_feed_connected": lambda: data_feed.is_connected(),
                    "planned_orders_count": lambda: len(trading_mgr.planned_orders)
                },
                decision_reason="System initialization failure"
            )
            # <Monitoring Failure Logging - End>
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
            
            # <Main Loop Start Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Entering main monitoring loop",
                context_provider={
                    "loop_interval": 60,
                    "system_state": "RUNNING"
                }
            )
            # <Main Loop Start Logging - End>
            
            while True:
                # <Session Management - Begin>
                # Reduced frequency for status messages since detailed logs go to session file
                print("💤 Monitoring... (Ctrl+C to stop)")
                # <Session Management - End>
                time.sleep(60)
                
        except KeyboardInterrupt:
            print("\n⏹️  Shutting down gracefully...")
            # <Graceful Shutdown Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Graceful shutdown initiated by user",
                context_provider={
                    "reason": "KeyboardInterrupt",
                    "system_state": "SHUTTING_DOWN"
                }
            )
            # <Graceful Shutdown Logging - End>
        except Exception as e:
            print(f"❌ Fatal error in monitoring loop: {e}")
            import traceback
            traceback.print_exc()
            # <Fatal Error Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Fatal error in monitoring loop",
                context_provider={
                    "error": lambda: str(e),
                    "system_state": "ERROR"
                },
                decision_reason="Unhandled exception in main loop"
            )
            # <Fatal Error Logging - End>

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        # <Global Exception Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Global fatal error in main execution",
            context_provider={
                "error": lambda: str(e),
                "system_state": "CRITICAL_ERROR"
            },
            decision_reason="Unhandled exception in main function"
        )
        # <Global Exception Logging - End>
        sys.exit(1)

    finally:
        print("🧹 Cleaning up resources...")
        # <Cleanup Start Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting system cleanup",
            context_provider={
                "system_state": "CLEANUP"
            }
        )
        # <Cleanup Start Logging - End>
        
        try:
            # <Session Management - Begin>
            # Stop trading manager first
            if trading_mgr:
                trading_mgr.stop_monitoring()  # This will also call end_trading_session() internally
                # <Trading Manager Stop Logging - Begin>
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Trading manager monitoring stopped",
                    context_provider={
                        "component": "TradingManager",
                        "operation": "stop_monitoring"
                    }
                )
                # <Trading Manager Stop Logging - End>
            else:
                # If trading manager wasn't created, end session manually
                end_trading_session()
                print("✅ Trading session ended")
                # <Session End Logging - Begin>
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Trading session ended manually",
                    context_provider={
                        "operation": "end_trading_session"
                    }
                )
                # <Session End Logging - End>
            # <Session Management - End>
            
            # Disconnect IBKR client
            if ibkr_client:
                ibkr_client.disconnect()
                # <IBKR Disconnect Logging - Begin>
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "IBKR client disconnected",
                    context_provider={
                        "component": "IbkrClient",
                        "operation": "disconnect"
                    }
                )
                # <IBKR Disconnect Logging - End>
                
            print("✅ Cleanup completed")
            # <Session Management - Begin>
            print(f"📁 Session log saved: {session_file}")
            # <Session Management - End>
            
            # <Cleanup Complete Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "System cleanup completed successfully",
                context_provider={
                    "session_file": lambda: session_file,
                    "system_state": "SHUTDOWN"
                }
            )
            # <Cleanup Complete Logging - End>
            
        except Exception as e:
            print(f"⚠️  Cleanup warning: {e}")
            # <Cleanup Error Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Warning during system cleanup",
                context_provider={
                    "error": lambda: str(e),
                    "system_state": "CLEANUP_WARNING"
                }
            )
            # <Cleanup Error Logging - End>
            # <Session Management - Begin>
            # Ensure session is ended even if cleanup fails
            end_trading_session()
            print("✅ Trading session ended (with cleanup warnings)")
            # <Session Management - End>

if __name__ == "__main__":
    main()