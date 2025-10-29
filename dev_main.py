# Development Entry Point for Accelerated Mock Testing - Begin
import time
import sys
import argparse
import datetime
import random
import threading
import sqlite3
import os

from src.trading.orders.planned_order import (
    Action,
    OrderType,
    PositionStrategy,
    SecurityType,
    PlannedOrderManager,
)
from src.trading.execution.trading_manager import TradingManager
from src.data_feeds.mock_feed import MockFeed
from src.core.database import init_database
from src.core.event_bus import EventBus
from src.core.context_aware_logger import get_context_logger, start_trading_session, end_trading_session, TradingEventType

# Acceleration Configuration - Hardcoded for Development
ACCELERATION_CONFIG = {
    'monitoring_interval': 2,  # 2 seconds for rapid checks
    'price_movement_speed': 0.02,  # 2% per step for fast movement
    'initial_price_deviation': 0.03,  # Start 3% away from entry
    'target_proximity_threshold': 0.005,  # Consider "at target" when within 0.5%
}

def get_db_connection():
    """Get direct database connection."""
    db_path = "trading_automation.db"
    return sqlite3.connect(db_path)

def accelerate_mock_prices(mock_feed, planned_orders):
    """Configure mock feed prices to rapidly approach entry prices."""
    accelerated_prices = {}
    
    for order in planned_orders:
        if hasattr(order, 'entry_price') and order.entry_price and hasattr(order, 'symbol'):
            # Start price slightly away from entry for realistic movement
            deviation = ACCELERATION_CONFIG['initial_price_deviation']
            direction = random.choice([-1, 1])  # Randomly above or below
            start_price = order.entry_price * (1 + direction * deviation)
            
            accelerated_prices[order.symbol] = {
                'current': start_price,
                'target': order.entry_price,
                'movement_rate': ACCELERATION_CONFIG['price_movement_speed']
            }
    
    # Apply accelerated prices to mock feed
    if hasattr(mock_feed, 'current_prices'):
        for symbol, price_info in accelerated_prices.items():
            mock_feed.current_prices[symbol] = price_info['current']
    
    return accelerated_prices

def create_direct_order_executor():
    """Create a direct order executor that completely bypasses TradingManager."""
    class DirectOrderExecutor:
        def __init__(self):
            self.execution_count = 0
            print("✅ DIRECT ORDER EXECUTOR CREATED")
        
        def execute_order(self, order, current_price=None):
            print(f"🚀 DIRECT EXECUTOR: Attempting to execute {order.symbol}")
            print(f"   Action: {order.action.value}, Entry: ${order.entry_price}, Current: ${current_price}")
            
            # Validation checks
            if not hasattr(order, 'symbol'):
                print("❌ Order missing symbol")
                return False
            
            if not hasattr(order, 'entry_price') or order.entry_price is None:
                print("❌ Order missing entry_price")
                return False
                
            if current_price is None:
                print("❌ No current price provided")
                return False
            
            # Price condition check
            if order.action == Action.BUY:
                should_execute = current_price <= order.entry_price
                condition = f"BUY: {current_price} <= {order.entry_price}"
            else:  # SELL
                should_execute = current_price >= order.entry_price  
                condition = f"SELL: {current_price} >= {order.entry_price}"
            
            print(f"   Condition: {condition} -> {should_execute}")
            
            if should_execute:
                print(f"🎯 DIRECT EXECUTOR: EXECUTING {order.symbol}")
                return self._execute_directly(order, current_price)
            else:
                print("⏭️ Price condition not met")
                return False
        
        def _execute_directly(self, order, current_price):
            """Execute order directly via database update."""
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                
                # First check if order exists and is pending
                cursor.execute('''
                    SELECT status FROM planned_orders 
                    WHERE symbol = ? AND status = 'PENDING'
                ''', (order.symbol,))
                
                result = cursor.fetchone()
                if not result:
                    print(f"❌ No pending order found for {order.symbol}")
                    conn.close()
                    return False
                
                # Update order status to EXECUTED (using correct schema)
                cursor.execute('''
                    UPDATE planned_orders 
                    SET status = 'EXECUTED', last_updated = datetime('now')
                    WHERE symbol = ? AND status = 'PENDING'
                ''', (order.symbol,))
                
                affected_rows = cursor.rowcount
                conn.commit()
                conn.close()
                
                if affected_rows > 0:
                    self.execution_count += 1
                    print(f"✅ DIRECT SUCCESS: {order.symbol} marked as EXECUTED in database")
                    print(f"   Execution #{self.execution_count} - {order.symbol} {order.action.value} @ ${current_price}")
                    return True
                else:
                    print(f"❌ DIRECT FAILED: No rows updated for {order.symbol}")
                    return False
                    
            except Exception as e:
                print(f"💥 DIRECT EXECUTION ERROR: {e}")
                import traceback
                traceback.print_exc()
                return False    
            return DirectOrderExecutor()

def create_direct_order_executor():
    """Create a direct order executor that completely bypasses TradingManager."""
    print("🎯 CREATING DIRECT ORDER EXECUTOR...")
    
    class DirectOrderExecutor:
        def __init__(self):
            self.execution_count = 0
            print("✅ DIRECT ORDER EXECUTOR CREATED - Instance initialized")
        
        def execute_order(self, order, current_price=None):
            print(f"🚀 DIRECT EXECUTOR: Attempting to execute {order.symbol}")
            print(f"   Action: {order.action.value}, Entry: ${order.entry_price}, Current: ${current_price}")
            
            # Validation checks
            if not hasattr(order, 'symbol'):
                print("❌ Order missing symbol")
                return False
            
            if not hasattr(order, 'entry_price') or order.entry_price is None:
                print("❌ Order missing entry_price")
                return False
                
            if current_price is None:
                print("❌ No current price provided")
                return False
            
            # Price condition check
            if order.action == Action.BUY:
                should_execute = current_price <= order.entry_price
                condition = f"BUY: {current_price} <= {order.entry_price}"
            else:  # SELL
                should_execute = current_price >= order.entry_price  
                condition = f"SELL: {current_price} >= {order.entry_price}"
            
            print(f"   Condition: {condition} -> {should_execute}")
            
            if should_execute:
                print(f"🎯 DIRECT EXECUTOR: EXECUTING {order.symbol}")
                return self._execute_directly(order, current_price)
            else:
                print("⏭️ Price condition not met")
                return False
        
        def _execute_directly(self, order, current_price):
            """Execute order directly via database update."""
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                
                # First check if order exists and is pending
                cursor.execute('''
                    SELECT status FROM planned_orders 
                    WHERE symbol = ? AND status = 'PENDING'
                ''', (order.symbol,))
                
                result = cursor.fetchone()
                if not result:
                    print(f"❌ No pending order found for {order.symbol}")
                    conn.close()
                    return False
                
                # SIMPLEST POSSIBLE UPDATE - just change status to EXECUTED
                cursor.execute('''
                    UPDATE planned_orders 
                    SET status = 'EXECUTED'
                    WHERE symbol = ? AND status = 'PENDING'
                ''', (order.symbol,))
                
                affected_rows = cursor.rowcount
                conn.commit()
                conn.close()
                
                if affected_rows > 0:
                    self.execution_count += 1
                    print(f"✅ DIRECT SUCCESS: {order.symbol} marked as EXECUTED in database")
                    print(f"   Execution #{self.execution_count} - {order.symbol} {order.action.value} @ ${current_price}")
                    return True
                else:
                    print(f"❌ DIRECT FAILED: No rows updated for {order.symbol}")
                    return False
                    
            except Exception as e:
                print(f"💥 DIRECT EXECUTION ERROR: {e}")
                import traceback
                traceback.print_exc()
                return False    
            
                executor = DirectOrderExecutor()
                print(f"✅ Executor instance created: {executor}")
                return executor

def start_nuclear_execution_loop(planned_orders, mock_feed):
    """Start a nuclear execution loop that completely bypasses TradingManager."""
    print("🎯 STARTING NUCLEAR EXECUTION LOOP...")
    
    # Create executor first
    executor = create_direct_order_executor()
    print(f"✅ Executor created: {executor}")
    
    execution_count = 0
    cycle_count = 0
    
    def nuclear_loop():
        nonlocal execution_count, cycle_count
        print("🔥 NUCLEAR EXECUTION: Loop started - checking every 5 seconds")
        
        while True:
            try:
                cycle_count += 1
                print(f"\n🔄 NUCLEAR EXECUTION: Cycle #{cycle_count}")
                current_executions = 0
                
                for order in planned_orders:
                    if (hasattr(order, 'symbol') and hasattr(order, 'entry_price') and 
                        hasattr(order, 'action')):
                        
                        symbol = order.symbol
                        current_price_data = mock_feed.get_current_price(symbol)
                        
                        if current_price_data and 'price' in current_price_data:
                            current_price = current_price_data['price']
                            entry_price = order.entry_price
                            
                            print(f"🔍 NUCLEAR CHECK: {symbol} - Current: ${current_price:.2f}, Entry: ${entry_price:.2f}")
                            
                            # Check execution condition
                            should_execute = False
                            if order.action == Action.BUY and current_price <= entry_price:
                                should_execute = True
                                condition = f"BUY: ${current_price:.2f} <= ${entry_price:.2f}"
                            elif order.action == Action.SELL and current_price >= entry_price:
                                should_execute = True
                                condition = f"SELL: ${current_price:.2f} >= ${entry_price:.2f}"
                            else:
                                condition = f"NO: ${current_price:.2f} {'<' if order.action == Action.BUY else '>'} ${entry_price:.2f}"
                            
                            print(f"   {condition}")
                            
                            if should_execute:
                                print(f"🚀 NUCLEAR EXECUTION: {symbol} {order.action.value} @ ${current_price:.2f}")
                                if executor:  # Check if executor exists
                                    success = executor.execute_order(order, current_price)
                                    if success:
                                        current_executions += 1
                                        execution_count += 1
                                else:
                                    print("❌ Executor not available for execution")
                
                if current_executions > 0:
                    print(f"🎉 NUCLEAR EXECUTION: {current_executions} orders executed this cycle")
                else:
                    print(f"ℹ️  NUCLEAR EXECUTION: No orders executed this cycle")
                
                # Wait before next check
                time.sleep(5)
                
            except Exception as e:
                print(f"💥 NUCLEAR EXECUTION ERROR: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(10)  # Wait longer on error
    
    # Start the nuclear execution thread
    thread = threading.Thread(target=nuclear_loop, daemon=True)
    thread.start()
    print("✅ NUCLEAR EXECUTION LOOP STARTED")
    
    return executor

def monitor_price_progress(accelerated_prices, mock_feed):
    """Monitor price progress in a separate thread."""
    def progress_loop():
        print("📊 PRICE MONITOR: Started - checking every 30 seconds")
        
        while True:
            try:
                print(f"\n📊 PRICE PROGRESS UPDATE:")
                for symbol, price_info in accelerated_prices.items():
                    current_price_data = mock_feed.get_current_price(symbol)
                    if current_price_data and 'price' in current_price_data:
                        current_val = current_price_data['price']
                        target_val = price_info['target']
                        distance_pct = abs(current_val - target_val) / target_val * 100
                        direction = "↓" if current_val < target_val else "↑"
                        status_icon = "🎯" if distance_pct < 1.0 else "📈"
                        print(f"   {status_icon} {symbol}: ${current_val:.2f} {direction} ${target_val:.2f} ({distance_pct:.1f}% away)")
                
                time.sleep(30)
                
            except Exception as e:
                print(f"💥 PRICE MONITOR ERROR: {e}")
                time.sleep(60)
    
    thread = threading.Thread(target=progress_loop, daemon=True)
    thread.start()
    print("✅ PRICE MONITOR STARTED")

def main():
    session_file = None
    
    # Initialize logging
    context_logger = get_context_logger()
    
    try:
        # Start trading session
        session_file = start_trading_session()
        print(f"🚀 DEVELOPMENT MODE - NUCLEAR EXECUTION TESTING")
        print(f"📝 Trading session started: {session_file}")
        print(f"⏰ Session start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⚡ Acceleration: 2s intervals, 2.0% price steps")
        print(f"🎯 Execution: COMPLETELY BYPASSING TradingManager")
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Development mode - nuclear execution testing starting",
            context_provider={
                "start_time": lambda: datetime.datetime.now().isoformat(),
                "session_file": lambda: session_file,
                "acceleration_config": lambda: ACCELERATION_CONFIG,
                "execution_strategy": "nuclear_direct_execution"
            }
        )

        parser = argparse.ArgumentParser(description="Development Trading System - Nuclear Execution Mode")
        parser.add_argument(
            "--import-excel",
            action="store_true",
            help="Load planned orders from Excel template",
        )
        
        parser.add_argument(
            "--excel-path",
            type=str,
            default="plan.xlsx",
            help="Path to Excel template file (default: plan.xlsx)",
        )

        args = parser.parse_args()

        # Initialize DB
        init_database()
        
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            "Database initialized for nuclear execution testing",
            context_provider={
                "operation": "init_database",
                "mode": "nuclear_execution"
            }
        )

        # Print valid values for reference
        print("📋 Valid Security Types:", [st.value for st in SecurityType])
        print("📋 Valid Actions:", [a.value for a in Action])
        print("📋 Valid Order Types:", [ot.value for ot in OrderType])
        print("📋 Valid Position Strategies:", [ps.value for ps in PositionStrategy])

        # Load planned orders DIRECTLY - no TradingManager
        planned_orders = []
        
        if args.import_excel:
            try:
                planned_orders = PlannedOrderManager.from_excel(args.excel_path)
                print(f"✅ Loaded {len(planned_orders)} planned orders from Excel: {args.excel_path}")
                
                # Debug: Show loaded orders
                for order in planned_orders:
                    if hasattr(order, 'symbol') and hasattr(order, 'entry_price'):
                        print(f"   📋 {order.symbol}: {order.action.value} @ ${order.entry_price}")
                
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Planned orders loaded from Excel for nuclear execution",
                    context_provider={
                        "order_count": lambda: len(planned_orders),
                        "file_path": lambda: args.excel_path,
                        "source": "excel_import",
                        "mode": "nuclear_execution"
                    }
                )
            except Exception as e:
                print(f"❌ Failed to load planned orders from Excel: {e}")
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Failed to load planned orders from Excel in nuclear mode",
                    context_provider={
                        "error": lambda: str(e),
                        "file_path": lambda: args.excel_path
                    },
                    decision_reason="File may be missing or corrupted"
                )
                return

        # Create mock feed DIRECTLY
        mock_feed = MockFeed(planned_orders=planned_orders)
        print("✅ MockFeed created for nuclear execution")
        
        # CONNECT THE MOCK FEED
        if not mock_feed.connect():
            print("❌ Failed to connect MockFeed")
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "MockFeed connection failed in nuclear mode",
                context_provider={
                    "component": "MockFeed"
                },
                decision_reason="MockFeed initialization error"
            )
            end_trading_session()
            return
            
        print("✅ MockFeed connected successfully")
        
        # Accelerate prices toward entry points
        accelerated_prices = accelerate_mock_prices(mock_feed, planned_orders)
        print(f"✅ Accelerated {len(accelerated_prices)} symbols toward entry prices")
        
        for symbol, price_info in accelerated_prices.items():
            print(f"   📈 {symbol}: ${price_info['current']:.2f} → ${price_info['target']:.2f}")

        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Mock feed accelerated for nuclear execution",
            context_provider={
                "accelerated_symbols_count": lambda: len(accelerated_prices),
                "acceleration_config": lambda: ACCELERATION_CONFIG,
                "mock_feed_connected": True
            }
        )

        # NUCLEAR EXECUTION SYSTEM STARTING
        print("\n" + "="*60)
        print("🎯 NUCLEAR EXECUTION SYSTEM STARTING...")
        print("="*60)
        
        # STEP 1: Start nuclear execution loop
        executor = start_nuclear_execution_loop(planned_orders, mock_feed)
        
        # STEP 2: Start price progress monitor
        monitor_price_progress(accelerated_prices, mock_feed)
        
        # STEP 3: Immediate test execution
        print("\n🎯 IMMEDIATE NUCLEAR EXECUTION TEST...")
        immediate_count = 0
        if executor:  # Check if executor exists
            for order in planned_orders:
                if (hasattr(order, 'symbol') and hasattr(order, 'entry_price') and 
                    hasattr(order, 'action')):
                    
                    symbol = order.symbol
                    current_price_data = mock_feed.get_current_price(symbol)
                    
                    if current_price_data and 'price' in current_price_data:
                        current_price = current_price_data['price']
                        entry_price = order.entry_price
                        
                        # Check if we should execute immediately
                        if (order.action == Action.BUY and current_price <= entry_price) or \
                        (order.action == Action.SELL and current_price >= entry_price):
                            print(f"🚀 IMMEDIATE EXECUTION: {symbol}")
                            success = executor.execute_order(order, current_price)
                            if success:
                                immediate_count += 1
        else:
            print("❌ Executor not available for immediate test")

        print(f"🎯 IMMEDIATE TEST RESULT: {immediate_count} orders executed immediately")

        print("✅ NUCLEAR EXECUTION SYSTEM READY")
        print("="*60)

        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Nuclear execution system initialized",
            context_provider={
                "planned_orders_count": lambda: len(planned_orders),
                "accelerated_symbols_count": lambda: len(accelerated_prices),
                "execution_strategy": "nuclear_direct_execution"
            }
        )

        # Main monitoring loop
        print(f"\n🔄 NUCLEAR TRADING SESSION ACTIVE")
        print("💡 Nuclear executor checking orders every 5 seconds")
        print("💡 Price monitor updating every 30 seconds")
        print("⏹️  Press Ctrl+C to stop\n")
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Entering nuclear monitoring loop",
            context_provider={
                "loop_interval": 10,
                "system_state": "NUCLEAR_RUNNING"
            }
        )
        
        # Simple main loop
        try:
            execution_start = datetime.datetime.now()
            
            while True:
                elapsed = (datetime.datetime.now() - execution_start).total_seconds()
                print(f"⏱️  Elapsed: {elapsed:.0f}s - Nuclear executor active...")
                time.sleep(10)
                
        except KeyboardInterrupt:
            print("\n⏹️  Stopping nuclear testing...")
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Nuclear testing stopped by user",
                context_provider={
                    "reason": "KeyboardInterrupt",
                    "system_state": "NUCLEAR_SHUTTING_DOWN"
                }
            )

    except Exception as e:
        print(f"❌ Fatal error in nuclear mode: {e}")
        import traceback
        traceback.print_exc()
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Global fatal error in nuclear execution",
            context_provider={
                "error": lambda: str(e),
                "system_state": "NUCLEAR_CRITICAL_ERROR"
            },
            decision_reason="Unhandled exception in nuclear main"
        )
        sys.exit(1)

    finally:
        print("🧹 Cleaning up nuclear resources...")
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting nuclear system cleanup",
            context_provider={
                "system_state": "NUCLEAR_CLEANUP"
            }
        )
        
        try:
            end_trading_session()
            print("✅ Nuclear session ended")
            print("✅ Nuclear cleanup completed")
            print(f"📁 Nuclear session log saved: {session_file}")
            
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Nuclear system cleanup completed",
                context_provider={
                    "session_file": lambda: session_file,
                    "system_state": "NUCLEAR_SHUTDOWN"
                }
            )
            
        except Exception as e:
            print(f"⚠️  Nuclear cleanup warning: {e}")
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Warning during nuclear cleanup",
                context_provider={
                    "error": lambda: str(e),
                    "system_state": "NUCLEAR_CLEANUP_WARNING"
                }
            )
            end_trading_session()

if __name__ == "__main__":
    main()
# Development Entry Point for Accelerated Mock Testing - End