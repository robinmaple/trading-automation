"""
The main orchestration engine for the automated trading system.
Manages the entire trading workflow: loading orders, market data subscription,
continuous monitoring, order execution, and active order management.
Coordinates between data feeds, the IBKR client, service layer, and database.
"""

import datetime
from typing import List, Dict, Optional
import threading
import time
import pandas as pd

from services.prioritization_service import PrioritizationService
from src.core.ibkr_client import IbkrClient
from src.core.planned_order import PlannedOrder, ActiveOrder
from src.core.probability_engine import FillProbabilityEngine
from src.core.abstract_data_feed import AbstractDataFeed
from src.core.database import get_db_session
from src.core.models import PlannedOrderDB
from src.services.order_eligibility_service import OrderEligibilityService
from src.services.order_execution_service import OrderExecutionService
from src.services.position_sizing_service import PositionSizingService
from src.services.order_loading_service import OrderLoadingService
from src.services.order_persistence_service import OrderPersistenceService
from src.services.state_service import StateService
from src.core.events import OrderEvent
from src.core.reconciliation_engine import ReconciliationEngine
from src.services.market_hours_service import MarketHoursService
# Phase B Additions - Begin
from src.services.prioritization_service import PrioritizationService
from src.services.outcome_labeling_service import OutcomeLabelingService
# Phase B Additions - End

class TradingManager:
    """Orchestrates the complete trading lifecycle and manages system state."""

    def __init__(self, data_feed: AbstractDataFeed, excel_path: str = "plan.xlsx",
                 ibkr_client: Optional[IbkrClient] = None,
                 order_persistence_service: Optional[OrderPersistenceService] = None):
        """Initialize the trading manager with all necessary dependencies and services."""
        self.data_feed = data_feed
        self.excel_path = excel_path
        self.ibkr_client = ibkr_client
        self.planned_orders: List[PlannedOrder] = []
        self.active_orders: Dict[int, ActiveOrder] = {}
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.total_capital = 100000
        self.max_open_orders = 5
        self.execution_threshold = 0.7
        self.subscribed_symbols = set()
        self.market_data_updates = {}
        self._initialized = False

        # Database and persistence setup
        self.db_session = get_db_session()
        self.order_persistence_service = order_persistence_service or OrderPersistenceService(self.db_session)

        # State and reconciliation management
        self.state_service = StateService(self.db_session)
        self.state_service.subscribe('order_state_change', self._handle_order_state_change)
        self.reconciliation_engine = ReconciliationEngine(ibkr_client, self.state_service)

        # Service layer initialization
        self.execution_service = OrderExecutionService(self, self.ibkr_client)
        self.sizing_service = PositionSizingService(self)
        self.loading_service = OrderLoadingService(self, self.db_session)
        self.eligibility_service = None

        # Market hours service
        self.market_hours = MarketHoursService()
        self.last_position_close_check = None

        # Service layer initialization
        self.execution_service = OrderExecutionService(self, self.ibkr_client)
        self.sizing_service = PositionSizingService(self)
        self.loading_service = OrderLoadingService(self, self.db_session)
        self.eligibility_service = None
        # Phase B Additions - Begin
        self.prioritization_service = None
        self.outcome_labeling_service = OutcomeLabelingService(self.db_session)
        # Phase B Additions - End

    def _initialize(self) -> bool:
        """Complete initialization that requires a connected data feed."""
        if self._initialized:
            return True

        if not self.data_feed.is_connected():
            print("Cannot initialize - data feed not connected")
            return False

        self.probability_engine = FillProbabilityEngine(self.data_feed)
        self._validate_ibkr_connection()

        # Initialize services with complex dependencies
        self.eligibility_service = OrderEligibilityService(self.planned_orders, self.probability_engine)
        self.execution_service.set_dependencies(self.order_persistence_service, self.active_orders)

        self._initialized = True
        print("✅ Trading manager initialized")

        self.validate_data_source()

        if self.ibkr_client and self.ibkr_client.connected:
            print("✅ Real order execution enabled (IBKR connected)")
        elif self.ibkr_client:
            print("⚠️  Order executor provided but not connected to IBKR - will use simulation")
        else:
            print("ℹ️  No order executor provided - using simulation mode")

        # Initialize services with complex dependencies
        self.eligibility_service = OrderEligibilityService(self.planned_orders, self.probability_engine, self.db_session)
        self.execution_service.set_dependencies(self.order_persistence_service, self.active_orders)
        # Phase B Additions - Begin
        self.prioritization_service = PrioritizationService(self.sizing_service)
        # Phase B Additions - End

        return True

    def _handle_order_state_change(self, event: OrderEvent) -> None:
        """Handle order state change events from the StateService."""
        print(f"📢 State Event: {event.symbol} {event.old_state} -> {event.new_state} via {event.source}")
        if event.new_state == 'FILLED':
            print(f"🎉 Order {event.order_id} filled! Details: {event.details}")
            # Phase B Additions - Begin
            # Immediately label filled orders for real-time learning
            try:
                self.outcome_labeling_service.label_completed_orders(hours_back=1)  # Last hour only
            except Exception as e:
                print(f"⚠️  Could not label filled order immediately: {e}")
            # Phase B Additions - End
        elif event.new_state == 'CANCELLED':
            print(f"❌ Order {event.order_id} was cancelled")

    def _execute_single_order(self, order, fill_probability, total_capital, quantity, capital_commitment, is_live_trading):
        """Execute a single order by delegating to the execution service."""
        return self.execution_service.execute_single_order(
            order, fill_probability, total_capital, quantity, capital_commitment, is_live_trading
        )

    def cancel_active_order(self, active_order: ActiveOrder) -> bool:
        """Cancel an active order through the IBKR API."""
        if not self.ibkr_client or not self.ibkr_client.connected:
            print("❌ Cannot cancel order - not connected to IBKR")
            return False

        try:
            for order_id in active_order.order_ids:
                success = self.ibkr_client.cancel_order(order_id)
                if not success:
                    print(f"❌ Failed to cancel order {order_id}")
                    return False

            active_order.update_status('CANCELLED')
            print(f"✅ Cancelled order {active_order.symbol} (IDs: {active_order.order_ids})")
            return True

        except Exception as e:
            print(f"❌ Error cancelling order {active_order.symbol}: {e}")
            return False

    def cleanup_completed_orders(self) -> None:
        """Remove filled, cancelled, or replaced orders from active tracking."""
        orders_to_remove = [order_id for order_id, active_order in self.active_orders.items() if not active_order.is_working()]
        for order_id in orders_to_remove:
            del self.active_orders[order_id]
        if orders_to_remove:
            print(f"🧹 Cleaned up {len(orders_to_remove)} completed orders")

    def get_active_orders_summary(self) -> List[Dict]:
        """Get a summary of all active orders for monitoring purposes."""
        return [active_order.to_dict() for active_order in self.active_orders.values()]

    def _calculate_quantity(self, security_type, entry_price, stop_loss, total_capital, risk_per_trade) -> float:
        """Calculate position size by delegating to the sizing service."""
        return self.sizing_service.calculate_quantity(
            security_type, entry_price, stop_loss, total_capital, risk_per_trade
        )

    def start_monitoring(self, interval_seconds: int = 5) -> bool:
        """Start the continuous monitoring loop with automatic initialization."""
        if not self._initialize():
            print("❌ Failed to initialize trading manager")
            return False

        if self.ibkr_client and self.ibkr_client.connected:
            self.reconciliation_engine.start()

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

    def _monitoring_loop(self, interval_seconds: int) -> None:
        """Main monitoring loop for Phase A with error handling and recovery."""
        error_count = 0
        max_errors = 10

        # Subscribe to all symbols in planned orders
        self._subscribe_to_all_symbols()

        while self.monitoring and error_count < max_errors:
            try:
                # Phase A: Check and execute eligible orders
                self._check_and_execute_orders()

                # Check market close actions (if any)
                self._check_market_close_actions()

                # Reset error counter and sleep
                error_count = 0
                time.sleep(interval_seconds)

                # Phase B Additions - Begin
                # Label completed orders periodically (every 10 minutes)
                current_time = datetime.datetime.now()
                if (not hasattr(self, 'last_labeling_time') or 
                    (current_time - self.last_labeling_time).total_seconds() >= 600):  # 10 minutes
                    
                    self._label_completed_orders()
                    self.last_labeling_time = current_time
                # Phase B Additions - End

            except Exception as e:
                error_count += 1
                print(f"Monitoring error ({error_count}/{max_errors}): {e}")
                import traceback
                traceback.print_exc()

                # Backoff on repeated errors
                backoff_time = min(60 * error_count, 300)
                time.sleep(backoff_time)

        if error_count >= max_errors:
            print("Too many errors, stopping monitoring")
            self.monitoring = False

    def _subscribe_to_all_symbols(self) -> None:
        """Subscribe to market data for all symbols in the planned orders."""
        if not self.planned_orders:
            return

        print("\n" + "="*60)
        print("SUBSCRIBING TO MARKET DATA")
        print("="*60)

        for order in self.planned_orders:
            if order.symbol not in self.subscribed_symbols:
                try:
                    contract = order.to_ib_contract()
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

    def _check_and_execute_orders(self) -> None:
        """Check market conditions and execute orders that meet the criteria (Phase B)."""
        if not self.planned_orders:
            print("No planned orders to monitor")
            return

        print(f"\n📊 PLANNED ORDERS SUMMARY ({len(self.planned_orders)} orders)")
        print("-" * 50)

        # Display planned orders with market prices (keep for monitoring)
        for i, order in enumerate(self.planned_orders):
            price_data = self.data_feed.get_current_price(order.symbol)
            current_price = price_data['price'] if price_data and price_data.get('price') else None
            entry_display = f"${order.entry_price:8.4f}" if order.entry_price is not None else "None"

            if current_price and order.entry_price is not None:
                price_diff = current_price - order.entry_price
                percent_diff = (price_diff / order.entry_price * 100)
                print(f"{i+1:2}. {order.action.value:4} {order.symbol:6} | "
                    f"Current: ${current_price:8.4f} | Entry: {entry_display} | "
                    f"Diff: {percent_diff:6.2f}%")
            else:
                print(f"{i+1:2}. {order.action.value:4} {order.symbol:6} | "
                    f"No market data yet | Entry: {entry_display}")

        # Get executable orders from the eligibility service
        executable_orders = self.eligibility_service.find_executable_orders()

        if not executable_orders:
            print("💡 No executable orders found at this time")
            print("-" * 50)
            return

        print(f"🎯 Found {len(executable_orders)} executable orders")

        # Phase B Additions - Begin
        # Use prioritization service instead of simple execution
        self._execute_prioritized_orders(executable_orders)
        # Phase B Additions - End

        print("-" * 50)

    def load_planned_orders(self) -> List[PlannedOrder]:
        """Load and validate planned orders from Excel, persisting valid ones to the database."""
        valid_orders = self.loading_service.load_and_validate_orders(self.excel_path)
        for order in valid_orders:
            try:
                db_order = self.order_persistence_service.convert_to_db_model(order)
                self.db_session.add(db_order)
                print(f"✅ Persisted order: {order.symbol}")
            except Exception as e:
                print(f"❌ Failed to persist order {order.symbol}: {e}")

        self.db_session.commit()
        self.planned_orders = valid_orders
        return valid_orders

    def _find_existing_planned_order(self, order: PlannedOrder) -> Optional[PlannedOrderDB]:
        """Check if an order with the same parameters already exists in the database."""
        try:
            existing_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value
            ).first()
            return existing_order
        except Exception as e:
            print(f"❌ Error checking for existing order {order.symbol}: {e}")
            return None

    def _validate_order_basic(self, order: PlannedOrder) -> bool:
        """Perform basic validation on an order's parameters."""
        if order.entry_price is None or order.stop_loss is None:
            return False
        if (order.action.value == 'BUY' and order.stop_loss >= order.entry_price) or \
           (order.action.value == 'SELL' and order.stop_loss <= order.entry_price):
            return False
        if order.risk_per_trade <= 0 or order.risk_per_trade > 0.02:
            return False
        if order.risk_reward_ratio < 1.0:
            return False
        if not 1 <= order.priority <= 5:
            return False
        return True

    def _find_executable_orders(self) -> List[Dict]:
        """Find orders that meet execution criteria by delegating to the eligibility service."""
        return self.eligibility_service.find_executable_orders()

    def stop_monitoring(self) -> None:
        """Stop the monitoring loop and perform cleanup of resources."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        self.reconciliation_engine.stop()
        if self.db_session:
            self.db_session.close()
        print("Monitoring stopped")

    def _get_trading_mode(self) -> bool:
        """Determine if the system is in live trading mode based on the IBKR connection."""
        if self.ibkr_client and self.ibkr_client.connected:
            is_live_trading = not self.ibkr_client.is_paper_account
            print(f"📊 Trading mode detected: {'LIVE' if is_live_trading else 'PAPER'} (Account: {self.ibkr_client.account_number})")
            return is_live_trading
        else:
            print("📊 Trading mode: PAPER (Simulation/No IBKR connection)")
            return False

    def _find_planned_order_db_id(self, order) -> Optional[int]:
        """Find the database ID for a planned order based on its parameters."""
        try:
            db_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value,
                order_type=order.order_type.value
            ).first()
            return db_order.id if db_order else None
        except Exception as e:
            print(f"❌ Error finding planned order in database: {e}")
            return None

    def _calculate_capital_commitment(self, order, total_capital: float) -> float:
        """Calculate the capital commitment required for an order."""
        try:
            quantity = order.calculate_quantity(total_capital)
            capital_commitment = order.entry_price * quantity
            return capital_commitment
        except Exception as e:
            print(f"❌ Error calculating capital commitment for {order.symbol}: {e}")
            return 0.0

    def _can_place_order(self, order) -> bool:
        """Check if an order can be placed based on basic constraints and existing active orders."""
        working_orders = sum(1 for ao in self.active_orders.values() if ao.is_working())
        if working_orders >= self.max_open_orders:
            return False
        if order.entry_price is None:
            return False

        order_key = f"{order.symbol}_{order.action.value}_{order.entry_price}_{order.stop_loss}"
        for active_order in self.active_orders.values():
            if not active_order.is_working():
                continue
            active_order_obj = active_order.planned_order
            active_key = f"{active_order_obj.symbol}_{active_order_obj.action.value}_{active_order_obj.entry_price}_{active_order_obj.stop_loss}"
            if order_key == active_key:
                print(f"⚠️  Order already active: {order.symbol} {order.action.value} @ {order.entry_price}")
                return False
        return True

    def _calculate_order_score(self, order: PlannedOrder, fill_probability: float) -> float:
        """Calculate a prioritization score for an order based on its priority and fill probability."""
        return order.priority * fill_probability

    def _get_eligible_orders(self) -> List[Dict]:
        """Find all eligible orders that meet execution criteria, sorted by their score."""
        eligible_orders = []
        for order in self.planned_orders:
            if not self._can_place_order(order):
                continue
            should_execute, fill_prob = self.probability_engine.should_execute_order(order)
            if should_execute:
                score = self._calculate_order_score(order, fill_prob)
                eligible_orders.append({
                    'order': order,
                    'fill_probability': fill_prob,
                    'score': score,
                    'timestamp': datetime.datetime.now()
                })
        eligible_orders.sort(key=lambda x: x['score'], reverse=True)
        return eligible_orders

    def _get_committed_capital(self) -> float:
        """Calculate the total capital currently committed to working orders."""
        return sum(active_order.capital_commitment for active_order in self.active_orders.values() if active_order.is_working())

    def _find_worst_active_order(self, min_score_threshold: float = 0.0) -> Optional[ActiveOrder]:
        """Find the worst-performing active order that is stale and eligible for replacement."""
        worst_order = None
        worst_score = float('inf')
        current_time = datetime.datetime.now()

        for active_order in self.active_orders.values():
            if not active_order.is_working():
                continue
            age_minutes = (current_time - active_order.timestamp).total_seconds() / 60
            if age_minutes < 30:
                continue
            current_score = self._calculate_order_score(active_order.planned_order, active_order.fill_probability)
            if current_score < min_score_threshold:
                continue
            if current_score < worst_score:
                worst_score = current_score
                worst_order = active_order

        return worst_order

    def _validate_ibkr_connection(self) -> bool:
        """Validate that the data feed is connected to IBKR and providing live data."""
        if not self.data_feed.is_connected():
            print("❌ Data feed not connected")
            return False

        from src.data_feeds.ibkr_data_feed import IBKRDataFeed
        if not isinstance(self.data_feed, IBKRDataFeed):
            print(f"❌ Wrong data feed type: {type(self.data_feed)}")
            return False

        print("✅ Using IBKRDataFeed")
        test_symbol = "SPY"
        price_data = self.data_feed.get_current_price(test_symbol)

        if price_data and price_data.get('price') not in [0, None]:
            print(f"✅ Live data received for {test_symbol}: ${price_data['price']:.2f}")
            print(f"   Data type: {price_data.get('data_type', 'UNKNOWN')}")
            print(f"   Timestamp: {price_data.get('timestamp')}")
            return True
        else:
            print("❌ No market data received")
            return False

    def validate_data_source(self) -> None:
        """Perform a quick validation of the data source and connection."""
        print("\n" + "="*60)
        print("DATA SOURCE VALIDATION")
        print("="*60)

        from src.data_feeds.ibkr_data_feed import IBKRDataFeed
        if isinstance(self.data_feed, IBKRDataFeed):
            print("✅ Data Feed: IBKRDataFeed (Real IBKR API)")
        else:
            print(f"❌ Data Feed: {type(self.data_feed)} (Unexpected)")

        if self.data_feed.is_connected():
            print("✅ Connection: Connected to IBKR")
        else:
            print("❌ Connection: Not connected to IBKR")

        if self.planned_orders:
            test_symbol = self.planned_orders[0].symbol
            price_data = self.data_feed.get_current_price(test_symbol)
            if price_data and price_data.get('price'):
                print(f"✅ Market Data: Live price for {test_symbol}: ${price_data['price']:.2f}")
            else:
                print(f"❌ Market Data: No data for {test_symbol}")

        print("="*60)

    def _close_single_position(self, position) -> None:
        """Orchestrate the closing of a single position through the execution service."""
        try:
            print(f"🔚 Closing position: {position.symbol} ({position.action} {position.quantity})")
            print(f"   Cancelling existing orders for {position.symbol}...")
            cancel_success = self.execution_service.cancel_orders_for_symbol(position.symbol)
            if not cancel_success:
                print(f"⚠️  Order cancellation failed for {position.symbol}, proceeding anyway")

            close_action = 'SELL' if position.action == 'BUY' else 'BUY'
            print(f"   Closing action: {close_action} (was {position.action})")

            order_id = self.execution_service.close_position({
                'symbol': position.symbol,
                'action': close_action,
                'quantity': position.quantity,
                'security_type': position.security_type,
                'exchange': position.exchange,
                'currency': position.currency
            })

            if order_id is not None:
                position.status = 'CLOSING'
                self.db_session.commit()
                print(f"✅ Position closing initiated for {position.symbol} (Order ID: {order_id})")
            else:
                print(f"✅ Simulation: Position would be closed for {position.symbol}")

        except Exception as e:
            print(f"❌ Failed to close position {position.symbol}: {e}")
            import traceback
            traceback.print_exc()

    def _check_market_close_actions(self) -> None:
        """Check if any positions need to be closed due to market close strategies."""
        # Implementation would go here
        pass

    def _execute_order(self, order, fill_probability, effective_priority=None) -> None:
        """Orchestrate the execution of a single order, including capital calculation and persistence.

        Phase B: Enhanced to pass effective_priority for comprehensive attempt tracking.
        """
        try:
            # Determine total capital based on live account or simulation
            total_capital = (
                self.ibkr_client.get_account_value()
                if self.ibkr_client and self.ibkr_client.connected
                else self.total_capital
            )

            # Determine trading mode
            is_live_trading = self._get_trading_mode()
            mode_str = "LIVE" if is_live_trading else "PAPER"

            # Compute position size and capital commitment
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity

            # Log Phase B details
            print(f"🎯 Phase B: Processing {order.symbol} ({order.action.value} {order.order_type.value})")
            print(f"   Mode: {mode_str}")
            print(f"   Account Value: ${total_capital:,.2f}")
            print(f"   Fill Probability: {fill_probability:.2%}")
            if effective_priority is not None:
                print(f"   Effective Priority: {effective_priority:.3f}")
            print(f"   Stop Loss: {order.stop_loss}, Profit Target: {order.calculate_profit_target()}")
            print(f"   Quantity: {quantity}, Capital Commitment: ${capital_commitment:,.2f}")

            # Decide if order would execute based on threshold
            execute_order = fill_probability >= self.execution_threshold
            if execute_order:
                print(f"✅ Order eligible to execute")
            else:
                print(f"⚠️  Order skipped due to low fill probability")

            # Proceed with actual execution only if live/paper order execution is enabled
            if self.execution_service and execute_order:
                # Phase B Additions - Begin
                # Calculate effective priority if not provided (backward compatibility)
                if effective_priority is None:
                    effective_priority = order.priority * fill_probability
                # Phase B Additions - End
                
                self.execution_service.place_order(
                    order, fill_probability, effective_priority, total_capital, 
                    quantity, capital_commitment, is_live_trading
                )

        except Exception as e:
            print(f"❌ Failed to execute order for {order.symbol}: {e}")
            import traceback
            traceback.print_exc()
            self.order_persistence_service.update_order_status(
                order, 'PENDING', f"Execution failed: {str(e)}"
            )

    # Phase B Additions - Begin
    def _execute_prioritized_orders(self, executable_orders: List[Dict]) -> None:
        """Execute orders using Phase B prioritization and capital allocation."""
        # Determine total capital
        total_capital = (
            self.ibkr_client.get_account_value()
            if self.ibkr_client and self.ibkr_client.connected
            else self.total_capital
        )

        # Convert active orders to format expected by prioritization service
        working_orders = []
        for active_order in self.active_orders.values():
            if active_order.is_working():
                working_orders.append({
                    'capital_commitment': active_order.capital_commitment
                })

        # Prioritize orders using Phase B service
        prioritized_orders = self.prioritization_service.prioritize_orders(
            executable_orders, total_capital, working_orders
        )

        # Get summary for logging
        summary = self.prioritization_service.get_prioritization_summary(prioritized_orders)

        print(f"📈 Phase B Prioritization Results:")
        print(f"   Allocated: {summary['total_allocated']}, Rejected: {summary['total_rejected']}")
        print(f"   Capital Commitment: ${summary['total_capital_commitment']:,.2f}")
        print(f"   Average Score: {summary['average_score']:.3f}")

        # Log rejection reasons
        if summary['allocation_reasons']:
            print(f"   Rejection Reasons: {summary['allocation_reasons']}")

        # Execute allocated orders
        executed_count = 0
        for order_data in prioritized_orders:
            if not order_data['allocated']:
                continue

            order = order_data['order']
            fill_prob = order_data['fill_probability']

            # Skip if there is an open position
            if self.state_service.has_open_position(order.symbol):
                print(f"⏩ Skipping {order.symbol} - open position exists")
                continue

            # Skip duplicates or already filled orders
            db_order = self._find_existing_planned_order(order)
            if db_order and db_order.status in ['LIVE', 'LIVE_WORKING', 'FILLED']:
                same_action = db_order.action == order.action.value
                same_entry = abs(db_order.entry_price - order.entry_price) < 0.0001
                same_stop = abs(db_order.stop_loss - order.stop_loss) < 0.0001
                if same_action and same_entry and same_stop:
                    print(f"⏩ Skipping {order.symbol} {order.action.value} @ {order.entry_price:.4f} - "
                        f"already in state: {db_order.status}")
                    continue

            # Calculate effective priority for Phase B tracking
            effective_priority = order.priority * fill_prob

            # Execute the prioritized order with effective_priority for Phase B tracking
            print(f"✅ Executing {order.symbol} with score={order_data['deterministic_score']:.3f}, "
                f"fill_prob={fill_prob:.2%}, capital=${order_data['capital_commitment']:,.2f}")
            self._execute_order(order, fill_prob, effective_priority)
            executed_count += 1

        if executed_count == 0:
            print("💡 No orders executed after prioritization filtering")
    # Phase B Additions - End

    def replace_active_order(self, old_order: ActiveOrder, new_planned_order: PlannedOrder,
                        new_fill_probability: float) -> bool:
        """Replace a stale active order with a new order."""
        print(f"🔄 Replacing stale order {old_order.symbol} with new order")
        if not self.cancel_active_order(old_order):
            print("❌ Replacement failed - could not cancel old order")
            return False

        # Phase B Additions - Begin
        # Calculate effective priority for replacement order
        effective_priority = new_planned_order.priority * new_fill_probability
        self._execute_order(new_planned_order, new_fill_probability, effective_priority)
        # Phase B Additions - End
        
        old_order.update_status('REPLACED')
        print(f"✅ Successfully replaced order {old_order.symbol}")
        return True
    
    def replace_active_order(self, old_order: ActiveOrder, new_planned_order: PlannedOrder,
                       new_fill_probability: float) -> bool:
        """Replace a stale active order with a new order."""
        print(f"🔄 Replacing stale order {old_order.symbol} with new order")
        if not self.cancel_active_order(old_order):
            print("❌ Replacement failed - could not cancel old order")
            return False

        # Phase B Additions - Begin
        # Calculate effective priority for replacement order
        effective_priority = new_planned_order.priority * new_fill_probability
        self._execute_order(new_planned_order, new_fill_probability, effective_priority)
        # Phase B Additions - End
        
        old_order.update_status('REPLACED')
        print(f"✅ Successfully replaced order {old_order.symbol}")
        return True
    
    def _process_executable_orders_phase_a(self) -> None:
        """
        Phase A order orchestration:
        1. Compute fill probability for planned orders.
        2. Calculate effective priority (priority * fill_probability).
        3. Sort eligible orders by effective priority.
        4. Execute orders while respecting capital and active order limits.
        """
        if not self.planned_orders:
            print("No planned orders to process")
            return

        print(f"\n🎯 Phase A: Processing {len(self.planned_orders)} planned orders")

        eligible_orders = []
        for order in self.planned_orders:
            if not self._can_place_order(order):
                continue

            should_execute, fill_prob = self.probability_engine.should_execute_order(order)
            if should_execute:
                effective_priority = order.priority * fill_prob
                eligible_orders.append({
                    'order': order,
                    'fill_probability': fill_prob,
                    'effective_priority': effective_priority
                })

        # Sort by effective_priority descending
        eligible_orders.sort(key=lambda x: x['effective_priority'], reverse=True)

        for item in eligible_orders:
            order = item['order']
            fill_prob = item['fill_probability']
            effective_priority = item['effective_priority']

            # Skip if we already have a position
            if self.state_service.has_open_position(order.symbol):
                print(f"⏩ Skipping {order.symbol} - open position exists")
                continue

            print(f"✅ Executing {order.symbol} with effective_priority={effective_priority:.3f}, fill_probability={fill_prob:.2%}")
            
            # Phase B Additions - Begin
            # Pass effective_priority for Phase B attempt tracking
            self._execute_order(order, fill_prob, effective_priority)
            # Phase B Additions - End

    # Phase B Additions - Begin
    def _label_completed_orders(self) -> None:
        """Label completed orders for ML training data."""
        try:
            print("\n🏷️  Labeling completed orders for ML training...")
            summary = self.outcome_labeling_service.label_completed_orders(hours_back=24)
            
            if summary['total_orders'] > 0:
                print(f"   Labeled {summary['labeled_orders']} orders with {summary['labels_created']} labels")
                if summary['errors'] > 0:
                    print(f"   ⚠️  {summary['errors']} errors during labeling")
            else:
                print("   No completed orders found to label")
                
        except Exception as e:
            print(f"❌ Error in order labeling: {e}")
            import traceback
            traceback.print_exc()
    # Phase B Additions - End

    # Phase B Additions - Begin
    def generate_training_data(self, output_path: str = "training_data.csv") -> bool:
        """
        Generate and export training data from labeled orders.
        
        Args:
            output_path: Path to save the CSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            success = self.outcome_labeling_service.export_training_data(output_path)
            if success:
                print(f"✅ Training data exported to {output_path}")
            return success
        except Exception as e:
            print(f"❌ Error generating training data: {e}")
            return False
    # Phase B Additions - End
