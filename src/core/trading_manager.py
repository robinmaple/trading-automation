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

from src.core.order_execution_orchestrator import OrderExecutionOrchestrator
from src.core.monitoring_service import MonitoringService
from src.core.order_lifecycle_manager import OrderLifecycleManager
from src.core.advanced_feature_coordinator import AdvancedFeatureCoordinator
from src.services.prioritization_service import PrioritizationService
from src.core.ibkr_client import IbkrClient
from src.core.planned_order import PlannedOrder, ActiveOrder, PositionStrategy
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
from src.services.outcome_labeling_service import OutcomeLabelingService
from src.services.market_context_service import MarketContextService
from src.services.historical_performance_service import HistoricalPerformanceService
from config.prioritization_config import get_config

class TradingManager:
    """Orchestrates the complete trading lifecycle and manages system state."""

    def __init__(self, data_feed: AbstractDataFeed, excel_path: str = "plan.xlsx",
                ibkr_client: Optional[IbkrClient] = None,
                order_persistence_service: Optional[OrderPersistenceService] = None,
                enable_advanced_features: bool = False):
        """Initialize the trading manager with all necessary dependencies and services."""
        # Core dependencies
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
        self._initialized = False

        # Database and persistence setup
        self.db_session = get_db_session()
        self.order_persistence_service = order_persistence_service or OrderPersistenceService(self.db_session)

        # State and reconciliation management
        self.state_service = StateService(self.db_session)
        self.state_service.subscribe('order_state_change', self._handle_order_state_change)
        self.reconciliation_engine = ReconciliationEngine(ibkr_client, self.state_service)

        # Service layer initialization - initialize core services first
        self.execution_service = OrderExecutionService(self, self.ibkr_client)
        self.sizing_service = PositionSizingService(self)
        self.loading_service = OrderLoadingService(self, self.db_session)
        
        # Initialize probability engine early to avoid attribute errors
        self.probability_engine = FillProbabilityEngine(self.data_feed)
        
        # Initialize eligibility service with probability engine
        self.eligibility_service = OrderEligibilityService(
            self.probability_engine, self.db_session
        )

        # Risk Management Service - Begin
        from src.services.risk_management_service import RiskManagementService
        self.risk_service = RiskManagementService(
            self.state_service,
            self.order_persistence_service,
            self.ibkr_client
        )
        # Risk Management Service - End

        # Market hours service
        self.market_hours = MarketHoursService()
        self.last_position_close_check = None

        # <Advanced Feature Integration - Begin>
        # Store advanced feature flag
        self.enable_advanced_features = enable_advanced_features
        self.market_context_service = None
        self.historical_performance_service = None
        self.prioritization_config = None
        # <Advanced Feature Integration - End>

        # Add configuration loading
        self._load_configuration()
        
        # Initialize prioritization service with basic configuration
        self.prioritization_service = PrioritizationService(
            sizing_service=self.sizing_service,
            config=self.prioritization_config
        )
        
        self.outcome_labeling_service = OutcomeLabelingService(self.db_session)

        # Phase B services that require advanced features are initialized conditionally
        if self.enable_advanced_features:
            self._initialize_advanced_services()

        self._initialize_components(enable_advanced_features)

    def _initialize_components(self, enable_advanced_features: bool) -> None:
        """Initialize all component managers and coordinators."""
        # Order execution orchestrator
        self.execution_orchestrator = OrderExecutionOrchestrator(
            execution_service=self.execution_service,
            sizing_service=self.sizing_service,
            persistence_service=self.order_persistence_service,
            state_service=self.state_service,
            probability_engine=self.probability_engine,
            ibkr_client=self.ibkr_client
        )

        # Monitoring service
        self.monitoring_service = MonitoringService(self.data_feed)

        # Order lifecycle manager
        self.order_lifecycle_manager = OrderLifecycleManager(
            loading_service=self.loading_service,
            persistence_service=self.order_persistence_service,
            state_service=self.state_service,
            db_session=self.db_session
        )

        # Advanced feature coordinator
        self.advanced_features = AdvancedFeatureCoordinator(enable_advanced_features)
        self._load_configuration()

        # Basic prioritization service (for backward compatibility)
        self.prioritization_service = PrioritizationService(
            sizing_service=self.sizing_service,
            config=self.prioritization_config
        )

    def _load_configuration(self) -> None:
        """Load trading configuration including prioritization config."""
        try:
            self.prioritization_config = get_config('default')
        except Exception:
            self.prioritization_config = PrioritizationService(self.sizing_service)._get_default_config()

    def _initialize(self) -> bool:
        """Complete initialization with advanced services and validation."""
        if self._initialized:
            return True

        if not self.data_feed.is_connected():
            return False

        self._validate_ibkr_connection()

        # Initialize advanced services if enabled
        if self.advanced_features.enabled:
            self.advanced_features.initialize_services(
                data_feed=self.data_feed,
                sizing_service=self.sizing_service,
                db_session=self.db_session,
                prioritization_config=self.prioritization_config
            )

        self.execution_service.set_dependencies(self.order_persistence_service, self.active_orders)
        self._initialized = True
        return True

    def _handle_order_state_change(self, event: OrderEvent) -> None:
        """Handle order state change events from the StateService."""
        if event.new_state == 'FILLED':
            if self.advanced_features.enabled:
                try:
                    self.advanced_features.label_completed_orders(hours_back=1)
                except Exception:
                    pass

    def cancel_active_order(self, active_order: ActiveOrder) -> bool:
        """Cancel an active order through the IBKR API."""
        if not self.ibkr_client or not self.ibkr_client.connected:
            return False

        try:
            for order_id in active_order.order_ids:
                success = self.ibkr_client.cancel_order(order_id)
                if not success:
                    return False

            active_order.update_status('CANCELLED')
            return True
        except Exception:
            return False

    def cleanup_completed_orders(self) -> None:
        """Remove filled, cancelled, or replaced orders from active tracking."""
        orders_to_remove = [order_id for order_id, active_order in self.active_orders.items() if not active_order.is_working()]
        for order_id in orders_to_remove:
            del self.active_orders[order_id]

    def get_active_orders_summary(self) -> List[Dict]:
        """Get a summary of all active orders for monitoring purposes."""
        return [active_order.to_dict() for active_order in self.active_orders.values()]

    def start_monitoring(self, interval_seconds: int = 5) -> bool:
        """Start the continuous monitoring loop with automatic initialization."""
        if not self._initialize():
            return False

        if self.ibkr_client and self.ibkr_client.connected:
            self.reconciliation_engine.start()

        if not self.data_feed.is_connected():
            raise Exception("Data feed not connected")

        return self.monitoring_service.start_monitoring(
            check_callback=self._check_and_execute_orders,
            label_callback=self._label_completed_orders
        )

    def _monitoring_loop(self, interval_seconds: int) -> None:
        """Main monitoring loop for Phase A with error handling and recovery."""
        error_count = 0
        max_errors = 10

        self.monitoring_service.subscribe_to_symbols(self.planned_orders)

        while self.monitoring and error_count < max_errors:
            try:
                self._check_and_execute_orders()
                self._check_market_close_actions()
                error_count = 0
                time.sleep(interval_seconds)
            except Exception:
                error_count += 1
                time.sleep(min(60 * error_count, 300))

    def _check_and_execute_orders(self) -> None:
        """Check market conditions and execute orders that meet the criteria."""
        if not self.planned_orders:
            return

        executable_orders = self.eligibility_service.find_executable_orders()
        if not executable_orders:
            return

        self._execute_prioritized_orders(executable_orders)

    def load_planned_orders(self) -> List[PlannedOrder]:
        """Load and validate planned orders from Excel, persisting valid ones to the database."""
        self.planned_orders = self.order_lifecycle_manager.load_and_persist_orders(self.excel_path)
        return self.planned_orders

    def stop_monitoring(self) -> None:
        """Stop the monitoring loop and perform cleanup of resources."""
        self.monitoring_service.stop_monitoring()
        self.reconciliation_engine.stop()
        if self.db_session:
            self.db_session.close()

    def _get_trading_mode(self) -> bool:
        """Determine if the system is in live trading mode based on the IBKR connection."""
        if self.ibkr_client and self.ibkr_client.connected:
            return not self.ibkr_client.is_paper_account
        return False

    def _calculate_capital_commitment(self, order, total_capital: float) -> float:
        """Calculate the capital commitment required for an order."""
        try:
            quantity = order.calculate_quantity(total_capital)
            return order.entry_price * quantity
        except Exception:
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
                return False
        return True

    def _execute_prioritized_orders(self, executable_orders: List[Dict]) -> None:
        """Execute orders using two-layer prioritization with viability gating."""
        total_capital = self._get_total_capital()
        working_orders = self._get_working_orders()

        prioritized_orders = self.prioritization_service.prioritize_orders(
            executable_orders, total_capital, working_orders
        )

        executed_count = 0
        for order_data in prioritized_orders:
            if not order_data.get('allocated', False) or not order_data.get('viable', False):
                continue

            order = order_data['order']
            fill_prob = order_data['fill_probability']

            if self.state_service.has_open_position(order.symbol):
                continue

            db_order = self.order_lifecycle_manager.find_existing_order(order)
            if db_order and db_order.status in ['LIVE', 'LIVE_WORKING', 'FILLED']:
                same_action = db_order.action == order.action.value
                same_entry = abs(db_order.entry_price - order.entry_price) < 0.0001
                same_stop = abs(db_order.stop_loss - order.stop_loss) < 0.0001
                if same_action and same_entry and same_stop:
                    continue

            effective_priority = order.priority * fill_prob
            self.execution_orchestrator.execute_single_order(order, fill_prob, effective_priority)
            executed_count += 1

    def _get_total_capital(self) -> float:
        """Get total capital from IBKR or use default."""
        if self.ibkr_client and self.ibkr_client.connected:
            return self.ibkr_client.get_account_value()
        return self.total_capital

    def _get_working_orders(self) -> List[Dict]:
        """Get working orders in format expected by prioritization service."""
        return [{'capital_commitment': ao.capital_commitment} 
                for ao in self.active_orders.values() if ao.is_working()]

    def replace_active_order(self, old_order: ActiveOrder, new_planned_order: PlannedOrder,
                           new_fill_probability: float) -> bool:
        """Replace a stale active order with a new order."""
        if not self.cancel_active_order(old_order):
            return False

        effective_priority = new_planned_order.priority * new_fill_probability
        success = self.execution_orchestrator.execute_single_order(
            new_planned_order, new_fill_probability, effective_priority
        )
        
        if success:
            old_order.update_status('REPLACED')
        
        return success

    def _label_completed_orders(self) -> None:
        """Label completed orders for ML training data."""
        if self.advanced_features.enabled:
            self.advanced_features.label_completed_orders(hours_back=24)

    def generate_training_data(self, output_path: str = "training_data.csv") -> bool:
        """Generate and export training data from labeled orders."""
        if self.advanced_features.enabled:
            return self.advanced_features.generate_training_data(output_path)
        return False

    def _validate_ibkr_connection(self) -> bool:
        """Validate that the data feed is connected to IBKR and providing live data."""
        if not self.data_feed.is_connected():
            return False

        from src.data_feeds.ibkr_data_feed import IBKRDataFeed
        if not isinstance(self.data_feed, IBKRDataFeed):
            return False

        test_symbol = "SPY"
        price_data = self.data_feed.get_current_price(test_symbol)
        return price_data and price_data.get('price') not in [0, None]

    def validate_data_source(self) -> None:
        """Perform a quick validation of the data source and connection."""
        from src.data_feeds.ibkr_data_feed import IBKRDataFeed
        is_ibkr = isinstance(self.data_feed, IBKRDataFeed)
        is_connected = self.data_feed.is_connected()
        
        if self.planned_orders:
            test_symbol = self.planned_orders[0].symbol
            # Use MonitoringService instead of direct data feed access
            current_price = self.monitoring_service.get_current_price(test_symbol)
            print(f"Market Data: Live price for {test_symbol}: ${current_price:.2f}" if current_price else "No market data")

    def _check_market_close_actions(self) -> None:
        """Check if any DAY positions need to be closed 10 minutes before market close."""
        if self.market_hours.should_close_positions(buffer_minutes=10):
            # Close all DAY strategy positions 10 minutes before market close
            day_positions = self.state_service.get_positions_by_strategy(PositionStrategy.DAY)
            for position in day_positions:
                print(f"🔚 Closing DAY position {position.symbol} before market close")
                self._close_single_position(position)

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
                
                # Risk Management - Record P&L on position close - Begin
                try:
                    # Delegate P&L calculation and recording to RiskManagementService
                    # Find the active order for this position
                    for active_order in self.active_orders.values():
                        if active_order.symbol == position.symbol and active_order.is_working():
                            # Risk service will handle P&L calculation internally
                            self.risk_service.record_trade_outcome(active_order, None)
                            break
                except Exception as e:
                    print(f"⚠️  Could not record P&L for {position.symbol}: {e}")
                # Risk Management - Record P&L on position close - End
                
            else:
                print(f"✅ Simulation: Position would be closed for {position.symbol}")

        except Exception as e:
            print(f"❌ Failed to close position {position.symbol}: {e}")
            import traceback
            traceback.print_exc()