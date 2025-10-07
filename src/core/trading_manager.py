"""
The main orchestration engine for the automated trading system.
Manages the entire trading workflow: loading orders, market data subscription,
continuous monitoring, order execution, and active order management.
Coordinates between data feeds, the IBKR client, service layer, and database.
"""
import datetime
from decimal import Decimal
from typing import List, Dict, Optional, Set
import threading
import time
import pandas as pd

from src.core.account_utils import detect_trading_environment
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
from config.trading_core_config import get_config as get_trading_core_config
from src.services.risk_management_service import RiskManagementService
from src.core.order_loading_orchestrator import OrderLoadingOrchestrator
from src.core.event_bus import EventBus
from src.core.events import PriceUpdateEvent, EventType
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)

class TradingManager:
    """Orchestrates the complete trading lifecycle and manages system state."""

    def __init__(self, data_feed: AbstractDataFeed, excel_path: str = "plan.xlsx",
                ibkr_client: Optional[IbkrClient] = None,
                order_persistence_service: Optional[OrderPersistenceService] = None,
                enable_advanced_features: bool = False,
                risk_config: Optional[Dict] = None,
                event_bus: EventBus = None):
        """Initialize the trading manager with all necessary dependencies and services."""
        # Minimal logging
        if logger:
            logger.info(f"Initializing TradingManager with {excel_path}")
        
        # Core dependencies
        self.data_feed = data_feed
        self.excel_path = excel_path
        self.ibkr_client = ibkr_client
        self.event_bus = event_bus  # <Event Bus Dependency - Begin>
        self.planned_orders: List[PlannedOrder] = []
        self.active_orders: Dict[int, ActiveOrder] = {}
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None

        # <Context-Aware Logging Integration - Begin>
        # Initialize context-aware logger
        self.context_logger = get_context_logger()
        # <Context-Aware Logging Integration - End>

        # Account Context Tracking - Begin
        self.current_account_number: Optional[str] = None
        # Account Context Tracking - End

        self._load_configuration()
        
        # Now use values from config instead of hardcoding
        self.total_capital = float(self.trading_config['simulation']['default_equity'])
        self.max_open_orders = self.trading_config['risk_limits']['max_open_orders']
        self.execution_threshold = float(self.trading_config['execution']['fill_probability_threshold'])

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
        self.loading_service = OrderLoadingService(
            trading_manager=self,
            db_session=self.db_session,
            config=self.trading_config  # <-- PASS CONFIG HERE
        )        
        # Initialize probability engine early to avoid attribute errors
        self.probability_engine = FillProbabilityEngine(
            data_feed=self.data_feed,
            config=self.trading_config  # <-- PASS CONFIG HERE
        )        
        # <Fix OrderEligibilityService Parameter Order - Begin>
        # Initialize eligibility service with CORRECT parameter order
        self.eligibility_service = OrderEligibilityService(
            self.planned_orders,           # planned_orders parameter (first)
            self.probability_engine,       # probability_engine parameter (second)  
            self.db_session                # db_session parameter (third)
        )
        
        # <Fix OrderEligibilityService Parameter Order - End>

        # Risk Management Service - UPDATED WITH CONFIG
        self.risk_service = RiskManagementService(
            state_service=self.state_service,
            persistence_service=self.order_persistence_service,  # ← Fixed parameter name
            ibkr_client=self.ibkr_client,
            config=risk_config
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

        # <Event Bus Subscription - Begin>
        # Subscribe to price events if event bus is available
        if self.event_bus:
            self.event_bus.subscribe(EventType.PRICE_UPDATE, self._handle_price_update)
            if logger:
                logger.info("TradingManager subscribed to PRICE_UPDATE events")
        # <Event Bus Subscription - End>

    # <Price Event Handler - Begin>
    def _handle_price_update(self, event: PriceUpdateEvent) -> None:
        """Handle price update events and trigger order execution checks."""
        try:
            # Only process if we have planned orders and the symbol is being monitored
            if not self.planned_orders:
                return
                
            # Check if this symbol is in our planned orders
            symbol_monitored = any(order.symbol == event.symbol for order in self.planned_orders)
            if symbol_monitored:
                if logger:
                    logger.debug(f"Price update for monitored symbol {event.symbol}: ${event.price}")
                self._check_and_execute_orders()
                
        except Exception as e:
            if logger:
                logger.error(f"Error handling price update for {event.symbol}: {e}")
    # <Price Event Handler - End>

    # <Monitored Symbols Management - Begin>
    def _update_monitored_symbols(self) -> None:
        """
        Update MarketDataManager with symbols that need price events.
        Includes symbols from planned orders and current positions.
        """
        if not hasattr(self.data_feed, 'market_data_manager') or not self.data_feed.market_data_manager:
            return
            
        monitored_symbols: Set[str] = set()
        
        # Add symbols from planned orders
        for order in self.planned_orders:
            monitored_symbols.add(order.symbol)
            
        # Add symbols from current positions
        try:
            positions = self.state_service.get_all_positions()
            for position in positions:
                monitored_symbols.add(position.symbol)
        except Exception as e:
            if logger:
                logger.warning(f"Could not fetch positions for symbol monitoring: {e}")
            
        # Update MarketDataManager
        if monitored_symbols:
            self.data_feed.market_data_manager.set_monitored_symbols(monitored_symbols)
            if logger:
                logger.info(f"Monitoring {len(monitored_symbols)} symbols for price events")
    # <Monitored Symbols Management - End>

    # Account Context Methods - Begin
    def _get_current_account_number(self) -> Optional[str]:
        """Get the current account number from IBKR client or use default."""
        if self.ibkr_client and self.ibkr_client.connected:
            try:
                # Get account number from IBKR client
                account_number = self.ibkr_client.get_account_number()
                if account_number:
                    self.current_account_number = account_number
                    return account_number
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to get account number from IBKR: {e}")
        
        # Fallback: use simulation account or previously set account
        if not self.current_account_number:
            # Default simulation account number
            self.current_account_number = "SIM0001"
        
        return self.current_account_number

    def set_account_number(self, account_number: str) -> None:
        """Explicitly set the account number for simulation or testing."""
        self.current_account_number = account_number
        if logger:
            logger.info(f"Account number set to: {account_number}")
    # Account Context Methods - End

    def _initialize_components(self, enable_advanced_features: bool) -> None:
        """Initialize all component managers and coordinators."""
        # Get monitoring interval from config
        monitoring_config = self.trading_config.get('monitoring', {})
        interval_seconds = monitoring_config.get('interval_seconds', 5)
        
        # Order execution orchestrator
        self.execution_orchestrator = OrderExecutionOrchestrator(
            execution_service=self.execution_service,
            sizing_service=self.sizing_service,
            persistence_service=self.order_persistence_service,
            state_service=self.state_service,
            probability_engine=self.probability_engine,
            ibkr_client=self.ibkr_client,
            config=self.trading_config
        )

        # Monitoring service - FIXED: Pass interval_seconds during initialization
        self.monitoring_service = MonitoringService(self.data_feed, interval_seconds)

        # <Order Loading Orchestrator Integration - Begin>
        # Initialize OrderLoadingOrchestrator for multi-source order loading
        self.order_loading_orchestrator = OrderLoadingOrchestrator(
            loading_service=self.loading_service,
            persistence_service=self.order_persistence_service,
            state_service=self.state_service,
            db_session=self.db_session
        )
        # <Order Loading Orchestrator Integration - End>

        # Order lifecycle manager - UPDATED with orchestrator injection
        self.order_lifecycle_manager = OrderLifecycleManager(
            loading_service=self.loading_service,
            persistence_service=self.order_persistence_service,
            state_service=self.state_service,
            db_session=self.db_session,
            # <Order Loading Orchestrator Integration - Begin>
            order_loading_orchestrator=self.order_loading_orchestrator
            # <Order Loading Orchestrator Integration - End>
        )

        # Advanced feature coordinator
        self.advanced_features = AdvancedFeatureCoordinator(enable_advanced_features)
        self._load_configuration()

        # Basic prioritization service (for backward compatibility)
        self.prioritization_service = PrioritizationService(
            sizing_service=self.sizing_service,
            config=self.prioritization_config
        )

    def _initialize(self) -> bool:
        """Complete initialization with advanced services and validation."""
        if self._initialized:
            return True

        if not self.data_feed.is_connected():
            return False

        self._validate_ibkr_connection()

        # Initialize account context
        self._get_current_account_number()

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
            if logger:
                logger.info(f"Cancelled active order: {active_order.symbol}")
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

    def load_planned_orders(self) -> List[PlannedOrder]:
        """Load and validate planned orders from Excel, persisting valid ones to the database."""
        self.planned_orders = self.order_lifecycle_manager.load_and_persist_orders(self.excel_path)
        
        if logger:
            logger.info(f"Loaded {len(self.planned_orders)} planned orders")
        
        # <Update Monitored Symbols After Loading Orders - Begin>
        # Update the monitored symbols after loading new planned orders
        self._update_monitored_symbols()
        # <Update Monitored Symbols After Loading Orders - End>
        
        return self.planned_orders

    def stop_monitoring(self) -> None:
        """Stop the monitoring loop and perform cleanup of resources."""
        if logger:
            logger.info("Stopping trading monitoring")
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
        if working_orders >= self.trading_config['risk_limits']['max_open_orders']:
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

    def _get_total_capital(self) -> float:
        """Get total capital from IBKR or use default."""
        if self.ibkr_client and self.ibkr_client.connected:
            try:
                return self.ibkr_client.get_account_value()
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to get account value from IBKR: {e}")
        
        return float(self.trading_config['simulation']['default_equity'])

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
        # Pass account number to execution orchestrator
        account_number = self._get_current_account_number()
        success = self.execution_orchestrator.execute_single_order(
            new_planned_order, new_fill_probability, effective_priority, account_number
        )
        
        if success:
            old_order.update_status('REPLACED')
            if logger:
                logger.info(f"Replaced order: {old_order.symbol} -> {new_planned_order.symbol}")
        
        return success

    def generate_training_data(self, output_path: str = "training_data.csv") -> bool:
        """Generate and export training data from labeled orders."""
        if self.advanced_features.enabled:
            if logger:
                logger.info(f"Generating training data to: {output_path}")
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
        
        if logger:
            logger.info(f"Data source validation: IBKR={is_ibkr}, Connected={is_connected}")
        
        if self.planned_orders:
            test_symbol = self.planned_orders[0].symbol
            # Use MonitoringService instead of direct data feed access
            current_price = self.monitoring_service.get_current_price(test_symbol)
            if current_price:
                if logger:
                    logger.info(f"Market Data: Live price for {test_symbol}: ${current_price:.2f}")
            else:
                if logger:
                    logger.warning(f"No market data available for {test_symbol}")

    def _get_trading_environment(self) -> str:
        """
        Detect trading environment based on connected account.
        
        Returns:
            'paper' for paper trading, 'live' for live trading
        """
        if self.ibkr_client and self.ibkr_client.connected:
            account_name = self.ibkr_client.get_account_name()
            return detect_trading_environment(account_name)
        else:
            # No IBKR connection = simulation/paper mode
            return 'paper'

    def _load_configuration(self) -> None:
        """Load trading configuration for the detected environment."""
        try:
            # Detect environment automatically
            environment = self._get_trading_environment()
            
            # Load appropriate trading core config
            self.trading_config = get_trading_core_config(environment)
            
            if logger:
                logger.info(f"Loaded {environment} trading configuration")
            
        except Exception as e:
            # Fallback to hardcoded defaults
            if logger:
                logger.warning(f"Failed to load configuration: {e}. Using defaults.")
            self._load_fallback_config()
    
    def _load_fallback_config(self) -> None:
        """Load fallback configuration with hardcoded defaults."""
        if logger:
            logger.warning("Loading fallback configuration due to configuration failure")
        self.trading_config = {
            'risk_limits': {
                'max_open_orders': 5,
                'daily_loss_pct': Decimal('0.02'),
                'weekly_loss_pct': Decimal('0.05'),
                'monthly_loss_pct': Decimal('0.08'),
                'max_risk_per_trade': Decimal('0.02')
            },
            'execution': {
                'fill_probability_threshold': Decimal('0.7'),
                'min_fill_probability': Decimal('0.4')
            },
            'order_defaults': {
                'risk_per_trade': Decimal('0.005'),
                'risk_reward_ratio': Decimal('2.0'),
                'priority': 3
            },
            'simulation': {
                'default_equity': Decimal('100000')
            },
            'monitoring': {
                'interval_seconds': 5,
                'max_errors': 10,
                'error_backoff_base': 60,
                'max_backoff': 300
            },
            'market_close': {
                'buffer_minutes': 10
            },
            'labeling': {
                'hours_back': 24,
                'state_change_hours_back': 1
            }
        }

    # src/core/trading_manager.py - Fix other config access methods
    def _label_completed_orders(self) -> None:
        """Label completed orders for ML training data."""
        if self.advanced_features.enabled:
            # Safely get hours_back from config with fallback
            labeling_config = self.trading_config.get('labeling', {})
            hours_back = labeling_config.get('hours_back', 24)
            self.advanced_features.label_completed_orders(hours_back=hours_back)

    def _handle_order_state_change(self, event: OrderEvent) -> None:
        """Handle order state change events from the StateService."""
        if event.new_state == 'FILLED':
            if self.advanced_features.enabled:
                try:
                    # Safely get state_change_hours_back from config with fallback
                    labeling_config = self.trading_config.get('labeling', {})
                    hours_back = labeling_config.get('state_change_hours_back', 1)
                    self.advanced_features.label_completed_orders(hours_back=hours_back)
                except Exception:
                    pass
        
        # <Update Monitored Symbols on Position Changes - Begin>
        # Update monitored symbols when positions change (new fills or closes)
        if event.new_state in ['FILLED', 'CANCELLED', 'CLOSED']:
            self._update_monitored_symbols()
        # <Update Monitored Symbols on Position Changes - End>

    def _monitoring_loop(self, interval_seconds: int) -> None:

        """Main monitoring loop for Phase A with error handling and recovery."""
        monitoring_config = self.trading_config.get('monitoring', {})
        max_errors = monitoring_config.get('max_errors', 10)
        error_backoff_base = monitoring_config.get('error_backoff_base', 60)
        max_backoff = monitoring_config.get('max_backoff', 300)
        
        error_count = 0

        self.monitoring_service.subscribe_to_symbols(self.planned_orders)

        while self.monitoring and error_count < max_errors:
            try:
                self._check_and_execute_orders()
                self._check_market_close_actions()
                error_count = 0
                time.sleep(interval_seconds)
            except Exception:
                error_count += 1
                backoff_time = min(error_backoff_base * error_count, max_backoff)
                time.sleep(backoff_time)

    def start_monitoring(self, interval_seconds: Optional[int] = None) -> bool:
        """Start the continuous monitoring loop with automatic initialization."""
        if logger:
            logger.info("Starting trading monitoring")
            
        if not self._initialize():
            return False

        if self.ibkr_client and self.ibkr_client.connected:
            self.reconciliation_engine.start()

        if not self.data_feed.is_connected():
            raise Exception("Data feed not connected")

        # Update monitoring interval if provided
        if interval_seconds is not None:
            self.monitoring_service.set_monitoring_interval(interval_seconds)

        # <Initialize Monitored Symbols at Startup - Begin>
        # Initialize monitored symbols before starting monitoring
        self._update_monitored_symbols()
        # <Initialize Monitored Symbols at Startup - End>

        # <Event-Driven Symbol Subscription - Begin>
        # Symbol subscription is now handled by event system - keep for backward compatibility
        self._subscribe_to_planned_order_symbols()
        # <Event-Driven Symbol Subscription - End>

        self.debug_order_status()
        
        # FIX: Remove interval_seconds parameter
        return self.monitoring_service.start_monitoring(
            check_callback=self._check_and_execute_orders,
            label_callback=self._label_completed_orders
        )    

    # ADD NEW METHOD TO TradingManager
    def _subscribe_to_planned_order_symbols(self) -> None:
        """Subscribe to market data for all planned order symbols."""
        if not self.planned_orders:
            return
            
        from ibapi.contract import Contract
        
        subscribed_count = 0
        for order in self.planned_orders:
            try:
                contract = Contract()
                contract.symbol = order.symbol
                contract.secType = order.security_type.value
                contract.exchange = order.exchange
                contract.currency = order.currency
                
                success = self.data_feed.subscribe(order.symbol, contract)
                
                if success:
                    subscribed_count += 1
                    
            except Exception as e:
                if logger:
                    logger.error(f"Error subscribing to {order.symbol}: {e}")
        
        if logger:
            logger.info(f"Market data subscriptions: {subscribed_count}/{len(self.planned_orders)} symbols")

    def debug_order_status(self):
        """Debug method to check order status"""
        from src.core.models import PlannedOrderDB
        from sqlalchemy import select
        
        # Check what's in the database
        db_orders = self.db_session.scalars(select(PlannedOrderDB)).all()
        if logger:
            logger.info(f"Database has {len(db_orders)} orders")
        
        # Check planned orders in memory
        if logger:
            logger.info(f"Memory has {len(self.planned_orders)} planned orders")

    # Add missing methods
    def _initialize_advanced_services(self) -> None:
        """Initialize advanced feature services if enabled."""
        if logger:
            logger.info("Initializing advanced trading services")
        try:
            self.market_context_service = MarketContextService(self.data_feed)
            self.historical_performance_service = HistoricalPerformanceService(self.db_session)
        except Exception as e:
            if logger:
                logger.error(f"Failed to initialize advanced services: {e}")
    
    def _check_market_close_actions(self) -> None:
        """Check if any DAY positions need to be closed before market close."""
        # Safely get buffer_minutes from config with fallback
        market_close_config = self.trading_config.get('market_close', {})
        buffer_minutes = market_close_config.get('buffer_minutes', 10)
        
        # <Context-Aware Logging - Market Close Check - Begin>
        # Log market close check with context
        should_close = self.market_hours.should_close_positions(buffer_minutes=buffer_minutes)
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Market close position check",
            context_provider={
                'buffer_minutes': buffer_minutes,
                'should_close': should_close,
                'current_time': lambda: datetime.datetime.now().isoformat(),
                'minutes_until_close': lambda: self.market_hours.minutes_until_close(),
                'market_status': lambda: self.market_hours.get_market_status()
            },
            decision_reason=f"Market close check: should_close={should_close}"
        )
        # <Context-Aware Logging - Market Close Check - End>
        
        if should_close:
            # Close all DAY strategy positions
            day_positions = self.state_service.get_positions_by_strategy(PositionStrategy.DAY)
            
            # <Context-Aware Logging - DAY Positions Found - Begin>
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Found {len(day_positions)} DAY positions to close",
                context_provider={
                    'day_positions_count': len(day_positions),
                    'position_symbols': lambda: [p.symbol for p in day_positions] if day_positions else []
                },
                decision_reason=f"Market close: closing {len(day_positions)} DAY positions"
            )
            # <Context-Aware Logging - DAY Positions Found - End>
            
            if logger and day_positions:
                logger.info(f"Market close approaching - closing {len(day_positions)} DAY positions")
            for position in day_positions:
                if logger:
                    logger.info(f"Closing DAY position {position.symbol} before market close")
                self._close_single_position(position)

    def _check_and_execute_orders(self) -> None:
        """Check market conditions and execute orders that meet the criteria."""
        if not self.planned_orders:
            return

        # <Context-Aware Logging - Execution Cycle Start - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Starting order execution cycle",
            context_provider={
                'planned_orders_count': len(self.planned_orders),
                'active_orders_count': len(self.active_orders),
                'market_open': lambda: self.market_hours.is_market_open()
            }
        )
        # <Context-Aware Logging - Execution Cycle Start - End>

        executable_orders = self.eligibility_service.find_executable_orders()
        if not executable_orders:
            # <Context-Aware Logging - No Executable Orders - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "No executable orders found",
                context_provider={
                    'planned_orders_count': len(self.planned_orders),
                    'executable_orders_count': 0
                },
                decision_reason="Eligibility service returned no executable orders"
            )
            # <Context-Aware Logging - No Executable Orders - End>
            return

        self._execute_prioritized_orders(executable_orders)

    def _execute_prioritized_orders(self, executable_orders: List[Dict]) -> None:
        """Execute orders using two-layer prioritization with viability gating."""
        total_capital = self._get_total_capital()
        working_orders = self._get_working_orders()

        # <Context-Aware Logging - Prioritization Start - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Starting order prioritization",
            context_provider={
                'executable_orders_count': len(executable_orders),
                'total_capital': total_capital,
                'working_orders_count': len(working_orders),
                'max_open_orders': self.max_open_orders
            }
        )
        # <Context-Aware Logging - Prioritization Start - End>

        prioritized_orders = self.prioritization_service.prioritize_orders(
            executable_orders, total_capital, working_orders
        )

        executed_count = 0
        skipped_reasons = {}
        
        for order_data in prioritized_orders:
            order = order_data['order']
            fill_prob = order_data['fill_probability']
            symbol = order.symbol

            if not order_data.get('allocated', False) or not order_data.get('viable', False):
                skipped_reasons[symbol] = f"Not allocated/viable (allocated={order_data.get('allocated')}, viable={order_data.get('viable')})"
                continue

            if self.state_service.has_open_position(symbol):
                skipped_reasons[symbol] = "Open position exists"
                continue

            db_order = self.order_lifecycle_manager.find_existing_order(order)
            if db_order and db_order.status in ['LIVE', 'LIVE_WORKING', 'FILLED']:
                same_action = db_order.action == order.action.value
                same_entry = abs(db_order.entry_price - order.entry_price) < 0.0001
                same_stop = abs(db_order.stop_loss - order.stop_loss) < 0.0001
                if same_action and same_entry and same_stop:
                    skipped_reasons[symbol] = f"Duplicate active order (status: {db_order.status})"
                    continue

            effective_priority = order.priority * fill_prob
            account_number = self._get_current_account_number()
            
            # <Context-Aware Logging - Order Execution Attempt - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Attempting order execution for {symbol}",
                symbol=symbol,
                context_provider={
                    'entry_price': order.entry_price,
                    'stop_loss': order.stop_loss,
                    'action': order.action.value,
                    'fill_probability': fill_prob,
                    'effective_priority': effective_priority,
                    'account_number': account_number
                },
                decision_reason=f"Order meets execution criteria"
            )
            # <Context-Aware Logging - Order Execution Attempt - End>
            
            if logger:
                logger.info(f"Executing order: {symbol} {order.action.value} @ ${order.entry_price}")
                
            success = self.execution_orchestrator.execute_single_order(
                order, fill_prob, effective_priority, account_number
            )
            
            if success:
                executed_count += 1
                # <Context-Aware Logging - Order Execution Success - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Order execution successful for {symbol}",
                    symbol=symbol,
                    context_provider={
                        'entry_price': order.entry_price,
                        'order_type': order.order_type.value
                    },
                    decision_reason="Execution orchestrator returned success"
                )
                # <Context-Aware Logging - Order Execution Success - End>
                if logger:
                    logger.info(f"Successfully executed order for {symbol}")
            else:
                # <Context-Aware Logging - Order Execution Failure - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Order execution failed for {symbol}",
                    symbol=symbol,
                    context_provider={
                        'entry_price': order.entry_price
                    },
                    decision_reason="Execution orchestrator returned failure"
                )
                # <Context-Aware Logging - Order Execution Failure - End>
                if logger:
                    logger.error(f"Failed to execute order for {symbol}")

        # <Context-Aware Logging - Execution Summary - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Order execution cycle completed: {executed_count} executed, {len(skipped_reasons)} skipped",
            context_provider={
                'executed_count': executed_count,
                'skipped_count': len(skipped_reasons),
                'skipped_reasons': skipped_reasons,
                'total_considered': len(prioritized_orders)
            },
            decision_reason=f"Execution summary: {executed_count} executed"
        )
        # <Context-Aware Logging - Execution Summary - End>

        if logger and executed_count > 0:
            logger.info(f"Executed {executed_count} orders in this cycle")

    def _close_single_position(self, position) -> None:
        """Orchestrate the closing of a single position through the execution service."""
        try:
            # <Context-Aware Logging - Position Close Start - Begin>
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Closing position: {position.symbol}",
                symbol=position.symbol,
                context_provider={
                    'position_action': position.action,
                    'position_quantity': position.quantity,
                    'position_strategy': getattr(position, 'position_strategy', 'UNKNOWN'),
                    'reason': 'market_close' if hasattr(self, '_check_market_close_actions') else 'manual'
                },
                decision_reason="Closing position"
            )
            # <Context-Aware Logging - Position Close Start - End>
            
            if logger:
                logger.info(f"Closing position: {position.symbol} ({position.action} {position.quantity})")
            
            cancel_success = self.execution_service.cancel_orders_for_symbol(position.symbol)
            if not cancel_success:
                if logger:
                    logger.warning(f"Order cancellation failed for {position.symbol}, proceeding anyway")

            close_action = 'SELL' if position.action == 'BUY' else 'BUY'

            # Pass account number to execution service
            account_number = self._get_current_account_number()
            order_id = self.execution_service.close_position({
                'symbol': position.symbol,
                'action': close_action,
                'quantity': position.quantity,
                'security_type': position.security_type,
                'exchange': position.exchange,
                'currency': position.currency
            }, account_number)

            if order_id is not None:
                position.status = 'CLOSING'
                self.db_session.commit()
                
                # <Context-Aware Logging - Position Close Success - Begin>
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Position closing initiated for {position.symbol}",
                    symbol=position.symbol,
                    context_provider={
                        'close_order_id': order_id,
                        'close_action': close_action,
                        'quantity': position.quantity
                    },
                    decision_reason="Position close order placed successfully"
                )
                # <Context-Aware Logging - Position Close Success - End>
                
                if logger:
                    logger.info(f"Position closing initiated for {position.symbol} (Order ID: {order_id})")
                
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
                    if logger:
                        logger.warning(f"Could not record P&L for {position.symbol}: {e}")
                # Risk Management - Record P&L on position close - End
                
            else:
                # <Context-Aware Logging - Position Close Simulation - Begin>
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Position close simulated for {position.symbol}",
                    symbol=position.symbol,
                    context_provider={
                        'close_action': close_action,
                        'quantity': position.quantity
                    },
                    decision_reason="Simulation mode - no actual order placed"
                )
                # <Context-Aware Logging - Position Close Simulation - End>
                if logger:
                    logger.info(f"Simulation: Position would be closed for {position.symbol}")

        except Exception as e:
            # <Context-Aware Logging - Position Close Error - Begin>
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Failed to close position {position.symbol}",
                symbol=position.symbol,
                context_provider={
                    'error': str(e)
                },
                decision_reason=f"Position close failed: {e}"
            )
            # <Context-Aware Logging - Position Close Error - End>
            if logger:
                logger.error(f"Failed to close position {position.symbol}: {e}")