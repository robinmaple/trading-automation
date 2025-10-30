"""
TradingInitializer - Handles complex initialization and setup of TradingManager components.
Separates initialization logic from main business logic for better testability and maintainability.
"""

from typing import Optional, Dict, Any
from src.trading.risk.account_utils import detect_trading_environment
from src.trading.execution.order_execution_orchestrator import OrderExecutionOrchestrator
from src.trading.monitoring.monitoring_service import MonitoringService
from src.trading.orders.order_lifecycle_manager import OrderLifecycleManager
from src.trading.coordination.advanced_feature_coordinator import AdvancedFeatureCoordinator
from src.services.prioritization_service import PrioritizationService
from src.brokers.ibkr.ibkr_client import IbkrClient
from src.trading.execution.probability_engine import FillProbabilityEngine
from src.market_data.feeds.abstract_data_feed import AbstractDataFeed
from src.core.database import get_db_session
from src.services.order_eligibility_service import OrderEligibilityService
from src.trading.execution.order_execution_service import OrderExecutionService
from src.trading.risk.position_sizing_service import PositionSizingService
from src.trading.orders.order_loading_service import OrderLoadingService
from src.trading.orders.order_persistence_service import OrderPersistenceService
from src.services.state_service import StateService
from src.trading.positions.reconciliation_engine import ReconciliationEngine
from src.services.market_hours_service import MarketHoursService
from src.services.outcome_labeling_service import OutcomeLabelingService
from src.services.market_context_service import MarketContextService
from src.services.historical_performance_service import HistoricalPerformanceService
from config.trading_core_config import get_config as get_trading_core_config
from src.trading.risk.risk_management_service import RiskManagementService
from src.trading.orders.order_loading_orchestrator import OrderLoadingOrchestrator
from src.core.event_bus import EventBus
from src.core.events import EventType
from src.core.context_aware_logger import TradingEventType
from src.services.end_of_day_service import EndOfDayService, EODConfig


class TradingInitializer:
    """Handles complex initialization and setup of TradingManager components."""
    
    def __init__(self, trading_manager):
        self.tm = trading_manager
        self.context_logger = trading_manager.context_logger
        
    def initialize(self, data_feed: AbstractDataFeed, excel_path: Optional[str], 
                  ibkr_client: Optional[IbkrClient], enable_advanced_features: bool,
                  order_persistence_service: Optional[OrderPersistenceService],
                  risk_config: Optional[Dict], event_bus: EventBus) -> bool:
        """Initialize all TradingManager components and dependencies."""
        # <Context-Aware Logging - TradingManager Initialization Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "TradingManager initialization starting via TradingInitializer",
            context_provider={
                "excel_path": excel_path,
                "enable_advanced_features": enable_advanced_features,
                "has_event_bus": event_bus is not None,
                "has_ibkr_client": ibkr_client is not None
            }
        )
        # <Context-Aware Logging - TradingManager Initialization Start - End>
        
        # Set core dependencies
        self.tm.data_feed = data_feed
        self.tm.excel_path = excel_path
        self.tm.ibkr_client = ibkr_client
        self.tm.event_bus = event_bus
        self.tm.planned_orders = []
        self.tm.active_orders = {}
        self.tm.monitoring = False
        self.tm.monitor_thread = None
        
        # Initialize tracking sets
        self.tm._execution_symbols = set()
        self.tm._orders_in_progress = set()
        self.tm._last_execution_time = {}
        self.tm._execution_cooldown_seconds = 5
        self.tm.current_account_number = None
        
        # Initialize loading state tracking
        self.tm._excel_loaded = False
        self.tm._excel_load_attempts = 0
        self.tm._db_loaded = False
        self.tm._initialized = False

        # Load configuration first
        self._load_configuration()
        
        # Set config values
        self.tm.total_capital = float(self.tm.trading_config['simulation']['default_equity'])
        self.tm.max_open_orders = self.tm.trading_config['risk_limits']['max_open_orders']
        self.tm.execution_threshold = float(self.tm.trading_config['execution']['fill_probability_threshold'])

        # Initialize database and core services
        self._initialize_database_services(order_persistence_service)
        
        # Initialize service layer
        self._initialize_core_services(risk_config)
        
        # Initialize market hours service
        self.tm.market_hours = MarketHoursService()
        self.tm.last_position_close_check = None

        # Initialize advanced features
        self.tm.enable_advanced_features = enable_advanced_features
        self.tm.market_context_service = None
        self.tm.historical_performance_service = None
        self.tm.prioritization_config = None

        # Initialize prioritization service
        self.tm.prioritization_service = PrioritizationService(
            sizing_service=self.tm.sizing_service,
            config=self.tm.prioritization_config
        )
        
        self.tm.outcome_labeling_service = OutcomeLabelingService(self.tm.db_session)

        # Initialize advanced services if enabled
        if self.tm.enable_advanced_features:
            self._initialize_advanced_services()

        # Initialize components
        self._initialize_components(enable_advanced_features)

        # Set up event bus subscription
        if self.tm.event_bus:
            self.tm.event_bus.subscribe(EventType.PRICE_UPDATE, self.tm._handle_price_update)
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Event bus subscription established via TradingInitializer",
                context_provider={
                    "event_type": "PRICE_UPDATE",
                    "component": "TradingManager"
                }
            )

        # Initialize end of day service
        self._initialize_end_of_day_service()
        
        # <Context-Aware Logging - TradingManager Initialization Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "TradingManager initialization completed successfully via TradingInitializer",
            context_provider={
                "services_initialized": [
                    "OrderExecutionService", "PositionSizingService", "OrderLoadingService",
                    "FillProbabilityEngine", "OrderEligibilityService", "RiskManagementService",
                    "PrioritizationService"
                ],
                "advanced_features_enabled": enable_advanced_features,
                "total_capital": self.tm.total_capital,
                "max_open_orders": self.tm.max_open_orders,
                "execution_threshold": self.tm.execution_threshold
            }
        )
        # <Context-Aware Logging - TradingManager Initialization Complete - End>
        
        return True

    def _initialize_database_services(self, order_persistence_service: Optional[OrderPersistenceService]) -> None:
        """Initialize database-related services."""
        self.tm.db_session = get_db_session()
        self.tm.order_persistence_service = order_persistence_service or OrderPersistenceService(self.tm.db_session)

        # State and reconciliation management
        self.tm.state_service = StateService(self.tm.db_session)
        self.tm.state_service.subscribe('order_state_change', self.tm._handle_order_state_change)
        self.tm.reconciliation_engine = ReconciliationEngine(self.tm.ibkr_client, self.tm.state_service)

    def _initialize_core_services(self, risk_config: Optional[Dict]) -> None:
        """Initialize core business logic services."""
        # Execution service
        self.tm.execution_service = OrderExecutionService(self.tm, self.tm.ibkr_client)
        
        # Sizing service
        self.tm.sizing_service = PositionSizingService(self.tm)
        
        # Loading service
        self.tm.loading_service = OrderLoadingService(
            trading_manager=self.tm,
            db_session=self.tm.db_session,
            config=self.tm.trading_config
        )
        
        # Probability engine
        self.tm.probability_engine = FillProbabilityEngine(
            data_feed=self.tm.data_feed,
            config=self.tm.trading_config
        )
        
        # Eligibility service
        self.tm.eligibility_service = OrderEligibilityService(
            self.tm.probability_engine,
            self.tm.db_session
        )
        
        # Risk management service
        self.tm.risk_service = RiskManagementService(
            state_service=self.tm.state_service,
            persistence_service=self.tm.order_persistence_service,
            ibkr_client=self.tm.ibkr_client,
            config=risk_config
        )

    def _initialize_advanced_services(self) -> None:
        """Initialize advanced feature services if enabled."""
        try:
            self.tm.market_context_service = MarketContextService(self.tm.data_feed)
            self.tm.historical_performance_service = HistoricalPerformanceService(self.tm.db_session)
            
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Advanced trading services initialized via TradingInitializer",
                context_provider={
                    'services': ['MarketContextService', 'HistoricalPerformanceService'],
                    'advanced_features_enabled': True
                }
            )
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Advanced services initialization failed in TradingInitializer",
                context_provider={
                    'error': str(e),
                    'advanced_features_enabled': True
                },
                decision_reason=f"Advanced services initialization error: {e}"
            )

    def _initialize_components(self, enable_advanced_features: bool) -> None:
        """Initialize all component managers and coordinators."""
        # Get monitoring interval from config
        monitoring_config = self.tm.trading_config.get('monitoring', {})
        interval_seconds = monitoring_config.get('interval_seconds', 5)
        
        # Order execution orchestrator
        self.tm.execution_orchestrator = OrderExecutionOrchestrator(
            execution_service=self.tm.execution_service,
            sizing_service=self.tm.sizing_service,
            persistence_service=self.tm.order_persistence_service,
            state_service=self.tm.state_service,
            probability_engine=self.tm.probability_engine,
            ibkr_client=self.tm.ibkr_client,
            config=self.tm.trading_config
        )

        # Monitoring service
        self.tm.monitoring_service = MonitoringService(self.tm.data_feed, interval_seconds)

        # Order loading orchestrator
        self.tm.order_loading_orchestrator = OrderLoadingOrchestrator(
            loading_service=self.tm.loading_service,
            persistence_service=self.tm.order_persistence_service,
            state_service=self.tm.state_service,
            db_session=self.tm.db_session
        )

        # Order lifecycle manager
        self.tm.order_lifecycle_manager = OrderLifecycleManager(
            loading_service=self.tm.loading_service,
            persistence_service=self.tm.order_persistence_service,
            state_service=self.tm.state_service,
            db_session=self.tm.db_session,
            order_loading_orchestrator=self.tm.order_loading_orchestrator
        )

        # Advanced feature coordinator
        self.tm.advanced_features = AdvancedFeatureCoordinator(enable_advanced_features)

        # Basic prioritization service (for backward compatibility)
        self.tm.prioritization_service = PrioritizationService(
            sizing_service=self.tm.sizing_service,
            config=self.tm.prioritization_config
        )
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "TradingManager components initialized via TradingInitializer",
            context_provider={
                'components': [
                    'OrderExecutionOrchestrator', 'MonitoringService', 
                    'OrderLoadingOrchestrator', 'OrderLifecycleManager',
                    'AdvancedFeatureCoordinator', 'PrioritizationService'
                ],
                'monitoring_interval_seconds': interval_seconds,
                'advanced_features_enabled': enable_advanced_features
            }
        )

    def _initialize_end_of_day_service(self) -> None:
        """Initialize the End of Day service with configuration from trading config."""
        try:
            # Get EOD configuration from trading config with defaults
            eod_config_section = self.tm.trading_config.get('end_of_day', {})
            
            eod_config = EODConfig(
                enabled=eod_config_section.get('enabled', True),
                close_buffer_minutes=eod_config_section.get('close_buffer_minutes', 15),
                pre_market_start_minutes=eod_config_section.get('pre_market_start_minutes', 30),
                post_market_end_minutes=eod_config_section.get('post_market_end_minutes', 30),
                max_close_attempts=eod_config_section.get('max_close_attempts', 3)
            )
            
            self.tm.end_of_day_service = EndOfDayService(
                state_service=self.tm.state_service,
                market_hours_service=self.tm.market_hours,
                config=eod_config
            )
            
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "EndOfDayService initialized successfully via TradingInitializer",
                context_provider={
                    'enabled': eod_config.enabled,
                    'close_buffer_minutes': eod_config.close_buffer_minutes,
                    'pre_market_start_minutes': eod_config.pre_market_start_minutes,
                    'post_market_end_minutes': eod_config.post_market_end_minutes
                },
                decision_reason="EOD service startup via TradingInitializer"
            )
            
        except Exception as e:
            # Fallback to default configuration if initialization fails
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "EndOfDayService initialization failed in TradingInitializer, using defaults",
                context_provider={
                    'error_type': type(e).__name__,
                    'error_message': str(e)
                },
                decision_reason="EOD service fallback initialization"
            )
            
            self.tm.end_of_day_service = EndOfDayService(
                state_service=self.tm.state_service,
                market_hours_service=self.tm.market_hours
            )

    def _load_configuration(self) -> None:
        """Load trading configuration for the detected environment."""
        try:
            # Detect environment automatically
            environment = self._get_trading_environment()
            
            # Load appropriate trading core config
            self.tm.trading_config = get_trading_core_config(environment)

            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Trading configuration loaded successfully via TradingInitializer",
                context_provider={
                    'environment': environment,
                    'config_sections': list(self.tm.trading_config.keys()),
                    'max_open_orders': self.tm.trading_config['risk_limits']['max_open_orders'],
                    'fill_probability_threshold': float(self.tm.trading_config['execution']['fill_probability_threshold'])
                }
            )
            
        except Exception as e:
            # Fallback to hardcoded defaults
            self._load_fallback_config()
    
    def _load_fallback_config(self) -> None:
        """Load fallback configuration with hardcoded defaults."""
        self.tm.trading_config = {
            'risk_limits': {
                'max_open_orders': 5,
                'daily_loss_pct': 0.02,
                'weekly_loss_pct': 0.05,
                'monthly_loss_pct': 0.08,
                'max_risk_per_trade': 0.02
            },
            'execution': {
                'fill_probability_threshold': 0.7,
                'min_fill_probability': 0.4
            },
            'order_defaults': {
                'risk_per_trade': 0.005,
                'risk_reward_ratio': 2.0,
                'priority': 3
            },
            'simulation': {
                'default_equity': 100000
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
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Fallback configuration loaded via TradingInitializer due to configuration failure",
            context_provider={
                'reason': 'configuration_load_failed',
                'fallback_max_open_orders': 5,
                'fallback_default_equity': 100000
            },
            decision_reason="Using hardcoded fallback configuration via TradingInitializer"
        )

    def _get_trading_environment(self) -> str:
        """Detect trading environment based on connected account."""
        if self.tm.ibkr_client and self.tm.ibkr_client.connected:
            account_name = self.tm.ibkr_client.get_account_name()
            return detect_trading_environment(account_name)
        else:
            # No IBKR connection = simulation/paper mode
            return 'paper'

    def finalize_initialization(self) -> bool:
        """Complete initialization with advanced services and validation."""
        if self.tm._initialized:
            return True

        if not self.tm.data_feed.is_connected():
            return False

        self._validate_ibkr_connection()

        # Initialize account context
        account_number = self.tm._get_current_account_number()
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Account context initialized via TradingInitializer",
            context_provider={
                'account_number': account_number,
                'data_feed_connected': self.tm.data_feed.is_connected(),
                'ibkr_client_connected': self.tm.ibkr_client.connected if self.tm.ibkr_client else False
            }
        )

        # Initialize advanced services if enabled
        if self.tm.advanced_features.enabled:
            self.tm.advanced_features.initialize_services(
                data_feed=self.tm.data_feed,
                sizing_service=self.tm.sizing_service,
                db_session=self.tm.db_session,
                prioritization_config=self.tm.prioritization_config
            )

        self.tm.execution_service.set_dependencies(self.tm.order_persistence_service, self.tm.active_orders)
        self.tm._initialized = True
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "TradingManager initialization finalized via TradingInitializer",
            context_provider={
                'advanced_features_initialized': self.tm.advanced_features.enabled,
                'execution_service_ready': True,
                'total_initialized_components': 8
            }
        )
        
        return True

    def _validate_ibkr_connection(self) -> bool:
        """Validate that the data feed is connected to IBKR and providing live data."""
        if not self.tm.data_feed.is_connected():
            return False

        from src.market_data.feeds.ibkr_data_feed import IBKRDataFeed
        if not isinstance(self.tm.data_feed, IBKRDataFeed):
            return False

        test_symbol = "SPY"
        price_data = self.tm.data_feed.get_current_price(test_symbol)
        return price_data and price_data.get('price') not in [0, None]