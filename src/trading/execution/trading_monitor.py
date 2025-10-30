"""
TradingMonitor - Handles trading monitoring and event processing logic.
Manages monitoring loops, market hours checks, operational windows, and event handling.
"""

import datetime
import time
import threading
from typing import Optional
from src.core.events import PriceUpdateEvent
from src.core.context_aware_logger import TradingEventType


class TradingMonitor:
    """Handles trading monitoring, event processing, and operational window management."""
    
    def __init__(self, trading_manager):
        self.tm = trading_manager
        self.context_logger = trading_manager.context_logger
        
    def start_monitoring(self, interval_seconds: Optional[int] = None) -> bool:
        """Start the continuous monitoring loop with automatic initialization."""
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting trading monitoring process via TradingMonitor",
            context_provider={
                'provided_interval_seconds': interval_seconds,
                'data_feed_connected': self.tm.data_feed.is_connected(),
                'planned_orders_count': len(self.tm.planned_orders),
                'initialized': self.tm._initialized
            }
        )
        
        if not self.tm.initializer.finalize_initialization():
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Monitoring start failed - initialization unsuccessful",
                context_provider={
                    'data_feed_connected': self.tm.data_feed.is_connected(),
                    'initialized': self.tm._initialized
                },
                decision_reason="System initialization failed, monitoring cannot start"
            )
            return False

        if self.tm.ibkr_client and self.tm.ibkr_client.connected:
            self.tm.reconciliation_engine.start()

        if not self.tm.data_feed.is_connected():
            raise Exception("Data feed not connected")

        # Update monitoring interval if provided
        if interval_seconds is not None:
            self.tm.monitoring_service.set_monitoring_interval(interval_seconds)

        # Initialize monitored symbols before starting monitoring
        self.tm._update_monitored_symbols()

        # Symbol subscription is now handled by event system - keep for backward compatibility
        self.tm._subscribe_to_planned_order_symbols()

        self.tm.debug_order_status()
        
        # Start monitoring service
        success = self.tm.monitoring_service.start_monitoring(
            check_callback=self.tm.orchestrator.check_and_execute_orders,
            label_callback=self.tm._label_completed_orders
        )
        
        if success:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Trading monitoring started successfully via TradingMonitor",
                context_provider={
                    'monitoring_interval': interval_seconds,
                    'planned_orders_monitored': len(self.tm.planned_orders),
                    'reconciliation_engine_active': self.tm.ibkr_client.connected if self.tm.ibkr_client else False
                },
                decision_reason="Monitoring service started successfully via TradingMonitor"
            )
        else:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Trading monitoring failed to start via TradingMonitor",
                context_provider={
                    'monitoring_interval': interval_seconds
                },
                decision_reason="Monitoring service returned failure via TradingMonitor"
            )
        
        return success

    def stop_monitoring(self) -> None:
        """Stop the monitoring loop and perform cleanup of resources."""
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Stopping trading monitoring and performing cleanup via TradingMonitor",
            context_provider={
                'monitoring_active': self.tm.monitoring,
                'active_orders_count': len(self.tm.active_orders),
                'planned_orders_count': len(self.tm.planned_orders)
            }
        )
        
        self.tm.monitoring_service.stop_monitoring()
        self.tm.reconciliation_engine.stop()
        if self.tm.db_session:
            self.tm.db_session.close()
            
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Trading monitoring stopped and resources cleaned up via TradingMonitor",
            context_provider={
                'monitoring_active': False,
                'db_session_closed': True,
                'reconciliation_engine_stopped': True
            }
        )

    def handle_price_update(self, event: PriceUpdateEvent) -> None:
        """Handle price update events and trigger order execution checks."""
        try:
            # Only process if we have planned orders
            if not self.tm.planned_orders:
                return
                
            # Check if this symbol is in our execution symbols (has executable orders)
            symbol_has_executable_orders = event.symbol in self.tm._execution_symbols
            
            # Always process price updates for symbols with executable orders
            # Bypass any filtering that might block execution flow
            if symbol_has_executable_orders:
                self.context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    f"Execution price update received - bypassing filtering",
                    symbol=event.symbol,
                    context_provider={
                        'price': event.price,
                        'timestamp': event.timestamp,
                        'execution_symbols_count': len(self.tm._execution_symbols),
                        'symbol_has_executable_orders': True
                    }
                )
                
                # Trigger immediate execution check for this symbol
                self.tm.orchestrator.check_and_execute_orders()
                
            else:
                # For other symbols, check if they're in planned orders
                symbol_in_planned_orders = any(order.symbol == event.symbol for order in self.tm.planned_orders)
                if symbol_in_planned_orders:
                    self.context_logger.log_event(
                        TradingEventType.MARKET_CONDITION,
                        f"Price update received for monitored symbol",
                        symbol=event.symbol,
                        context_provider={
                            'price': event.price,
                            'timestamp': event.timestamp,
                            'monitored_symbols_count': len(self.tm.planned_orders),
                            'symbol_has_executable_orders': False
                        }
                    )
                    
                    # Regular processing for monitored symbols
                    self.tm.orchestrator.check_and_execute_orders()
                
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error processing price update for {event.symbol}",
                symbol=event.symbol,
                context_provider={
                    'error': str(e),
                    'price': event.price if hasattr(event, 'price') else 'unknown',
                    'symbol_has_executable_orders': event.symbol in self.tm._execution_symbols if hasattr(self.tm, '_execution_symbols') else 'unknown'
                },
                decision_reason=f"Price update processing failed: {e}"
            )

    def run_monitoring_loop(self, interval_seconds: int) -> None:
        """Main monitoring loop for Phase A with error handling and recovery."""
        monitoring_config = self.tm.trading_config.get('monitoring', {})
        max_errors = monitoring_config.get('max_errors', 10)
        error_backoff_base = monitoring_config.get('error_backoff_base', 60)
        max_backoff = monitoring_config.get('max_backoff', 300)
        
        error_count = 0

        self.tm.monitoring_service.subscribe_to_symbols(self.tm.planned_orders)

        while self.tm.monitoring and error_count < max_errors:
            try:
                # Operational Window Check
                if not self.should_run_in_operational_window():
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Outside operational window - skipping monitoring cycle",
                        context_provider={
                            'current_time': datetime.datetime.now().isoformat(),
                            'operational_window_active': False
                        },
                        decision_reason="Outside pre/post market operational hours"
                    )
                    time.sleep(interval_seconds)
                    continue

                self.tm.orchestrator.check_and_execute_orders()
                self.tm.orchestrator.check_market_close_actions()
                
                # End of Day Process Integration
                self.run_end_of_day_process()
                
                error_count = 0
                time.sleep(interval_seconds)
            except Exception:
                error_count += 1
                backoff_time = min(error_backoff_base * error_count, max_backoff)
                time.sleep(backoff_time)

    def should_run_in_operational_window(self) -> bool:
        """
        Check if the system should run based on operational window configuration.
        Only runs 30min pre-market to 30min post-market by default.
        """
        # Always run during market hours
        if self.tm.market_hours.is_market_open():
            return True
            
        # Check if EOD service is available and in operational window
        if hasattr(self.tm, 'end_of_day_service'):
            return self.tm.end_of_day_service.should_run_eod_process()
            
        # Fallback: use MarketHoursService for basic operational window check
        return self.is_in_basic_operational_window()

    def is_in_basic_operational_window(self) -> bool:
        """Basic operational window check as fallback."""
        now_et = datetime.datetime.now(self.tm.market_hours.et_timezone)
        current_time = now_et.time()
        current_weekday = now_et.weekday()
        
        # Only run on weekdays
        if current_weekday >= 5:  # Saturday, Sunday
            return False

        # Pre-market window (30 minutes before market open)
        pre_market_start = (
            datetime.datetime.combine(now_et.date(), self.tm.market_hours.MARKET_OPEN) - 
            datetime.timedelta(minutes=30)
        ).time()

        # Post-market window (30 minutes after market close)
        post_market_end = (
            datetime.datetime.combine(now_et.date(), self.tm.market_hours.MARKET_CLOSE) + 
            datetime.timedelta(minutes=30)
        ).time()

        # Check if in pre-market window
        if pre_market_start <= current_time < self.tm.market_hours.MARKET_OPEN:
            return True

        # Check if in post-market window
        if self.tm.market_hours.MARKET_CLOSE <= current_time <= post_market_end:
            return True

        return False

    def run_end_of_day_process(self) -> None:
        """Execute the End of Day process for position management."""
        if not hasattr(self.tm, 'end_of_day_service'):
            return
            
        try:
            eod_results = self.tm.end_of_day_service.run_eod_process()
            
            # Log EOD process results
            if eod_results.get('status') == 'completed' and (
                eod_results.get('day_positions_closed', 0) > 0 or 
                eod_results.get('hybrid_positions_closed', 0) > 0
            ):
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    "EOD process executed successfully via TradingMonitor",
                    context_provider={
                        'day_positions_closed': eod_results.get('day_positions_closed', 0),
                        'hybrid_positions_closed': eod_results.get('hybrid_positions_closed', 0),
                        'orders_expired': eod_results.get('orders_expired', 0),
                        'error_count': len(eod_results.get('errors', []))
                    },
                    decision_reason="EOD position management completed via TradingMonitor"
                )
                
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "EOD process execution failed via TradingMonitor",
                context_provider={
                    'error_type': type(e).__name__,
                    'error_message': str(e)
                },
                decision_reason=f"EOD process exception via TradingMonitor: {e}"
            )

    def validate_data_source(self) -> None:
        """Perform a quick validation of the data source and connection."""
        from src.market_data.feeds.ibkr_data_feed import IBKRDataFeed
        is_ibkr = isinstance(self.tm.data_feed, IBKRDataFeed)
        is_connected = self.tm.data_feed.is_connected()
        
        if self.tm.planned_orders:
            test_symbol = self.tm.planned_orders[0].symbol
            # Use MonitoringService instead of direct data feed access
            current_price = self.tm.monitoring_service.get_current_price(test_symbol)
            if current_price:
                pass
            else:
                pass