"""
Manages the continuous monitoring loop for market data and order execution.
Handles market data subscriptions, periodic checks, and error recovery.
Provides robust monitoring with configurable intervals and error handling.
"""

import threading
import time
import datetime
from typing import Set, Dict, List, Optional, Callable
from src.core.abstract_data_feed import AbstractDataFeed
from src.core.planned_order import PlannedOrder

# Context-aware logging import
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()


class MonitoringService:
    """Service for continuous market monitoring and order execution checks."""
    
    def __init__(self, data_feed: AbstractDataFeed, interval_seconds: int = 5):
        """Initialize the monitoring service with data feed and monitoring interval."""
        self.data_feed = data_feed
        self.interval_seconds = interval_seconds
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.subscribed_symbols: Set[str] = set()
        self.market_data_updates: Dict[str, int] = {}
        self.last_labeling_time: Optional[datetime.datetime] = None
        self.error_count = 0
        self.max_errors = 10
        self._check_callback: Optional[Callable] = None
        self._label_callback: Optional[Callable] = None
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "MonitoringService initialized",
            context_provider={
                "data_feed_provided": data_feed is not None,
                "data_feed_type": type(data_feed).__name__ if data_feed else "None",
                "initial_interval_seconds": interval_seconds,
                "max_errors_allowed": self.max_errors
            },
            decision_reason="MONITORING_SERVICE_INITIALIZED"
        )
        
    def start_monitoring(self, check_callback: Callable, label_callback: Callable) -> bool:
        """Start the monitoring loop with provided order check and labeling callbacks."""
        if not self.data_feed.is_connected():
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Cannot start monitoring - data feed not connected",
                context_provider={
                    "data_feed_connected": False,
                    "monitoring_requested": True
                },
                decision_reason="MONITORING_START_FAILED_DATA_FEED_DISCONNECTED"
            )
            return False
            
        self._check_callback = check_callback
        self._label_callback = label_callback
        self.monitoring = True
        
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True,
            name="MonitoringServiceThread"
        )
        self.monitor_thread.start()
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Monitoring started successfully",
            context_provider={
                "monitoring_interval_seconds": self.interval_seconds,
                "check_callback_provided": check_callback is not None,
                "label_callback_provided": label_callback is not None,
                "thread_name": self.monitor_thread.name,
                "thread_daemon": self.monitor_thread.daemon
            },
            decision_reason="MONITORING_SERVICE_STARTED"
        )
        return True
        
    def stop_monitoring(self) -> None:
        """Stop the monitoring loop and clean up resources."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
            if self.monitor_thread.is_alive():
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Monitoring thread did not terminate cleanly",
                    context_provider={
                        "join_timeout_seconds": 5.0,
                        "thread_still_alive": True
                    },
                    decision_reason="MONITORING_THREAD_TERMINATION_WARNING"
                )
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Monitoring stopped",
            context_provider={
                "final_error_count": self.error_count,
                "active_subscriptions": len(self.subscribed_symbols),
                "total_market_updates": sum(self.market_data_updates.values())
            },
            decision_reason="MONITORING_SERVICE_STOPPED"
        )
        
    def _monitoring_loop(self) -> None:
        """Main monitoring loop with error handling and recovery mechanisms."""
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Monitoring loop started",
            context_provider={
                "thread_name": threading.current_thread().name,
                "monitoring_interval_seconds": self.interval_seconds,
                "initial_error_count": self.error_count
            },
            decision_reason="MONITORING_LOOP_STARTED"
        )
        
        while self.monitoring and self.error_count < self.max_errors:
            try:
                # Execute the main check callback
                if self._check_callback:
                    self._check_callback()
                
                # Handle periodic labeling
                self._handle_periodic_labeling()
                
                # Reset error counter on successful iteration
                self.error_count = 0
                
                # Sleep for the configured interval
                time.sleep(self.interval_seconds)
                
            except Exception as e:
                self._handle_monitoring_error(e)
                
        if self.error_count >= self.max_errors:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Too many errors - stopping monitoring",
                context_provider={
                    "final_error_count": self.error_count,
                    "max_errors_allowed": self.max_errors,
                    "monitoring_cycles_completed": "unknown",  # Could track this if needed
                    "automatic_shutdown": True
                },
                decision_reason="MONITORING_STOPPED_DUE_TO_ERRORS"
            )
            self.monitoring = False
            
    def _handle_monitoring_error(self, error: Exception) -> None:
        """Handle monitoring errors with exponential backoff."""
        self.error_count += 1
        backoff_time = min(60 * self.error_count, 300)
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Monitoring error encountered",
            context_provider={
                "error_count": self.error_count,
                "max_errors": self.max_errors,
                "error_type": type(error).__name__,
                "error_details": str(error),
                "backoff_time_seconds": backoff_time,
                "backoff_strategy": "exponential_with_cap"
            },
            decision_reason="MONITORING_ERROR_HANDLED"
        )
        
        time.sleep(backoff_time)
        
    def _handle_periodic_labeling(self) -> None:
        """Handle periodic order labeling based on configured interval."""
        if not self._label_callback:
            return
            
        current_time = datetime.datetime.now()
        labeling_interval = datetime.timedelta(minutes=10)  # Label every 10 minutes
        
        if (self.last_labeling_time is None or 
            current_time - self.last_labeling_time >= labeling_interval):
            try:
                self._label_callback()
                self.last_labeling_time = current_time
                
                context_logger.log_event(
                    TradingEventType.STATE_TRANSITION,
                    "Periodic order labeling completed",
                    context_provider={
                        "labeling_interval_minutes": 10,
                        "previous_labeling_time": self.last_labeling_time.isoformat() if self.last_labeling_time else None,
                        "current_labeling_time": current_time.isoformat(),
                        "time_since_last_labeling_seconds": (current_time - self.last_labeling_time).total_seconds() if self.last_labeling_time else 0
                    },
                    decision_reason="PERIODIC_ORDER_LABELING_COMPLETED"
                )
            except Exception as e:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Periodic labeling failed",
                    context_provider={
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "labeling_interval_minutes": 10,
                        "last_successful_labeling": self.last_labeling_time.isoformat() if self.last_labeling_time else None
                    },
                    decision_reason="PERIODIC_LABELING_FAILED"
                )
                
    def subscribe_to_symbols(self, orders: List[PlannedOrder]) -> Dict[str, bool]:
        """Subscribe to market data for all symbols in planned orders."""
        if not orders:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "No orders provided for subscription",
                context_provider={
                    "orders_provided": 0,
                    "action_taken": "skip_subscription"
                },
                decision_reason="NO_ORDERS_FOR_SUBSCRIPTION"
            )
            return {}
            
        subscription_results = {}
        successful_subscriptions = 0
        
        context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Starting symbol subscriptions",
            context_provider={
                "total_orders": len(orders),
                "existing_subscriptions": len(self.subscribed_symbols),
                "symbols_to_subscribe": [order.symbol for order in orders]
            },
            decision_reason="SYMBOL_SUBSCRIPTION_BATCH_STARTED"
        )
        
        for order in orders:
            symbol = order.symbol
            if symbol in self.subscribed_symbols:
                subscription_results[symbol] = True
                successful_subscriptions += 1
                continue
                
            try:
                contract = order.to_ib_contract()
                success = self.data_feed.subscribe(symbol, contract)
                
                if success:
                    self.subscribed_symbols.add(symbol)
                    self.market_data_updates[symbol] = 0
                    subscription_results[symbol] = True
                    successful_subscriptions += 1
                    
                    context_logger.log_event(
                        TradingEventType.MARKET_CONDITION,
                        "Symbol subscription successful",
                        symbol=symbol,
                        context_provider={
                            "order_symbol": symbol,
                            "contract_details": {
                                "security_type": contract.secType,
                                "exchange": contract.exchange,
                                "currency": contract.currency
                            },
                            "total_successful_subscriptions": successful_subscriptions
                        },
                        decision_reason="SYMBOL_SUBSCRIPTION_SUCCESS"
                    )
                else:
                    subscription_results[symbol] = False
                    context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Symbol subscription failed",
                        symbol=symbol,
                        context_provider={
                            "order_symbol": symbol,
                            "failure_reason": "data_feed_subscribe_returned_false",
                            "data_feed_connected": self.data_feed.is_connected()
                        },
                        decision_reason="SYMBOL_SUBSCRIPTION_FAILED"
                    )
                    
            except Exception as e:
                subscription_results[symbol] = False
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Symbol subscription error",
                    symbol=symbol,
                    context_provider={
                        "order_symbol": symbol,
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "data_feed_connected": self.data_feed.is_connected()
                    },
                    decision_reason="SYMBOL_SUBSCRIPTION_ERROR"
                )
                
        context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Symbol subscription batch completed",
            context_provider={
                "total_attempted": len(orders),
                "successful_subscriptions": successful_subscriptions,
                "failed_subscriptions": len(orders) - successful_subscriptions,
                "success_rate_percent": (successful_subscriptions / len(orders)) * 100 if orders else 0,
                "new_total_subscriptions": len(self.subscribed_symbols),
                "subscription_results_summary": {
                    symbol: "success" if success else "failed"
                    for symbol, success in subscription_results.items()
                }
            },
            decision_reason="SYMBOL_SUBSCRIPTION_BATCH_COMPLETED"
        )
        
        return subscription_results
        
    def unsubscribe_from_symbol(self, symbol: str) -> bool:
        """Unsubscribe from market data for a specific symbol."""
        if symbol not in self.subscribed_symbols:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Symbol not subscribed - no unsubscribe needed",
                symbol=symbol,
                context_provider={
                    "symbol": symbol,
                    "currently_subscribed": False
                },
                decision_reason="SYMBOL_UNSUBSCRIBE_NOT_NEEDED"
            )
            return True
            
        try:
            success = self.data_feed.unsubscribe(symbol)
            if success:
                self.subscribed_symbols.remove(symbol)
                update_count = self.market_data_updates.pop(symbol, 0)
                
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Symbol unsubscribed successfully",
                    symbol=symbol,
                    context_provider={
                        "symbol": symbol,
                        "market_data_updates_received": update_count,
                        "remaining_subscriptions": len(self.subscribed_symbols)
                    },
                    decision_reason="SYMBOL_UNSUBSCRIBE_SUCCESS"
                )
            else:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Symbol unsubscribe failed",
                    symbol=symbol,
                    context_provider={
                        "symbol": symbol,
                        "failure_reason": "data_feed_unsubscribe_returned_false",
                        "still_subscribed": True
                    },
                    decision_reason="SYMBOL_UNSUBSCRIBE_FAILED"
                )
            return success
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Symbol unsubscribe error",
                symbol=symbol,
                context_provider={
                    "symbol": symbol,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "still_subscribed": True
                },
                decision_reason="SYMBOL_UNSUBSCRIBE_ERROR"
            )
            return False
            
    def unsubscribe_all(self) -> None:
        """Unsubscribe from all currently subscribed symbols."""
        symbols_to_unsubscribe = list(self.subscribed_symbols)
        successful_unsubscribes = 0
        
        context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Starting bulk unsubscribe from all symbols",
            context_provider={
                "total_symbols_to_unsubscribe": len(symbols_to_unsubscribe),
                "symbols_list": symbols_to_unsubscribe
            },
            decision_reason="BULK_UNSUBSCRIBE_STARTED"
        )
        
        for symbol in symbols_to_unsubscribe:
            if self.unsubscribe_from_symbol(symbol):
                successful_unsubscribes += 1
                
        context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Bulk unsubscribe completed",
            context_provider={
                "total_attempted": len(symbols_to_unsubscribe),
                "successful_unsubscribes": successful_unsubscribes,
                "failed_unsubscribes": len(symbols_to_unsubscribe) - successful_unsubscribes,
                "success_rate_percent": (successful_unsubscribes / len(symbols_to_unsubscribe)) * 100 if symbols_to_unsubscribe else 0,
                "final_subscription_count": len(self.subscribed_symbols)
            },
            decision_reason="BULK_UNSUBSCRIBE_COMPLETED"
        )
        
    def get_subscription_stats(self) -> Dict[str, any]:
        """Get statistics about current subscriptions and market data updates."""
        total_updates = sum(self.market_data_updates.values())
        avg_updates = total_updates / len(self.market_data_updates) if self.market_data_updates else 0
        most_active_symbol = self._get_most_active_symbol()
        
        stats = {
            'total_subscriptions': len(self.subscribed_symbols),
            'subscribed_symbols': list(self.subscribed_symbols),
            'total_market_data_updates': total_updates,
            'average_updates_per_symbol': avg_updates,
            'most_active_symbol': most_active_symbol,
            'monitoring_active': self.monitoring,
            'error_count': self.error_count
        }
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Subscription statistics retrieved",
            context_provider=stats,
            decision_reason="SUBSCRIPTION_STATS_RETRIEVED"
        )
        
        return stats
        
    def _get_most_active_symbol(self) -> Optional[str]:
        """Get the symbol with the most market data updates."""
        if not self.market_data_updates:
            return None
            
        most_active = max(self.market_data_updates.items(), key=lambda x: x[1])
        return most_active[0]
        
    def record_market_data_update(self, symbol: str) -> None:
        """Record a market data update for a symbol (called by data feed callbacks)."""
        if symbol in self.market_data_updates:
            self.market_data_updates[symbol] += 1
            
            # Log every 100 updates for active symbols
            if self.market_data_updates[symbol] % 100 == 0:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Symbol reached market data update milestone",
                    symbol=symbol,
                    context_provider={
                        "symbol": symbol,
                        "total_updates": self.market_data_updates[symbol],
                        "milestone": "every_100_updates"
                    },
                    decision_reason="MARKET_DATA_UPDATE_MILESTONE"
                )
        
    def is_symbol_subscribed(self, symbol: str) -> bool:
        """Check if a symbol is currently subscribed for market data."""
        is_subscribed = symbol in self.subscribed_symbols
        
        # Only log when checking non-subscribed symbols to reduce noise
        if not is_subscribed:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Symbol subscription status checked - not subscribed",
                symbol=symbol,
                context_provider={
                    "symbol": symbol,
                    "is_subscribed": False,
                    "total_subscribed_symbols": len(self.subscribed_symbols)
                }
            )
            
        return is_subscribed
        
    def get_market_data_for_symbol(self, symbol: str) -> Optional[Dict]:
        """Get current market data for a subscribed symbol."""
        if not self.is_symbol_subscribed(symbol):
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Market data requested for non-subscribed symbol",
                symbol=symbol,
                context_provider={
                    "symbol": symbol,
                    "is_subscribed": False,
                    "action_taken": "return_none"
                },
                decision_reason="MARKET_DATA_REQUEST_FOR_NON_SUBSCRIBED_SYMBOL"
            )
            return None
            
        try:
            market_data = self.data_feed.get_current_price(symbol)
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Market data retrieved successfully",
                symbol=symbol,
                context_provider={
                    "symbol": symbol,
                    "data_available": market_data is not None,
                    "price_in_data": market_data.get('price') if market_data else None,
                    "data_feed_connected": self.data_feed.is_connected()
                },
                decision_reason="MARKET_DATA_RETRIEVAL_SUCCESS"
            )
            return market_data
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Market data retrieval failed",
                symbol=symbol,
                context_provider={
                    "symbol": symbol,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "data_feed_connected": self.data_feed.is_connected()
                },
                decision_reason="MARKET_DATA_RETRIEVAL_FAILED"
            )
            return None
        
    def set_monitoring_interval(self, interval_seconds: int) -> None:
        """Update the monitoring interval (takes effect on next iteration)."""
        if interval_seconds < 1:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Invalid monitoring interval requested",
                context_provider={
                    "requested_interval": interval_seconds,
                    "minimum_allowed": 1,
                    "action_taken": "reject_change"
                },
                decision_reason="INVALID_MONITORING_INTERVAL_REJECTED"
            )
            return
            
        old_interval = self.interval_seconds
        self.interval_seconds = interval_seconds
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Monitoring interval updated",
            context_provider={
                "old_interval_seconds": old_interval,
                "new_interval_seconds": interval_seconds,
                "change_percent": ((interval_seconds - old_interval) / old_interval) * 100 if old_interval else 100
            },
            decision_reason="MONITORING_INTERVAL_UPDATED"
        )
        
    def reset_error_count(self) -> None:
        """Reset the error counter for monitoring recovery."""
        old_count = self.error_count
        self.error_count = 0
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Error counter reset",
            context_provider={
                "previous_error_count": old_count,
                "new_error_count": 0,
                "reset_reason": "manual_reset"
            },
            decision_reason="MONITORING_ERROR_COUNT_RESET"
        )
        
    def is_healthy(self) -> bool:
        """Check if the monitoring service is healthy and functioning properly."""
        is_healthy = (self.monitoring and 
                     self.error_count < self.max_errors and 
                     self.data_feed.is_connected())
        
        # Only log health checks when unhealthy to reduce noise
        if not is_healthy:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Monitoring service health check failed",
                context_provider={
                    "monitoring_active": self.monitoring,
                    "error_count": self.error_count,
                    "max_errors": self.max_errors,
                    "data_feed_connected": self.data_feed.is_connected(),
                    "health_status": "unhealthy"
                },
                decision_reason="MONITORING_SERVICE_HEALTH_CHECK_FAILED"
            )
            
        return is_healthy