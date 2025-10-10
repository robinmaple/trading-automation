"""
Manages market data subscriptions and price tracking from the broker.
Handles auto-detection of data types (real-time vs. delayed) and provides
a clean interface for accessing current price information.
"""

import datetime
import threading
import time
# <Market Hours Service Import - Begin>
from src.services.market_hours_service import MarketHoursService
# <Market Hours Service Import - End>
# <Event Bus Integration - Begin>
from src.core.event_bus import EventBus
from src.core.events import EventType, PriceUpdateEvent
# <Event Bus Integration - End>
# <Price Filtering Import - Begin>
from decimal import Decimal
from typing import Set
# <Price Filtering Import - End>

# Context-aware logging import
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()


class MarketDataManager:
    """Manages subscriptions to market data and tracks current prices for symbols."""

    def __init__(self, order_executor, event_bus: EventBus = None):
        """Initialize the manager, auto-detecting data type based on account type."""
        self.executor = order_executor
        self.event_bus = event_bus  # <Event Bus Dependency - Begin>
        self.prices = {}  # symbol -> {'price': float, 'timestamp': datetime, 'history': list}
        self.subscriptions = {}  # symbol -> req_id
        self.lock = threading.RLock()
        self.next_req_id = 9000
        # <Enhanced Data Type Detection - Begin>
        self.use_delayed_data = order_executor.is_paper_account
        self.market_hours = MarketHoursService()
        self.data_type_errors = {}  # Track errors per symbol for fallback
        
        # Log environment detection
        env = "PAPER" if self.use_delayed_data else "PRODUCTION"
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "MarketDataManager initialized with environment detection",
            context_provider={
                "environment": env,
                "event_bus_provided": event_bus is not None,
                "market_hours_service_initialized": True,
                "initial_request_id": self.next_req_id
            },
            decision_reason="MARKET_DATA_MANAGER_INITIALIZED"
        )
        # <Enhanced Data Type Detection - End>
        
        # <Price Filtering Configuration - Begin>
        # Default filtering configuration
        self.filter_config = {
            'min_percent_change': Decimal('0.01'),  # 0.01% minimum change
            'min_absolute_change': Decimal('0.05'),  # $0.05 minimum change
            'enabled': True  # Enable filtering by default
        }
        # Track symbols that should receive events (PlannedOrder symbols + positions)
        self.monitored_symbols: Set[str] = set()
        # <Price Filtering Configuration - End>
        
        # <Data Type Configuration Removed - Will be determined per subscription>

    # <Price Filtering Methods - Begin>
    def update_filter_config(self, config: dict) -> None:
        """Update price filtering configuration."""
        with self.lock:
            if 'min_percent_change' in config:
                self.filter_config['min_percent_change'] = Decimal(str(config['min_percent_change']))
            if 'min_absolute_change' in config:
                self.filter_config['min_absolute_change'] = Decimal(str(config['min_absolute_change']))
            if 'enabled' in config:
                self.filter_config['enabled'] = config['enabled']
            
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Price filter configuration updated",
                context_provider={
                    "new_min_percent_change": float(self.filter_config['min_percent_change']),
                    "new_min_absolute_change": float(self.filter_config['min_absolute_change']),
                    "filtering_enabled": self.filter_config['enabled'],
                    "config_source": "manual_update"
                },
                decision_reason="PRICE_FILTER_CONFIG_UPDATED"
            )

    def set_monitored_symbols(self, symbols: Set[str]) -> None:
        """Set the symbols that should receive price events (PlannedOrder symbols + positions)."""
        with self.lock:
            previous_count = len(self.monitored_symbols)
            self.monitored_symbols = symbols.copy()
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Monitored symbols updated",
                context_provider={
                    "previous_symbols_count": previous_count,
                    "new_symbols_count": len(self.monitored_symbols),
                    "symbols_added": len(symbols - set(self.monitored_symbols)) if previous_count > 0 else len(symbols),
                    "symbols_removed": len(set(self.monitored_symbols) - symbols) if previous_count > 0 else 0,
                    "monitored_symbols_list": sorted(list(symbols))
                },
                decision_reason="MONITORED_SYMBOLS_UPDATED"
            )

    def add_monitored_symbol(self, symbol: str) -> None:
        """Add a symbol to the monitored set."""
        with self.lock:
            was_monitored = symbol in self.monitored_symbols
            self.monitored_symbols.add(symbol)
            
            if not was_monitored:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Symbol added to monitoring",
                    symbol=symbol,
                    context_provider={
                        "previous_monitoring_state": False,
                        "new_monitoring_state": True,
                        "total_monitored_symbols": len(self.monitored_symbols)
                    },
                    decision_reason="SYMBOL_ADDED_TO_MONITORING"
                )

    def remove_monitored_symbol(self, symbol: str) -> None:
        """Remove a symbol from the monitored set."""
        with self.lock:
            was_monitored = symbol in self.monitored_symbols
            self.monitored_symbols.discard(symbol)
            
            if was_monitored:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Symbol removed from monitoring",
                    symbol=symbol,
                    context_provider={
                        "previous_monitoring_state": True,
                        "new_monitoring_state": False,
                        "total_monitored_symbols": len(self.monitored_symbols)
                    },
                    decision_reason="SYMBOL_REMOVED_FROM_MONITORING"
                )

    def _should_publish_price_update(self, symbol: str, new_price: float, old_price: float) -> bool:
        """
        Determine if a price update should be published based on filtering rules.
        
        Rules:
        1. If filtering is disabled, publish all updates
        2. Only publish for monitored symbols
        3. Publish if price change meets either percentage OR absolute threshold (whichever is higher)
        4. Always publish first price (old_price == 0)
        """
        # If filtering is disabled, publish all updates
        if not self.filter_config['enabled']:
            return True
            
        # Only publish for monitored symbols
        if symbol not in self.monitored_symbols:
            return False
            
        # Always publish first price
        if old_price == 0.0:
            return True
            
        # Calculate changes
        price_change = abs(new_price - old_price)
        percent_change = (Decimal(str(price_change)) / Decimal(str(old_price))) * Decimal('100')
        
        # Check if change meets either threshold
        min_absolute = self.filter_config['min_absolute_change']
        min_percent = self.filter_config['min_percent_change']
        
        meets_absolute = price_change >= float(min_absolute)
        meets_percent = percent_change >= min_percent
        
        should_publish = meets_absolute or meets_percent
        
        # Log filtering decision for significant changes
        if should_publish and price_change > 0:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Significant price change detected",
                symbol=symbol,
                context_provider={
                    "old_price": old_price,
                    "new_price": new_price,
                    "absolute_change": price_change,
                    "percent_change": float(percent_change),
                    "absolute_threshold": float(min_absolute),
                    "percent_threshold": float(min_percent),
                    "meets_absolute_threshold": meets_absolute,
                    "meets_percent_threshold": meets_percent,
                    "filtering_enabled": True
                },
                decision_reason="PRICE_UPDATE_PASSED_FILTER"
            )
        
        return should_publish
    # <Price Filtering Methods - End>

    def subscribe(self, symbol, contract) -> None:
        """Subscribe to market data for a symbol, with fallback to snapshot data on failure."""
        with self.lock:
            if symbol in self.subscriptions:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Symbol already subscribed",
                    symbol=symbol,
                    context_provider={
                        "existing_request_id": self.subscriptions[symbol],
                        "current_price": self.prices[symbol].get('price', 0) if symbol in self.prices else 0,
                        "subscription_count": len(self.subscriptions)
                    },
                    decision_reason="DUPLICATE_SUBSCRIPTION_ATTEMPT"
                )
                return

            req_id = self.next_req_id
            self.next_req_id += 1

            try:
                # <Smart Data Type Selection - Begin>
                data_type = self._determine_optimal_data_type(symbol)
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Requesting market data subscription",
                    symbol=symbol,
                    context_provider={
                        "request_id": req_id,
                        "selected_data_type": data_type,
                        "data_type_description": self._get_data_type_description(data_type),
                        "market_open": self.market_hours.is_market_open(),
                        "paper_account": self.use_delayed_data,
                        "previous_errors": self.data_type_errors.get(symbol, 0)
                    },
                    decision_reason="MARKET_DATA_SUBSCRIPTION_REQUESTED"
                )
                
                self.executor.reqMarketDataType(data_type)
                self.executor.reqMktData(req_id, contract, "", False, False, [])
                # <Smart Data Type Selection - End>

                self.subscriptions[symbol] = req_id
                self.prices[symbol] = {
                    'price': 0.0,
                    'timestamp': None,
                    'history': [],
                    'type': 'PENDING',
                    'updates': 0,
                    'data_type': 'pending',
                    'requested_type': data_type  # <Track requested data type - Begin>
                }
                
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Market data subscription established",
                    symbol=symbol,
                    context_provider={
                        "request_id": req_id,
                        "data_type": data_type,
                        "total_subscriptions": len(self.subscriptions),
                        "contract_details": {
                            "symbol": contract.symbol,
                            "sec_type": contract.secType,
                            "exchange": contract.exchange,
                            "currency": contract.currency
                        }
                    },
                    decision_reason="MARKET_DATA_SUBSCRIPTION_SUCCESS"
                )

            except Exception as e:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Initial subscription failed",
                    symbol=symbol,
                    context_provider={
                        "request_id": req_id,
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "data_type_attempted": data_type,
                        "operation": "subscribe"
                    },
                    decision_reason="MARKET_DATA_SUBSCRIPTION_FAILED"
                )
                self._handle_subscription_error(symbol, contract, req_id, e)

    # <New Method - Smart Data Type Determination - Begin>
    def _determine_optimal_data_type(self, symbol: str) -> int:
        """
        Determine the best market data type based on account, market hours, and previous errors.
        
        IBKR Data Types:
        1 = Live (Real-time) - Requires subscription & market open
        2 = Frozen - Last traded price when market closed  
        3 = Delayed - 15-20 min delayed (free)
        4 = Delayed Frozen - Delayed + frozen behavior
        """
        # Reset error count if market just opened
        if self.market_hours.is_market_open():
            self.data_type_errors.pop(symbol, None)
        
        # Check for previous errors with this symbol
        if symbol in self.data_type_errors:
            error_count = self.data_type_errors[symbol]
            if error_count >= 2:  # After 2 errors, use delayed data
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using delayed data due to previous errors",
                    symbol=symbol,
                    context_provider={
                        "previous_errors": error_count,
                        "selected_data_type": 3,
                        "reason": "error_count_exceeded_threshold"
                    },
                    decision_reason="DELAYED_DATA_SELECTED_DUE_TO_ERRORS"
                )
                return 3
        
        # Live account: try real-time first, with market hours awareness
        if not self.use_delayed_data:
            if self.market_hours.is_market_open():
                return 1  # Real-time during market hours
            else:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Market closed - using frozen data",
                    symbol=symbol,
                    context_provider={
                        "market_open": False,
                        "selected_data_type": 2,
                        "reason": "market_closed"
                    },
                    decision_reason="FROZEN_DATA_SELECTED_MARKET_CLOSED"
                )
                return 2  # Frozen data when market closed
        
        # Paper account: use delayed data
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Paper account - using delayed data",
            symbol=symbol,
            context_provider={
                "account_type": "paper",
                "selected_data_type": 3,
                "reason": "paper_account_default"
            },
            decision_reason="DELAYED_DATA_SELECTED_PAPER_ACCOUNT"
        )
        return 3

    def _get_data_type_description(self, data_type: int) -> str:
        """Get human-readable description of IBKR data type."""
        descriptions = {
            1: "LIVE_REAL_TIME",
            2: "FROZEN", 
            3: "DELAYED",
            4: "DELAYED_FROZEN"
        }
        return descriptions.get(data_type, f"UNKNOWN_{data_type}")
    # <New Method - Smart Data Type Determination - End>

    # <Enhanced Error Handling Method - Begin>
    def _handle_subscription_error(self, symbol: str, contract, req_id: int, error: Exception):
        """Handle subscription errors with intelligent fallback strategy."""
        error_msg = str(error).lower()
        
        # Track errors for this symbol
        self.data_type_errors[symbol] = self.data_type_errors.get(symbol, 0) + 1
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Handling subscription error",
            symbol=symbol,
            context_provider={
                "request_id": req_id,
                "error_type": type(error).__name__,
                "error_message": error_msg,
                "error_count_for_symbol": self.data_type_errors[symbol],
                "fallback_strategy": "intelligent_fallback"
            },
            decision_reason="SUBSCRIPTION_ERROR_HANDLING_STARTED"
        )
        
        # Permission errors (10167) - downgrade to delayed data
        if '10167' in error_msg or 'permission' in error_msg or 'not subscribed' in error_msg:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No real-time permissions - trying delayed data",
                symbol=symbol,
                context_provider={
                    "error_category": "permission_denied",
                    "fallback_action": "try_delayed_data"
                },
                decision_reason="PERMISSION_ERROR_FALLBACK_TO_DELAYED"
            )
            self._try_delayed_data(symbol, contract, req_id)
        # Market closed or frozen data issues - try snapshot
        elif 'frozen' in error_msg or 'closed' in error_msg:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Market data unavailable - trying snapshot",
                symbol=symbol,
                context_provider={
                    "error_category": "market_unavailable",
                    "fallback_action": "try_snapshot_data"
                },
                decision_reason="MARKET_UNAVAILABLE_FALLBACK_TO_SNAPSHOT"
            )
            self._try_snapshot_data(symbol, contract, req_id)
        else:
            # Generic error - try snapshot as last resort
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Generic error - trying snapshot as last resort",
                symbol=symbol,
                context_provider={
                    "error_category": "generic_error",
                    "fallback_action": "try_snapshot_data"
                },
                decision_reason="GENERIC_ERROR_FALLBACK_TO_SNAPSHOT"
            )
            self._try_snapshot_data(symbol, contract, req_id)
    # <Enhanced Error Handling Method - End>

    # <New Method - Delayed Data Fallback - Begin>
    def _try_delayed_data(self, symbol: str, contract, req_id: int) -> None:
        """Attempt to subscribe to delayed data as fallback."""
        try:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Attempting delayed data fallback",
                symbol=symbol,
                context_provider={
                    "request_id": req_id,
                    "data_type_attempted": 3,
                    "fallback_attempt": "delayed_data"
                }
            )
            
            self.executor.reqMarketDataType(3)  # Delayed data
            self.executor.reqMktData(req_id, contract, "", False, False, [])
            
            self.subscriptions[symbol] = req_id
            self.prices[symbol] = {
                'price': 0.0,
                'timestamp': None,
                'history': [],
                'type': 'PENDING',
                'updates': 0,
                'data_type': 'delayed',
                'requested_type': 3
            }
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Delayed data subscription successful",
                symbol=symbol,
                context_provider={
                    "request_id": req_id,
                    "data_type": "delayed",
                    "fallback_success": True
                },
                decision_reason="DELAYED_DATA_SUBSCRIPTION_SUCCESS"
            )
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Delayed data fallback failed",
                symbol=symbol,
                context_provider={
                    "request_id": req_id,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "fallback_attempt": "delayed_data",
                    "next_action": "try_snapshot_data"
                },
                decision_reason="DELAYED_DATA_FALLBACK_FAILED"
            )
            self._try_snapshot_data(symbol, contract, req_id)
    # <New Method - Delayed Data Fallback - End>

    def _try_snapshot_data(self, symbol, contract, req_id) -> None:
        """Attempt to subscribe to snapshot data as a fallback when streaming fails."""
        try:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Attempting snapshot data fallback",
                symbol=symbol,
                context_provider={
                    "request_id": req_id,
                    "fallback_attempt": "snapshot_data",
                    "last_resort": True
                }
            )
            
            self.executor.reqMktData(req_id, contract, "", True, False, [])
            
            self.subscriptions[symbol] = req_id
            self.prices[symbol] = {
                'price': 0.0,
                'timestamp': None,
                'history': [],
                'type': 'SNAPSHOT',
                'updates': 0,
                'data_type': 'snapshot',
                'requested_type': 'snapshot'  # <Track snapshot requests - Begin>
            }
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Snapshot data subscription successful",
                symbol=symbol,
                context_provider={
                    "request_id": req_id,
                    "data_type": "snapshot",
                    "fallback_success": True,
                    "data_availability": "single_snapshot_only"
                },
                decision_reason="SNAPSHOT_DATA_SUBSCRIPTION_SUCCESS"
            )

        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "All subscription attempts failed",
                symbol=symbol,
                context_provider={
                    "request_id": req_id,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "total_fallback_attempts": 2,
                    "final_outcome": "no_market_data_available"
                },
                decision_reason="ALL_SUBSCRIPTION_ATTEMPTS_FAILED"
            )

    def get_current_price(self, symbol) -> dict:
        """Get the current price data dictionary for a subscribed symbol."""
        with self.lock:
            if symbol in self.prices:
                return self.prices[symbol]
        return None

    def on_tick_price(self, req_id, tick_type, price, attrib) -> None:
        """Handle incoming market data price ticks from the IBKR API."""
        if tick_type in [1, 2, 4]:  # BID, ASK, LAST
            tick_type_name = {1: 'BID', 2: 'ASK', 4: 'LAST'}.get(tick_type, 'UNKNOWN')

            with self.lock:
                for symbol, sub_req_id in self.subscriptions.items():
                    if sub_req_id == req_id:
                        data = self.prices[symbol]
                        old_price = data['price']
                        data['price'] = price
                        data['type'] = tick_type_name
                        data['timestamp'] = datetime.datetime.now()
                        data['updates'] += 1
                        data['history'].append({
                            'price': price,
                            'type': tick_type_name,
                            'timestamp': datetime.datetime.now()
                        })

                        if len(data['history']) > 100:
                            data['history'].pop(0)
                        
                        # <Enhanced Price Update Logging - Begin>
                        if old_price == 0.0 and price > 0:
                            data_type = data.get('data_type', 'unknown')
                            context_logger.log_event(
                                TradingEventType.MARKET_CONDITION,
                                "First price update received",
                                symbol=symbol,
                                context_provider={
                                    "price": price,
                                    "price_type": tick_type_name,
                                    "data_type": data_type,
                                    "request_id": req_id,
                                    "total_updates": data['updates'],
                                    "subscription_age_seconds": (datetime.datetime.now() - data.get('timestamp', datetime.datetime.now())).total_seconds() if data.get('timestamp') else 0
                                },
                                decision_reason="FIRST_PRICE_UPDATE_RECEIVED"
                            )
                        # <Enhanced Price Update Logging - End>
                        
                        # <Price Event Publishing with Filtering - Begin>
                        # Publish price update event if event bus is available and price is valid
                        if self.event_bus and price > 0:
                            # Apply filtering logic
                            if self._should_publish_price_update(symbol, price, old_price):
                                event = PriceUpdateEvent(
                                    event_type=EventType.PRICE_UPDATE,  # Add required parameter
                                    symbol=symbol,
                                    price=price,
                                    price_type=tick_type_name,
                                    source="MarketDataManager"
                                )
                                self.event_bus.publish(event)
                                
                                if old_price == 0.0:  # Log first event publication
                                    context_logger.log_event(
                                        TradingEventType.MARKET_CONDITION,
                                        "First price event published",
                                        symbol=symbol,
                                        context_provider={
                                            "price": price,
                                            "price_type": tick_type_name,
                                            "event_bus_available": True,
                                            "event_type": "PriceUpdateEvent"
                                        },
                                        decision_reason="FIRST_PRICE_EVENT_PUBLISHED"
                                    )
                        # <Price Event Publishing with Filtering - End>
                        break

    def subscribe_with_retry(self, symbol, contract, retries=2) -> bool:
        """Subscribe to market data for a symbol with retry logic for unreliable connections."""
        for attempt in range(retries + 1):
            try:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Subscription attempt started",
                    symbol=symbol,
                    context_provider={
                        "attempt_number": attempt + 1,
                        "max_attempts": retries + 1,
                        "request_id": self.next_req_id + attempt
                    }
                )
                
                self.subscribe(symbol, contract)
                # <Verify Subscription Success - Begin>
                time.sleep(1)  # Wait for initial price update
                price_data = self.get_current_price(symbol)
                if price_data and price_data.get('price', 0) > 0:
                    context_logger.log_event(
                        TradingEventType.MARKET_CONDITION,
                        "Subscription verified successful",
                        symbol=symbol,
                        context_provider={
                            "attempt_number": attempt + 1,
                            "final_price": price_data['price'],
                            "data_type": price_data.get('data_type', 'unknown'),
                            "total_attempts_used": attempt + 1
                        },
                        decision_reason="SUBSCRIPTION_VERIFIED_SUCCESS"
                    )
                    return True
                elif attempt < retries:
                    context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Subscription got zero price - retrying",
                        symbol=symbol,
                        context_provider={
                            "attempt_number": attempt + 1,
                            "current_price": price_data.get('price', 0) if price_data else 0,
                            "remaining_attempts": retries - attempt
                        },
                        decision_reason="SUBSCRIPTION_RETRY_ZERO_PRICE"
                    )
                # <Verify Subscription Success - End>
            except Exception as e:
                if attempt < retries:
                    context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Subscription attempt failed - retrying",
                        symbol=symbol,
                        context_provider={
                            "attempt_number": attempt + 1,
                            "error_type": type(e).__name__,
                            "error_details": str(e),
                            "remaining_attempts": retries - attempt
                        },
                        decision_reason="SUBSCRIPTION_RETRY_ERROR"
                    )
                    time.sleep(1)
                else:
                    context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "All subscription attempts failed",
                        symbol=symbol,
                        context_provider={
                            "total_attempts": retries + 1,
                            "final_error_type": type(e).__name__,
                            "final_error_details": str(e),
                            "outcome": "subscription_failed"
                        },
                        decision_reason="ALL_SUBSCRIPTION_ATTEMPTS_FAILED"
                    )
                    return False
        return False