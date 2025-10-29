"""
Manages market data subscriptions and price tracking from the broker.
Handles auto-detection of data types (real-time vs. delayed) and provides
a clean interface for accessing current price information.
"""

import datetime
import threading
import time
from src.services.market_hours_service import MarketHoursService
from src.core.event_bus import EventBus
from src.core.events import EventType, PriceUpdateEvent
from decimal import Decimal
from typing import Any, Dict, Set
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()


class MarketDataManager:
    """Manages subscriptions to market data and tracks current prices for symbols."""

    def __init__(self, order_executor, event_bus: EventBus = None):
        """Initialize the manager, auto-detecting data type based on account type."""
        self.executor = order_executor
        self.event_bus = event_bus
        self.prices = {}  # symbol -> {'price': float, 'timestamp': datetime, 'history': list}
        self.subscriptions = {}  # symbol -> req_id
        self.lock = threading.RLock()
        self.next_req_id = 9000
        
        # Callback tracking for flow verification
        self._callback_stats = {
            'total_ticks_received': 0,
            'ticks_by_symbol': {},
            'last_tick_time': None,
            'first_tick_time': None,
            'callback_errors': 0
        }
        
        # Data type detection
        self.use_delayed_data = order_executor.is_paper_account
        self.market_hours = MarketHoursService()
        self.data_type_errors = {}  # Track errors per symbol for fallback
        
        # Minimal initialization logging
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Market data manager ready",
            context_provider={
                "environment": "PAPER" if self.use_delayed_data else "PRODUCTION",
                "event_bus_ready": event_bus is not None
            }
        )
        
        # Price filtering configuration
        self.filter_config = {
            'min_percent_change': Decimal('0.01'),
            'min_absolute_change': Decimal('0.05'),
            'enabled': True
        }
        self.monitored_symbols: Set[str] = set()
        
        # Execution symbols tracking
        self._execution_symbols: Set[str] = set()

    def on_tick_price(self, req_id, tick_type, price, attrib) -> None:
        """Handle incoming market data price ticks with minimal debugging."""
        # Find symbol for this request ID
        symbol_found = None
        for symbol, sub_req_id in self.subscriptions.items():
            if sub_req_id == req_id:
                symbol_found = symbol
                break
        
        # Track callback statistics
        self._callback_stats['total_ticks_received'] += 1
        current_time = datetime.datetime.now()
        self._callback_stats['last_tick_time'] = current_time
        
        # Log first callback for flow verification
        if not self._callback_stats['first_tick_time']:
            self._callback_stats['first_tick_time'] = current_time
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "First market data received",
                context_provider={
                    "req_id": req_id,
                    "symbol": symbol_found or "UNKNOWN"
                }
            )
        
        if tick_type in [1, 2, 4]:  # BID, ASK, LAST
            with self.lock:
                for symbol, sub_req_id in self.subscriptions.items():
                    if sub_req_id == req_id:
                        # Initialize price data if needed
                        if symbol not in self.prices:
                            self.prices[symbol] = {
                                'price': 0.0, 
                                'timestamp': None,
                                'history': [],
                                'type': 'PENDING',
                                'updates': 0,
                                'data_type': 'live'
                            }
                        
                        data = self.prices[symbol]
                        old_price = data['price']
                        
                        # Track ticks by symbol
                        if symbol not in self._callback_stats['ticks_by_symbol']:
                            self._callback_stats['ticks_by_symbol'][symbol] = 0
                        self._callback_stats['ticks_by_symbol'][symbol] += 1
                        
                        # Update price data
                        tick_type_name = {1: 'BID', 2: 'ASK', 4: 'LAST'}.get(tick_type, 'OTHER')
                        data['price'] = price
                        data['type'] = tick_type_name
                        data['timestamp'] = current_time
                        data['updates'] += 1
                        
                        # Maintain history (limited size)
                        data['history'].append({
                            'price': price,
                            'type': tick_type_name,
                            'timestamp': current_time
                        })
                        if len(data['history']) > 100:
                            data['history'].pop(0)
                        
                        # Log first price for each symbol
                        if old_price == 0.0 and price > 0:
                            context_logger.log_event(
                                TradingEventType.MARKET_CONDITION,
                                "First price received",
                                symbol=symbol,
                                context_provider={
                                    "price": price,
                                    "price_type": tick_type_name
                                }
                            )
                        
                        # Price Event Publishing - CRITICAL EXECUTION LOGIC
                        if self.event_bus and price > 0:
                            is_execution_symbol = symbol in self._execution_symbols
                            should_publish = self._should_publish_price_update(symbol, price, old_price)
                            
                            # Console output for execution symbols
                            if is_execution_symbol:
                                print(f"🎯 EXECUTION: {symbol} {tick_type_name} ${price:.2f}")
                            
                            if should_publish:
                                event = PriceUpdateEvent(
                                    event_type=EventType.PRICE_UPDATE,
                                    symbol=symbol,
                                    price=price,
                                    price_type=tick_type_name,
                                    source="MarketDataManager"
                                )
                                self.event_bus.publish(event)
                                
                                # Log only execution events and first prices
                                if is_execution_symbol:
                                    context_logger.log_event(
                                        TradingEventType.MARKET_CONDITION,
                                        "Execution price published",
                                        symbol=symbol,
                                        context_provider={
                                            "price": price,
                                            "price_type": tick_type_name
                                        }
                                    )
                        
                        break

    def verify_callback_receipt(self, symbol: str = None, timeout_seconds: int = 10) -> Dict[str, Any]:
        """Verify that callbacks are being received for subscribed symbols."""
        verification = {
            'timestamp': datetime.datetime.now(),
            'callback_flow_verified': False,
            'total_ticks_received': self._callback_stats['total_ticks_received'],
            'last_tick_time': self._callback_stats['last_tick_time'],
            'first_tick_time': self._callback_stats['first_tick_time'],
            'details': {}
        }
        
        # Check if we've received any callbacks at all
        if self._callback_stats['total_ticks_received'] > 0:
            verification['callback_flow_verified'] = True
            verification['details']['overall_status'] = 'CALLBACKS_FLOWING'
        else:
            verification['details']['overall_status'] = 'NO_CALLBACKS_RECEIVED'
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No callbacks received",
                context_provider=verification
            )
        
        return verification

    def get_callback_stats(self) -> Dict[str, Any]:
        """Get comprehensive callback statistics for monitoring."""
        with self.lock:
            stats = self._callback_stats.copy()
            
            # Add subscription information
            stats['total_subscriptions'] = len(self.subscriptions)
            stats['execution_symbols_count'] = len(self._execution_symbols)
            stats['monitored_symbols_count'] = len(self.monitored_symbols)
            
            # Calculate time since last tick
            if stats['last_tick_time']:
                stats['seconds_since_last_tick'] = (datetime.datetime.now() - stats['last_tick_time']).total_seconds()
            else:
                stats['seconds_since_last_tick'] = None
                
            return stats

    def subscribe(self, symbol, contract) -> None:
        """Subscribe to market data for a symbol, with fallback to snapshot data on failure."""
        with self.lock:
            if symbol in self.subscriptions:
                return  # Already subscribed

            req_id = self.next_req_id
            self.next_req_id += 1

            try:
                data_type = self._determine_optimal_data_type(symbol)
                
                # Only log subscription for execution symbols
                if symbol in self._execution_symbols:
                    context_logger.log_event(
                        TradingEventType.MARKET_CONDITION,
                        "Subscribing execution symbol",
                        symbol=symbol,
                        context_provider={
                            "request_id": req_id,
                            "data_type": data_type
                        }
                    )
                
                self.executor.reqMarketDataType(data_type)
                self.executor.reqMktData(req_id, contract, "", False, False, [])

                self.subscriptions[symbol] = req_id
                self.prices[symbol] = {
                    'price': 0.0,
                    'timestamp': None,
                    'history': [],
                    'type': 'PENDING',
                    'updates': 0,
                    'data_type': 'pending',
                    'requested_type': data_type,
                    'subscription_time': datetime.datetime.now()
                }
                
                # Initialize callback tracking for this symbol
                if symbol not in self._callback_stats['ticks_by_symbol']:
                    self._callback_stats['ticks_by_symbol'][symbol] = 0

            except Exception as e:
                # Log subscription errors
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Subscription failed",
                    symbol=symbol,
                    context_provider={
                        "error": str(e),
                        "data_type": data_type
                    }
                )
                self._handle_subscription_error(symbol, contract, req_id, e)

    def update_filter_config(self, config: dict) -> None:
        """Update price filtering configuration."""
        with self.lock:
            if 'min_percent_change' in config:
                self.filter_config['min_percent_change'] = Decimal(str(config['min_percent_change']))
            if 'min_absolute_change' in config:
                self.filter_config['min_absolute_change'] = Decimal(str(config['min_absolute_change']))
            if 'enabled' in config:
                self.filter_config['enabled'] = config['enabled']

    def set_monitored_symbols(self, symbols: Set[str]) -> None:
        """Set the symbols that should receive price events (PlannedOrder symbols + positions)."""
        with self.lock:
            previous_count = len(self.monitored_symbols)
            self.monitored_symbols = symbols.copy()
            
            # Only log if significant change
            if abs(len(self.monitored_symbols) - previous_count) > 2:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Monitored symbols updated",
                    context_provider={
                        "new_count": len(self.monitored_symbols)
                    }
                )

    def add_monitored_symbol(self, symbol: str) -> None:
        """Add a symbol to the monitored set."""
        with self.lock:
            was_monitored = symbol in self.monitored_symbols
            self.monitored_symbols.add(symbol)

    def remove_monitored_symbol(self, symbol: str) -> None:
        """Remove a symbol from the monitored set."""
        with self.lock:
            was_monitored = symbol in self.monitored_symbols
            self.monitored_symbols.discard(symbol)

    def _should_publish_price_update(self, symbol: str, new_price: float, old_price: float) -> bool:
        """Determine if a price update should be published based on filtering rules."""
        # CRITICAL: Always publish for execution symbols (bypass all filtering)
        if symbol in self._execution_symbols:
            return True
            
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
        
        return meets_absolute or meets_percent

    def _determine_optimal_data_type(self, symbol: str) -> int:
        """Determine the best market data type based on account, market hours, and previous errors."""
        # Reset error count if market just opened
        if self.market_hours.is_market_open():
            self.data_type_errors.pop(symbol, None)
        
        # Check for previous errors with this symbol
        if symbol in self.data_type_errors:
            error_count = self.data_type_errors[symbol]
            if error_count >= 2:  # After 2 errors, use delayed data
                return 3
        
        # Live account: try real-time first, with market hours awareness
        if not self.use_delayed_data:
            if self.market_hours.is_market_open():
                return 1  # Real-time during market hours
            else:
                return 2  # Frozen data when market closed
        
        # Paper account: use delayed data
        return 3

    def _handle_subscription_error(self, symbol: str, contract, req_id: int, error: Exception):
        """Handle subscription errors with intelligent fallback strategy."""
        error_msg = str(error).lower()
        
        # Track errors for this symbol
        self.data_type_errors[symbol] = self.data_type_errors.get(symbol, 0) + 1
        
        # Permission errors (10167) - downgrade to delayed data
        if '10167' in error_msg or 'permission' in error_msg or 'not subscribed' in error_msg:
            self._try_delayed_data(symbol, contract, req_id)
        # Market closed or frozen data issues - try snapshot
        elif 'frozen' in error_msg or 'closed' in error_msg:
            self._try_snapshot_data(symbol, contract, req_id)
        else:
            # Generic error - try snapshot as last resort
            self._try_snapshot_data(symbol, contract, req_id)

    def _try_delayed_data(self, symbol: str, contract, req_id: int) -> None:
        """Attempt to subscribe to delayed data as fallback."""
        try:
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
            
        except Exception as e:
            self._try_snapshot_data(symbol, contract, req_id)

    def _try_snapshot_data(self, symbol, contract, req_id) -> None:
        """Attempt to subscribe to snapshot data as a fallback when streaming fails."""
        try:
            self.executor.reqMktData(req_id, contract, "", True, False, [])
            
            self.subscriptions[symbol] = req_id
            self.prices[symbol] = {
                'price': 0.0,
                'timestamp': None,
                'history': [],
                'type': 'SNAPSHOT',
                'updates': 0,
                'data_type': 'snapshot',
                'requested_type': 'snapshot'
            }

        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "All subscription attempts failed",
                symbol=symbol,
                context_provider={
                    "error": str(e)
                }
            )

    def get_current_price(self, symbol) -> dict:
        """Get the current price data dictionary for a subscribed symbol."""
        with self.lock:
            if symbol in self.prices:
                return self.prices[symbol]
        return None

    def subscribe_with_retry(self, symbol, contract, retries=2) -> bool:
        """Subscribe to market data for a symbol with retry logic for unreliable connections."""
        for attempt in range(retries + 1):
            try:
                self.subscribe(symbol, contract)
                # Verify subscription success
                time.sleep(1)  # Wait for initial price update
                price_data = self.get_current_price(symbol)
                if price_data and price_data.get('price', 0) > 0:
                    return True
            except Exception as e:
                if attempt == retries:  # Only log final failure
                    context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Subscription failed after retries",
                        symbol=symbol,
                        context_provider={
                            "attempts": retries + 1,
                            "error": str(e)
                        }
                    )
                time.sleep(1)
        return False
    
    def set_execution_symbols(self, execution_symbols: Set[str]) -> None:
        """Set symbols that require unfiltered price events for order execution."""
        with self.lock:
            previous_count = len(self._execution_symbols)
            self._execution_symbols = execution_symbols.copy() if execution_symbols else set()
            
            # CRITICAL: Ensure execution symbols are also in monitored symbols
            self.monitored_symbols.update(self._execution_symbols)
            
            # Only log if significant change
            if abs(len(self._execution_symbols) - previous_count) > 0:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Execution symbols updated",
                    context_provider={
                        "count": len(self._execution_symbols),
                        "symbols": list(self._execution_symbols)[:3]  # Log first 3
                    }
                )