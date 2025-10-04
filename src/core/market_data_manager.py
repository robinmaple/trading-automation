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
from src.core.events import PriceUpdateEvent
# <Event Bus Integration - End>
# <Price Filtering Import - Begin>
from decimal import Decimal
from typing import Set
# <Price Filtering Import - End>


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
        print(f"📊 Auto-detected: {env} account environment")
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
            
            print(f"🔧 Price filter updated: {self.filter_config}")

    def set_monitored_symbols(self, symbols: Set[str]) -> None:
        """Set the symbols that should receive price events (PlannedOrder symbols + positions)."""
        with self.lock:
            self.monitored_symbols = symbols.copy()
            print(f"🔍 Monitoring {len(self.monitored_symbols)} symbols for price events: {sorted(list(symbols))}")

    def add_monitored_symbol(self, symbol: str) -> None:
        """Add a symbol to the monitored set."""
        with self.lock:
            self.monitored_symbols.add(symbol)

    def remove_monitored_symbol(self, symbol: str) -> None:
        """Remove a symbol from the monitored set."""
        with self.lock:
            self.monitored_symbols.discard(symbol)

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
        
        # Debug logging for significant changes
        if should_publish and price_change > 0:
            print(f"📈 Significant price change for {symbol}: ${old_price:.2f} → ${new_price:.2f} "
                  f"(Δ${price_change:.2f}, {percent_change:.2f}%)")
        
        return should_publish
    # <Price Filtering Methods - End>

    def subscribe(self, symbol, contract) -> None:
        """Subscribe to market data for a symbol, with fallback to snapshot data on failure."""
        with self.lock:
            if symbol in self.subscriptions:
                return

            req_id = self.next_req_id
            self.next_req_id += 1

            try:
                # <Smart Data Type Selection - Begin>
                data_type = self._determine_optimal_data_type(symbol)
                print(f"🔍 Requesting market data type {data_type} for {symbol}")
                
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
                print(f"✅ Subscribed to {symbol} with data type {data_type}")

            except Exception as e:
                print(f"❌ Initial subscription failed for {symbol}: {e}")
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
                print(f"⚠️  Using delayed data for {symbol} due to previous errors")
                return 3
        
        # Live account: try real-time first, with market hours awareness
        if not self.use_delayed_data:
            if self.market_hours.is_market_open():
                return 1  # Real-time during market hours
            else:
                print(f"⏰ Market closed - using frozen data for {symbol}")
                return 2  # Frozen data when market closed
        
        # Paper account: use delayed data
        return 3
    # <New Method - Smart Data Type Determination - End>

    # <Enhanced Error Handling Method - Begin>
    def _handle_subscription_error(self, symbol: str, contract, req_id: int, error: Exception):
        """Handle subscription errors with intelligent fallback strategy."""
        error_msg = str(error).lower()
        
        # Track errors for this symbol
        self.data_type_errors[symbol] = self.data_type_errors.get(symbol, 0) + 1
        
        # Permission errors (10167) - downgrade to delayed data
        if '10167' in error_msg or 'permission' in error_msg or 'not subscribed' in error_msg:
            print(f"🔁 No real-time permissions for {symbol}, trying delayed data...")
            self._try_delayed_data(symbol, contract, req_id)
        # Market closed or frozen data issues - try snapshot
        elif 'frozen' in error_msg or 'closed' in error_msg:
            print(f"🔁 Market data unavailable for {symbol}, trying snapshot...")
            self._try_snapshot_data(symbol, contract, req_id)
        else:
            # Generic error - try snapshot as last resort
            print(f"🔁 Generic error for {symbol}, trying snapshot...")
            self._try_snapshot_data(symbol, contract, req_id)
    # <Enhanced Error Handling Method - End>

    # <New Method - Delayed Data Fallback - Begin>
    def _try_delayed_data(self, symbol: str, contract, req_id: int) -> None:
        """Attempt to subscribe to delayed data as fallback."""
        try:
            print(f"📡 Attempting delayed data (type 3) for {symbol}...")
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
            print(f"✅ Using DELAYED data for {symbol}")
            
        except Exception as e:
            print(f"❌ Delayed data also failed for {symbol}: {e}")
            self._try_snapshot_data(symbol, contract, req_id)
    # <New Method - Delayed Data Fallback - End>

    def _try_snapshot_data(self, symbol, contract, req_id) -> None:
        """Attempt to subscribe to snapshot data as a fallback when streaming fails."""
        try:
            # <Enhanced Snapshot Logging - Begin>
            print(f"📸 Attempting snapshot data for {symbol}...")
            self.executor.reqMktData(req_id, contract, "", True, False, [])
            # <Enhanced Snapshot Logging - End>
            
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
            print(f"✅ Using SNAPSHOT data for {symbol}")

        except Exception as e:
            print(f"❌ Snapshot also failed for {symbol}: {e}")
            print(f"💡 No market data available for {symbol} - manual entry required")

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
                            print(f"💰 FIRST PRICE UPDATE for {symbol}: ${price} ({tick_type_name}) - Data type: {data_type}")
                        # <Enhanced Price Update Logging - End>
                        
                        # <Price Event Publishing with Filtering - Begin>
                        # Publish price update event if event bus is available and price is valid
                        if self.event_bus and price > 0:
                            # Apply filtering logic
                            if self._should_publish_price_update(symbol, price, old_price):
                                event = PriceUpdateEvent(
                                    symbol=symbol,
                                    price=price,
                                    price_type=tick_type_name,
                                    source="MarketDataManager"
                                )
                                self.event_bus.publish(event)
                                if old_price == 0.0:  # Log first event publication
                                    print(f"📢 Published FIRST price event for {symbol}: ${price}")
                            # else: # Debug logging for filtered events
                            #     print(f"🔇 Filtered price update for {symbol}: ${old_price} → ${price}")
                        # <Price Event Publishing with Filtering - End>
                        break

    def subscribe_with_retry(self, symbol, contract, retries=2) -> bool:
        """Subscribe to market data for a symbol with retry logic for unreliable connections."""
        for attempt in range(retries + 1):
            try:
                self.subscribe(symbol, contract)
                # <Verify Subscription Success - Begin>
                time.sleep(1)  # Wait for initial price update
                price_data = self.get_current_price(symbol)
                if price_data and price_data.get('price', 0) > 0:
                    return True
                elif attempt < retries:
                    print(f"⚠️  Subscription attempt {attempt + 1} got price 0, retrying...")
                # <Verify Subscription Success - End>
            except Exception as e:
                if attempt < retries:
                    print(f"⚠️  Subscription attempt {attempt + 1} failed, retrying...")
                    time.sleep(1)
                else:
                    print(f"❌ All subscription attempts failed for {symbol}: {e}")
                    return False
        return False