"""
Manages market data subscriptions and price tracking from the broker.
Handles auto-detection of data types (real-time vs. delayed) and provides
a clean interface for accessing current price information.
"""

import datetime
import threading
import time


class MarketDataManager:
    """Manages subscriptions to market data and tracks current prices for symbols."""

    def __init__(self, order_executor):
        """Initialize the manager, auto-detecting data type based on account type."""
        self.executor = order_executor
        self.prices = {}  # symbol -> {'price': float, 'timestamp': datetime, 'history': list}
        self.subscriptions = {}  # symbol -> req_id
        self.lock = threading.RLock()
        self.next_req_id = 9000
        self.use_delayed_data = order_executor.is_paper_account

        data_type = "FROZEN DELAYED" if self.use_delayed_data else "REAL-TIME"
        env = "PAPER" if self.use_delayed_data else "PRODUCTION"
        print(f"📊 Auto-configured: {env} environment → {data_type} market data")

    def subscribe(self, symbol, contract) -> None:
        """Subscribe to market data for a symbol, with fallback to snapshot data on failure."""
        with self.lock:
            if symbol in self.subscriptions:
                return

            req_id = self.next_req_id
            self.next_req_id += 1

            try:
                data_type = 1  # Frozen delayed data, 3 Delayed data
                self.executor.reqMarketDataType(data_type)
                self.executor.reqMktData(req_id, contract, "", False, False, [])

                self.subscriptions[symbol] = req_id
                self.prices[symbol] = {
                    'price': 0.0,
                    'timestamp': None,
                    'history': [],
                    'type': 'PENDING',
                    'updates': 0,
                    'data_type': 'delayed'
                }
                print(f"✅ Subscribed to {symbol} with FROZEN DELAYED data")

            except Exception as e:
                print(f"❌ Frozen delayed data failed for {symbol}: {e}")
                print("💡 Trying snapshot data as fallback...")
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
                'data_type': 'snapshot'
            }
            print(f"✅ Using SNAPSHOT data for {symbol} (paper account limits)")

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
                        data['price'] = price
                        data['type'] = tick_type_name
                        data['timestamp'] = datetime.datetime.now()
                        data['history'].append(price)

                        if len(data['history']) > 100:
                            data['history'].pop(0)
                        break

    def subscribe_with_retry(self, symbol, contract, retries=2) -> bool:
        """Subscribe to market data for a symbol with retry logic for unreliable connections."""
        for attempt in range(retries + 1):
            try:
                self.subscribe(symbol, contract)
                return True
            except Exception as e:
                if attempt < retries:
                    print(f"⚠️  Subscription attempt {attempt + 1} failed, retrying...")
                    time.sleep(1)
                else:
                    print(f"❌ All subscription attempts failed for {symbol}: {e}")
                    return False
        return False