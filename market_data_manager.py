# market_data_manager.py
import datetime
import threading
import time


class MarketDataManager:
    def __init__(self, order_executor):
        self.executor = order_executor
        self.prices = {}  # symbol -> {'price': float, 'timestamp': datetime, 'history': list}
        self.subscriptions = {}  # symbol -> req_id
        self.lock = threading.RLock()
        self.next_req_id = 9000

        # Auto-detect data type based on account
        self.use_delayed_data = order_executor.is_paper_account
        
        data_type = "FROZEN DELAYED" if self.use_delayed_data else "REAL-TIME"
        env = "PAPER" if self.use_delayed_data else "PRODUCTION"
        print(f"📊 Auto-configured: {env} environment → {data_type} market data")

    def subscribe(self, symbol, contract):
        """Subscribe to market data with paper account fallbacks"""
        with self.lock:
            if symbol in self.subscriptions:
                return
            
            req_id = self.next_req_id
            self.next_req_id += 1
            
            try:
                # Try delayed data first
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

    def _try_snapshot_data(self, symbol, contract, req_id):
        """Try to get snapshot data when streaming fails"""
        try:
            # Request snapshot instead of streaming
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
                        
    def get_current_price(self, symbol):
        """Get current price data for a symbol"""
        with self.lock:
            if symbol in self.prices:
                return self.prices[symbol]
        return None
    
    def on_tick_price(self, req_id, tick_type, price, attrib):
        """Handle incoming market data with detailed logging"""
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
                        
                        # Keep limited history
                        if len(data['history']) > 100:
                            data['history'].pop(0)
                        
                        # Optional: Log each update for debugging
                        # print(f"📈 {symbol} {tick_type_name}: ${price}")
                        break

    def subscribe_with_retry(self, symbol, contract, retries=2):
        """Subscribe with retry logic for unreliable connections"""
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