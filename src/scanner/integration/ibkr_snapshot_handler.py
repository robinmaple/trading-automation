# src/scanner/integration/ibkr_snapshot_handler.py
import threading
import time
from typing import Dict, Optional
from datetime import datetime
from ibapi.contract import Contract

class IBKRSnapshotHandler:
    """
    Handles one-time snapshot requests from IBKR
    Properly manages async data reception
    """
    
    def __init__(self, market_data_manager):
        self.market_data = market_data_manager
        self._pending_requests = {}
        self._results = {}
        self._lock = threading.Lock()
        self._next_req_id = 5000
        
        # Connect to IBKR callbacks if possible
        self._connect_to_ibkr_callbacks()
    
    def _connect_to_ibkr_callbacks(self):
        """Connect our handler to IBKR's price callbacks"""
        try:
            # If MarketDataManager has a way to register callbacks, use it
            if hasattr(self.market_data, 'register_tick_handler'):
                self.market_data.register_tick_handler(self.on_tick_price)
                print("✅ Snapshot handler connected to MarketDataManager callbacks")
            else:
                print("⚠️  No callback registration available in MarketDataManager")
        except Exception as e:
            print(f"❌ Could not connect to IBKR callbacks: {e}")
    
    def request_snapshot(self, symbol: str, contract: Contract, timeout: int = 10) -> Optional[Dict]:
        """
        Request a one-time snapshot for a symbol
        Returns price data or None if timeout/failure
        """
        req_id = self._get_next_req_id()  # FIXED: Get unique ID for each request
        
        with self._lock:
            self._pending_requests[req_id] = {
                'symbol': symbol,
                'request_time': datetime.now(),
                'completed': False
            }
        
        try:
            print(f"📡 IBKR Snapshot: Requesting {symbol} (ReqID: {req_id})")
            
            # Request snapshot data (snapshot=True for one-time)
            self.market_data.executor.reqMktData(req_id, contract, "", True, False, [])
            
            # Wait for response
            return self._wait_for_response(req_id, symbol, timeout)
            
        except Exception as e:
            print(f"❌ IBKR Snapshot failed for {symbol}: {e}")
            self._cleanup_request(req_id)
            return None
    
    def _wait_for_response(self, req_id: int, symbol: str, timeout: int) -> Optional[Dict]:
        """Wait for snapshot response with proper async handling"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Check if we have a result
            with self._lock:
                if req_id in self._results:
                    result = self._results[req_id]
                    del self._results[req_id]
                    self._cleanup_request(req_id)
                    
                    if result and result.get('price', 0) > 0:
                        print(f"✅ IBKR Snapshot: {symbol} = ${result['price']:.2f}")
                        return result
                    else:
                        print(f"❌ IBKR Snapshot: {symbol} has no price data")
                        return None
            
            # Also check MarketDataManager directly
            price_data = self._check_market_data_manager(symbol)
            if price_data and price_data.get('price', 0) > 0:
                print(f"✅ IBKR Snapshot (via MDM): {symbol} = ${price_data['price']:.2f}")
                self._cleanup_request(req_id)
                return price_data
            
            # Small delay
            time.sleep(0.5)
        
        print(f"⏰ IBKR Snapshot timeout for {symbol} (ReqID: {req_id})")
        self._cleanup_request(req_id)
        return None
    
    def _check_market_data_manager(self, symbol: str) -> Optional[Dict]:
        """Check if MarketDataManager already has data for this symbol"""
        try:
            price_data = self.market_data.get_current_price(symbol)
            if price_data and price_data.get('price', 0) > 0:
                return price_data
        except Exception as e:
            print(f"❌ Error checking MDM for {symbol}: {e}")
        return None
    
    def on_tick_price(self, req_id: int, tick_type: int, price: float, attrib):
        """Callback when we receive price data from IBKR"""
        print(f"📊 Snapshot handler received tick: ReqID={req_id}, Type={tick_type}, Price=${price}")
        
        if req_id in self._pending_requests:
            with self._lock:
                if tick_type == 4:  # LAST price
                    symbol = self._pending_requests[req_id]['symbol']
                    self._results[req_id] = {
                        'price': price,
                        'timestamp': datetime.now(),
                        'tick_type': 'LAST',
                        'symbol': symbol
                    }
                    print(f"🎯 IBKR Snapshot MATCH: {symbol} = ${price:.2f}")
    
    def _get_next_req_id(self) -> int:
        """Get next unique request ID"""
        with self._lock:
            req_id = self._next_req_id
            self._next_req_id += 1
            return req_id
    
    def _cleanup_request(self, req_id: int):
        """Clean up completed or timed out requests"""
        with self._lock:
            if req_id in self._pending_requests:
                del self._pending_requests[req_id]