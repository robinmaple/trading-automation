"""
Dedicated Historical Data Manager for IBKR API
Handles all historical data requests, callbacks, and retry logic separately from real-time trading
"""

import threading
import time
import pandas as pd
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
from ibapi.contract import Contract

from src.core.context_aware_logger import get_context_logger, TradingEventType


class HistoricalDataManager:
    """
    Manages historical data requests and callbacks for IBKR API
    Provides clean interface for scanner and analysis components
    Handles retry logic, error management, and data processing
    """
    
    def __init__(self, ibkr_client=None):
        """Initialize the historical data manager with connection to IBKR client."""
        # <Context-Aware Logger Initialization - Begin>
        self.context_logger = get_context_logger()
        # <Context-Aware Logger Initialization - End>
        
        # <Manager Initialization Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "HistoricalDataManager initialization starting",
            context_provider={
                "ibkr_client_provided": ibkr_client is not None,
                "architecture": "dedicated_historical_data_manager"
            }
        )
        # <Manager Initialization Logging - End>
        
        self.ibkr_client = ibkr_client
        self._lock = threading.RLock()
        
        # Request tracking
        self._active_requests: Dict[int, Dict[str, Any]] = {}
        self._next_req_id = 10000  # Start from high number to avoid conflicts
        
        # Retry configuration
        self._max_retries = 3
        self._retry_delays = [2, 5, 10]  # Exponential backoff in seconds
        self._request_timeout = 15  # seconds
        
        # Performance tracking
        self._total_requests = 0
        self._successful_requests = 0
        self._failed_requests = 0
        self._last_request_time = None
        
        # <Manager Ready Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "HistoricalDataManager initialized successfully",
            context_provider={
                "ibkr_client_connected": ibkr_client is not None,
                "max_retries": self._max_retries,
                "request_timeout": self._request_timeout,
                "starting_req_id": self._next_req_id
            },
            decision_reason="Historical data manager ready for requests"
        )
        # <Manager Ready Logging - End>
        
        print("✅ HistoricalDataManager initialized - dedicated historical data handling")
    
    def set_ibkr_client(self, ibkr_client) -> None:
        """Set or update the IBKR client connection (thread-safe)."""
        with self._lock:
            self.ibkr_client = ibkr_client
            
            # <Client Update Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR client updated in HistoricalDataManager",
                context_provider={
                    "ibkr_client_provided": ibkr_client is not None,
                    "client_type": type(ibkr_client).__name__ if ibkr_client else "None"
                }
            )
            # <Client Update Logging - End>
            
            print("🔗 HistoricalDataManager: IBKR client connected")
    
    def request_historical_data(self, symbol: str, days: int = 100, bar_size: str = "1 day") -> Optional[pd.DataFrame]:
        """
        Request historical data for a symbol with retry logic and error handling.
        
        Args:
            symbol: Stock symbol to request data for
            days: Number of days of historical data to retrieve
            bar_size: Bar size setting (e.g., "1 day", "1 hour", "5 mins")
            
        Returns:
            DataFrame with OHLCV data or None if request fails
        """
        # <Historical Data Request Start Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting historical data request",
            symbol=symbol,
            context_provider={
                "days_requested": days,
                "bar_size": bar_size,
                "max_retries": self._max_retries,
                "ibkr_client_available": self.ibkr_client is not None
            }
        )
        # <Historical Data Request Start Logging - End>
        
        print(f"📈 HistoricalDataManager: Requesting {days} days of {bar_size} data for {symbol}")
        
        if not self.ibkr_client or not hasattr(self.ibkr_client, 'connected') or not self.ibkr_client.connected:
            # <No Client Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Historical data request failed - no IBKR client connection",
                symbol=symbol,
                context_provider={
                    "ibkr_client_available": self.ibkr_client is not None,
                    "ibkr_connected": getattr(self.ibkr_client, 'connected', False) if self.ibkr_client else False,
                    "fallback_triggered": False
                },
                decision_reason="IBKR client not available for historical data request"
            )
            # <No Client Logging - End>
            print(f"❌ {symbol}: No IBKR client connection available")
            return None
        
        # Update metrics
        with self._lock:
            self._total_requests += 1
            self._last_request_time = datetime.now()
        
        # Calculate duration string
        duration_str = f"{days} D"
        
        # Try with retry logic for IBKR error codes
        for attempt in range(self._max_retries):
            try:
                # <Historical Data Attempt Logging - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Historical data request attempt {attempt + 1}",
                    symbol=symbol,
                    context_provider={
                        "attempt_number": attempt + 1,
                        "max_attempts": self._max_retries,
                        "duration_string": duration_str,
                        "bar_size": bar_size
                    }
                )
                # <Historical Data Attempt Logging - End>
                
                print(f"🔄 {symbol}: Historical data attempt {attempt + 1}/{self._max_retries}")
                
                historical_data = self._send_historical_request(symbol, duration_str, bar_size)
                
                if historical_data is not None and not historical_data.empty:
                    # <Historical Data Success Logging - Begin>
                    with self._lock:
                        self._successful_requests += 1
                    
                    self.context_logger.log_event(
                        TradingEventType.MARKET_CONDITION,
                        "Historical data retrieved successfully",
                        symbol=symbol,
                        context_provider={
                            "attempts_required": attempt + 1,
                            "data_points": len(historical_data),
                            "date_range": {
                                "start": historical_data.index.min().strftime('%Y-%m-%d') if not historical_data.empty else "N/A",
                                "end": historical_data.index.max().strftime('%Y-%m-%d') if not historical_data.empty else "N/A"
                            },
                            "success_rate": f"{(self._successful_requests/self._total_requests)*100:.1f}%"
                        },
                        decision_reason=f"Retrieved {len(historical_data)} bars of historical data for {symbol}"
                    )
                    # <Historical Data Success Logging - End>
                    
                    print(f"✅ {symbol}: Retrieved {len(historical_data)} bars of historical data")
                    return historical_data
                else:
                    # <Historical Data Empty Logging - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Historical data request returned empty data",
                        symbol=symbol,
                        context_provider={
                            "attempt_number": attempt + 1,
                            "data_available": False,
                            "request_completed": True
                        },
                        decision_reason="Historical data request completed but returned no data"
                    )
                    # <Historical Data Empty Logging - End>
                    
                    if attempt < self._max_retries - 1:
                        retry_delay = self._retry_delays[attempt]
                        print(f"⏳ {symbol}: No data received, retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        with self._lock:
                            self._failed_requests += 1
                        print(f"❌ {symbol}: All historical data attempts failed")
                        
            except Exception as e:
                # <Historical Data Error Logging - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Historical data attempt {attempt + 1} failed with exception",
                    symbol=symbol,
                    context_provider={
                        "attempt_number": attempt + 1,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    decision_reason=f"Historical data attempt failed: {e}"
                )
                # <Historical Data Error Logging - End>
                
                print(f"❌ {symbol}: Historical data attempt {attempt + 1} failed: {e}")
                
                if attempt < self._max_retries - 1:
                    retry_delay = self._retry_delays[attempt]
                    print(f"⏳ {symbol}: Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    with self._lock:
                        self._failed_requests += 1
                    print(f"❌ {symbol}: All historical data attempts failed after {self._max_retries} retries")
        
        # <Historical Data Final Failure Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "All historical data attempts failed",
            symbol=symbol,
            context_provider={
                "max_attempts": self._max_retries,
                "final_result": "no_data",
                "total_requests": self._total_requests,
                "successful_requests": self._successful_requests,
                "failed_requests": self._failed_requests
            },
            decision_reason=f"Could not retrieve historical data for {symbol} after {self._max_retries} attempts"
        )
        # <Historical Data Final Failure Logging - End>
        
        return None
    
    def _send_historical_request(self, symbol: str, duration_str: str, bar_size: str) -> Optional[pd.DataFrame]:
        """Send historical data request to IBKR and wait for response."""
        if not self.ibkr_client:
            return None
        
        # Create contract
        contract = self._create_contract(symbol)
        
        # Setup request tracking
        req_id = self._get_next_req_id()
        request_data = self._setup_request_tracking(req_id, symbol)
        
        try:
            # <Historical Request Detailed Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Sending historical data request to IBKR",
                symbol=symbol,
                context_provider={
                    "request_id": req_id,
                    "duration": duration_str,
                    "bar_size": bar_size,
                    "data_type": "TRADES",
                    "use_rth": 1,
                    "timeout_seconds": self._request_timeout
                }
            )
            # <Historical Request Detailed Logging - End>
            
            # Send historical data request
            self.ibkr_client.reqHistoricalData(
                reqId=req_id,
                contract=contract,
                endDateTime="",  # Current time
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=1,  # Regular trading hours only
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )
            
            # Wait for data with timeout
            return self._wait_for_historical_response(req_id, symbol)
            
        except Exception as e:
            # Clean up on error
            self._cleanup_request(req_id)
            raise e
    
    def _setup_request_tracking(self, req_id: int, symbol: str) -> Dict[str, Any]:
        """Setup tracking for a historical data request."""
        with self._lock:
            request_data = {
                'symbol': symbol,
                'bars': [],
                'completed': False,
                'error': None,
                'start_time': datetime.now(),
                'timeout': self._request_timeout
            }
            self._active_requests[req_id] = request_data
            return request_data
    
    def _wait_for_historical_response(self, req_id: int, symbol: str) -> Optional[pd.DataFrame]:
        """Wait for historical data response with proper timeout handling."""
        start_time = time.time()
        timeout = self._request_timeout
        
        while time.time() - start_time < timeout:
            with self._lock:
                request_data = self._active_requests.get(req_id)
                if not request_data:
                    break
                    
                if request_data['completed']:
                    bars = request_data['bars']
                    self._cleanup_request(req_id)
                    
                    if bars:
                        return self._process_historical_bars(bars, symbol)
                    else:
                        return pd.DataFrame()  # Empty but successful
            
            time.sleep(0.5)  # Polling interval
        
        # Timeout - clean up
        self._cleanup_request(req_id)
        
        # <Historical Data Timeout Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Historical data request timeout",
            symbol=symbol,
            context_provider={
                "request_id": req_id,
                "timeout_seconds": timeout,
                "wait_time_seconds": time.time() - start_time
            },
            decision_reason="Historical data request timed out waiting for response"
        )
        # <Historical Data Timeout Logging - End>
        
        print(f"⏰ {symbol}: Historical data request timeout (req_id: {req_id})")
        return None
    
    def _process_historical_bars(self, bars: List, symbol: str) -> pd.DataFrame:
        """Process historical bars into DataFrame format."""
        if not bars:
            return pd.DataFrame()
        
        data = []
        for bar in bars:
            try:
                # Parse date - IBKR format is typically 'YYYYMMDD' for daily bars
                if hasattr(bar, 'date'):
                    if ' ' in bar.date:
                        # Timestamp format 'YYYYMMDD HH:MM:SS'
                        date_str = bar.date.split(' ')[0]
                        bar_date = datetime.strptime(date_str, '%Y%m%d')
                    else:
                        # Date-only format 'YYYYMMDD'
                        bar_date = datetime.strptime(bar.date, '%Y%m%d')
                else:
                    bar_date = datetime.now()
                
                data.append({
                    'date': bar_date,
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                    'volume': bar.volume,
                    'symbol': symbol
                })
            except Exception as e:
                print(f"⚠️ Error processing bar for {symbol}: {e}")
                continue
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)
        
        return df
    
    def _create_contract(self, symbol: str) -> Contract:
        """Create IBKR contract for a symbol."""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract
    
    def _get_next_req_id(self) -> int:
        """Get next unique request ID (thread-safe)."""
        with self._lock:
            req_id = self._next_req_id
            self._next_req_id += 1
            return req_id
    
    def _cleanup_request(self, req_id: int):
        """Clean up completed or timed out requests (thread-safe)."""
        with self._lock:
            if req_id in self._active_requests:
                del self._active_requests[req_id]
    
    # --- IBKR Callback Methods ---
    def historical_data(self, req_id: int, bar) -> None:
        """Callback when historical data bar is received."""
        with self._lock:
            if req_id in self._active_requests:
                self._active_requests[req_id]['bars'].append(bar)
                
                # <Historical Bar Received Logging - Begin>
                symbol = self._active_requests[req_id]['symbol']
                bars_count = len(self._active_requests[req_id]['bars'])
                self.context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Historical data bar received",
                    symbol=symbol,
                    context_provider={
                        "request_id": req_id,
                        "bar_date": bar.date,
                        "close_price": bar.close,
                        "volume": bar.volume,
                        "total_bars_received": bars_count
                    }
                )
                # <Historical Bar Received Logging - End>
    
    def historical_data_end(self, req_id: int, start: str, end: str) -> None:
        """Callback when historical data request ends."""
        with self._lock:
            if req_id in self._active_requests:
                self._active_requests[req_id]['completed'] = True
                
                # <Historical Data End Logging - Begin>
                symbol = self._active_requests[req_id]['symbol']
                bars_count = len(self._active_requests[req_id]['bars'])
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Historical data request completed",
                    symbol=symbol,
                    context_provider={
                        "request_id": req_id,
                        "start_date": start,
                        "end_date": end,
                        "bars_received": bars_count,
                        "completion_status": "success" if bars_count > 0 else "no_data"
                    },
                    decision_reason=f"Historical data request completed with {bars_count} bars"
                )
                # <Historical Data End Logging - End>
                
                print(f"✅ Historical data completed for {symbol}: {bars_count} bars received")
    
    # --- Health and Metrics Methods ---
    def get_health_status(self) -> Dict[str, Any]:
        """Get health metrics for historical data manager."""
        with self._lock:
            active_requests = len(self._active_requests)
            success_rate = (self._successful_requests / self._total_requests * 100) if self._total_requests > 0 else 0
            
            health = {
                'ibkr_client_connected': self.ibkr_client is not None and getattr(self.ibkr_client, 'connected', False),
                'active_requests': active_requests,
                'total_requests': self._total_requests,
                'successful_requests': self._successful_requests,
                'failed_requests': self._failed_requests,
                'success_rate_percent': round(success_rate, 2),
                'last_request_time': self._last_request_time.isoformat() if self._last_request_time else None,
                'max_retries': self._max_retries,
                'request_timeout': self._request_timeout
            }
            
            return health
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get detailed performance metrics."""
        health = self.get_health_status()
        
        # Add additional performance metrics
        metrics = {
            **health,
            'manager_type': 'HistoricalDataManager',
            'retry_delays': self._retry_delays,
            'next_req_id': self._next_req_id
        }
        
        return metrics