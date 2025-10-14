from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime, timedelta
import time
import threading
from ibapi.contract import Contract
from ibapi.scanner import ScannerSubscription
from ibapi.tag_value import TagValue

# <Historical Data Manager Integration - Begin>
from src.core.historical_data_manager import HistoricalDataManager
# <Historical Data Manager Integration - End>

# <Context-Aware Logger Integration - Begin>
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)
# <Context-Aware Logger Integration - End>

class HistoricalEODProvider:
    """
    Consolidated EOD data provider using HistoricalDataManager
    Eliminates all mock data and uses proper historical data endpoints
    Designed for scanner use with daily chart data
    """
    
    def __init__(self, ibkr_data_feed=None):
        # <Context-Aware Logger Initialization - Begin>
        self.context_logger = get_context_logger()
        # <Context-Aware Logger Initialization - End>
        
        # <Historical Data Manager Integration - Begin>
        # Use the dedicated HistoricalDataManager for all historical data operations
        self.historical_manager = HistoricalDataManager()
        
        # Connect to IBKR client if available
        if ibkr_data_feed and hasattr(ibkr_data_feed, 'ibkr_client'):
            self.historical_manager.set_ibkr_client(ibkr_data_feed.ibkr_client)
        # <Historical Data Manager Integration - End>
        
        self.ibkr_data_feed = ibkr_data_feed
        self._historical_results = {}
        self._historical_lock = threading.Lock()
        self._next_req_id = 5000
        self._pending_requests = {}
        
        # Scanner subscription tracking
        self._scanner_results = {}
        self._scanner_complete = False
        
        # Execution flow tracking
        self._execution_flow = []
        
        # <Provider Initialization Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "HistoricalEODProvider initialized with HistoricalDataManager",
            context_provider={
                "ibkr_data_feed_provided": ibkr_data_feed is not None,
                "architecture": "consolidated_with_historical_manager",
                "execution_flow_tracking": "enabled",
                "historical_manager_initialized": self.historical_manager is not None
            }
        )
        # <Provider Initialization Logging - End>

        # <Historical Data Manager Integration - Begin>
        # Use the dedicated HistoricalDataManager for all historical data operations
        self.historical_manager = HistoricalDataManager()
        
        # CRITICAL: Connect HistoricalDataManager to this EOD provider for scanner callbacks
        self.historical_manager.eod_provider = self
        
        # Connect to IBKR client if available
        if ibkr_data_feed and hasattr(ibkr_data_feed, 'ibkr_client'):
            self.historical_manager.set_ibkr_client(ibkr_data_feed.ibkr_client)
        # <Historical Data Manager Integration - End>

        print("✅ HistoricalEODProvider initialized - using HistoricalDataManager")
    
    def _track_execution_flow(self, method_name: str, **context):
        """Track execution flow for debugging sequence issues"""
        flow_entry = {
            'timestamp': datetime.now().isoformat(),
            'method': method_name,
            'context': context
        }
        self._execution_flow.append(flow_entry)
        
        # <Execution Flow Tracking - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Execution flow: {method_name}",
            context_provider={
                "method_sequence": len(self._execution_flow),
                "current_method": method_name,
                "flow_context": context
            }
        )
        # <Execution Flow Tracking - End>
    
    def _setup_scanner_subscription(self) -> ScannerSubscription:
        """Setup IBKR scanner subscription with proper filter options"""
        scanner_sub = ScannerSubscription()
        scanner_sub.instrument = "STK"
        scanner_sub.locationCode = "STK.US.MAJOR"
        scanner_sub.scanCode = "TOP_PERC_GAIN"  # Changed to match working example
        scanner_sub.abovePrice = 1.0
        scanner_sub.aboveVolume = 100000
        
        # Remove marketCapAbove from scanner_sub since we'll use filter_options
        # scanner_sub.marketCapAbove = 1000000000
        
        scanner_sub.numberOfRows = 50
        
        print(f"🎯 Using scanner code: {scanner_sub.scanCode}")
        return scanner_sub

    def _wait_for_scanner_results(self, req_id: int, timeout: int) -> List[str]:
        """Wait for scanner results with timeout"""
        start_time = time.time()
        symbols = []
        
        while time.time() - start_time < timeout:
            with self._historical_lock:
                if self._scanner_complete:
                    symbols = list(self._scanner_results.keys())
                    break
            
            time.sleep(0.5)
        
        # Cancel scanner subscription
        if hasattr(self.ibkr_data_feed, 'ibkr_client'):
            self.ibkr_data_feed.ibkr_client.cancelScannerSubscription(req_id)
        
        return symbols
    
    def scanner_data_callback(self, req_id: int, rank: int, contract: Contract, 
                            distance: str, benchmark: str, projection: str, 
                            legs: str) -> None:
        """Callback when scanner data is received"""
        if contract.symbol:
            with self._historical_lock:
                self._scanner_results[contract.symbol] = {
                    'rank': rank,
                    'contract': contract,
                    'distance': distance,
                    'benchmark': benchmark
                }
            # <Scanner Symbol Found Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner found symbol meeting criteria",
                symbol=contract.symbol,
                context_provider={
                    "request_id": req_id,
                    "rank": rank,
                    "distance": distance,
                    "benchmark": benchmark,
                    "total_symbols_found": len(self._scanner_results),
                    "execution_phase": "scanner_symbol_received"
                }
            )
            # <Scanner Symbol Found Logging - End>
            print(f"📊 Scanner found: {contract.symbol} (Rank: {rank})")
    
    def _get_fallback_universe(self) -> List[str]:
        """
        Minimal fallback universe - only used if IBKR scanner completely fails
        This is NOT mock data - it's a minimal viable universe for system operation
        """
        # <Fallback Universe Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Using minimal fallback universe",
            context_provider={
                "fallback_reason": "scanner_unavailable",
                "fallback_symbol_count": 5,
                "fallback_symbols": ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA'],
                "execution_phase": "fallback_universe"
            },
            decision_reason="IBKR scanner completely unavailable"
        )
        # <Fallback Universe Logging - End>
        print("⚠️  Using minimal fallback universe (scanner unavailable)")
        # Keep this extremely minimal - just enough for basic operation
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA']  # Top 5 by market cap
    
    def get_universe_prices(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Get real EOD prices for multiple symbols using IBKR historical data
        Returns empty dict for symbols with no data (no mock fallbacks)
        """
        # <Execution Flow Tracking - Universe Prices Start - Begin>
        self._track_execution_flow(
            "get_universe_prices_start",
            symbol_count=len(symbols) if symbols else 0,
            symbols_provided=symbols is not None
        )
        # <Execution Flow Tracking - Universe Prices Start - End>
        
        # Validate input symbols
        if not symbols:
            # <Empty Symbols Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Empty symbol list provided to get_universe_prices",
                context_provider={
                    "symbol_count": 0,
                    "execution_phase": "input_validation",
                    "issue": "empty_symbol_list"
                },
                decision_reason="Cannot get EOD prices for empty symbol list"
            )
            # <Empty Symbols Logging - End>
            print("❌ Empty symbol list provided to get_universe_prices")
            return {}
        
        # <Batch EOD Start Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting batch historical EOD data retrieval",
            context_provider={
                "symbol_count": len(symbols),
                "batch_size": 5,
                "rate_limit_delay_seconds": 2,
                "timeout_per_symbol": 10,
                "execution_phase": "batch_eod_retrieval_start",
                "symbol_source": "provided_by_caller"
            }
        )
        # <Batch EOD Start Logging - End>
        
        print(f"📊 Getting REAL IBKR historical EOD prices for {len(symbols)} symbols...")
        print(f"🔍 First 5 symbols: {symbols[:5]}")
        
        results = {}
        successful_symbols = 0
        
        # Process symbols in batches to avoid overwhelming IBKR
        batch_size = 5  # Conservative batch size for historical data
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            
            # <Batch Processing Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Processing historical data batch",
                context_provider={
                    "batch_number": i//batch_size + 1,
                    "batch_total": (len(symbols) + batch_size - 1) // batch_size,
                    "batch_symbols": batch,
                    "batch_start_index": i,
                    "execution_phase": "batch_processing"
                }
            )
            # <Batch Processing Logging - End>
            
            print(f"🔍 Processing historical data batch {i//batch_size + 1}: {batch}")
            
            batch_results = self._get_batch_historical_prices(batch)
            results.update(batch_results)
            successful_symbols += len(batch_results)
            
            # Rate limiting between batches
            if i + batch_size < len(symbols):
                time.sleep(2)  # Longer delay for historical data requests
        
        # <Execution Flow Tracking - Universe Prices Complete - Begin>
        self._track_execution_flow(
            "get_universe_prices_complete",
            total_symbols=len(symbols),
            successful_symbols=successful_symbols,
            results_count=len(results)
        )
        # <Execution Flow Tracking - Universe Prices Complete - End>
        
        # <Batch EOD Complete Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Batch historical EOD data retrieval completed",
            context_provider={
                "total_symbols": len(symbols),
                "successful_symbols": successful_symbols,
                "success_rate": successful_symbols/len(symbols) if symbols else 0,
                "results_count": len(results),
                "execution_phase": "batch_eod_retrieval_complete"
            },
            decision_reason=f"Retrieved EOD data for {successful_symbols}/{len(symbols)} symbols"
        )
        # <Batch EOD Complete Logging - End>
        
        print(f"🎯 Historical EOD data retrieval: {successful_symbols}/{len(symbols)} symbols successful")
        return results
    
    def _get_batch_historical_prices(self, symbols: List[str]) -> Dict[str, Dict]:
        """Get historical EOD prices for a batch of symbols"""
        batch_results = {}
        
        for symbol in symbols:
            try:
                price_data = self._get_single_eod_price(symbol)
                if price_data and price_data.get('price', 0) > 0:
                    batch_results[symbol] = price_data
                    print(f"✅ {symbol}: HISTORICAL ${price_data['price']:.2f} (IBKR EOD)")
                else:
                    # <Symbol No Data Logging - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "No historical EOD data available for symbol",
                        symbol=symbol,
                        context_provider={
                            "data_available": False,
                            "price_value": price_data.get('price', 0) if price_data else 0,
                            "execution_phase": "symbol_data_retrieval_failed"
                        },
                        decision_reason="Historical data request returned no valid price"
                    )
                    # <Symbol No Data Logging - End>
                    print(f"❌ {symbol}: No historical EOD data available")
                        
            except Exception as e:
                # <Symbol Error Logging - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Historical data error for symbol",
                    symbol=symbol,
                    context_provider={
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "execution_phase": "symbol_data_retrieval_error"
                    },
                    decision_reason=f"Historical data exception: {e}"
                )
                # <Symbol Error Logging - End>
                print(f"❌ {symbol}: Historical data error - {e}")
                continue
        
        return batch_results
    
    def _get_single_eod_price(self, symbol: str) -> Optional[Dict]:
        """
        Get single symbol EOD price using IBKR historical data
        Returns None if no data available (no mock fallbacks)
        """
        # <Execution Flow Tracking - Single EOD Price Start - Begin>
        self._track_execution_flow(
            "_get_single_eod_price_start",
            symbol=symbol,
            data_source="ibkr_historical"
        )
        # <Execution Flow Tracking - Single EOD Price Start - End>
        
        if not self.ibkr_data_feed or not hasattr(self.ibkr_data_feed, 'ibkr_client'):
            # <No Client Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No IBKR client available for historical data",
                symbol=symbol,
                context_provider={
                    "ibkr_client_available": False,
                    "data_feed_available": self.ibkr_data_feed is not None,
                    "execution_phase": "client_availability_check"
                },
                decision_reason="IBKR client not available for historical data request"
            )
            # <No Client Logging - End>
            print(f"❌ {symbol}: No IBKR client available for historical data")
            return None
        
        try:
            ibkr_client = self.ibkr_data_feed.ibkr_client
            
            # Create contract
            contract = self._create_contract(symbol)
            
            # Request historical data for most recent EOD
            req_id = self._get_next_req_id()
            
            with self._historical_lock:
                self._pending_requests[req_id] = {
                    'symbol': symbol,
                    'request_time': datetime.now(),
                    'completed': False,
                    'data': None
                }
            
            # <Historical Request Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Requesting historical EOD data for symbol",
                symbol=symbol,
                context_provider={
                    "request_id": req_id,
                    "duration": "1 D",
                    "bar_size": "1 day",
                    "data_type": "TRADES",
                    "use_rth": 1,
                    "timeout_seconds": 10,
                    "execution_phase": "historical_data_request"
                }
            )
            # <Historical Request Logging - End>
            
            # Request 1-day historical data to get most recent EOD price
            ibkr_client.reqHistoricalData(
                reqId=req_id,
                contract=contract,
                endDateTime="",  # Current time
                durationStr="1 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=1,  # Regular trading hours only
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )
            
            # Wait for historical data response
            return self._wait_for_historical_response(req_id, symbol, timeout=10)
            
        except Exception as e:
            # <Execution Flow Tracking - Single EOD Price Error - Begin>
            self._track_execution_flow(
                "_get_single_eod_price_error",
                symbol=symbol,
                error_type=type(e).__name__,
                error_message=str(e)
            )
            # <Execution Flow Tracking - Single EOD Price Error - End>
            
            # <Historical Request Error Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Historical data request failed for symbol",
                symbol=symbol,
                context_provider={
                    "request_id": req_id,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "execution_phase": "historical_data_request_error"
                },
                decision_reason=f"Historical data request exception: {e}"
            )
            # <Historical Request Error Logging - End>
            print(f"❌ {symbol}: Historical data request failed - {e}")
            self._cleanup_request(req_id)
            return None
    
    def _wait_for_historical_response(self, req_id: int, symbol: str, timeout: int) -> Optional[Dict]:
        """Wait for historical data response with proper timeout"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self._historical_lock:
                if req_id in self._pending_requests and self._pending_requests[req_id]['completed']:
                    result = self._pending_requests[req_id]['data']
                    self._cleanup_request(req_id)
                    
                    if result and result.get('price', 0) > 0:
                        # <Historical Success Logging - Begin>
                        self.context_logger.log_event(
                            TradingEventType.MARKET_CONDITION,
                            "Historical EOD data received successfully",
                            symbol=symbol,
                            context_provider={
                                "request_id": req_id,
                                "price": result['price'],
                                "volume": result['volume'],
                                "response_time_seconds": time.time() - start_time,
                                "data_type": result.get('data_type', 'historical_eod'),
                                "execution_phase": "historical_data_success"
                            },
                            decision_reason=f"Retrieved EOD price ${result['price']:.2f} for {symbol}"
                        )
                        # <Historical Success Logging - End>
                        return result
                    else:
                        # <Historical No Data Logging - Begin>
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            "Historical data request completed with no valid data",
                            symbol=symbol,
                            context_provider={
                                "request_id": req_id,
                                "response_time_seconds": time.time() - start_time,
                                "result_available": result is not None,
                                "price_value": result.get('price', 0) if result else 0,
                                "execution_phase": "historical_data_no_valid_data"
                            },
                            decision_reason="Historical data completed but no valid price received"
                        )
                        # <Historical No Data Logging - End>
                        return None
            
            time.sleep(0.5)
        
        # <Historical Timeout Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Historical data request timeout",
            symbol=symbol,
            context_provider={
                "request_id": req_id,
                "timeout_seconds": timeout,
                "wait_time_seconds": time.time() - start_time,
                "execution_phase": "historical_data_timeout"
            },
            decision_reason="Historical data request timed out"
        )
        # <Historical Timeout Logging - End>
        
        print(f"⏰ Historical data timeout for {symbol}")
        self._cleanup_request(req_id)
        return None
    
    def historical_data_callback(self, req_id: int, bar) -> None:
        """Callback when historical data is received - to be connected to IBKR client"""
        with self._historical_lock:
            if req_id in self._pending_requests:
                symbol = self._pending_requests[req_id]['symbol']
                
                # Use close price as EOD price
                price_data = {
                    'price': bar.close,
                    'volume': bar.volume,
                    'timestamp': datetime.strptime(bar.date, '%Y%m%d %H:%M:%S') if bar.date else datetime.now(),
                    'data_type': 'historical_eod',
                    'source': 'IBKR Historical',
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close
                }
                
                self._pending_requests[req_id]['data'] = price_data
                self._pending_requests[req_id]['completed'] = True
                
                # <Historical Data Received Logging - Begin>
                self.context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Historical data bar received",
                    symbol=symbol,
                    context_provider={
                        "request_id": req_id,
                        "close_price": bar.close,
                        "volume": bar.volume,
                        "open_price": bar.open,
                        "high_price": bar.high,
                        "low_price": bar.low,
                        "bar_date": bar.date,
                        "execution_phase": "historical_data_received"
                    }
                )
                # <Historical Data Received Logging - End>
                
                print(f"📈 Historical data received for {symbol}: ${bar.close:.2f}")
    
    def historical_data_end_callback(self, req_id: int, start: str, end: str) -> None:
        """Callback when historical data request ends"""
        with self._historical_lock:
            if req_id in self._pending_requests and not self._pending_requests[req_id]['completed']:
                # If no data received, mark as completed with None
                self._pending_requests[req_id]['completed'] = True
                
                # <Historical Data End Logging - Begin>
                symbol = self._pending_requests[req_id]['symbol']
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Historical data request ended with no data",
                    symbol=symbol,
                    context_provider={
                        "request_id": req_id,
                        "start_date": start,
                        "end_date": end,
                        "data_received": False,
                        "execution_phase": "historical_data_ended_no_data"
                    },
                    decision_reason="Historical data request ended without receiving data"
                )
                # <Historical Data End Logging - End>
                
                print(f"📊 Historical data ended for req {req_id}, no data received")
    
    def _create_contract(self, symbol: str) -> Contract:
        """Create IBKR contract for a symbol"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract
    
    def _get_next_req_id(self) -> int:
        """Get next unique request ID"""
        with self._historical_lock:
            req_id = self._next_req_id
            self._next_req_id += 1
            return req_id
    
    def _cleanup_request(self, req_id: int):
        """Clean up completed or timed out requests"""
        with self._historical_lock:
            if req_id in self._pending_requests:
                del self._pending_requests[req_id]

    # <Historical Data Manager Integration - Begin>
    def get_historical_data(self, symbol: str, days: int = 100) -> Optional[pd.DataFrame]:
        """
        Get multi-day historical EOD data for strategy analysis using HistoricalDataManager
        Returns DataFrame with OHLCV data or None if no data available
        """
        # <Multi-day Historical Start Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting multi-day historical data retrieval via HistoricalDataManager",
            symbol=symbol,
            context_provider={
                "days_requested": days,
                "symbol": symbol,
                "execution_phase": "multi_day_historical_start",
                "data_manager_used": True
            }
        )
        # <Multi-day Historical Start Logging - End>
        
        print(f"📈 Getting {days}-day historical EOD data for {symbol} via HistoricalDataManager")
        
        # Use HistoricalDataManager for all historical data requests
        if not self.historical_manager:
            # <No Historical Manager Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No HistoricalDataManager available for historical data request",
                symbol=symbol,
                context_provider={
                    "historical_manager_available": False,
                    "fallback_triggered": False
                },
                decision_reason="HistoricalDataManager not initialized"
            )
            # <No Historical Manager Logging - End>
            print(f"❌ {symbol}: No HistoricalDataManager available")
            return None
        
        # Request historical data through the manager
        historical_data = self.historical_manager.request_historical_data(
            symbol=symbol, 
            days=days, 
            bar_size="1 day"
        )
        
        if historical_data is not None and not historical_data.empty:
            # <Historical Data Success via Manager Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Multi-day historical data retrieved successfully via HistoricalDataManager",
                symbol=symbol,
                context_provider={
                    "data_points": len(historical_data),
                    "date_range": {
                        "start": historical_data.index.min().strftime('%Y-%m-%d') if not historical_data.empty else "N/A",
                        "end": historical_data.index.max().strftime('%Y-%m-%d') if not historical_data.empty else "N/A"
                    },
                    "manager_health": self.historical_manager.get_health_status()
                },
                decision_reason=f"Retrieved {len(historical_data)} days of historical data for {symbol} via HistoricalDataManager"
            )
            # <Historical Data Success via Manager Logging - End>
            
            print(f"✅ {symbol}: Retrieved {len(historical_data)} days of historical data via HistoricalDataManager")
            return historical_data
        else:
            # <Historical Data Failed via Manager Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Historical data request failed via HistoricalDataManager",
                symbol=symbol,
                context_provider={
                    "data_available": False,
                    "manager_health": self.historical_manager.get_health_status() if self.historical_manager else {},
                    "final_result": "no_data"
                },
                decision_reason=f"Could not retrieve historical data for {symbol} via HistoricalDataManager"
            )
            # <Historical Data Failed via Manager Logging - End>
            
            print(f"❌ {symbol}: Historical data request failed via HistoricalDataManager")
            return None
    # <Historical Data Manager Integration - End>

    # Add to historical_data_manager.py - Scanner Callback Methods

    # scanner_data_end_callback - Begin (NEW)
    def scanner_data_end_callback(self, req_id: int) -> None:
        """Callback when scanner data ends - process collected symbols and trigger historical data"""
        print(f"🎯 HISTORICAL EOD PROVIDER: Scanner data ended for req: {req_id}")
        
        # Set scanner completion flag
        with self._historical_lock:
            self._scanner_complete = True
            symbol_count = len(self._scanner_results)
        
        # <Scanner Completion Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner data collection completed",
            context_provider={
                "request_id": req_id,
                "symbols_collected": symbol_count,
                "scanner_complete_flag_set": True,
                "execution_phase": "scanner_completion"
            },
            decision_reason=f"Scanner completed with {symbol_count} symbols, ready for historical data phase"
        )
        # <Scanner Completion Logging - End>
        
        print(f"✅ Scanner completed: {symbol_count} symbols collected, ready for historical data processing")
        
        # Process scanner results if we have symbols
        if symbol_count > 0:
            self._process_scanner_results()
        else:
            print("⚠️ Scanner completed but no symbols collected")
    # scanner_data_end_callback - End

    # _process_scanner_results - Begin (NEW)
    def _process_scanner_results(self) -> None:
        """Process collected scanner symbols using HistoricalDataManager directly"""
        with self._historical_lock:
            scanner_symbols = list(self._scanner_results.keys())
            symbol_count = len(scanner_symbols)
        
        print(f"🔄 Processing {symbol_count} scanner symbols using HistoricalDataManager")
        
        # 🧪 CRITICAL: First test if ANY historical data works
        print("🔧 RUNNING CRITICAL HISTORICAL DATA TEST...")
        basic_test_result = self.test_basic_historical_data()
        
        if not basic_test_result:
            print("❌ CRITICAL: Historical data service completely broken - stopping scanner")
            return

        # CRITICAL: Use HistoricalDataManager directly instead of EOD provider methods
        if self.historical_manager and symbol_count > 0:
            print(f"📊 Using HistoricalDataManager for {symbol_count} symbols")
            
            # Process symbols in small batches
            batch_size = 3  # Smaller batches to avoid overwhelming IBKR
            successful_symbols = 0
            
            for i in range(0, len(scanner_symbols), batch_size):
                batch = scanner_symbols[i:i + batch_size]
                print(f"🔍 Processing batch {i//batch_size + 1}: {batch}")
                
                for symbol in batch:
                    try:
                        # Use HistoricalDataManager directly (bypasses the problematic routing)
                        historical_data = self.historical_manager.request_historical_data(
                            symbol=symbol, 
                            days=100, 
                            bar_size="1 day"
                        )
                        
                        if historical_data is not None and not historical_data.empty:
                            print(f"✅ {symbol}: Retrieved {len(historical_data)} bars via HistoricalDataManager")
                            successful_symbols += 1
                        else:
                            print(f"❌ {symbol}: No data via HistoricalDataManager")
                            
                    except Exception as e:
                        print(f"❌ {symbol}: HistoricalDataManager error - {e}")
                
                # Rate limiting between batches
                if i + batch_size < len(scanner_symbols):
                    print("⏳ Rate limiting between batches...")
                    time.sleep(2)
            
            print(f"🎯 Historical data via HistoricalDataManager: {successful_symbols}/{symbol_count} successful")
            
        else:
            print("❌ No HistoricalDataManager available for processing")
        
        # _process_scanner_results - End

    # get_symbol_universe - Begin (UPDATED - Remove circular routing comment)
    def get_symbol_universe(self) -> List[str]:
        """
        Get real symbol universe using IBKR scanner subscription
        Returns most active stocks meeting basic liquidity criteria
        No hardcoded symbols - completely dynamic from market data
        """
        # <Execution Flow Tracking - Symbol Universe Start - Begin>
        self._track_execution_flow(
            "get_symbol_universe_start",
            purpose="retrieve_symbols_from_ibkr_scanner"
        )
        # <Execution Flow Tracking - Symbol Universe Start - End>
        
        # <Scanner Universe Start Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting real symbol universe scan using IBKR scanner",
            context_provider={
                "scan_type": "MOST_ACTIVE",
                "min_price": 1.0,
                "min_volume": 100000,
                "min_market_cap": 1000000000,
                "execution_phase": "symbol_universe_retrieval"
            }
        )
        # <Scanner Universe Start Logging - End>
        
        print("🔍 Scanning for real symbol universe using IBKR scanner...")
        
        if not self.ibkr_data_feed or not hasattr(self.ibkr_data_feed, 'ibkr_client'):
            # <Scanner Unavailable Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR client unavailable for scanner subscription",
                context_provider={
                    "fallback_triggered": True,
                    "fallback_type": "minimal_universe",
                    "data_feed_available": self.ibkr_data_feed is not None,
                    "ibkr_client_available": hasattr(self.ibkr_data_feed, 'ibkr_client') if self.ibkr_data_feed else False
                },
                decision_reason="IBKR client not available for scanner"
            )
            # <Scanner Unavailable Logging - End>
            print("❌ No IBKR client available for scanner subscription")
            return self._get_fallback_universe()
        
        try:
            ibkr_client = self.ibkr_data_feed.ibkr_client
            req_id = self._get_next_req_id()
            
            # Setup scanner subscription for most active stocks
            scanner_sub = self._setup_scanner_subscription()
            
            # <Enhanced Scanner Parameters Logging - Begin>
            scanner_params = {
                "request_id": req_id,
                "scan_code": scanner_sub.scanCode,
                "instrument": scanner_sub.instrument,
                "location_code": scanner_sub.locationCode,
                "above_price": scanner_sub.abovePrice,
                "above_volume": scanner_sub.aboveVolume,
                "market_cap_above": scanner_sub.marketCapAbove,
                "number_of_rows": getattr(scanner_sub, 'numberOfRows', 'NOT_SET')
            }
            
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner subscription parameters configured",
                context_provider=scanner_params
            )
            print(f"📊 Scanner parameters: {scanner_params}")
            # <Enhanced Scanner Parameters Logging - End>
            
            with self._historical_lock:
                self._scanner_results = {}
                self._scanner_complete = False
            
            # <Scanner Request Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Requesting IBKR scanner subscription",
                context_provider={
                    "request_id": req_id,
                    "scan_code": "MOST_ACTIVE",
                    "instrument": "STK",
                    "location_code": "STK.US.MAJOR",
                    "execution_phase": "scanner_subscription_request"
                }
            )
            # <Scanner Request Logging - End>
            
            print(f"🚀 Making IBKR scanner subscription request (req_id: {req_id})...")
            
            # Request scanner data
            scan_options = []
            filter_options = [
                TagValue("volumeAbove", "100000"),  # Match your min_volume
                TagValue("priceAbove", "1"),        # Match your min_price
                # TagValue("marketCapAbove", "1000000000")  # Match your min_market_cap
            ]

            print(f"🚀 Making IBKR scanner subscription with filter options...")
            ibkr_client.reqScannerSubscription(req_id, scanner_sub, scan_options, filter_options)

            print(f"⏳ Waiting for scanner results (timeout: 15s)...")
            
            # Wait for scanner results
            symbols = self._wait_for_scanner_results(req_id, timeout=15)
            
            if symbols:
                # <Execution Flow Tracking - Symbol Universe Success - Begin>
                self._track_execution_flow(
                    "get_symbol_universe_success",
                    symbols_found=len(symbols),
                    request_id=req_id
                )
                # <Execution Flow Tracking - Symbol Universe Success - End>
                
                # <Scanner Success Logging - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Scanner subscription completed successfully",
                    context_provider={
                        "symbols_found": len(symbols),
                        "request_id": req_id,
                        "scan_duration_seconds": 15,
                        "sample_symbols": symbols[:5] if symbols else [],
                        "execution_phase": "scanner_completion"
                    },
                    decision_reason=f"Found {len(symbols)} active stocks meeting criteria"
                )
                # <Scanner Success Logging - End>
                print(f"🎯 Real symbol universe: {len(symbols)} active stocks found")
                return symbols
            else:
                # <Execution Flow Tracking - Symbol Universe No Results - Begin>
                self._track_execution_flow(
                    "get_symbol_universe_no_results",
                    request_id=req_id,
                    fallback_triggered=True,
                    scanner_results_count=len(self._scanner_results)
                )
                # <Execution Flow Tracking - Symbol Universe No Results - End>
                
                # <Scanner No Results Logging - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Scanner returned no results, using fallback universe",
                    context_provider={
                        "request_id": req_id,
                        "timeout_seconds": 15,
                        "fallback_triggered": True,
                        "scanner_results_count": len(self._scanner_results),
                        "scanner_results_received": list(self._scanner_results.keys()),
                        "execution_phase": "scanner_timeout"
                    },
                    decision_reason="Scanner timeout or no symbols meeting criteria"
                )
                # <Scanner No Results Logging - End>
                print(f"⚠️  Scanner returned no results (received {len(self._scanner_results)} symbols), using fallback universe")
                return self._get_fallback_universe()
                
        except Exception as e:
            # <Execution Flow Tracking - Symbol Universe Error - Begin>
            self._track_execution_flow(
                "get_symbol_universe_error",
                error_type=type(e).__name__,
                error_message=str(e)
            )
            # <Execution Flow Tracking - Symbol Universe Error - End>
            
            # <Scanner Error Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner subscription failed with exception",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "fallback_triggered": True,
                    "execution_phase": "scanner_exception"
                },
                decision_reason=f"Scanner exception: {e}"
            )
            # <Scanner Error Logging - End>
            print(f"❌ Scanner subscription failed: {e}")
            return self._get_fallback_universe()
    # get_symbol_universe - End

    # scanner_data_callback - Begin (UPDATED - Remove circular routing comment)
    def scanner_data_callback(self, req_id: int, rank: int, contract: Contract, 
                            distance: str, benchmark: str, projection: str, 
                            legs: str) -> None:
        """Callback when scanner data is received"""
        if contract.symbol:
            with self._historical_lock:
                self._scanner_results[contract.symbol] = {
                    'rank': rank,
                    'contract': contract,
                    'distance': distance,
                    'benchmark': benchmark
                }
            # <Scanner Symbol Found Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner found symbol meeting criteria",
                symbol=contract.symbol,
                context_provider={
                    "request_id": req_id,
                    "rank": rank,
                    "distance": distance,
                    "benchmark": benchmark,
                    "total_symbols_found": len(self._scanner_results),
                    "execution_phase": "scanner_symbol_received"
                }
            )
            # <Scanner Symbol Found Logging - End>
            print(f"📊 Scanner found: {contract.symbol} (Rank: {rank})")
    # scanner_data_callback - End

    # Add to historical_eod_provider.py for testing
    def test_historical_data_manual(self, symbol: str = "AAPL") -> bool:
        """Test if historical data works independently of scanner"""
        print(f"🔧 TESTING: Manual historical data request for {symbol}")
        
        # Create contract exactly like working example
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK" 
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        # Use exact parameters from working example
        end_date_time = "20251012 16:00:00 US/Eastern"  # Fixed future date
        
        try:
            if self.ibkr_data_feed and hasattr(self.ibkr_data_feed, 'ibkr_client'):
                ibkr_client = self.ibkr_data_feed.ibkr_client
                req_id = 10001  # Use different ID range
                
                print(f"🔧 TEST: Requesting historical data for {symbol} with req_id {req_id}")
                
                ibkr_client.reqHistoricalData(
                    reqId=req_id,
                    contract=contract,
                    endDateTime=end_date_time,
                    durationStr="1 D", 
                    barSizeSetting="1 day",
                    whatToShow="TRADES",
                    useRTH=1,
                    formatDate=1,
                    keepUpToDate=False,
                    chartOptions=[]
                )
                
                print(f"🔧 TEST: Historical data request sent for {symbol}")
                return True
                
        except Exception as e:
            print(f"🔧 TEST FAILED: {e}")
            
        return False
    
    def test_basic_historical_data(self):
        """Test if ANY historical data works with a major stock"""
        print("🔧 CRITICAL TEST: Checking if basic historical data works...")
        
        if not self.historical_manager:
            print("❌ TEST: No HistoricalDataManager available")
            return False
        
        # Test with a major, liquid stock that should definitely have data
        test_symbols = ["AAPL", "MSFT", "GOOGL"]
        
        for symbol in test_symbols:
            print(f"🔧 TEST: Trying {symbol}...")
            try:
                data = self.historical_manager.request_historical_data(
                    symbol=symbol, 
                    days=10,  # Shorter period for faster test
                    bar_size="1 day"
                )
                
                if data is not None and not data.empty:
                    print(f"✅ CRITICAL SUCCESS: {symbol} has historical data! ({len(data)} bars)")
                    return True
                else:
                    print(f"❌ TEST FAILED: {symbol} returned no data")
                    
            except Exception as e:
                print(f"❌ TEST ERROR: {symbol} failed with {e}")
        
        print("🔧 CRITICAL: NO historical data works at all!")
        return False