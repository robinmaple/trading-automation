# src/brokers/ibkr/core/market_data_handler.py

"""
Handles all market data operations for IBKR including real-time data subscriptions,
tick processing, and market data health monitoring.
"""

import threading
import datetime
import queue
from typing import Optional, Dict, Any
from ibapi.contract import Contract

from src.core.context_aware_logger import get_context_logger, TradingEventType


class MarketDataHandler:
    """Manages market data subscriptions, tick processing, and data flow monitoring."""
    
    def __init__(self, connection_manager):
        """Initialize market data handler with connection reference."""
        self.context_logger = get_context_logger()
        self.connection_manager = connection_manager
        
        # Market data manager reference (will be set later)
        self.market_data_manager = None
        self.historical_data_manager = None
        self.historical_eod_provider = None
        
        # Thread safety and health monitoring
        self._manager_lock = threading.RLock()
        self._historical_manager_lock = threading.RLock()
        self._tick_errors = 0
        self._last_tick_time = None
        self._total_ticks_processed = 0
        
        # Early tick queue for manager connection delays
        self._early_tick_queue = queue.Queue(maxsize=1000)
        self._max_early_ticks_logged = 10
        self._early_ticks_logged = 0
        self._manager_connection_time = None
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "MarketDataHandler initialized",
            context_provider={
                "connection_manager_available": connection_manager is not None,
                "early_tick_queue_size": self._early_tick_queue.maxsize
            }
        )
    
    def set_market_data_manager(self, manager) -> None:
        """
        Thread-safe method to set the MarketDataManager instance.
        Processes any queued ticks that arrived before manager was available.
        """
        with self._manager_lock:
            self.market_data_manager = manager
            self._manager_connection_time = datetime.datetime.now()
            
            # Process any ticks that arrived before manager was ready
            processed_ticks = 0
            dropped_ticks = 0
            
            try:
                while not self._early_tick_queue.empty():
                    try:
                        tick_data = self._early_tick_queue.get_nowait()
                        reqId, tickType, price, attrib = tick_data
                        
                        if self.market_data_manager:
                            self.market_data_manager.on_tick_price(reqId, tickType, price, attrib)
                            processed_ticks += 1
                        else:
                            dropped_ticks += 1
                            
                        self._early_tick_queue.task_done()
                    except queue.Empty:
                        break
            except Exception as e:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Error processing early tick queue",
                    context_provider={
                        'error': str(e),
                        'processed_ticks': processed_ticks,
                        'dropped_ticks': dropped_ticks
                    }
                )
            
            connection_context = {
                'manager_type': type(manager).__name__,
                'connected': True,
                'early_ticks_processed': processed_ticks,
                'early_ticks_dropped': dropped_ticks,
                'remaining_queue_size': self._early_tick_queue.qsize(),
                'manager_connection_time': self._manager_connection_time.isoformat()
            }
            
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "MarketDataManager connected to MarketDataHandler",
                context_provider=connection_context
            )
            
            # Reset early tick logging counter
            self._early_ticks_logged = 0
    
    def set_historical_data_manager(self, manager) -> None:
        """
        Thread-safe method to set the HistoricalDataManager instance.
        """
        with self._historical_manager_lock:
            self.historical_data_manager = manager
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "HistoricalDataManager connected to MarketDataHandler",
                context_provider={
                    'manager_type': type(manager).__name__,
                    'connected': True
                }
            )
    
    def set_historical_eod_provider(self, eod_provider) -> None:
        """
        Thread-safe method to set the HistoricalEODProvider instance for scanner callbacks.
        """
        with self._historical_manager_lock:
            self.historical_eod_provider = eod_provider
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "HistoricalEODProvider connected for scanner callbacks",
                context_provider={
                    'eod_provider_type': type(eod_provider).__name__,
                    'connected': True
                }
            )
    
    def tickPrice(self, reqId: int, tickType: int, price: float, attrib) -> None:
        """
        Callback: Receive market data price tick and forward to MarketDataManager if set.
        Enhanced with early tick queuing to prevent data loss during manager connection.
        """
        # Update health metrics
        self._last_tick_time = datetime.datetime.now()
        self._total_ticks_processed += 1

        # Thread-safe access to market data manager
        with self._manager_lock:
            if self.market_data_manager:
                try:
                    self.market_data_manager.on_tick_price(reqId, tickType, price, attrib)
                    
                    # Log first successful tick for debugging
                    if self._total_ticks_processed == 1:
                        tick_type_name = {1: 'BID', 2: 'ASK', 4: 'LAST'}.get(tickType, f'UNKNOWN({tickType})')
                        self.context_logger.log_event(
                            TradingEventType.MARKET_CONDITION,
                            "First market data tick processed",
                            context_provider={
                                'tick_type': tick_type_name,
                                'price': price,
                                'req_id': reqId,
                                'total_ticks_processed': 1,
                                'manager_available': True
                            }
                        )
                        
                except Exception as e:
                    self._tick_errors += 1
                    error_count = self._tick_errors
                    
                    # Only log periodic errors to avoid spam
                    if error_count <= 3 or error_count % 10 == 0:
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            "Market data tick processing error",
                            context_provider={
                                'error_count': error_count,
                                'total_ticks_processed': self._total_ticks_processed,
                                'error': str(e),
                                'req_id': reqId,
                                'tick_type': tickType,
                                'manager_available': True
                            }
                        )
            else:
                # Manager not available - queue the tick for later processing
                try:
                    # Only log first few queued ticks to avoid spam
                    if self._early_ticks_logged < self._max_early_ticks_logged:
                        self._early_ticks_logged += 1
                        tick_type_name = {1: 'BID', 2: 'ASK', 4: 'LAST'}.get(tickType, f'UNKNOWN({tickType})')
                        
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            "Market data tick queued - no manager available",
                            context_provider={
                                'req_id': reqId,
                                'tick_type': tick_type_name,
                                'price': price,
                                'queued_ticks_count': self._early_tick_queue.qsize() + 1,
                                'early_ticks_logged': self._early_ticks_logged,
                                'total_ticks_processed': self._total_ticks_processed
                            }
                        )
                    
                    # Queue the tick for later processing
                    self._early_tick_queue.put((reqId, tickType, price, attrib))
                    
                except queue.Full:
                    # Queue is full - log the drop but don't spam
                    if self._early_ticks_logged < self._max_early_ticks_logged:
                        self._early_ticks_logged += 1
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            "Early tick queue full - tick dropped",
                            context_provider={
                                'req_id': reqId,
                                'tick_type': tickType,
                                'price': price,
                                'queue_size': self._early_tick_queue.qsize(),
                                'max_queue_size': self._early_tick_queue.maxsize
                            }
                        )
    
    def historicalData(self, reqId: int, bar) -> None:
        """
        Callback: Receive historical data bar and forward to HistoricalDataManager if set.
        """
        # Thread-safe access to historical data manager
        with self._historical_manager_lock:
            if self.historical_data_manager:
                try:
                    self.historical_data_manager.historical_data(reqId, bar)
                    
                    # Log first successful historical data for debugging
                    if hasattr(self, '_first_historical_received') and not self._first_historical_received:
                        self._first_historical_received = True
                        self.context_logger.log_event(
                            TradingEventType.MARKET_CONDITION,
                            "First historical data bar processed",
                            context_provider={
                                'req_id': reqId,
                                'bar_date': bar.date,
                                'close_price': bar.close,
                                'volume': bar.volume
                            }
                        )
                        
                except Exception as e:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Historical data processing error",
                        context_provider={
                            'req_id': reqId,
                            'error': str(e),
                            'bar_date': bar.date
                        }
                    )
    
    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        """
        Callback: Historical data request ended - forward to HistoricalDataManager if set.
        """
        with self._historical_manager_lock:
            if self.historical_data_manager:
                try:
                    self.historical_data_manager.historical_data_end(reqId, start, end)
                    
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Historical data end callback processed",
                        context_provider={
                            'req_id': reqId,
                            'start_date': start,
                            'end_date': end
                        }
                    )
                    
                except Exception as e:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Historical data end processing error",
                        context_provider={
                            'req_id': reqId,
                            'error': str(e),
                            'start_date': start,
                            'end_date': end
                        }
                    )
    
    def scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        """Callback: Receive scanner data results and route to appropriate provider."""
        symbol = contractDetails.contract.symbol if contractDetails and contractDetails.contract else "UNKNOWN"
        
        # PRIMARY: Route directly to HistoricalEODProvider if available
        with self._historical_manager_lock:
            if self.historical_eod_provider:
                try:
                    self.historical_eod_provider.scanner_data_callback(
                        reqId, rank, contractDetails.contract, distance, benchmark, projection, legsStr
                    )
                    return
                except Exception as e:
                    pass
        
        # SECONDARY: Try to route through HistoricalDataManager's EOD provider reference
        if (self.historical_data_manager and 
            hasattr(self.historical_data_manager, 'eod_provider') and 
            self.historical_data_manager.eod_provider):
            try:
                self.historical_data_manager.eod_provider.scanner_data_callback(
                    reqId, rank, contractDetails.contract, distance, benchmark, projection, legsStr
                )
                return
            except Exception as e:
                pass
        
        # FALLBACK: Log that scanner data was received but no provider available
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner data received but no EOD provider available",
            symbol=symbol,
            context_provider={
                'req_id': reqId,
                'rank': rank,
                'symbol': symbol
            }
        )
    
    def scannerDataEnd(self, reqId):
        """Callback: Scanner data request ended - route to appropriate provider."""
        # PRIMARY: Route directly to HistoricalEODProvider if available
        with self._historical_manager_lock:
            if self.historical_eod_provider:
                try:
                    if hasattr(self.historical_eod_provider, 'scanner_data_end_callback'):
                        self.historical_eod_provider.scanner_data_end_callback(reqId)
                        return
                except Exception as e:
                    pass
        
        # SECONDARY: Try to route through HistoricalDataManager's EOD provider reference
        if (self.historical_data_manager and 
            hasattr(self.historical_data_manager, 'eod_provider') and 
            self.historical_data_manager.eod_provider):
            try:
                if hasattr(self.historical_data_manager.eod_provider, 'scanner_data_end_callback'):
                    self.historical_data_manager.eod_provider.scanner_data_end_callback(reqId)
                    return
            except Exception as e:
                pass
        
        # FALLBACK: Log that scanner ended but no provider available
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner data ended but no EOD provider available for completion",
            context_provider={'req_id': reqId}
        )
    
    def get_market_data_health(self) -> dict:
        """Get health metrics for market data flow monitoring."""
        with self._manager_lock:
            health = {
                'manager_connected': self.market_data_manager is not None,
                'total_ticks_processed': self._total_ticks_processed,
                'tick_errors': self._tick_errors,
                'last_tick_time': self._last_tick_time,
                'connection_status': 'Connected' if self.connection_manager.connected else 'Disconnected',
                'manager_type': type(self.market_data_manager).__name__ if self.market_data_manager else 'None',
                'early_tick_queue_size': self._early_tick_queue.qsize(),
                'early_ticks_logged': self._early_ticks_logged,
                'manager_connection_time': self._manager_connection_time
            }
            
            # Calculate error rate if we have processed ticks
            if self._total_ticks_processed > 0:
                health['error_rate_percent'] = round(
                    (self._tick_errors / self._total_ticks_processed) * 100, 2
                )
            else:
                health['error_rate_percent'] = 0.0
                
            return health