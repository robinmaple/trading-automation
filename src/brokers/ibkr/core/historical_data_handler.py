# src/brokers/ibkr/core/historical_data_handler.py

"""
Handles historical data operations for IBKR including data requests,
scanner data processing, and historical data management.
"""

import threading
from typing import Optional, Dict, Any
from ibapi.contract import Contract

from src.core.context_aware_logger import get_context_logger, TradingEventType


class HistoricalDataHandler:
    """Manages historical data requests, scanner operations, and data processing."""
    
    def __init__(self, connection_manager):
        """Initialize historical data handler with connection reference."""
        self.context_logger = get_context_logger()
        self.connection_manager = connection_manager
        
        # Scanner and historical data tracking
        self._scanner_requests = {}
        self._historical_requests = {}
        self._scanner_lock = threading.RLock()
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "HistoricalDataHandler initialized",
            context_provider={
                "connection_manager_available": connection_manager is not None
            }
        )
    
    def req_historical_data(self, reqId: int, contract: Contract, endDateTime: str,
                          durationStr: str, barSizeSetting: str, whatToShow: str,
                          useRTH: int, formatDate: int, keepUpToDate: bool,
                          chartOptions: list) -> None:
        """
        Request historical data from IBKR API.
        """
        if not self.connection_manager.connected:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Historical data request failed - not connected to IBKR",
                context_provider={'req_id': reqId}
            )
            return
        
        try:
            self.connection_manager.reqHistoricalData(
                reqId, contract, endDateTime, durationStr, barSizeSetting,
                whatToShow, useRTH, formatDate, keepUpToDate, chartOptions
            )
            
            # Track the request
            self._historical_requests[reqId] = {
                'contract': contract,
                'symbol': contract.symbol,
                'end_date': endDateTime,
                'duration': durationStr,
                'bar_size': barSizeSetting,
                'request_time': threading.current_thread().name
            }
            
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Historical data request sent",
                context_provider={
                    'req_id': reqId,
                    'symbol': contract.symbol,
                    'duration': durationStr,
                    'bar_size': barSizeSetting
                }
            )
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Historical data request failed",
                context_provider={
                    'req_id': reqId,
                    'symbol': contract.symbol,
                    'error': str(e)
                }
            )
    
    def cancel_historical_data(self, reqId: int) -> None:
        """
        Cancel historical data request.
        """
        if not self.connection_manager.connected:
            return
        
        try:
            self.connection_manager.cancelHistoricalData(reqId)
            
            # Remove from tracking
            if reqId in self._historical_requests:
                symbol = self._historical_requests[reqId]['symbol']
                del self._historical_requests[reqId]
                
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Historical data request cancelled",
                    context_provider={
                        'req_id': reqId,
                        'symbol': symbol
                    }
                )
                
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Historical data cancellation failed",
                context_provider={
                    'req_id': reqId,
                    'error': str(e)
                }
            )
    
    def req_scanner_subscription(self, reqId: int, subscription) -> None:
        """
        Request scanner subscription.
        """
        if not self.connection_manager.connected:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner subscription failed - not connected to IBKR",
                context_provider={'req_id': reqId}
            )
            return
        
        try:
            self.connection_manager.reqScannerSubscription(reqId, subscription)
            
            with self._scanner_lock:
                self._scanner_requests[reqId] = {
                    'subscription': subscription,
                    'request_time': threading.current_thread().name
                }
            
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner subscription requested",
                context_provider={'req_id': reqId}
            )
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner subscription failed",
                context_provider={
                    'req_id': reqId,
                    'error': str(e)
                }
            )
    
    def cancel_scanner_subscription(self, reqId: int) -> None:
        """
        Cancel scanner subscription.
        """
        if not self.connection_manager.connected:
            return
        
        try:
            self.connection_manager.cancelScannerSubscription(reqId)
            
            with self._scanner_lock:
                if reqId in self._scanner_requests:
                    del self._scanner_requests[reqId]
            
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner subscription cancelled",
                context_provider={'req_id': reqId}
            )
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner cancellation failed",
                context_provider={
                    'req_id': reqId,
                    'error': str(e)
                }
            )
    
    def req_scanner_parameters(self) -> None:
        """
        Request scanner parameters.
        """
        if not self.connection_manager.connected:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner parameters request failed - not connected to IBKR"
            )
            return
        
        try:
            self.connection_manager.reqScannerParameters()
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner parameters requested"
            )
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Scanner parameters request failed",
                context_provider={'error': str(e)}
            )
    
    def get_historical_requests_status(self) -> Dict[str, Any]:
        """
        Get status of all historical data requests.
        """
        return {
            'active_historical_requests': len(self._historical_requests),
            'active_scanner_requests': len(self._scanner_requests),
            'historical_requests_details': {
                req_id: {
                    'symbol': details['symbol'],
                    'duration': details['duration'],
                    'bar_size': details['bar_size']
                }
                for req_id, details in self._historical_requests.items()
            }
        }
    
    # Scanner parameter callback (if needed by other components)
    def scannerParameters(self, xml: str) -> None:
        """
        Callback: Received scanner parameters.
        This can be handled by specific scanner components.
        """
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner parameters received",
            context_provider={'xml_length': len(xml) if xml else 0}
        )