# src/brokers/ibkr/core/connection_manager.py

"""
Manages IBKR TWS/Gateway connection lifecycle including connection, 
disconnection, and connection health monitoring.
"""

import threading
import time
import datetime
from typing import Optional
from ibapi.client import EClient
from ibapi.wrapper import EWrapper

from src.core.context_aware_logger import get_context_logger, TradingEventType
from src.trading.risk.account_utils import is_paper_account, get_ibkr_port


class ConnectionManager(EClient, EWrapper):
    """Manages the connection lifecycle and basic IBKR API communication."""
    
    def __init__(self, host='127.0.0.1', port=None, client_id=1, mode='auto'):
        """Initialize connection manager with connection parameters."""
        self.context_logger = get_context_logger()
        
        # Connection state
        self.host = host
        self.port = port or self._get_port_from_mode(mode)
        self.client_id = client_id
        self.mode = mode
        
        # Connection tracking
        self.next_valid_id = None
        self.connected = False
        self.connection_event = threading.Event()
        self.account_ready_event = threading.Event()
        
        # Account information
        self.account_number = None
        self.account_name = None
        self.is_paper_account = False
        
        # Error tracking
        self.displayed_errors = set()
        
        # Initialize EClient
        EClient.__init__(self, self)
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "ConnectionManager initialized",
            context_provider={
                "host": host,
                "port": self.port,
                "client_id": client_id,
                "mode": mode
            }
        )
    
    def connect(self, host: Optional[str] = None, port: Optional[int] = None, 
                client_id: Optional[int] = None) -> bool:
        """Establish connection to IBKR TWS/Gateway."""
        connect_host = host or self.host
        connect_port = port or self.port
        connect_client_id = client_id or self.client_id
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Attempting IBKR connection",
            context_provider={
                'host': connect_host,
                'port': connect_port,
                'client_id': connect_client_id,
                'mode': self.mode
            }
        )
        
        # Establish connection
        EClient.connect(self, connect_host, connect_port, connect_client_id)
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        
        # Wait for connection and account info
        ready = self.connection_event.wait(10) and self.account_ready_event.wait(10)
        if not ready:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR connection timeout",
                context_provider={
                    'timeout_seconds': 10,
                    'connection_event_set': self.connection_event.is_set(),
                    'account_ready_event_set': self.account_ready_event.is_set()
                }
            )
            return False
        
        # Validate account/port match
        expected_port = get_ibkr_port(self.account_name)
        if connect_port != expected_port:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR port mismatch detected",
                context_provider={
                    'account_name': self.account_name,
                    'expected_port': expected_port,
                    'actual_port': connect_port
                }
            )
            self.disconnect()
            return False
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKR connection established successfully",
            context_provider={
                'account_name': self.account_name,
                'account_number': self.account_number,
                'is_paper_account': self.is_paper_account,
                'port': connect_port
            }
        )
        return True
    
    def disconnect(self) -> None:
        """Cleanly disconnect from TWS/Gateway."""
        if self.connected:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Disconnecting from IBKR",
                context_provider={
                    'account_name': self.account_name
                }
            )
            super().disconnect()
            self.connected = False
    
    def _get_port_from_mode(self, mode: str) -> int:
        """Determine port based on mode parameter."""
        if mode == 'paper':
            return 7497
        elif mode == 'live':
            return 7496
        elif mode == 'auto':
            return 7497  # Default to paper for auto mode
        else:
            raise ValueError(f"Invalid mode: {mode}")
    
    # IBKR API Callbacks
    def nextValidId(self, orderId: int) -> None:
        """Callback: Connection ready with valid order ID."""
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKR connection ready - next valid order ID received",
            context_provider={
                'next_valid_id': orderId
            }
        )
        self.next_valid_id = orderId
        self.connected = True
        self.connection_event.set()
    
    def managedAccounts(self, accountsList: str) -> None:
        """Callback: Received managed account list."""
        if accountsList:
            self.account_number = accountsList.split(',')[0].strip()
            self.account_name = self.account_number
            self.is_paper_account = is_paper_account(self.account_name)
            self.account_ready_event.set()
    
    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="", *args) -> None:
        """Callback: Handle errors from IBKR API."""
        super().error(reqId, errorCode, errorString, advancedOrderRejectJson)
        
        error_key = (reqId, errorCode)
        if error_key in self.displayed_errors:
            return

        self.displayed_errors.add(error_key)
        
        # Log significant errors
        if errorCode in [2104, 2106, 1100, 1101, 1102]:  # Connection/data farm errors
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"IBKR connection error: {errorCode} - {errorString}",
                context_provider={
                    'error_code': errorCode,
                    'error_string': errorString
                }
            )
    
    def get_connection_status(self) -> dict:
        """Get current connection status and health."""
        return {
            'connected': self.connected,
            'account_name': self.account_name,
            'account_number': self.account_number,
            'is_paper_account': self.is_paper_account,
            'next_valid_id': self.next_valid_id,
            'host': self.host,
            'port': self.port,
            'client_id': self.client_id
        }