"""
Live data feed implementation using the Interactive Brokers API.
Adapts the existing IbkrClient and MarketDataManager to the AbstractDataFeed interface.
"""

import threading
import datetime
from typing import Dict, Any, Optional
from ibapi.contract import Contract

from src.core.abstract_data_feed import AbstractDataFeed
from src.core.market_data_manager import MarketDataManager
from src.core.ibkr_client import IbkrClient
from src.core.event_bus import EventBus
from src.core.context_aware_logger import get_context_logger, TradingEventType
from src.core.historical_data_manager import HistoricalDataManager


class IBKRDataFeed(AbstractDataFeed):
    """Concrete data feed implementation for Interactive Brokers market data."""

    def __init__(self, ibkr_client: IbkrClient, event_bus: EventBus = None):
        """Initialize the data feed with an existing IbkrClient instance."""
        self.context_logger = get_context_logger()
        
        # Minimal initialization logging
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Data feed initialized",
            context_provider={
                "ibkr_client_ready": ibkr_client is not None,
                "event_bus_provided": event_bus is not None
            }
        )
        
        self.ibkr_client = ibkr_client
        self.market_data = MarketDataManager(ibkr_client, event_bus)
        self.historical_data_manager = HistoricalDataManager(ibkr_client)

        # Set initial connection state based on client
        self._connected = ibkr_client.connected if ibkr_client else False
        
        # Manager connection verification
        self._manager_connection_verified = False
        self._verify_manager_connection()
        
        # Execution symbols coordination
        self._execution_symbols: set = set()
        
        # Connection state tracking
        self._last_logged_connection = None

    def _verify_manager_connection(self) -> bool:
        """
        Verify and ensure MarketDataManager and HistoricalDataManager are properly connected to IbkrClient.
        """
        if not self.ibkr_client:
            return False
        
        connection_results = {
            'market_data_manager_connected': False,
            'historical_data_manager_connected': False
        }
        
        try:
            # Connect MarketDataManager to IbkrClient for data flow
            if hasattr(self.ibkr_client, 'set_market_data_manager'):
                self.ibkr_client.set_market_data_manager(self.market_data)
                connection_results['market_data_manager_connected'] = True
            
            # Connect HistoricalDataManager to IbkrClient for historical data callbacks
            if hasattr(self.ibkr_client, 'set_historical_data_manager'):
                self.ibkr_client.set_historical_data_manager(self.historical_data_manager)
                connection_results['historical_data_manager_connected'] = True
            
            # Determine overall connection status
            self._manager_connection_verified = (
                connection_results['market_data_manager_connected'] and 
                connection_results['historical_data_manager_connected']
            )
            
            # Only log connection issues, not successes
            if not self._manager_connection_verified:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Manager connection incomplete",
                    context_provider=connection_results,
                    decision_reason="Some managers not connected"
                )
            
            return self._manager_connection_verified
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Manager connection failed",
                context_provider={
                    "error": str(e)
                },
                decision_reason="Manager connection exception"
            )
            return False

    def connect(self, host='127.0.0.1', port=None, client_id=0) -> bool:
        """Establish a connection to IB Gateway/TWS. Returns success status."""
        try:
            success = self.ibkr_client.connect(host, port, client_id)
            if success:
                self._connected = True
                
                # Re-verify manager connections after successful connection
                manager_reconnect_success = self._verify_manager_connection()
                
                # Only log connection issues
                if not manager_reconnect_success:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Manager reconnection failed after connect",
                        context_provider={
                            "host": host,
                            "port": port
                        },
                        decision_reason="Manager callback routing may be broken"
                    )
                
                # Log successful connection
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Connection established",
                    context_provider={
                        "host": host,
                        "port": port,
                        "manager_ready": manager_reconnect_success
                    }
                )
            else:
                # Log connection failure
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Connection failed",
                    context_provider={
                        "host": host,
                        "port": port
                    },
                    decision_reason="IBKR connection returned failure"
                )
                
            return success
        except Exception as e:
            # Log connection exception
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Connection exception",
                context_provider={
                    "host": host,
                    "port": port,
                    "error": str(e)
                },
                decision_reason="Connection exception"
            )
            return False

    def subscribe(self, symbol: str, contract: Contract) -> bool:
        """Subscribe to market data for a specific symbol using the MarketDataManager."""
        # Verify manager connection before subscription
        if not self._manager_connection_verified:
            connection_verified = self._verify_manager_connection()
            if not connection_verified:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Subscription aborted - no manager connection",
                    symbol=symbol,
                    context_provider={
                        "symbol": symbol
                    },
                    decision_reason="Unverified manager connection"
                )
                return False
        
        try:
            self.market_data.subscribe(symbol, contract)
            
            # Only log subscription for execution symbols or on errors
            if symbol in self._execution_symbols:
                self.context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Execution symbol subscribed",
                    symbol=symbol,
                    context_provider={
                        "subscription_count": len(self.market_data.subscriptions) if self.market_data else 0
                    }
                )
            
            return True
        except Exception as e:
            # Log subscription errors
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Subscription failed",
                symbol=symbol,
                context_provider={
                    "error": str(e),
                    "symbol": symbol
                },
                decision_reason=f"Failed to subscribe: {e}"
            )
            return False

    def get_health_status(self) -> Dict[str, Any]:
        """
        Get comprehensive health status of the data feed.
        """
        health = {
            'data_feed_connected': self.is_connected(),
            'ibkr_client_connected': self.ibkr_client.connected if self.ibkr_client else False,
            'market_data_manager_active': self.market_data is not None,
            'historical_data_manager_active': self.historical_data_manager is not None,
            'subscription_count': len(self.market_data.subscriptions) if self.market_data else 0,
            'manager_connection_verified': self._manager_connection_verified,
            'execution_symbols_tracking': len(self._execution_symbols)
        }
        
        # Add IbkrClient health metrics if available
        if self.ibkr_client and hasattr(self.ibkr_client, 'get_market_data_health'):
            client_health = self.ibkr_client.get_market_data_health()
            health.update({
                'market_data_flow': client_health,
                'overall_health': 'HEALTHY' if (client_health.get('manager_connected') and self._manager_connection_verified) else 'DEGRADED'
            })
        else:
            health.update({
                'market_data_flow': 'UNKNOWN',
                'overall_health': 'DEGRADED' if not self._manager_connection_verified else 'UNKNOWN'
            })
            
        # Only log health status changes or issues
        current_health = health['overall_health']
        if not hasattr(self, '_last_health_status') or self._last_health_status != current_health:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Health status changed",
                context_provider={
                    "new_status": current_health,
                    "subscriptions": health['subscription_count']
                },
                decision_reason=f"Health status: {current_health}"
            )
            self._last_health_status = current_health
            
        return health
 
    def is_connected(self) -> bool:
        """Check if the data feed is connected to IBKR and market data is available."""
        # Always check the actual client connection status
        client_connected = self.ibkr_client and self.ibkr_client.connected
        
        # If client is connected but our flag isn't set, update it
        if client_connected and not self._connected:
            self._connected = True
            # Log connection state recovery
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Connection state recovered",
                context_provider={
                    "client_connected": client_connected
                }
            )
        
        # Log connection status changes for monitoring
        if self._last_logged_connection != client_connected:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Connection status changed",
                context_provider={
                    "new_status": client_connected
                },
                decision_reason=f"Connection: {client_connected}"
            )
        
        self._last_logged_connection = client_connected
        return client_connected

    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current price data for a symbol, formatted for backward compatibility."""
        price_data = self.market_data.get_current_price(symbol)
        if not price_data:
            # Only log missing price data for execution symbols
            if symbol in self._execution_symbols:
                self.context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "No price data for execution symbol",
                    symbol=symbol,
                    context_provider={
                        "symbol": symbol
                    },
                    decision_reason="Missing price data"
                )
            return None

        result = {
            'price': price_data.get('price', 0.0),
            'timestamp': price_data.get('timestamp', datetime.datetime.now()),
            'data_type': price_data.get('type', 'UNKNOWN'),
            'updates': price_data.get('updates', 0)
        }

        # Add any additional fields
        for key, value in price_data.items():
            if key not in result:
                result[key] = value

        # Only log price retrieval for execution symbols or significant moves
        if symbol in self._execution_symbols:
            self.context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Price retrieved for execution",
                symbol=symbol,
                context_provider={
                    "price": result['price'],
                    "updates": result['updates']
                }
            )
        
        return result

    def validate_data_flow(self, symbol: str = None) -> Dict[str, Any]:
        """
        Validate that market data is flowing properly for a specific symbol or all symbols.
        """
        validation = {
            'timestamp': datetime.datetime.now(),
            'symbol': symbol,
            'data_flow_status': 'UNKNOWN',
            'details': {}
        }
        
        # Check basic connectivity
        if not self.is_connected():
            validation['data_flow_status'] = 'DISCONNECTED'
            validation['details']['error'] = 'Data feed not connected to IBKR'
            return validation
            
        # Check MarketDataManager
        if not self.market_data:
            validation['data_flow_status'] = 'DEGRADED'
            validation['details']['error'] = 'MarketDataManager not initialized'
            return validation
            
        # Check HistoricalDataManager
        if not self.historical_data_manager:
            validation['data_flow_status'] = 'DEGRADED'
            validation['details']['error'] = 'HistoricalDataManager not initialized'
            return validation
            
        # Check specific symbol or all symbols
        symbols_to_check = [symbol] if symbol else list(self.market_data.subscriptions.keys())
        
        symbol_status = {}
        for sym in symbols_to_check:
            price_data = self.get_current_price(sym)
            if price_data and price_data.get('price', 0) > 0:
                symbol_status[sym] = {
                    'status': 'ACTIVE',
                    'price': price_data.get('price'),
                    'updates': price_data.get('updates', 0),
                    'last_update': price_data.get('timestamp')
                }
            else:
                symbol_status[sym] = {
                    'status': 'NO_DATA',
                    'price': 0.0,
                    'updates': 0,
                    'last_update': None
                }
                
        validation['details']['symbols'] = symbol_status
        
        # Determine overall status
        active_symbols = [s for s, status in symbol_status.items() if status['status'] == 'ACTIVE']
        if not symbols_to_check:
            validation['data_flow_status'] = 'NO_SUBSCRIPTIONS'
        elif active_symbols:
            validation['data_flow_status'] = 'HEALTHY'
            validation['details']['active_symbol_count'] = len(active_symbols)
        else:
            validation['data_flow_status'] = 'NO_DATA_FLOW'
            
        # Only log validation failures or status changes
        if validation['data_flow_status'] != 'HEALTHY':
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Data flow validation issue",
                symbol=symbol,
                context_provider={
                    "status": validation['data_flow_status'],
                    "active_symbols": len(active_symbols)
                },
                decision_reason=f"Data flow: {validation['data_flow_status']}"
            )
            
        return validation

    def disconnect(self) -> None:
        """Disconnect from the IBKR API and clean up resources."""
        if self.ibkr_client and hasattr(self.ibkr_client, 'disconnect'):
            self.ibkr_client.disconnect()
            self._connected = False
            
            # Log disconnection
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Disconnected from IBKR",
                context_provider={
                    "subscriptions_cleared": len(self.market_data.subscriptions) if self.market_data else 0
                }
            )

    def set_execution_symbols(self, execution_symbols: set) -> None:
        """
        Set symbols that require execution flow bypassing filtering.
        """
        previous_count = len(self._execution_symbols)
        self._execution_symbols = execution_symbols.copy() if execution_symbols else set()
        
        # Only log execution symbols changes
        if previous_count != len(self._execution_symbols):
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Execution symbols updated",
                context_provider={
                    'new_count': len(self._execution_symbols),
                    'symbols': list(self._execution_symbols)[:5]  # Log first 5
                },
                decision_reason=f"Execution symbols: {len(self._execution_symbols)}"
            )
        
        # Update MarketDataManager with execution symbols if available
        if self.market_data and hasattr(self.market_data, 'set_execution_symbols'):
            self.market_data.set_execution_symbols(self._execution_symbols)