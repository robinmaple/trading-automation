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
        # <Context-Aware Logger Initialization - Begin>
        self.context_logger = get_context_logger()
        # <Context-Aware Logger Initialization - End>
        
        # <Context-Aware Logging - Data Feed Initialization Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKRDataFeed initialization starting",
            context_provider={
                "ibkr_client_provided": ibkr_client is not None,
                "event_bus_provided": event_bus is not None,
                "ibkr_client_connected": ibkr_client.connected if ibkr_client else False
            }
        )
        # <Context-Aware Logging - Data Feed Initialization Start - End>
        
        self.ibkr_client = ibkr_client
        # <Event-Driven Market Data Manager - Begin>
        # Create MarketDataManager with EventBus for price publishing
        self.market_data = MarketDataManager(ibkr_client, event_bus)
        # <Event-Driven Market Data Manager - End>
        
        # <Market Data Manager Connection - Begin>
        # CRITICAL: Connect MarketDataManager to IbkrClient to enable data flow
        if ibkr_client:
            ibkr_client.set_market_data_manager(self.market_data)
            print("🔗 MarketDataManager connected to IbkrClient for data flow")
        # <Market Data Manager Connection - End>

        # <Historical Data Manager Integration - Begin>
        # Create HistoricalDataManager for scanner historical data
        self.historical_data_manager = HistoricalDataManager(ibkr_client)
        
        # CRITICAL: Connect HistoricalDataManager to IbkrClient for historical data callbacks
        if ibkr_client:
            ibkr_client.set_historical_data_manager(self.historical_data_manager)
            print("🔗 HistoricalDataManager connected to IbkrClient for historical data flow")
        # <Historical Data Manager Integration - End>

        # Set initial connection state based on client
        self._connected = ibkr_client.connected if ibkr_client else False
        
        # <Context-Aware Logging - Data Feed Initialization Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKRDataFeed initialization completed",
            context_provider={
                "market_data_manager_initialized": self.market_data is not None,
                "historical_data_manager_initialized": self.historical_data_manager is not None,
                "initial_connection_state": self._connected,
                "event_bus_integrated": event_bus is not None
            }
        )
        # <Context-Aware Logging - Data Feed Initialization Complete - End>

    def connect(self, host='127.0.0.1', port=None, client_id=0) -> bool:
        """Establish a connection to IB Gateway/TWS. Returns success status."""
        # <Context-Aware Logging - Connection Attempt Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKRDataFeed connection attempt starting",
            context_provider={
                "host": host,
                "port": port,
                "client_id": client_id,
                "current_connection_state": self._connected
            }
        )
        # <Context-Aware Logging - Connection Attempt Start - End>
        
        try:
            success = self.ibkr_client.connect(host, port, client_id)
            if success:
                self._connected = True
                # <Ensure Manager Connection After Connect - Begin>
                # Re-affirm MarketDataManager connection after successful connection
                if hasattr(self.ibkr_client, 'set_market_data_manager'):
                    self.ibkr_client.set_market_data_manager(self.market_data)
                    print("✅ IBKRDataFeed: Market data flow established")
                
                # Re-affirm HistoricalDataManager connection after successful connection
                if hasattr(self.ibkr_client, 'set_historical_data_manager'):
                    self.ibkr_client.set_historical_data_manager(self.historical_data_manager)
                    print("✅ IBKRDataFeed: Historical data flow established")
                # <Ensure Manager Connection After Connect - End>
                
                # <Context-Aware Logging - Connection Success - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "IBKRDataFeed connection established successfully",
                    context_provider={
                        "host": host,
                        "port": port,
                        "client_id": client_id,
                        "market_data_flow_established": True,
                        "historical_data_flow_established": True
                    },
                    decision_reason="IBKR connection successful"
                )
                # <Context-Aware Logging - Connection Success - End>
            else:
                # <Context-Aware Logging - Connection Failed - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "IBKRDataFeed connection failed",
                    context_provider={
                        "host": host,
                        "port": port,
                        "client_id": client_id
                    },
                    decision_reason="IBKR connection returned failure"
                )
                # <Context-Aware Logging - Connection Failed - End>
                
            return success
        except Exception as e:
            # <Context-Aware Logging - Connection Exception - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKRDataFeed connection failed with exception",
                context_provider={
                    "host": host,
                    "port": port,
                    "client_id": client_id,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason=f"Connection exception: {e}"
            )
            # <Context-Aware Logging - Connection Exception - End>
            
            print(f"Connection failed: {e}")
            return False    
        
    def is_connected(self) -> bool:
        """Check if the data feed is connected to IBKR and market data is available."""
        # Always check the actual client connection status
        client_connected = self.ibkr_client and self.ibkr_client.connected
        
        # If client is connected but our flag isn't set, update it
        if client_connected and not self._connected:
            self._connected = True
            # <Context-Aware Logging - Connection State Update - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKRDataFeed detected active connection",
                context_provider={
                    "previous_connection_state": False,
                    "new_connection_state": True,
                    "ibkr_client_connected": client_connected
                }
            )
            # <Context-Aware Logging - Connection State Update - End>
            print("✅ IBKRDataFeed detected active connection")
        
        # Log connection status changes for monitoring
        if hasattr(self, '_last_logged_connection') and self._last_logged_connection != client_connected:
            # <Context-Aware Logging - Connection Status Change - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKRDataFeed connection status changed",
                context_provider={
                    "previous_status": self._last_logged_connection,
                    "current_status": client_connected,
                    "ibkr_client_available": self.ibkr_client is not None
                },
                decision_reason=f"Connection status changed to {client_connected}"
            )
            # <Context-Aware Logging - Connection Status Change - End>
        
        self._last_logged_connection = client_connected
        return client_connected

    def subscribe(self, symbol: str, contract: Contract) -> bool:
        """Subscribe to market data for a specific symbol using the MarketDataManager."""
        # <Context-Aware Logging - Subscription Start - Begin>
        self.context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Starting market data subscription",
            symbol=symbol,
            context_provider={
                "contract_symbol": contract.symbol if contract else "UNKNOWN",
                "security_type": contract.secType if contract else "UNKNOWN",
                "exchange": contract.exchange if contract else "UNKNOWN",
                "data_feed_connected": self.is_connected()
            }
        )
        # <Context-Aware Logging - Subscription Start - End>
        
        try:
            self.market_data.subscribe(symbol, contract)
            
            # <Context-Aware Logging - Subscription Success - Begin>
            self.context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Market data subscription successful",
                symbol=symbol,
                context_provider={
                    "subscription_count": len(self.market_data.subscriptions) if self.market_data else 0,
                    "contract_details": {
                        "symbol": contract.symbol if contract else "UNKNOWN",
                        "security_type": contract.secType if contract else "UNKNOWN",
                        "exchange": contract.exchange if contract else "UNKNOWN"
                    }
                },
                decision_reason=f"Successfully subscribed to {symbol}"
            )
            # <Context-Aware Logging - Subscription Success - End>
            
            return True
        except Exception as e:
            # <Context-Aware Logging - Subscription Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Market data subscription failed",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "contract_symbol": contract.symbol if contract else "UNKNOWN"
                },
                decision_reason=f"Failed to subscribe to {symbol}: {e}"
            )
            # <Context-Aware Logging - Subscription Error - End>
            
            print(f"Failed to subscribe to {symbol}: {e}")
            return False

    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current price data for a symbol, formatted for backward compatibility."""
        # <Context-Aware Logging - Price Request Start - Begin>
        self.context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Requesting current price data",
            symbol=symbol,
            context_provider={
                "data_feed_connected": self.is_connected(),
                "market_data_manager_available": self.market_data is not None
            }
        )
        # <Context-Aware Logging - Price Request Start - End>
        
        price_data = self.market_data.get_current_price(symbol)
        if not price_data:
            # <Context-Aware Logging - No Price Data - Begin>
            self.context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "No price data available for symbol",
                symbol=symbol,
                context_provider={
                    "price_data_available": False,
                    "subscription_active": symbol in self.market_data.subscriptions if self.market_data else False
                },
                decision_reason=f"No price data returned for {symbol}"
            )
            # <Context-Aware Logging - No Price Data - End>
            
            return None

        result = {
            'price': price_data.get('price', 0.0),
            'timestamp': price_data.get('timestamp', datetime.datetime.now()),
            'data_type': price_data.get('type', 'UNKNOWN'),
            'updates': price_data.get('updates', 0)
        }

        for key, value in price_data.items():
            if key not in result:
                result[key] = value

        # <Context-Aware Logging - Price Data Retrieved - Begin>
        self.context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Price data retrieved successfully",
            symbol=symbol,
            context_provider={
                "price": result['price'],
                "data_type": result['data_type'],
                "updates": result['updates'],
                "timestamp": result['timestamp'].isoformat() if hasattr(result['timestamp'], 'isoformat') else str(result['timestamp'])
            },
            decision_reason=f"Retrieved price ${result['price']:.2f} for {symbol}"
        )
        # <Context-Aware Logging - Price Data Retrieved - End>
        
        return result

    # <Health Monitoring Methods - Begin>
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get comprehensive health status of the data feed.
        
        Returns:
            Dictionary with health metrics for monitoring and troubleshooting
        """
        # <Context-Aware Logging - Health Status Request - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Generating data feed health status",
            context_provider={
                "health_check_type": "comprehensive"
            }
        )
        # <Context-Aware Logging - Health Status Request - End>
        
        health = {
            'data_feed_connected': self.is_connected(),
            'ibkr_client_connected': self.ibkr_client.connected if self.ibkr_client else False,
            'market_data_manager_active': self.market_data is not None,
            'historical_data_manager_active': self.historical_data_manager is not None,
            'subscription_count': len(self.market_data.subscriptions) if self.market_data else 0
        }
        
        # Add IbkrClient health metrics if available
        if self.ibkr_client and hasattr(self.ibkr_client, 'get_market_data_health'):
            client_health = self.ibkr_client.get_market_data_health()
            health.update({
                'market_data_flow': client_health,
                'overall_health': 'HEALTHY' if client_health.get('manager_connected') else 'DEGRADED'
            })
        else:
            health.update({
                'market_data_flow': 'UNKNOWN',
                'overall_health': 'UNKNOWN'
            })
            
        # <Context-Aware Logging - Health Status Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Data feed health status generated",
            context_provider=health,
            decision_reason=f"Health status: {health['overall_health']}"
        )
        # <Context-Aware Logging - Health Status Complete - End>
            
        return health

    def validate_data_flow(self, symbol: str = None) -> Dict[str, Any]:
        """
        Validate that market data is flowing properly for a specific symbol or all symbols.
        
        Args:
            symbol: Specific symbol to validate, or None for all subscribed symbols
            
        Returns:
            Dictionary with validation results
        """
        # <Context-Aware Logging - Data Flow Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting data flow validation",
            symbol=symbol,
            context_provider={
                "validation_scope": "specific_symbol" if symbol else "all_subscriptions"
            }
        )
        # <Context-Aware Logging - Data Flow Validation Start - End>
        
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
            
            # <Context-Aware Logging - Data Flow Validation Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Data flow validation failed - disconnected",
                symbol=symbol,
                context_provider=validation,
                decision_reason="Data feed not connected to IBKR"
            )
            # <Context-Aware Logging - Data Flow Validation Failed - End>
            
            return validation
            
        # Check MarketDataManager
        if not self.market_data:
            validation['data_flow_status'] = 'DEGRADED'
            validation['details']['error'] = 'MarketDataManager not initialized'
            
            # <Context-Aware Logging - Data Flow Validation Degraded - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Data flow validation degraded - no market data manager",
                symbol=symbol,
                context_provider=validation,
                decision_reason="MarketDataManager not initialized"
            )
            # <Context-Aware Logging - Data Flow Validation Degraded - End>
            
            return validation
            
        # Check HistoricalDataManager
        if not self.historical_data_manager:
            validation['data_flow_status'] = 'DEGRADED'
            validation['details']['error'] = 'HistoricalDataManager not initialized'
            
            # <Context-Aware Logging - Data Flow Validation Degraded - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Data flow validation degraded - no historical data manager",
                symbol=symbol,
                context_provider=validation,
                decision_reason="HistoricalDataManager not initialized"
            )
            # <Context-Aware Logging - Data Flow Validation Degraded - End>
            
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
            
        # <Context-Aware Logging - Data Flow Validation Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Data flow validation completed",
            symbol=symbol,
            context_provider=validation,
            decision_reason=f"Data flow status: {validation['data_flow_status']}"
        )
        # <Context-Aware Logging - Data Flow Validation Complete - End>
            
        return validation
    # <Health Monitoring Methods - End>

    def disconnect(self) -> None:
        """Disconnect from the IBKR API and clean up resources."""
        # <Context-Aware Logging - Disconnection Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting IBKRDataFeed disconnection",
            context_provider={
                "current_connection_state": self._connected,
                "ibkr_client_available": self.ibkr_client is not None
            }
        )
        # <Context-Aware Logging - Disconnection Start - End>
        
        if self.ibkr_client and hasattr(self.ibkr_client, 'disconnect'):
            self.ibkr_client.disconnect()
            self._connected = False
            
            # <Context-Aware Logging - Disconnection Complete - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKRDataFeed disconnected successfully",
                context_provider={
                    "new_connection_state": False,
                    "subscriptions_cleared": len(self.market_data.subscriptions) if self.market_data else 0
                },
                decision_reason="Disconnected from IBKR API"
            )
            # <Context-Aware Logging - Disconnection Complete - End>
            
            print("✅ Disconnected from IBKR")