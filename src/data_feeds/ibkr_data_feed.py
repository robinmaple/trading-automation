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


class IBKRDataFeed(AbstractDataFeed):
    """Concrete data feed implementation for Interactive Brokers market data."""

    def __init__(self, ibkr_client: IbkrClient):
        """Initialize the data feed with an existing IbkrClient instance."""
        self.ibkr_client = ibkr_client
        self.market_data = MarketDataManager(ibkr_client)
        
        # <Market Data Manager Connection - Begin>
        # CRITICAL: Connect MarketDataManager to IbkrClient to enable data flow
        if ibkr_client:
            ibkr_client.set_market_data_manager(self.market_data)
            print("🔗 MarketDataManager connected to IbkrClient for data flow")
        # <Market Data Manager Connection - End>
        
        # Set initial connection state based on client
        self._connected = ibkr_client.connected if ibkr_client else False

    def connect(self, host='127.0.0.1', port=None, client_id=0) -> bool:
        """Establish a connection to IB Gateway/TWS. Returns success status."""
        try:
            success = self.ibkr_client.connect(host, port, client_id)
            if success:
                self._connected = True
                # <Ensure Manager Connection After Connect - Begin>
                # Re-affirm MarketDataManager connection after successful connection
                if hasattr(self.ibkr_client, 'set_market_data_manager'):
                    self.ibkr_client.set_market_data_manager(self.market_data)
                    print("✅ IBKRDataFeed: Market data flow established")
                # <Ensure Manager Connection After Connect - End>
            return success
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def is_connected(self) -> bool:
        """Check if the data feed is connected to IBKR and market data is available."""
        # Always check the actual client connection status
        client_connected = self.ibkr_client and self.ibkr_client.connected
        
        # If client is connected but our flag isn't set, update it
        if client_connected and not self._connected:
            self._connected = True
            print("✅ IBKRDataFeed detected active connection")
        
        return client_connected

    def subscribe(self, symbol: str, contract: Contract) -> bool:
        """Subscribe to market data for a specific symbol using the MarketDataManager."""
        try:
            self.market_data.subscribe(symbol, contract)
            return True
        except Exception as e:
            print(f"Failed to subscribe to {symbol}: {e}")
            return False

    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current price data for a symbol, formatted for backward compatibility."""
        price_data = self.market_data.get_current_price(symbol)
        if not price_data:
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

        return result

    # <Health Monitoring Methods - Begin>
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get comprehensive health status of the data feed.
        
        Returns:
            Dictionary with health metrics for monitoring and troubleshooting
        """
        health = {
            'data_feed_connected': self.is_connected(),
            'ibkr_client_connected': self.ibkr_client.connected if self.ibkr_client else False,
            'market_data_manager_active': self.market_data is not None,
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
            
        return health

    def validate_data_flow(self, symbol: str = None) -> Dict[str, Any]:
        """
        Validate that market data is flowing properly for a specific symbol or all symbols.
        
        Args:
            symbol: Specific symbol to validate, or None for all subscribed symbols
            
        Returns:
            Dictionary with validation results
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
            
        return validation
    # <Health Monitoring Methods - End>

    def disconnect(self) -> None:
        """Disconnect from the IBKR API and clean up resources."""
        if self.ibkr_client and hasattr(self.ibkr_client, 'disconnect'):
            self.ibkr_client.disconnect()
            self._connected = False
            print("✅ Disconnected from IBKR")