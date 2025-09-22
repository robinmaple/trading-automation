"""
Market Data Debugging Tool for IBKR API Issues
Use this to diagnose and log market data problems for IBKR support
"""

import datetime
from typing import Dict, List, Optional, Any
from src.core.ibkr_client import IbkrClient
from src.core.planned_order import PlannedOrder, SecurityType, Action, OrderType
from src.data_feeds.ibkr_data_feed import IBKRDataFeed  # Fixed case sensitivity
import logging

logger = logging.getLogger(__name__)

class MarketDataDebugger:
    """Debug tool for market data issues with IBKR API"""
    
    def __init__(self, ibkr_client: IbkrClient):
        self.ibkr_client = ibkr_client
        self.data_feed = IBKRDataFeed(ibkr_client)  # Fixed case
        self.diagnostic_log = []
        self._connected = False
    
    def connect(self) -> bool:
        """Connect to IBKR for debugging"""
        try:
            self._connected = self.data_feed.connect()
            if self._connected:
                self.log_diagnostic("✅ Connected to IBKR for debugging")
            else:
                self.log_diagnostic("❌ Failed to connect to IBKR", "ERROR")
            return self._connected
        except Exception as e:
            self.log_diagnostic(f"❌ Connection failed: {e}", "ERROR")
            return False
    
    def log_diagnostic(self, message: str, level: str = "INFO"):
        """Add diagnostic message with timestamp"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp} [{level}] {message}"
        self.diagnostic_log.append(log_entry)
        print(log_entry)
    
    def check_connection_status(self) -> Dict:
        """Check IBKR connection status"""
        status = {
            "connected": self.ibkr_client.connected if self.ibkr_client else False,
            "connection_time": getattr(self.ibkr_client, 'connection_time', None),
            "client_id": self.ibkr_client.client_id if self.ibkr_client else None,
            "account_number": getattr(self.ibkr_client, 'account_number', 'Unknown'),
            "is_paper_account": getattr(self.ibkr_client, 'is_paper_account', False)
        }
        
        self.log_diagnostic(f"Connection Status: {status}")
        return status
    
    def check_market_data_subscriptions(self) -> Dict:
        """Check current market data subscriptions"""
        # Access subscriptions from the market data manager
        market_data_mgr = getattr(self.data_feed, 'market_data', None)
        if not market_data_mgr:
            self.log_diagnostic("❌ Market data manager not available", "ERROR")
            return {}
        
        subscriptions = getattr(market_data_mgr, 'subscriptions', {})
        subscription_info = {}
        
        for symbol, sub_data in subscriptions.items():
            subscription_info[symbol] = {
                "subscribed": sub_data.get('subscribed', False),
                "last_update": sub_data.get('last_update'),
                "update_count": sub_data.get('update_count', 0),
                "contract": str(sub_data.get('contract', {}))
            }
        
        self.log_diagnostic(f"Active Subscriptions: {len(subscriptions)} symbols")
        for symbol, info in subscription_info.items():
            self.log_diagnostic(f"  {symbol}: {info}")
        
        return subscription_info
    
    def _create_contract_from_symbol(self, symbol: str, sec_type: str = "STK") -> Any:
        """Create an IBKR contract using your existing PlannedOrder pattern"""
        try:
            # Use your existing PlannedOrder.to_ib_contract() pattern
            planned_order = PlannedOrder(
                security_type=SecurityType(sec_type),
                exchange=self._get_default_exchange(sec_type),
                currency="USD",
                action=Action.BUY,  # Default action for subscription
                symbol=symbol,
                order_type=OrderType.LMT  # Default order type
            )
            return planned_order.to_ib_contract()
        except Exception as e:
            self.log_diagnostic(f"❌ Failed to create contract for {symbol}: {e}", "ERROR")
            return None
    
    def _get_default_exchange(self, sec_type: str) -> str:
        """Get default exchange based on security type"""
        if sec_type == "CASH":
            return "IDEALPRO"
        elif sec_type == "FUT":
            return "GLOBEX"
        else:  # STK, OPT, etc.
            return "SMART"
    
    def test_market_data_request(self, symbol: str, sec_type: str = "STK") -> Dict:
        """Test market data request for a specific symbol"""
        self.log_diagnostic(f"Testing market data for {symbol} ({sec_type})")
        
        # Create contract using your existing pattern
        contract = self._create_contract_from_symbol(symbol, sec_type)
        if not contract:
            return {"success": False, "error": "Failed to create contract"}
        
        # Test subscription
        subscribe_success = self.data_feed.subscribe(symbol, contract)
        
        # Get current price
        price_data = self.data_feed.get_current_price(symbol)
        
        result = {
            "symbol": symbol,
            "security_type": sec_type,
            "contract": str(contract),
            "subscribe_success": subscribe_success,
            "price_data": price_data,
            "has_price": price_data is not None and price_data.get('price') not in [None, 0],
            "price_value": price_data.get('price') if price_data else None,
            "timestamp": price_data.get('timestamp') if price_data else None,
            "data_type": price_data.get('data_type') if price_data else None
        }
        
        self.log_diagnostic(f"Market Data Test Result: {result}")
        return result
    
    def run_comprehensive_diagnostic(self, test_symbols: List[str] = None) -> Dict:
        """Run comprehensive diagnostic of market data system"""
        if not test_symbols:
            test_symbols = ["EUR", "AAPL", "ES", "GLD", "GBP", "NQ", "CL", "GC", "IBM", "TSLA"]
        
        self.log_diagnostic("=" * 60)
        self.log_diagnostic("STARTING COMPREHENSIVE MARKET DATA DIAGNOSTIC")
        self.log_diagnostic("=" * 60)
        
        # 1. Check connection status
        connection_status = self.check_connection_status()
        
        # 2. Check existing subscriptions
        subscriptions = self.check_market_data_subscriptions()
        
        # 3. Test market data for each symbol
        test_results = {}
        for symbol in test_symbols:
            # Determine contract type based on symbol
            if symbol in ["EUR", "GBP", "JPY", "AUD", "CAD", "USD"]:
                sec_type = "CASH"
            elif symbol in ["ES", "NQ", "YM", "CL", "GC"]:
                sec_type = "FUT"
            else:
                sec_type = "STK"
            
            test_results[symbol] = self.test_market_data_request(symbol, sec_type)
        
        # 4. Generate summary
        successful_requests = sum(1 for result in test_results.values() if result.get('has_price'))
        
        summary = {
            "diagnostic_time": datetime.datetime.now().isoformat(),
            "connection_status": connection_status,
            "total_subscriptions": len(subscriptions),
            "test_symbols_count": len(test_symbols),
            "successful_requests": successful_requests,
            "failed_requests": len(test_symbols) - successful_requests,
            "detailed_results": test_results
        }
        
        self.log_diagnostic("=" * 60)
        self.log_diagnostic("DIAGNOSTIC SUMMARY:")
        self.log_diagnostic(f"Connected: {connection_status['connected']}")
        self.log_diagnostic(f"Account: {connection_status['account_number']}")
        self.log_diagnostic(f"Paper Trading: {connection_status['is_paper_account']}")
        self.log_diagnostic(f"Test Symbols: {len(test_symbols)}")
        self.log_diagnostic(f"Successful Data: {successful_requests}")
        self.log_diagnostic(f"Failed Data: {len(test_symbols) - successful_requests}")
        self.log_diagnostic("=" * 60)
        
        return summary
    
    def get_diagnostic_log(self) -> str:
        """Get complete diagnostic log as string"""
        return "\n".join(self.diagnostic_log)
    
    def save_diagnostic_report(self, filename: str = None) -> str:
        """Save diagnostic report to file"""
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"market_data_diagnostic_{timestamp}.log"
        
        try:
            with open(filename, 'w') as f:
                f.write(self.get_diagnostic_log())
            
            self.log_diagnostic(f"Diagnostic report saved to: {filename}")
            return filename
        except Exception as e:
            self.log_diagnostic(f"❌ Failed to save diagnostic report: {e}", "ERROR")
            return ""