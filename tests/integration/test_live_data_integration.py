import unittest
from src.trading.execution.trading_manager import TradingManager
from src.market_data.feeds.ibkr_data_feed import IBKRDataFeed
from src.brokers.ibkr.ibkr_client import IbkrClient

class TestLiveDataIntegration(unittest.TestCase):
    """Integration test with actual IBKR connection"""
    
    @unittest.skipUnless(False, "Enable for live testing only")  # Disabled by default
    def test_live_ibkr_connection(self):
        """Test actual connection to IBKR (requires TWS/Gateway running)"""
        ibkr_client = IbkrClient()
        data_feed = IBKRDataFeed(ibkr_client)
        
        # Try to connect
        connected = data_feed.connect()
        
        if connected:
            print("✅ Successfully connected to IBKR")
            
            # Test basic functionality
            self.assertTrue(data_feed.is_connected())
            
            # Test market data subscription
            from ibapi.contract import Contract
            contract = Contract()
            contract.symbol = "SPY"
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"
            
            subscribed = data_feed.subscribe("SPY", contract)
            self.assertTrue(subscribed)
            
            # Get some data
            import time
            time.sleep(2)  # Wait for data
            price_data = data_feed.get_current_price("SPY")
            
            self.assertIsNotNone(price_data)
            self.assertNotEqual(price_data['price'], 0)
            print(f"✅ Live data received: ${price_data['price']:.2f}")
            
            data_feed.disconnect()
        else:
            print("⚠️  IBKR not available for testing (TWS/Gateway may not be running)")
            self.skipTest("IBKR connection not available")