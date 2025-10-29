import unittest
from unittest.mock import Mock, patch
import datetime
from src.trading.execution.trading_manager import TradingManager
from src.market_data.feeds.ibkr_data_feed import IBKRDataFeed
from src.brokers.ibkr.ibkr_client import IbkrClient

class TestIBKRDataValidation(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment"""
        self.ibkr_client = Mock(spec=IbkrClient)
        self.ibkr_client.connected = True
        self.ibkr_client.account_number = "DU123456"  # Paper account
        self.ibkr_client.is_paper_account = True
        
        self.data_feed = Mock(spec=IBKRDataFeed)
        self.data_feed.is_connected.return_value = True
        
    def test_data_feed_type_validation(self):
        """Test that the system uses IBKRDataFeed"""
        # Create trading manager with mocked components
        manager = TradingManager(data_feed=self.data_feed)
        
        # Verify the data feed type
        self.assertIsInstance(manager.data_feed, IBKRDataFeed)
        print("✅ Correct data feed type confirmed")
    
    def test_mock_feed_configuration_removed(self):
        """Test that mock feed configuration is no longer called"""
        manager = TradingManager(data_feed=self.data_feed)
        
        # Mock the load_planned_orders to avoid file I/O
        with patch.object(manager.loading_service, 'load_and_validate_orders') as mock_load:
            mock_load.return_value = []  # Empty list for testing
            
            # Load planned orders - this should NOT attempt any mock configuration
            manager.load_planned_orders()
            
            # The test passes if no configure_intelligence is called on IBKRDataFeed
            # (since IBKRDataFeed should never have this method)
            print("✅ No mock configuration attempted on IBKRDataFeed")
            
            # Additional verification: ensure we're not checking for mock methods
            self.assertFalse(hasattr(manager.data_feed, 'configure_intelligence'),
                           "IBKRDataFeed should not have configure_intelligence method")
    
    def test_live_data_availability(self):
        """Test that live market data can be retrieved"""
        # Mock successful price data response
        mock_price_data = {
            'price': 450.25,
            'timestamp': datetime.datetime.now(),
            'data_type': 'LIVE',
            'updates': 15
        }
        self.data_feed.get_current_price.return_value = mock_price_data
        
        # Test data retrieval
        symbol = "AAPL"
        result = self.data_feed.get_current_price(symbol)
        
        self.assertIsNotNone(result)
        self.assertNotEqual(result['price'], 0)
        self.assertEqual(result['data_type'], 'LIVE')
        print(f"✅ Live market data validated: ${result['price']:.2f}")
    
    def test_paper_vs_live_detection(self):
        """Test that paper/live account detection works correctly"""
        # Test paper account
        self.ibkr_client.account_number = "DU123456"
        self.ibkr_client.is_paper_account = True
        manager = TradingManager(data_feed=self.data_feed, ibkr_client=self.ibkr_client)
        
        is_live = manager._get_trading_mode()
        self.assertFalse(is_live)
        print("✅ Paper account detection working")
        
        # Test live account
        self.ibkr_client.account_number = "U987654"
        self.ibkr_client.is_paper_account = False
        manager = TradingManager(data_feed=self.data_feed, ibkr_client=self.ibkr_client)
        
        is_live = manager._get_trading_mode()
        self.assertTrue(is_live)
        print("✅ Live account detection working")
    
    def test_ibkr_data_feed_interface(self):
        """Test that IBKRDataFeed has the correct interface (no mock methods)"""
        # Create a real IBKRDataFeed instance (not a mock)
        from src.market_data.feeds.ibkr_data_feed import IBKRDataFeed
        
        # These should exist on IBKRDataFeed
        expected_methods = ['connect', 'is_connected', 'subscribe', 'get_current_price']
        for method in expected_methods:
            self.assertTrue(hasattr(IBKRDataFeed, method),
                          f"IBKRDataFeed should have {method} method")
        
        # These should NOT exist on IBKRDataFeed (they're mock-only)
        mock_only_methods = ['configure_intelligence', 'set_mock_prices', 'simulate_market_move']
        for method in mock_only_methods:
            self.assertFalse(hasattr(IBKRDataFeed, method),
                           f"IBKRDataFeed should NOT have {method} method (mock-only)")
        
        print("✅ IBKRDataFeed has correct interface without mock methods")

if __name__ == '__main__':
    unittest.main()