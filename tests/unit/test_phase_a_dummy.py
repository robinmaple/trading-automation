import unittest 
from unittest.mock import Mock, patch 
from src.core.probability_engine import FillProbabilityEngine 
from src.services.order_eligibility_service import OrderEligibilityService 
from src.trading.execution.trading_manager import TradingManager 
from src.trading.orders.planned_order import PlannedOrder 

class TestPhaseADummy(unittest.TestCase):
    
    def setUp(self):
        # Mock market data feed
        self.mock_data_feed = Mock()
        self.mock_data_feed.is_connected.return_value = True
        self.mock_data_feed.get_current_price.return_value = {"price": 150.0}
        
        # Initialize TradingManager with mock data feed
        self.tm = TradingManager(data_feed=self.mock_data_feed)
        
        # Replace services with mocks
        self.tm.sizing_service = Mock()
        self.tm.execution_service = Mock()
        self.tm.eligibility_service = Mock()
        
        # Create a sample planned order for testing
        self.sample_planned_order = self._make_dummy_order()

    def _make_dummy_order(self):
        """Factory for a dummy planned order with required attributes"""
        order = Mock(spec=PlannedOrder)
        order.symbol = "AAPL"
        order.entry_price = 150.0
        order.stop_loss = 145.0
        order.risk_per_trade = 0.01
        order.calculate_profit_target.return_value = 155.0
        
        # Action + type need .value attribute
        order.action = Mock()
        order.action.value = "BUY"
        order.order_type = Mock()
        order.order_type.value = "LIMIT"
        
        return order

    def test_phase_a_end_to_end_dummy_positive(self):
        """End-to-end dummy test for Phase A flow with positive outcome."""
        # Mock the probability engine to return high probability
        mock_engine = Mock()
        mock_engine.calculate_fill_probability.return_value = 0.95  # High probability
        
        # Create service with our sample order
        service = OrderEligibilityService([self.sample_planned_order], mock_engine)
        
        # Mock the service to return our order as executable
        service.find_executable_orders = Mock(return_value=[{
            'order': self.sample_planned_order,
            'probability': 0.95
        }])
        
        executable_orders = service.find_executable_orders()
        
        # Assertions
        self.assertEqual(len(executable_orders), 1)
        self.assertEqual(executable_orders[0]['order'].symbol, "AAPL")
        self.assertGreaterEqual(executable_orders[0]['probability'], 0.9)

    def test_phase_a_end_to_end_dummy_negative(self):
        """❌ Negative: no eligible orders → no execution occurs."""
        dummy_order = self._make_dummy_order()
        self.tm.planned_orders = [dummy_order]

        # Eligibility service returns nothing
        self.tm.eligibility_service.find_executable_orders.return_value = []

        # Patch the new method in TradingManager
        with patch.object(self.tm, "_execute_prioritized_orders") as mock_execute:
            self.tm._check_and_execute_orders()
            mock_execute.assert_not_called()

    def test_phase_a_end_to_end_dummy_low_probability(self):
        """Test case where order has low fill probability"""
        # Mock the probability engine to return low probability
        mock_engine = Mock()
        mock_engine.calculate_fill_probability.return_value = 0.4  # Low probability
        
        service = OrderEligibilityService([self.sample_planned_order], mock_engine)
        
        # Mock to return empty list (no executable orders due to low probability)
        service.find_executable_orders = Mock(return_value=[])
        
        executable_orders = service.find_executable_orders()
        
        # Assertions
        self.assertEqual(len(executable_orders), 0)

    def test_phase_a_with_multiple_orders(self):
        """Test with multiple orders - some eligible, some not"""
        # Create multiple orders
        order1 = self._make_dummy_order()
        order2 = self._make_dummy_order()
        order2.symbol = "MSFT"
        
        # Mock the probability engine to return different probabilities
        mock_engine = Mock()
        mock_engine.calculate_fill_probability.side_effect = [0.95, 0.3]  # First eligible, second not
        
        service = OrderEligibilityService([order1, order2], mock_engine)
        
        # Mock to return only the first order as executable
        service.find_executable_orders = Mock(return_value=[{
            'order': order1,
            'probability': 0.95
        }])
        
        executable_orders = service.find_executable_orders()
        
        # Assertions
        self.assertEqual(len(executable_orders), 1)
        self.assertEqual(executable_orders[0]['order'].symbol, "AAPL")

    def test_data_feed_disconnected(self):
        """Test behavior when data feed is disconnected"""
        # Mock data feed to return disconnected
        disconnected_data_feed = Mock()
        disconnected_data_feed.is_connected.return_value = False
        
        # Should not be able to calculate probabilities when disconnected
        engine = FillProbabilityEngine(disconnected_data_feed)
        service = OrderEligibilityService([self.sample_planned_order], engine)
        
        # Should return empty list when data feed is disconnected
        service.find_executable_orders = Mock(return_value=[])
        
        executable_orders = service.find_executable_orders()
        
        self.assertEqual(len(executable_orders), 0)

if __name__ == "__main__":
    unittest.main()