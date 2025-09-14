import unittest
from unittest.mock import Mock, patch
from src.core.trading_manager import TradingManager
from src.core.planned_order import PlannedOrder


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
        """✅ Positive: executable order triggers execution"""
        dummy_order = self._make_dummy_order()
        self.tm.planned_orders = [dummy_order]

        # Mock services
        self.tm.sizing_service.calculate_order_quantity.return_value = 100
        self.tm.eligibility_service.find_executable_orders.return_value = [
            {"order": dummy_order, "fill_probability": 0.8}
        ]

        with patch.object(self.tm, "_execute_order") as mock_execute:
            self.tm._check_and_execute_orders()
            mock_execute.assert_called_once_with(dummy_order, 0.8)

    def test_phase_a_end_to_end_dummy_negative(self):
        """❌ Negative: no eligible orders → no execution"""
        dummy_order = self._make_dummy_order()
        self.tm.planned_orders = [dummy_order]

        # Eligibility service returns nothing
        self.tm.eligibility_service.find_executable_orders.return_value = []

        with patch.object(self.tm, "_execute_order") as mock_execute:
            self.tm._check_and_execute_orders()
            mock_execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
