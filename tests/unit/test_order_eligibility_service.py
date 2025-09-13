import pytest
from unittest.mock import Mock, patch
import datetime
from src.services.order_eligibility_service import OrderEligibilityService

class TestOrderEligibilityService:
    """Test suite for OrderEligibilityService"""
    
    @patch("src.core.probability_engine.FillProbabilityEngine")
    def test_find_executable_orders_filters_by_probability(self, mock_fill_engine):
        mock_order = Mock()
        mock_order.symbol = "AAPL"
        mock_order.action.value = "BUY"
        mock_order.order_type.value = "LMT"
        mock_order.entry_price = 100
        mock_order.priority = 1  # numeric

        # Configure patched FillProbabilityEngine to return numeric fill probability
        mock_fill_engine.return_value.score_fill.return_value = 0.8

        service = OrderEligibilityService([mock_order], mock_fill_engine.return_value)
        executable_orders = service.find_executable_orders()
        assert len(executable_orders) == 1
        assert executable_orders[0]['effective_priority'] == pytest.approx(0.8)

    @patch("src.core.probability_engine.FillProbabilityEngine")
    def test_find_executable_orders_includes_timestamp(self, mock_fill_engine):
        mock_order = Mock()
        mock_order.symbol = "AAPL"
        mock_order.action.value = "BUY"
        mock_order.order_type.value = "LMT"
        mock_order.entry_price = 100
        mock_order.priority = 1

        mock_fill_engine.return_value.score_fill.return_value = 0.9

        service = OrderEligibilityService([mock_order], mock_fill_engine.return_value)
        executable_orders = service.find_executable_orders()
        assert 'timestamp' in executable_orders[0]
    
    def test_can_trade_returns_true_by_default(self):
        """Test that can_trade method returns True (placeholder implementation)"""
        mock_orders = []
        mock_probability_engine = Mock()
        service = OrderEligibilityService(mock_orders, mock_probability_engine)
        
        test_order = Mock()
        
        result = service.can_trade(test_order)
        
        assert result == True
    
    def test_find_executable_orders_empty_when_no_orders(self):
        """Test that empty list is returned when no planned orders"""
        mock_orders = []
        mock_probability_engine = Mock()
        
        service = OrderEligibilityService(mock_orders, mock_probability_engine)
        
        executable = service.find_executable_orders()
        
        assert len(executable) == 0
        assert executable == []