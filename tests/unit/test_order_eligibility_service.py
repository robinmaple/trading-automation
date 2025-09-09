import pytest
from unittest.mock import Mock, patch
import datetime
from src.services.order_eligibility_service import OrderEligibilityService

class TestOrderEligibilityService:
    """Test suite for OrderEligibilityService"""
    
    def test_find_executable_orders_filters_by_probability(self):
        """Test that only orders with sufficient probability are executed"""
        # Mock dependencies
        mock_orders = [Mock(), Mock()]
        mock_probability_engine = Mock()
        
        service = OrderEligibilityService(mock_orders, mock_probability_engine)
        
        # Setup mock behavior
        mock_orders[0].symbol = "AAPL"
        mock_orders[1].symbol = "GOOGL"
        mock_orders[0].action.value = "BUY"
        mock_orders[1].action.value = "BUY"
        
        # First order should execute, second should not
        mock_probability_engine.should_execute_order.side_effect = [
            (True, 0.85),  # AAPL should execute
            (False, 0.45)  # GOOGL should not execute
        ]
        
        executable = service.find_executable_orders()
        
        assert len(executable) == 1
        assert executable[0]['order'].symbol == "AAPL"
        assert executable[0]['fill_probability'] == 0.85
    
    def test_can_trade_returns_true_by_default(self):
        """Test that can_trade method returns True (placeholder implementation)"""
        mock_orders = []
        mock_probability_engine = Mock()
        service = OrderEligibilityService(mock_orders, mock_probability_engine)
        
        test_order = Mock()
        
        result = service.can_trade(test_order)
        
        assert result == True
    
    def test_find_executable_orders_includes_timestamp(self):
        """Test that executable orders include timestamp"""
        mock_orders = [Mock()]
        mock_probability_engine = Mock()
        
        service = OrderEligibilityService(mock_orders, mock_probability_engine)
        
        mock_orders[0].symbol = "TEST"
        mock_orders[0].action.value = "BUY"
        mock_probability_engine.should_execute_order.return_value = (True, 0.90)
        
        executable = service.find_executable_orders()
        
        assert len(executable) == 1
        assert 'timestamp' in executable[0]
        assert isinstance(executable[0]['timestamp'], datetime.datetime)
    
    def test_find_executable_orders_empty_when_no_orders(self):
        """Test that empty list is returned when no planned orders"""
        mock_orders = []
        mock_probability_engine = Mock()
        
        service = OrderEligibilityService(mock_orders, mock_probability_engine)
        
        executable = service.find_executable_orders()
        
        assert len(executable) == 0
        assert executable == []