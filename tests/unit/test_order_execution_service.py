import pytest
from unittest.mock import Mock, patch
import datetime
from src.services.order_execution_service import OrderExecutionService
from src.core.planned_order import PlannedOrder, Action, OrderType, SecurityType

class TestOrderExecutionService:
    """Test suite for OrderExecutionService"""
    
    def test_place_order_calls_execute_single_order(self):
        """Test that place_order properly calls execute_single_order"""
        mock_tm = Mock()
        mock_ibkr_client = Mock()
        service = OrderExecutionService(mock_tm, mock_ibkr_client)
        
        # Mock dependencies
        service.order_persistence = Mock()
        service.active_orders = {}
        
        test_order = Mock()
        
        with patch.object(service, 'execute_single_order') as mock_execute:
            service.place_order(
                test_order, 0.85, 100000, 100, 10000, False
            )
            
            mock_execute.assert_called_once_with(
                test_order, 0.85, 100000, 100, 10000, False
            )
    
    def test_execute_single_order_simulation_mode(self):
        """Test order execution in simulation mode"""
        mock_tm = Mock()
        mock_ibkr_client = Mock()
        mock_ibkr_client.connected = False  # Simulation mode
        
        service = OrderExecutionService(mock_tm, mock_ibkr_client)
        service.order_persistence = Mock()
        service.active_orders = {}
        
        # Mock planned order
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.entry_price = 100.0
        test_order.action.value = "BUY"
        
        # Mock the database ID lookup
        mock_tm._find_planned_order_db_id.return_value = 123
        
        with patch('builtins.print'):
            service.execute_single_order(
                test_order, 0.85, 100000, 100, 10000, False
            )
        
        # Verify simulation path was taken
        service.order_persistence.update_order_status.assert_called_once()
        service.order_persistence.record_order_execution.assert_called_once()
    
    def test_execute_single_order_live_mode_success(self):
        """Test successful order execution in live mode"""
        mock_tm = Mock()
        mock_ibkr_client = Mock()
        mock_ibkr_client.connected = True  # Live mode
        mock_ibkr_client.place_bracket_order.return_value = [111, 222, 333]  # Success
        
        service = OrderExecutionService(mock_tm, mock_ibkr_client)
        service.order_persistence = Mock()
        service.active_orders = {}
        
        # Mock planned order
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.entry_price = 100.0
        test_order.action.value = "BUY"
        test_order.to_ib_contract.return_value = "mock_contract"
        
        # Mock the database ID lookup
        mock_tm._find_planned_order_db_id.return_value = 123
        
        with patch('builtins.print'):
            service.execute_single_order(
                test_order, 0.85, 100000, 100, 10000, True
            )
        
        # Verify live path was taken
        mock_ibkr_client.place_bracket_order.assert_called_once()
        service.order_persistence.update_order_status.assert_called_once()
        service.order_persistence.record_order_execution.assert_called_once()
        assert len(service.active_orders) == 1
    
    def test_execute_single_order_live_mode_failure(self):
        """Test failed order execution in live mode"""
        mock_tm = Mock()
        mock_ibkr_client = Mock()
        mock_ibkr_client.connected = True  # Live mode
        mock_ibkr_client.place_bracket_order.return_value = None  # Failure
        
        service = OrderExecutionService(mock_tm, mock_ibkr_client)
        service.order_persistence = Mock()
        service.active_orders = {}
        
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.to_ib_contract.return_value = "mock_contract"
        
        with patch('builtins.print'):
            service.execute_single_order(
                test_order, 0.85, 100000, 100, 10000, True
            )
        
        # Verify failure was handled
        mock_ibkr_client.place_bracket_order.assert_called_once()
        service.order_persistence.update_order_status.assert_not_called()  # No order IDs to update
        assert len(service.active_orders) == 0
    
    def test_cancel_order_delegates_to_trading_manager(self):
        """Test that cancel_order delegates to trading manager"""
        mock_tm = Mock()
        mock_ibkr_client = Mock()
        service = OrderExecutionService(mock_tm, mock_ibkr_client)
        
        mock_tm._cancel_single_order.return_value = True
        
        result = service.cancel_order(123)
        
        assert result == True
        mock_tm._cancel_single_order.assert_called_once_with(123)