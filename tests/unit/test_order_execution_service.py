import pytest
from unittest.mock import Mock, patch
import datetime
from src.services.order_execution_service import OrderExecutionService
from src.core.planned_order import PlannedOrder, Action, OrderType, SecurityType

class TestOrderExecutionService:
    """Test suite for OrderExecutionService"""
    
    def setup_method(self):
        """Set up test fixtures before each test"""
        self.mock_tm = Mock()
        self.mock_ibkr_client = Mock()
        self.service = OrderExecutionService(self.mock_tm, self.mock_ibkr_client)
        
        # Mock dependencies that are set via set_dependencies()
        self.order_persistence = Mock()
        self.active_orders = {}
        self.service.set_dependencies(self.order_persistence, self.active_orders)
        
    def test_place_order_calls_execute_single_order(self):
        """Test that place_order properly calls execute_single_order"""
        test_order = Mock()
        
        with patch.object(self.service, 'execute_single_order') as mock_execute:
            self.service.place_order(
                test_order, 0.85, 0.0, 100000, 100, 10000, False
            )
            
            mock_execute.assert_called_once_with(
                test_order, 0.85, 0.0, 100000, 100, 10000, False
            )

    @patch('src.services.order_execution_service.OrderExecutionService._validate_order_margin')
    def test_execute_single_order_simulation_mode(self, mock_validate_margin):
        """Test order execution in simulation mode"""
        # Mock margin validation to return True
        mock_validate_margin.return_value = (True, "Validation passed")
        
        self.mock_ibkr_client.connected = False  # Simulation mode
        
        # Mock planned order
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.entry_price = 100.0
        test_order.action.value = "BUY"
        
        # Mock the database ID lookup
        self.mock_tm._find_planned_order_db_id.return_value = 123
        
        with patch('builtins.print'):
            self.service.execute_single_order(
                test_order, 0.85, 100000, 100, 10000, False
            )
        
        # Verify simulation path was taken
        self.order_persistence.update_order_status.assert_called_once()
        self.order_persistence.record_order_execution.assert_called_once()
    
    @patch('src.services.order_execution_service.OrderExecutionService._validate_order_margin')
    def test_execute_single_order_live_mode_success(self, mock_validate_margin):
        """Test successful order execution in live mode"""
        # Mock margin validation to return True
        mock_validate_margin.return_value = (True, "Validation passed")
        
        self.mock_ibkr_client.connected = True  # Live mode
        self.mock_ibkr_client.place_bracket_order.return_value = [111, 222, 333]  # Success
        
        # Mock planned order
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.entry_price = 100.0
        test_order.action.value = "BUY"
        test_order.to_ib_contract.return_value = "mock_contract"
        
        # Mock the database ID lookup
        self.mock_tm._find_planned_order_db_id.return_value = 123
        
        with patch('builtins.print'):
            self.service.execute_single_order(
                test_order, 0.85, 100000, 100, 10000, True
            )
        
        # Verify live path was taken
        self.mock_ibkr_client.place_bracket_order.assert_called_once()
        self.order_persistence.update_order_status.assert_called_once()
        self.order_persistence.record_order_execution.assert_called_once()
        assert len(self.active_orders) == 1
    
    @patch('src.services.order_execution_service.OrderExecutionService._validate_order_margin')
    def test_execute_single_order_live_mode_failure(self, mock_validate_margin):
        """Test failed order execution in live mode"""
        # Mock margin validation to return True
        mock_validate_margin.return_value = (True, "Validation passed")
        
        self.mock_ibkr_client.connected = True  # Live mode
        self.mock_ibkr_client.place_bracket_order.return_value = None  # Failure
        
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.to_ib_contract.return_value = "mock_contract"
        
        with patch('builtins.print'):
            self.service.execute_single_order(
                test_order, 0.85, 100000, 100, 10000, True
            )
        
        # Verify failure was handled
        self.mock_ibkr_client.place_bracket_order.assert_called_once()
        self.order_persistence.update_order_status.assert_not_called()  # No order IDs to update
        assert len(self.active_orders) == 0
    
    def test_cancel_order_delegates_to_trading_manager(self):
        """Test that cancel_order delegates to trading manager"""
        self.mock_tm._cancel_single_order.return_value = True
        
        result = self.service.cancel_order(123)
        
        assert result == True
        self.mock_tm._cancel_single_order.assert_called_once_with(123)