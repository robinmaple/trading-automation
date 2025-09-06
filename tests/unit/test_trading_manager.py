import pytest
from unittest.mock import Mock, patch, MagicMock
from src.core.trading_manager import TradingManager
from src.core.planned_order import PlannedOrder, SecurityType, Action, OrderType, PositionStrategy
from src.core.probability_engine import FillProbabilityEngine  # ADDED: Import the actual class

class TestTradingManager:
    
    def test_initialization(self, mock_data_feed):
        """Test that TradingManager initializes correctly"""
        tm = TradingManager(mock_data_feed, "test.xlsx")
        assert tm.data_feed == mock_data_feed
        assert tm.excel_path == "test.xlsx"
        assert tm.planned_orders == []
        assert not tm.monitoring
        
    def test_load_planned_orders(self, mock_data_feed):
        """Test loading planned orders from Excel (mocked)"""
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        with patch('src.core.trading_manager.PlannedOrderManager.from_excel') as mock_loader:
            mock_loader.return_value = [Mock(spec=PlannedOrder)]
            orders = tm.load_planned_orders()
            
            assert len(orders) == 1
            mock_loader.assert_called_once_with("test.xlsx")
    
    def test_can_place_order_basic_validation(self, mock_data_feed, sample_planned_order):
        """Test basic order validation logic"""
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Test valid order
        assert tm._can_place_order(sample_planned_order) == True
        
        # Test order without entry price
        invalid_order = sample_planned_order
        invalid_order.entry_price = None
        assert tm._can_place_order(invalid_order) == False
    
    def test_can_place_order_max_orders(self, mock_data_feed, sample_planned_order):
        """Test max open orders validation"""
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Fill active orders to max capacity
        for i in range(5):
            tm.active_orders[i] = {'order': Mock(spec=PlannedOrder)}
        
        assert tm._can_place_order(sample_planned_order) == False
    
    def test_can_place_order_duplicate_prevention(self, mock_data_feed, sample_planned_order):
        """Test duplicate order prevention logic"""
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Add the same order to active orders
        tm.active_orders[1] = {'order': sample_planned_order}
        
        # Should prevent duplicate
        assert tm._can_place_order(sample_planned_order) == False
    
    def test_find_executable_orders(self, mock_data_feed, sample_planned_order):
        """Test finding executable orders based on conditions"""
        tm = TradingManager(mock_data_feed, "test.xlsx")
        tm.planned_orders = [sample_planned_order]
        
        # Initialize the probability engine properly
        tm.probability_engine = Mock(spec=FillProbabilityEngine)  # FIXED: Initialize the engine
        tm.probability_engine.should_execute_order.return_value = (True, 0.95)
            
        executable = tm._find_executable_orders()
        
        assert len(executable) == 1
        assert executable[0]['order'] == sample_planned_order
        assert executable[0]['fill_probability'] == 0.95
    
    @patch('src.core.trading_manager.FillProbabilityEngine')  # FIXED: Correct class name
    def test_order_execution_simulation(self, mock_engine_class, mock_data_feed, sample_planned_order):
        """Test order execution in simulation mode"""
        mock_engine_instance = Mock()
        mock_engine_instance.should_execute_order.return_value = (True, 0.95)
        mock_engine_class.return_value = mock_engine_instance
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        tm.planned_orders = [sample_planned_order]
        
        # Mock the probability engine
        tm.probability_engine = mock_engine_instance  # FIXED: Use the mock instance
        
        executable = tm._find_executable_orders()
        assert len(executable) == 1
        
        # Should execute in simulation mode (no IBKR client)
        with patch('builtins.print') as mock_print:
            tm._execute_order(sample_planned_order, 0.95)
            mock_print.assert_any_call("✅ SIMULATION: Order for EUR executed successfully")