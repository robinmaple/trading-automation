import pytest
from unittest.mock import Mock, patch, MagicMock
import datetime

from src.core.trading_manager import TradingManager
from src.core.order_loading_orchestrator import OrderLoadingOrchestrator
from src.core.order_lifecycle_manager import OrderLifecycleManager


class TestTradingManagerIntegration:
    """Test TradingManager integration with new order loading orchestration."""
    
    @pytest.fixture
    def trading_manager_with_orchestrator(self):
        """Create TradingManager with orchestrator setup."""
        mock_data_feed = Mock()
        mock_data_feed.is_connected.return_value = True
        
        mock_ibkr_client = Mock()
        mock_ibkr_client.connected = True
        
        manager = TradingManager(data_feed=mock_data_feed, ibkr_client=mock_ibkr_client)
        
        # Mock the order loading orchestrator
        manager.order_loading_orchestrator = Mock(spec=OrderLoadingOrchestrator)
        manager.order_lifecycle_manager = Mock(spec=OrderLifecycleManager)
        
        # Mock configuration
        manager.trading_config = {
            'monitoring': {'interval_seconds': 5},
            'risk_limits': {'max_open_orders': 5},
            'simulation': {'default_equity': 100000},
            'execution': {'fill_probability_threshold': 0.7}
        }
        
        return manager
    
    def test_order_loading_orchestrator_initialization(self, trading_manager_with_orchestrator):
        """Test that OrderLoadingOrchestrator is properly initialized in TradingManager."""
        # The orchestrator should be created during _initialize_components
        assert hasattr(trading_manager_with_orchestrator, 'order_loading_orchestrator')
        assert isinstance(trading_manager_with_orchestrator.order_loading_orchestrator, Mock)
    
    def test_order_lifecycle_manager_injection(self, trading_manager_with_orchestrator):
        """Test that OrderLifecycleManager receives the orchestrator via dependency injection."""
        # Verify that OrderLifecycleManager was created with orchestrator parameter
        trading_manager_with_orchestrator.order_lifecycle_manager.load_and_persist_orders.assert_not_called()
        
        # When load_planned_orders is called, it should use the enhanced manager
        sample_orders = [Mock()]
        trading_manager_with_orchestrator.order_lifecycle_manager.load_and_persist_orders.return_value = sample_orders
        
        result = trading_manager_with_orchestrator.load_planned_orders()
        
        assert result == sample_orders
        trading_manager_with_orchestrator.order_lifecycle_manager.load_and_persist_orders.assert_called_once()
    
    @patch('builtins.print')
    def test_multi_source_order_loading_flow(self, mock_print, trading_manager_with_orchestrator):
        """Test the complete multi-source order loading flow."""
        # Mock orders from multiple sources
        db_orders = [Mock(symbol="DB_ORDER")]
        excel_orders = [Mock(symbol="EXCEL_ORDER")]
        all_orders = db_orders + excel_orders
        
        # Mock orchestrator to return combined orders
        trading_manager_with_orchestrator.order_loading_orchestrator.load_all_orders.return_value = all_orders
        
        # Mock lifecycle manager persistence
        trading_manager_with_orchestrator.order_lifecycle_manager.load_and_persist_orders.return_value = all_orders
        
        result = trading_manager_with_orchestrator.load_planned_orders()
        
        # Verify multi-source loading was used
        assert result == all_orders
        assert len(result) == 2
    
    def test_backward_compatibility(self, trading_manager_with_orchestrator):
        """Test that the system maintains backward compatibility."""
        # The original load_planned_orders method should still work
        trading_manager_with_orchestrator.planned_orders = []
        
        sample_orders = [Mock(symbol="TEST")]
        trading_manager_with_orchestrator.order_lifecycle_manager.load_and_persist_orders.return_value = sample_orders
        
        result = trading_manager_with_orchestrator.load_planned_orders()
        
        # Should populate planned_orders and return them
        assert trading_manager_with_orchestrator.planned_orders == sample_orders
        assert result == sample_orders
    
    def test_error_handling_in_loading_flow(self, trading_manager_with_orchestrator):
        """Test error handling in the enhanced loading flow."""
        # Mock orchestrator failure
        trading_manager_with_orchestrator.order_loading_orchestrator.load_all_orders.side_effect = Exception("Orchestrator failed")
        
        # Mock fallback behavior
        trading_manager_with_orchestrator.order_lifecycle_manager.load_and_persist_orders.return_value = []
        
        result = trading_manager_with_orchestrator.load_planned_orders()
        
        # Should handle error gracefully and return empty list
        assert result == []