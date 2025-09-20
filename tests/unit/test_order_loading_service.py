# tests/unit/test_order_loading_service.py
import pytest
from decimal import Decimal
from src.services.order_loading_service import OrderLoadingService
from unittest.mock import Mock, patch, MagicMock

class TestOrderLoadingServiceConfig:
    """Test OrderLoadingService configuration handling."""
    
    def test_service_stores_configuration(self):
        """Test that service stores configuration properly."""
        # Arrange - Create proper mocks for all required dependencies
        mock_manager = Mock()
        mock_session = Mock()
        test_config = {
            'order_defaults': {
                'risk_per_trade': 0.01,
                'risk_reward_ratio': 2.5,
                'priority': 4
            }
        }
        
        # Act
        service = OrderLoadingService(mock_manager, mock_session, test_config)
        
        # Assert
        assert service.config == test_config
        # Verify the service has the required attributes
        assert service._trading_manager == mock_manager
        assert service._db_session == mock_session
    
    def test_service_uses_empty_config_when_none_provided(self):
        """Test that service uses empty config when None is provided."""
        # Arrange
        mock_manager = Mock()
        mock_session = Mock()
        
        # Act
        service = OrderLoadingService(mock_manager, mock_session, None)
        
        # Assert
        assert service.config == {}
    
    @patch('src.core.planned_order.PlannedOrderManager.from_excel')
    def test_load_orders_passes_config_to_manager(self, mock_from_excel):
        """Test that load_and_validate_orders passes config to PlannedOrderManager."""
        # Arrange
        mock_manager = Mock()
        mock_session = Mock()
        test_config = {
            'order_defaults': {
                'risk_per_trade': 0.01,
                'priority': 4
            }
        }
        
        # Mock the trading manager's validation method
        mock_manager._validate_order_basic = Mock(return_value=True)
        
        service = OrderLoadingService(mock_manager, mock_session, test_config)
        mock_from_excel.return_value = []  # Return empty list for simplicity
        
        # Mock the database query method to return None (no duplicates)
        with patch.object(service, '_find_existing_planned_order', return_value=None):
            # Act
            result = service.load_and_validate_orders('dummy_path.xlsx')
            
            # Assert - Check that from_excel was called with the config
            mock_from_excel.assert_called_once_with('dummy_path.xlsx', test_config)
            assert result == []  # Should return empty list since mock returns no orders