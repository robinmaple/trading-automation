import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from src.core.order_lifecycle_manager import OrderLifecycleManager
from src.core.order_loading_orchestrator import OrderLoadingOrchestrator
from src.core.planned_order import PlannedOrder, Action, OrderType, PositionStrategy, SecurityType
from src.core.models import PlannedOrderDB
from src.core.events import OrderState


class TestOrderLifecycleManagerEnhanced:
    """Test enhanced OrderLifecycleManager with orchestrator integration."""
    
    @pytest.fixture
    def mock_services_with_orchestrator(self):
        """Create mock services including orchestrator."""
        loading_service = Mock()
        persistence_service = Mock()
        state_service = Mock()
        db_session = Mock()
        order_loading_orchestrator = Mock(spec=OrderLoadingOrchestrator)
        
        return {
            'loading_service': loading_service,
            'persistence_service': persistence_service,
            'state_service': state_service,
            'db_session': db_session,
            'order_loading_orchestrator': order_loading_orchestrator
        }
    
    @pytest.fixture
    def manager_with_orchestrator(self, mock_services_with_orchestrator):
        """Create OrderLifecycleManager with orchestrator."""
        return OrderLifecycleManager(**mock_services_with_orchestrator)
    
    def test_initialization_with_orchestrator(self, mock_services_with_orchestrator):
        """Test that manager initializes correctly with orchestrator."""
        manager = OrderLifecycleManager(**mock_services_with_orchestrator)
        
        assert manager.order_loading_orchestrator == mock_services_with_orchestrator['order_loading_orchestrator']
        assert manager.loading_service == mock_services_with_orchestrator['loading_service']
    
    def test_load_and_persist_orders_uses_orchestrator(self, manager_with_orchestrator, mock_services_with_orchestrator):
        """Test that load_and_persist_orders uses orchestrator when available."""
        sample_orders = [
            PlannedOrder(
                security_type=SecurityType.STK,  # Added missing parameter
                exchange="SMART",  # Added missing parameter
                currency="USD",  # Added missing parameter
                symbol="AAPL", 
                action=Action.BUY, 
                entry_price=150.0, 
                stop_loss=145.0,
                order_type=OrderType.LMT, 
                position_strategy=PositionStrategy.DAY
            )
        ]
        
        # Mock orchestrator to return orders
        mock_services_with_orchestrator['order_loading_orchestrator'].load_all_orders.return_value = sample_orders
        
        # Mock that orders should be persisted
        manager_with_orchestrator._should_persist_order = Mock(return_value=True)
        manager_with_orchestrator._persist_single_order = Mock(return_value=True)
        
        result = manager_with_orchestrator.load_and_persist_orders("test_path.xlsx")
        
        # Verify orchestrator was used
        mock_services_with_orchestrator['order_loading_orchestrator'].load_all_orders.assert_called_once_with("test_path.xlsx")
        assert result == sample_orders
    
    def test_load_and_persist_orders_fallback_without_orchestrator(self, mock_services_with_orchestrator):
        """Test fallback to original loading when orchestrator is not available."""
        # Remove orchestrator
        mock_services_with_orchestrator.pop('order_loading_orchestrator')
        manager = OrderLifecycleManager(**mock_services_with_orchestrator)
        
        sample_orders = [
            PlannedOrder(
                security_type=SecurityType.STK,  # Added missing parameter
                exchange="SMART",  # Added missing parameter
                currency="USD",  # Added missing parameter
                symbol="MSFT", 
                action=Action.SELL, 
                entry_price=300.0, 
                stop_loss=310.0,
                order_type=OrderType.LMT, 
                position_strategy=PositionStrategy.CORE
            )
        ]
        
        # Mock original loading service
        mock_services_with_orchestrator['loading_service'].load_and_validate_orders.return_value = sample_orders
        manager._should_persist_order = Mock(return_value=True)
        manager._persist_single_order = Mock(return_value=True)
        
        result = manager.load_and_persist_orders("test_path.xlsx")
        
        # Verify original loading service was used
        mock_services_with_orchestrator['loading_service'].load_and_validate_orders.assert_called_once_with("test_path.xlsx")
        assert result == sample_orders
    
    def test_persist_single_order_duplicate_handling(self, manager_with_orchestrator):
        """Test that duplicate orders are properly handled during persistence."""
        order = PlannedOrder(
            security_type=SecurityType.STK,  # Added missing parameter
            exchange="SMART",  # Added missing parameter
            currency="USD",  # Added missing parameter
            symbol="DUPLICATE", 
            action=Action.BUY, 
            entry_price=100.0, 
            stop_loss=95.0,
            order_type=OrderType.LMT, 
            position_strategy=PositionStrategy.DAY
        )
        
        # Mock existing duplicate order
        existing_order = Mock(spec=PlannedOrderDB)
        manager_with_orchestrator.find_existing_order = Mock(return_value=existing_order)
        manager_with_orchestrator._is_duplicate_order = Mock(return_value=True)
        
        result = manager_with_orchestrator._persist_single_order(order)
        
        # Should return False and not persist
        assert result is False
        manager_with_orchestrator.db_session.add.assert_not_called()
    
    def test_persist_single_order_success(self, manager_with_orchestrator):
        """Test successful order persistence."""
        order = PlannedOrder(
            security_type=SecurityType.STK,  # Added missing parameter
            exchange="SMART",  # Added missing parameter
            currency="USD",  # Added missing parameter
            symbol="NEW", 
            action=Action.BUY, 
            entry_price=100.0, 
            stop_loss=95.0,
            order_type=OrderType.LMT, 
            position_strategy=PositionStrategy.DAY
        )
        
        # Mock no existing order
        manager_with_orchestrator.find_existing_order = Mock(return_value=None)
        manager_with_orchestrator._is_duplicate_order = Mock(return_value=False)
        
        result = manager_with_orchestrator._persist_single_order(order)
        
        # Should return True and persist
        assert result is True
        manager_with_orchestrator.persistence_service.convert_to_db_model.assert_called_once_with(order)
        manager_with_orchestrator.db_session.add.assert_called_once()