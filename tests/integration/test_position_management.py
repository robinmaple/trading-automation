# Simplified Integration Tests - Begin
import datetime
from unittest.mock import Mock, patch, MagicMock
import pytest

class TestPositionManagementIntegration:
    
    @pytest.fixture
    def trading_manager(self):
        """Create a trading manager with proper mocking"""
        from src.core.trading_manager import TradingManager
        
        manager = Mock(spec=TradingManager)
        manager.market_hours = Mock()
        manager.db_session = Mock()
        manager.execution_service = Mock()
        manager.active_orders = {}
        manager.monitoring = False
        
        # Mock the private methods we want to test
        manager._check_market_close_actions = Mock()
        manager._check_hybrid_position_expiration = Mock()
        
        return manager
    
    def test_position_strategy_integration(self):
        """Test that position strategies are properly integrated"""
        from src.core.planned_order import PlannedOrder, PositionStrategy, SecurityType, Action
        
        # Test that all strategies have proper expiration logic
        day_order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NYSE",
            currency="USD", 
            action=Action.BUY,
            symbol="TEST",
            position_strategy=PositionStrategy.DAY
        )
        
        hybrid_order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NYSE",
            currency="USD",
            action=Action.BUY, 
            symbol="TEST",
            position_strategy=PositionStrategy.HYBRID
        )
        
        core_order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NYSE",
            currency="USD",
            action=Action.BUY,
            symbol="TEST",
            position_strategy=PositionStrategy.CORE
        )
        
        # Verify strategy properties
        assert day_order.position_strategy.requires_market_close_action()
        assert hybrid_order.position_strategy.requires_market_close_action()
        assert not core_order.position_strategy.requires_market_close_action()
        
        assert hybrid_order.position_strategy.get_expiration_days() == 10
        assert day_order.position_strategy.get_expiration_days() == 1
        assert core_order.position_strategy.get_expiration_days() is None
# Simplified Integration Tests - End