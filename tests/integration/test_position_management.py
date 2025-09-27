# Simplified Integration Tests - Begin
import datetime
from unittest.mock import Mock, patch, MagicMock
import pytest

from core.models import PositionStrategy
from core.planned_order import is_market_hours

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

    def test_position_strategy_expiration(self):
        from src.core.planned_order import PlannedOrder, PositionStrategy, SecurityType, Action

        now = datetime.datetime.now()

        # DAY order - ADD REQUIRED FIELDS
        day_order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NYSE",
            currency="USD",
            action=Action.BUY,
            symbol="TEST_DAY",
            entry_price=100.0,  # ← ADD REQUIRED FIELD
            stop_loss=95.0,     # ← ADD REQUIRED FIELD
            overall_trend="Bull", # ← ADD REQUIRED FIELD
            position_strategy=PositionStrategy("DAY")
        )
        day_exp = day_order.position_strategy.get_expiration_days(now)
        if is_market_hours(now):
            assert day_exp == 0
        else:
            assert day_exp >= 1

        # CORE order - ADD REQUIRED FIELDS
        core_order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NYSE",
            currency="USD",
            action=Action.BUY,
            symbol="TEST_CORE",
            entry_price=100.0,  # ← ADD REQUIRED FIELD
            stop_loss=95.0,     # ← ADD REQUIRED FIELD
            overall_trend="Bull", # ← ADD REQUIRED FIELD
            position_strategy=PositionStrategy("CORE")
        )
        assert core_order.position_strategy.get_expiration_days(now) is None

        # HYBRID order - ADD REQUIRED FIELDS
        hybrid_order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NYSE",
            currency="USD",
            action=Action.BUY,
            symbol="TEST_HYBRID",
            entry_price=100.0,  # ← ADD REQUIRED FIELD
            stop_loss=95.0,     # ← ADD REQUIRED FIELD
            overall_trend="Bull", # ← ADD REQUIRED FIELD
            position_strategy=PositionStrategy("HYBRID")
        )
        assert hybrid_order.position_strategy.get_expiration_days(now) == 10

# Simplified Integration Tests - End