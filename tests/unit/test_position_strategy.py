import pytest
from src.trading.orders.planned_order import PositionStrategy, SecurityType, Action

class TestPositionStrategy:
    
    def test_market_close_action_required(self):
        """Test market close action requirements"""
        assert PositionStrategy.DAY.requires_market_close_action() == True
        assert PositionStrategy.HYBRID.requires_market_close_action() == True
        assert PositionStrategy.CORE.requires_market_close_action() == False
    
    def test_position_strategy_parsing(self):
        """Test case-insensitive strategy parsing"""
        assert PositionStrategy('day') == PositionStrategy.DAY
        assert PositionStrategy('CORE') == PositionStrategy.CORE
        assert PositionStrategy('Hybrid') == PositionStrategy.HYBRID

    def test_position_strategy_expiration(self):
        """Test expiration days for each strategy"""
        # Test during market hours - DAY should expire same day (0 days)
        import datetime
        from unittest.mock import patch
        
        # Mock market hours to be consistent
        with patch('src.trading.orders.planned_order.is_market_hours') as mock_market_hours:
            mock_market_hours.return_value = True  # Market is open
            
            # DAY should expire same day (0 days) during market hours
            assert PositionStrategy.DAY.get_expiration_days() == 0
            
            # CORE has no expiration
            assert PositionStrategy.CORE.get_expiration_days() is None
            
            # HYBRID has fixed 10 days
            assert PositionStrategy.HYBRID.get_expiration_days() == 10

    def test_planned_order_expiration_date(self):
        """Test expiration date setting in PlannedOrder"""
        from src.trading.orders.planned_order import PlannedOrder
        
        # Test HYBRID strategy sets expiration date - WITH REQUIRED FIELDS
        order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NYSE",
            currency="USD",
            action=Action.BUY,
            symbol="TEST",
            entry_price=100.0,  # ← ADD REQUIRED FIELD
            stop_loss=95.0,     # ← ADD REQUIRED FIELD  
            overall_trend="Bull", # ← ADD REQUIRED FIELD
            position_strategy=PositionStrategy.HYBRID,
        )
        
        # Check if expiration_date is set
        assert order.expiration_date is not None
        assert order.position_strategy.get_expiration_days() == 10
        
        # Test CORE strategy has no expiration
        core_order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NYSE", 
            currency="USD",
            action=Action.BUY,
            symbol="TEST",
            entry_price=100.0,  # ← ADD REQUIRED FIELD
            stop_loss=95.0,     # ← ADD REQUIRED FIELD
            overall_trend="Bull", # ← ADD REQUIRED FIELD
            position_strategy=PositionStrategy.CORE
        )
        
        assert core_order.position_strategy.get_expiration_days() is None
        # CORE orders might not set expiration_date, or it might be None
        assert core_order.expiration_date is None