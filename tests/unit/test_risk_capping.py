"""
Test suite specifically for risk capping functionality.
"""

import unittest
from unittest.mock import MagicMock, patch
from decimal import Decimal

from src.services.risk_management_service import RiskManagementService
from src.services.state_service import StateService
from src.services.order_persistence_service import OrderPersistenceService
from src.core.planned_order import PlannedOrder, Action, PositionStrategy


class TestRiskCapping(unittest.TestCase):
    """Test suite for risk capping functionality."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.risk_service = RiskManagementService(
            state_service=MagicMock(spec=StateService),
            persistence_service=MagicMock(spec=OrderPersistenceService),
            ibkr_client=MagicMock(),
            config={
                'risk_limits': {
                    'max_risk_per_trade': Decimal('0.02'),  # 2% max
                    'daily_loss_pct': Decimal('0.02'),
                    'weekly_loss_pct': Decimal('0.05'),
                    'monthly_loss_pct': Decimal('0.08'),
                    'max_open_orders': 5
                },
                'defaults': {
                    'simulation_equity': Decimal('100000')
                }
            }
        )
    
    def test_risk_capping_exceeds_limit(self):
        """Test risk capping when risk exceeds maximum limit."""
        order = MagicMock()
        order.risk_per_trade = Decimal('0.03')  # 3% > 2% limit
        order.symbol = "TEST"
        order.action.value = "BUY"
        
        self.risk_service._cap_risk_to_max_limit(order)
        
        self.assertEqual(order.risk_per_trade, Decimal('0.02'))  # Should be capped to 2%
    
    def test_risk_capping_within_limit(self):
        """Test risk capping when risk is within limits."""
        order = MagicMock()
        order.risk_per_trade = Decimal('0.015')  # 1.5% < 2% limit
        order.symbol = "TEST"
        order.action.value = "BUY"
        
        original_risk = order.risk_per_trade
        self.risk_service._cap_risk_to_max_limit(order)
        
        self.assertEqual(order.risk_per_trade, original_risk)  # Should remain unchanged
    
    def test_risk_capping_equal_to_limit(self):
        """Test risk capping when risk equals the maximum limit."""
        order = MagicMock()
        order.risk_per_trade = Decimal('0.02')  # Exactly 2% limit
        order.symbol = "TEST"
        order.action.value = "BUY"
        
        original_risk = order.risk_per_trade
        self.risk_service._cap_risk_to_max_limit(order)
        
        self.assertEqual(order.risk_per_trade, original_risk)  # Should remain unchanged
    
    def test_risk_capping_none_value(self):
        """Test risk capping with None risk value."""
        order = MagicMock()
        order.risk_per_trade = None
        order.symbol = "TEST"
        order.action.value = "BUY"
        
        # Should not raise exception
        try:
            self.risk_service._cap_risk_to_max_limit(order)
            success = True
        except Exception:
            success = False
        
        self.assertTrue(success)
    
    def test_risk_capping_zero_value(self):
        """Test risk capping with zero risk value."""
        order = MagicMock()
        order.risk_per_trade = Decimal('0.00')  # 0% risk
        order.symbol = "TEST"
        order.action.value = "BUY"
        
        self.risk_service._cap_risk_to_max_limit(order)
        
        self.assertEqual(order.risk_per_trade, Decimal('0.00'))  # Should remain 0%
    
    def test_can_place_order_with_capped_risk(self):
        """Test that orders with capped risk can still be placed."""
        order = MagicMock()
        order.risk_per_trade = Decimal('0.03')  # 3% > 2% limit
        order.symbol = "TEST"
        order.position_strategy = PositionStrategy.DAY  # No position limits
        
        # Mock other risk checks to pass
        with patch.object(self.risk_service, '_check_trading_halts', return_value=True), \
             patch.object(self.risk_service, '_validate_position_limits', return_value=True):
            
            result = self.risk_service.can_place_order(
                order, {}, 100000.0
            )
            
            self.assertTrue(result)
            # Risk should be capped to 2%
            self.assertEqual(order.risk_per_trade, Decimal('0.02'))
    
    def test_can_place_order_still_rejects_other_violations(self):
        """Test that other risk violations still cause rejection despite capping."""
        order = MagicMock()
        order.risk_per_trade = Decimal('0.03')  # 3% > 2% limit
        order.symbol = "TEST"
        order.position_strategy = PositionStrategy.CORE
        
        # Mock trading halts to fail
        with patch.object(self.risk_service, '_check_trading_halts', return_value=False):
            
            result = self.risk_service.can_place_order(
                order, {}, 100000.0
            )
            
            self.assertFalse(result)  # Should be rejected due to trading halt


if __name__ == '__main__':
    unittest.main()