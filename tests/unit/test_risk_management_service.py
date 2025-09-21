"""
Unit tests for RiskManagementService P&L calculation functionality.
Tests both successful calculations and error conditions.
"""

import unittest
from unittest.mock import MagicMock, patch
from decimal import Decimal
import datetime

from src.services.risk_management_service import RiskManagementService
from src.services.state_service import StateService
from src.services.order_persistence_service import OrderPersistenceService
from src.core.planned_order import PlannedOrder, PositionStrategy, ActiveOrder


class TestRiskManagementService(unittest.TestCase):
    """Test suite for RiskManagementService P&L calculation methods."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_state_service = MagicMock(spec=StateService)
        self.mock_persistence = MagicMock(spec=OrderPersistenceService)
        self.mock_ibkr_client = MagicMock()
        
        # Use keyword arguments to ensure correct parameter mapping
        self.risk_service = RiskManagementService(
            state_service=self.mock_state_service,
            persistence_service=self.mock_persistence,  # Correct parameter name
            ibkr_client=self.mock_ibkr_client
        )
    
    def test_calculate_position_pnl_long_profit(self):
        """Test P&L calculation for long position with profit."""
        # Long position: entry 100, exit 110, quantity 100 shares
        pnl = self.risk_service.calculate_position_pnl(
            entry_price=100.0,
            exit_price=110.0,
            quantity=100.0,
            action='BUY'
        )
        
        # Expected: (110 - 100) * 100 = 1000 profit
        self.assertEqual(pnl, 1000.0)
        self.assertGreater(pnl, 0)  # Should be profitable
    
    def test_calculate_position_pnl_long_loss(self):
        """Test P&L calculation for long position with loss."""
        # Long position: entry 100, exit 90, quantity 100 shares
        pnl = self.risk_service.calculate_position_pnl(
            entry_price=100.0,
            exit_price=90.0,
            quantity=100.0,
            action='BUY'
        )
        
        # Expected: (90 - 100) * 100 = -1000 loss
        self.assertEqual(pnl, -1000.0)
        self.assertLess(pnl, 0)  # Should be loss
    
    def test_calculate_position_pnl_short_profit(self):
        """Test P&L calculation for short position with profit."""
        # Short position: entry 100, exit 90, quantity 100 shares
        pnl = self.risk_service.calculate_position_pnl(
            entry_price=100.0,
            exit_price=90.0,
            quantity=100.0,
            action='SELL'
        )
        
        # Expected: (100 - 90) * 100 = 1000 profit
        self.assertEqual(pnl, 1000.0)
        self.assertGreater(pnl, 0)  # Should be profitable
    
    def test_calculate_position_pnl_short_loss(self):
        """Test P&L calculation for short position with loss."""
        # Short position: entry 100, exit 110, quantity 100 shares
        pnl = self.risk_service.calculate_position_pnl(
            entry_price=100.0,
            exit_price=110.0,
            quantity=100.0,
            action='SELL'
        )
        
        # Expected: (100 - 110) * 100 = -1000 loss
        self.assertEqual(pnl, -1000.0)
        self.assertLess(pnl, 0)  # Should be loss
    
    def test_calculate_position_pnl_with_decimal_inputs(self):
        """Test P&L calculation with Decimal inputs."""
        pnl = self.risk_service.calculate_position_pnl(
            entry_price=Decimal('100.50'),
            exit_price=Decimal('105.25'),
            quantity=Decimal('200'),
            action='BUY'
        )
        
        # Expected: (105.25 - 100.50) * 200 = 950.0
        self.assertAlmostEqual(pnl, 950.0, places=2)
    
    def test_calculate_position_pnl_zero_quantity(self):
        """Test P&L calculation with zero quantity raises error."""
        with self.assertRaises(ValueError) as context:
            self.risk_service.calculate_position_pnl(
                entry_price=100.0,
                exit_price=110.0,
                quantity=0.0,
                action='BUY'
            )
        
        self.assertIn("Quantity must be positive", str(context.exception))
    
    def test_calculate_position_pnl_negative_prices(self):
        """Test P&L calculation with negative prices raises error."""
        with self.assertRaises(ValueError) as context:
            self.risk_service.calculate_position_pnl(
                entry_price=-100.0,
                exit_price=110.0,
                quantity=100.0,
                action='BUY'
            )
        
        self.assertIn("Entry price must be positive", str(context.exception))
    
    def test_calculate_position_pnl_none_parameters(self):
        """Test P&L calculation with None parameters raises error."""
        with self.assertRaises(ValueError) as context:
            self.risk_service.calculate_position_pnl(
                entry_price=None,
                exit_price=110.0,
                quantity=100.0,
                action='BUY'
            )
        
        self.assertIn("Entry price cannot be None", str(context.exception))
    
    def test_calculate_position_pnl_invalid_action(self):
        """Test P&L calculation with invalid action raises error."""
        with self.assertRaises(ValueError) as context:
            self.risk_service.calculate_position_pnl(
                entry_price=100.0,
                exit_price=110.0,
                quantity=100.0,
                action='INVALID'
            )
        
        self.assertIn("Action must be 'BUY' or 'SELL'", str(context.exception))
    
    def test_calculate_position_pnl_wrong_parameter_types(self):
        """Test P&L calculation with wrong parameter types raises error."""
        with self.assertRaises(TypeError) as context:
            self.risk_service.calculate_position_pnl(
                entry_price="100",  # String instead of number
                exit_price=110.0,
                quantity=100.0,
                action='BUY'
            )
        
        self.assertIn("Entry price must be numeric", str(context.exception))
    
    def test_validate_pnl_parameters_success(self):
        """Test parameter validation with valid inputs."""
        # Should not raise any exceptions
        self.risk_service._validate_pnl_parameters(
            entry_price=100.0,
            exit_price=110.0,
            quantity=100.0,
            action='BUY'
        )
    
    def test_validate_pnl_parameters_case_insensitive_action(self):
        """Test parameter validation with case-insensitive action."""
        # Should not raise any exceptions
        self.risk_service._validate_pnl_parameters(
            entry_price=100.0,
            exit_price=110.0,
            quantity=100.0,
            action='buy'  # lowercase
        )
    
    def test_record_trade_outcome_calls_persistence(self):
        """Test that record_trade_outcome calls persistence service."""
        mock_active_order = MagicMock()
        mock_active_order.db_id = 123
        mock_active_order.symbol = 'TEST'
        
        test_pnl = 500.0
        
        # Mock the persistence method
        with patch.object(self.risk_service.persistence, 'record_realized_pnl') as mock_record:
            self.risk_service.record_trade_outcome(mock_active_order, test_pnl)
            
            # Verify persistence was called with correct parameters
            mock_record.assert_called_once_with(
                order_id=123,
                symbol='TEST',
                pnl=Decimal('500.0'),
                exit_date=unittest.mock.ANY  # Should be datetime
            )
    
    def test_record_trade_outcome_handles_exception(self):
        """Test that record_trade_outcome handles exceptions gracefully."""
        mock_active_order = MagicMock()
        mock_active_order.db_id = 123
        mock_active_order.symbol = 'TEST'
        
        # Make persistence throw an exception
        self.mock_persistence.record_realized_pnl.side_effect = Exception("DB error")
        
        # Should not raise exception, just log error
        try:
            self.risk_service.record_trade_outcome(mock_active_order, 500.0)
            # If we get here, the exception was handled properly
            success = True
        except Exception:
            success = False
        
        self.assertTrue(success, "Should handle persistence exceptions gracefully")
    
    def test_pnl_calculation_edge_cases(self):
        """Test P&L calculation with edge case values."""
        # Very small values
        pnl = self.risk_service.calculate_position_pnl(
            entry_price=0.0001,
            exit_price=0.0002,
            quantity=1000000,  # Large quantity
            action='BUY'
        )
        self.assertAlmostEqual(pnl, 100.0, places=4)
        
        # Very large values
        pnl = self.risk_service.calculate_position_pnl(
            entry_price=10000.0,
            exit_price=10100.0,
            quantity=1000.0,
            action='BUY'
        )
        self.assertEqual(pnl, 100000.0)  # 100 * 1000
    
    def test_pnl_precision_handling(self):
        """Test that P&L calculation handles precision correctly."""
        # Test with many decimal places
        pnl = self.risk_service.calculate_position_pnl(
            entry_price=123.456789,
            exit_price=123.567890,
            quantity=1000.0,
            action='BUY'
        )
        
        # Expected: (123.567890 - 123.456789) * 1000 = 111.101
        expected = (123.567890 - 123.456789) * 1000
        self.assertAlmostEqual(pnl, expected, places=6)

# Risk Capping Functionality Tests - Begin
# Risk Capping Functionality Tests - Begin
class TestRiskCappingFunctionality(unittest.TestCase):
    """Test suite for RiskManagementService risk capping functionality."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_state_service = MagicMock(spec=StateService)
        self.mock_persistence = MagicMock(spec=OrderPersistenceService)
        self.mock_ibkr_client = MagicMock()
        
        # Create risk service with specific config for testing
        # Use the proper config structure that matches RiskManagementService expectations
        test_config = {
            'position_limits': {},
            'loss_limits': {
                'daily': Decimal('0.02'),
                'weekly': Decimal('0.05'), 
                'monthly': Decimal('0.08')
            },
            'check_intervals': {
                'trading_halt_check': 300
            },
            'defaults': {
                'simulation_equity': Decimal('100000')
            }
        }
        
        self.risk_service = RiskManagementService(
            state_service=self.mock_state_service,
            persistence_service=self.mock_persistence,
            ibkr_client=self.mock_ibkr_client,
            config=test_config
        )
        
        # MANUALLY SET THE max_risk_per_trade ATTRIBUTE since config loading is complex
        self.risk_service.max_risk_per_trade = Decimal('0.02')  # 2% max
    
    def test_cap_risk_to_max_limit_exceeds_max(self):
        """Test that risk_per_trade is capped when exceeding max_risk_per_trade."""
        # Create a mock order with risk exceeding max
        mock_order = MagicMock()
        mock_order.risk_per_trade = Decimal('0.03')  # 3% > 2% max
        mock_order.symbol = 'TEST'
        mock_order.action.value = 'BUY'
        
        # Apply risk capping
        self.risk_service._cap_risk_to_max_limit(mock_order)
        
        # Should be capped to 2%
        self.assertEqual(mock_order.risk_per_trade, Decimal('0.02'))
    
    def test_cap_risk_to_max_limit_within_limit(self):
        """Test that risk_per_trade remains unchanged when within limits."""
        # Create a mock order with risk within limits
        mock_order = MagicMock()
        original_risk = Decimal('0.015')  # 1.5% < 2% max
        mock_order.risk_per_trade = original_risk
        mock_order.symbol = 'TEST'
        mock_order.action.value = 'BUY'
        
        # Apply risk capping
        self.risk_service._cap_risk_to_max_limit(mock_order)
        
        # Should remain unchanged
        self.assertEqual(mock_order.risk_per_trade, original_risk)
    
    def test_cap_risk_to_max_limit_equal_to_max(self):
        """Test that risk_per_trade remains unchanged when equal to max."""
        # Create a mock order with risk equal to max
        mock_order = MagicMock()
        original_risk = Decimal('0.02')  # 2% = 2% max
        mock_order.risk_per_trade = original_risk
        mock_order.symbol = 'TEST'
        mock_order.action.value = 'BUY'
        
        # Apply risk capping
        self.risk_service._cap_risk_to_max_limit(mock_order)
        
        # Should remain unchanged
        self.assertEqual(mock_order.risk_per_trade, original_risk)
    
    @patch('src.services.risk_management_service.logger')
    def test_cap_risk_logs_warning_when_capped(self, mock_logger):
        """Test that risk capping logs a warning when values are capped."""
        # Create a mock order with risk exceeding max
        mock_order = MagicMock()
        mock_order.risk_per_trade = Decimal('0.03')  # 3% > 2% max
        mock_order.symbol = 'TEST'
        mock_order.action.value = 'BUY'
        
        # Apply risk capping
        self.risk_service._cap_risk_to_max_limit(mock_order)
        
        # Should log warning
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        self.assertIn('capped', warning_msg.lower())
        self.assertIn('3.000%', warning_msg)
        self.assertIn('2.000%', warning_msg)
    
    @patch('src.services.risk_management_service.logger')
    def test_cap_risk_no_warning_when_within_limits(self, mock_logger):
        """Test that no warning is logged when risk is within limits."""
        # Create a mock order with risk within limits
        mock_order = MagicMock()
        mock_order.risk_per_trade = Decimal('0.015')  # 1.5% < 2% max
        mock_order.symbol = 'TEST'
        mock_order.action.value = 'BUY'
        
        # Apply risk capping
        self.risk_service._cap_risk_to_max_limit(mock_order)
        
        # Should not log warning
        mock_logger.warning.assert_not_called()
    
    def test_can_place_order_with_capped_risk(self):
        """Test that orders with capped risk can still be placed."""
        # Create a mock order with risk exceeding max
        mock_order = MagicMock()
        mock_order.risk_per_trade = Decimal('0.03')  # 3% > 2% max
        mock_order.symbol = 'TEST'
        mock_order.position_strategy = PositionStrategy.DAY  # No position limits
        
        # Mock other risk checks to pass
        with patch.object(self.risk_service, '_check_trading_halts', return_value=True), \
             patch.object(self.risk_service, '_validate_position_limits', return_value=True):
            
            # Should return True (order allowed with capped risk)
            result = self.risk_service.can_place_order(
                mock_order, {}, 100000.0
            )
            
            self.assertTrue(result)
            # Risk should be capped to 2%
            self.assertEqual(mock_order.risk_per_trade, Decimal('0.02'))
    
    def test_can_place_order_still_rejects_other_violations(self):
        """Test that other risk violations still cause rejection despite capping."""
        # Create a mock order with risk exceeding max
        mock_order = MagicMock()
        mock_order.risk_per_trade = Decimal('0.03')  # 3% > 2% max
        mock_order.symbol = 'TEST'
        mock_order.position_strategy = PositionStrategy.CORE
        
        # Mock trading halts to fail
        with patch.object(self.risk_service, '_check_trading_halts', return_value=False):
            
            # Should return False (trading halted)
            result = self.risk_service.can_place_order(
                mock_order, {}, 100000.0
            )
            
            self.assertFalse(result)
    
    def test_different_max_risk_configurations(self):
        """Test that different max_risk_per_trade config values work correctly."""
        # Create a new risk service with 1% max risk
        risk_service_1pct = RiskManagementService(
            state_service=self.mock_state_service,
            persistence_service=self.mock_persistence,
            ibkr_client=self.mock_ibkr_client,
            config={}  # Empty config, we'll set attribute manually
        )
        
        # MANUALLY SET THE max_risk_per_trade ATTRIBUTE
        risk_service_1pct.max_risk_per_trade = Decimal('0.01')  # 1% max
        
        mock_order = MagicMock()
        mock_order.risk_per_trade = Decimal('0.02')  # 2% > 1% max
        mock_order.symbol = 'TEST'
        mock_order.action.value = 'BUY'
        
        # Should cap to 1%
        risk_service_1pct._cap_risk_to_max_limit(mock_order)
        self.assertEqual(mock_order.risk_per_trade, Decimal('0.01'))
    
    def test_cap_risk_with_none_value(self):
        """Test that risk capping handles None risk_per_trade gracefully."""
        mock_order = MagicMock()
        mock_order.risk_per_trade = None  # None value
        mock_order.symbol = 'TEST'
        mock_order.action.value = 'BUY'
        
        # Should not raise exception
        try:
            self.risk_service._cap_risk_to_max_limit(mock_order)
            success = True
        except Exception:
            success = False
        
        self.assertTrue(success, "Should handle None risk_per_trade gracefully")
    
    def test_cap_risk_with_zero_value(self):
        """Test that risk capping handles zero risk_per_trade."""
        mock_order = MagicMock()
        mock_order.risk_per_trade = Decimal('0.00')  # 0% risk
        mock_order.symbol = 'TEST'
        mock_order.action.value = 'BUY'
        
        # Apply risk capping
        self.risk_service._cap_risk_to_max_limit(mock_order)
        
        # Should remain 0% (though this would fail other validations)
        self.assertEqual(mock_order.risk_per_trade, Decimal('0.00'))
# Risk Capping Functionality Tests - End# Risk Capping Functionality Tests - End

if __name__ == '__main__':
    unittest.main()