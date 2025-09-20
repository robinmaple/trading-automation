# tests/unit/test_planned_order.py (Add these tests)
import pytest
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from src.core.planned_order import PlannedOrderManager, SecurityType, Action, OrderType, PositionStrategy

class TestPlannedOrderManagerConfig:
    """Test PlannedOrderManager configuration handling."""
    
    @patch('pandas.read_excel')
    @patch('builtins.print')  # Suppress print output
    def test_from_excel_uses_config_defaults(self, mock_print, mock_read_excel):
        """Test that from_excel uses configurable defaults when Excel values are missing."""
        # Mock Excel data with proper structure
        mock_data = {
            'Security Type': ['STK'],
            'Exchange': ['NASDAQ'],
            'Currency': ['USD'],
            'Action': ['BUY'],
            'Symbol': ['AAPL'],
            'Order Type': ['LMT'],
            'Entry Price': [150.0],
            'Stop Loss': [145.0],
            'Position Management Strategy': ['CORE'],
            'Risk Per Trade': [None],  # Missing - should use config default
            'Risk Reward Ratio': [None],  # Missing - should use config default
            'Priority': [None]  # Missing - should use config default
        }
        
        mock_df = pd.DataFrame(mock_data)
        mock_read_excel.return_value = mock_df
        
        test_config = {
            'order_defaults': {
                'risk_per_trade': 0.01,
                'risk_reward_ratio': 2.5,
                'priority': 4
            }
        }
        
        # Act
        orders = PlannedOrderManager.from_excel('dummy_path.xlsx', test_config)
        
        # Assert
        assert len(orders) == 1
        assert float(orders[0].risk_per_trade) == 0.01  # From config
        assert float(orders[0].risk_reward_ratio) == 2.5  # From config
        assert orders[0].priority == 4  # From config
    
    @patch('pandas.read_excel')
    @patch('builtins.print')  # Suppress print output
    def test_from_excel_preserves_excel_values(self, mock_print, mock_read_excel):
        """Test that Excel values take precedence over config defaults."""
        # Mock Excel data with specific values
        mock_data = {
            'Security Type': ['STK'],
            'Exchange': ['NASDAQ'],
            'Currency': ['USD'],
            'Action': ['BUY'],
            'Symbol': ['AAPL'],
            'Order Type': ['LMT'],
            'Entry Price': [150.0],
            'Stop Loss': [145.0],
            'Position Management Strategy': ['CORE'],
            'Risk Per Trade': [0.015],  # Provided in Excel
            'Risk Reward Ratio': [3.0],  # Provided in Excel
            'Priority': [2]  # Provided in Excel
        }
        
        mock_df = pd.DataFrame(mock_data)
        mock_read_excel.return_value = mock_df
        
        test_config = {
            'order_defaults': {
                'risk_per_trade': 0.01,  # Should be ignored
                'risk_reward_ratio': 2.5,  # Should be ignored
                'priority': 4  # Should be ignored
            }
        }
        
        # Act
        orders = PlannedOrderManager.from_excel('dummy_path.xlsx', test_config)
        
        # Assert - Excel values should be preserved
        assert len(orders) == 1
        assert float(orders[0].risk_per_trade) == 0.015  # From Excel, not config
        assert float(orders[0].risk_reward_ratio) == 3.0  # From Excel, not config
        assert orders[0].priority == 2  # From Excel, not config
    
    @patch('pandas.read_excel')
    @patch('builtins.print')  # Suppress print output
    def test_from_excel_fallback_to_hardcoded_defaults(self, mock_print, mock_read_excel):

        """Test that falls back to hardcoded defaults when no config provided."""
        # Mock Excel data with missing values
        mock_data = {
            'Security Type': ['STK'],
            'Exchange': ['NASDAQ'],
            'Currency': ['USD'],
            'Action': ['BUY'],
            'Symbol': ['AAPL'],
            'Order Type': ['LMT'],
            'Entry Price': [150.0],
            'Stop Loss': [145.0],
            'Position Management Strategy': ['CORE'],
            'Risk Per Trade': [None],  # Missing - should use hardcoded default
            'Risk Reward Ratio': [None],  # Missing - should use hardcoded default
            'Priority': [None]  # Missing - should use hardcoded default
        }
        
        mock_df = pd.DataFrame(mock_data)
        mock_read_excel.return_value = mock_df
        
        # Act - No config provided
        orders = PlannedOrderManager.from_excel('dummy_path.xlsx', None)
        
        # Assert - Should use original hardcoded defaults
        assert len(orders) == 1
        assert float(orders[0].risk_per_trade) == 0.005  # Original hardcoded default
        assert float(orders[0].risk_reward_ratio) == 2.0  # Original hardcoded default
        assert orders[0].priority == 3  # Original hardcoded default
