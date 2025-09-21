import pytest
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from src.core.planned_order import PlannedOrderManager, SecurityType, Action, OrderType, PositionStrategy

class TestPlannedOrderManagerConfig:
    """Test PlannedOrderManager configuration handling."""
    
    @patch('pandas.read_excel')
    def test_from_excel_uses_config_defaults(self, mock_read_excel):
        """Test that from_excel uses configurable defaults when Excel values are missing."""
        # Mock Excel data
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
            'Risk Per Trade': [None],  # Missing
            'Risk Reward Ratio': [None],  # Missing
            'Priority': [None]  # Missing
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
        
        # Act - This should return PlannedOrder objects
        orders = PlannedOrderManager.from_excel('dummy_path.xlsx', test_config)
        
        # Assert - Check PlannedOrder attributes (they might be floats, not Decimals)
        assert len(orders) == 1
        assert orders[0].risk_per_trade == 0.01  # Float comparison
        assert orders[0].risk_reward_ratio == 2.5  # Float comparison
        assert orders[0].priority == 4  # Integer comparison

    @patch('pandas.read_excel')
    def test_from_excel_preserves_excel_values(self, mock_read_excel):
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
            'Risk Per Trade': [0.015],  # Provided
            'Risk Reward Ratio': [3.0],  # Provided
            'Priority': [2]  # Provided
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
        
        # Assert - Excel values should be preserved in PlannedOrder objects
        assert len(orders) == 1
        assert orders[0].risk_per_trade == 0.015  # Float comparison
        assert orders[0].risk_reward_ratio == 3.0  # Float comparison
        assert orders[0].priority == 2  # Integer comparison

    @patch('pandas.read_excel')
    def test_from_excel_fallback_to_hardcoded_defaults(self, mock_read_excel):
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
            'Risk Per Trade': [None],  # Missing
            'Risk Reward Ratio': [None],  # Missing
            'Priority': [None]  # Missing
        }
        
        mock_df = pd.DataFrame(mock_data)
        mock_read_excel.return_value = mock_df
        
        # Act - No config provided
        orders = PlannedOrderManager.from_excel('dummy_path.xlsx', None)
        
        # Assert - Should use original hardcoded defaults in PlannedOrder objects
        assert len(orders) == 1
        assert orders[0].risk_per_trade == 0.005  # Float comparison
        assert orders[0].risk_reward_ratio == 2.0  # Float comparison
        assert orders[0].priority == 3  # Integer comparison