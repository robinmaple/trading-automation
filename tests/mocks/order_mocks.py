"""
Mock order objects for testing.
"""
from unittest.mock import Mock
from src.core.planned_order import PlannedOrder, Action, OrderType, SecurityType, PositionStrategy
from tests.mocks.ibkr_mocks import mock_order


def create_mock_planned_order(**kwargs):
    """Create a properly configured mock PlannedOrder with correct constructor signature."""
    mock_order = Mock(spec=PlannedOrder)
    
    # Set required fields with correct types
    mock_order.security_type = kwargs.get('security_type', SecurityType.STK)
    mock_order.exchange = kwargs.get('exchange', 'SMART')
    mock_order.currency = kwargs.get('currency', 'USD')
    mock_order.action = kwargs.get('action', Action.BUY)
    mock_order.symbol = kwargs.get('symbol', 'AAPL')
    
    # Set optional fields
    mock_order.order_type = kwargs.get('order_type', OrderType.LMT)
    mock_order.risk_per_trade = kwargs.get('risk_per_trade', 0.01)
    mock_order.entry_price = kwargs.get('entry_price', 150.0)
    mock_order.stop_loss = kwargs.get('stop_loss', 145.0)
    mock_order.risk_reward_ratio = kwargs.get('risk_reward_ratio', 2.0)
    mock_order.position_strategy = kwargs.get('position_strategy', PositionStrategy.CORE)
    mock_order.priority = kwargs.get('priority', 3)
    mock_order.trading_setup = kwargs.get('trading_setup', None)
    mock_order.core_timeframe = kwargs.get('core_timeframe', None)
    mock_order.expiration_date = kwargs.get('expiration_date', None)
    
    # Mock methods
    mock_order.calculate_quantity.return_value = kwargs.get('quantity', 100)
    mock_order.calculate_profit_target.return_value = kwargs.get('entry_price', 150.0) * 1.1
    mock_order.to_ib_contract.return_value = Mock()
    mock_order.to_ib_order.return_value = Mock()
    mock_order.validate.return_value = None
    
    return mock_order


def create_mock_active_order(**kwargs):
    """Create a properly configured mock ActiveOrder."""
    mock_active = Mock()
    mock_active.order_ids = kwargs.get('order_ids', [12345])
    mock_active.planned_order = kwargs.get('planned_order', create_mock_planned_order())
    mock_active.capital_commitment = kwargs.get('capital_commitment', 15000.0)
    mock_active.fill_probability = kwargs.get('fill_probability', 0.8)
    mock_active.timestamp = kwargs.get('timestamp', '2024-01-01 10:00:00')
    mock_active.status = kwargs.get('status', 'WORKING')
    mock_active.is_live_trading = kwargs.get('is_live_trading', False)
    
    # Mock status methods
    mock_active.is_working.return_value = kwargs.get('is_working', True)
    mock_active.update_status.return_value = None
    mock_active.age_seconds.return_value = kwargs.get('age_seconds', 300.0)
    
    return mock_order