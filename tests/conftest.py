import sys
import os
from pathlib import Path

# Add the src directory to Python path for imports
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

import pytest
from unittest.mock import Mock, MagicMock, patch

@pytest.fixture
def mock_data_feed():
    """Fixture for mocking AbstractDataFeed"""
    mock_feed = Mock()
    mock_feed.is_connected.return_value = True
    mock_feed.get_current_price.return_value = {
        'price': 100.0,
        'timestamp': '2024-01-01 12:00:00',
        'data_type': 'MOCK'
    }
    return mock_feed

@pytest.fixture
def mock_ibkr_client():
    """Fixture for mocking IbkrClient"""
    mock_client = Mock()
    mock_client.connected = True
    mock_client.get_account_value.return_value = 100000.0
    mock_client.place_bracket_order.return_value = [1, 2, 3]  # Mock order IDs
    return mock_client

@pytest.fixture
def sample_planned_order():
    """Fixture for creating a sample planned order"""
    from src.core.planned_order import PlannedOrder, SecurityType, Action, OrderType, PositionStrategy
    
    return PlannedOrder(
        security_type=SecurityType.CASH,
        exchange="IDEALPRO",
        currency="USD",
        action=Action.BUY,
        symbol="EUR",
        order_type=OrderType.LMT,
        risk_per_trade=0.001,
        entry_price=1.1000,
        stop_loss=1.0950,
        risk_reward_ratio=2.0,
        position_strategy=PositionStrategy.DAY
    )