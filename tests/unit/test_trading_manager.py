import datetime
import pytest
from unittest.mock import MagicMock, Mock, patch

from src.core.trading_manager import TradingManager
from src.core.planned_order import PlannedOrder, ActiveOrder
from src.core.events import OrderEvent

@pytest.fixture
def mock_data_feed():
    feed = MagicMock()
    feed.is_connected.return_value = True
    feed.get_current_price.return_value = {"price": 100}
    return feed

@pytest.fixture
def mock_ibkr_client():
    client = MagicMock()
    client.connected = True
    client.is_paper_account = True
    client.get_account_value.return_value = 200000
    return client

@pytest.fixture
def manager(mock_data_feed, mock_ibkr_client):
    return TradingManager(data_feed=mock_data_feed, ibkr_client=mock_ibkr_client)

def test_initialize_success(manager):
    assert manager._initialize() is True
    assert manager._initialized is True

def test_initialize_fails_if_datafeed_disconnected(mock_ibkr_client):
    df = MagicMock()
    df.is_connected.return_value = False
    m = TradingManager(data_feed=df, ibkr_client=mock_ibkr_client)
    assert m._initialize() is False

def test_cancel_active_order_success(manager, mock_ibkr_client):
    ao = MagicMock()
    ao.order_ids = [1, 2]
    ao.is_working.return_value = True
    manager.ibkr_client.cancel_order.return_value = True
    result = manager.cancel_active_order(ao)
    assert result is True
    ao.update_status.assert_called_once_with('CANCELLED')

def test_cancel_active_order_fails_if_ibkr_not_connected(mock_data_feed):
    client = MagicMock()
    client.connected = False
    m = TradingManager(data_feed=mock_data_feed, ibkr_client=client)
    ao = MagicMock(order_ids=[1])
    assert m.cancel_active_order(ao) is False

def test_cleanup_completed_orders(manager):
    ao1, ao2 = MagicMock(), MagicMock()
    ao1.is_working.return_value = False
    ao2.is_working.return_value = True
    manager.active_orders = {1: ao1, 2: ao2}
    manager.cleanup_completed_orders()
    assert 1 not in manager.active_orders
    assert 2 in manager.active_orders

def test_execute_prioritized_orders_runs(manager):
    mock_order = MagicMock(symbol="AAPL", priority=1, action=MagicMock(), entry_price=100, stop_loss=95)
    executable_orders = [{"order": mock_order, "allocated": True, "viable": True, "fill_probability": 0.8}]
    manager.prioritization_service.prioritize_orders = MagicMock(return_value=executable_orders)
    manager.execution_orchestrator.execute_single_order = MagicMock(return_value=True)
    manager.state_service.has_open_position = MagicMock(return_value=False)
    manager._execute_prioritized_orders(executable_orders)
    manager.execution_orchestrator.execute_single_order.assert_called_once()

# In your test file, update the OrderEvent creation:
def test_handle_order_state_change_triggers_labeling(manager):
    manager.advanced_features.enabled = True
    manager.advanced_features.label_completed_orders = MagicMock()
    
    # Fix: Provide all required parameters for OrderEvent
    event = OrderEvent(
        order_id=1, 
        symbol="TEST",
        old_state="PENDING",
        new_state="FILLED",
        timestamp=datetime.datetime.now(),
        source="test"
    )
    
    manager._handle_order_state_change(event)
    manager.advanced_features.label_completed_orders.assert_called_once_with(hours_back=1)
    
def test_get_active_orders_summary(manager):
    ao = MagicMock()
    ao.to_dict.return_value = {"id": 1, "symbol": "AAPL"}
    manager.active_orders = {1: ao}
    summary = manager.get_active_orders_summary()
    assert {"id": 1, "symbol": "AAPL"} in summary
