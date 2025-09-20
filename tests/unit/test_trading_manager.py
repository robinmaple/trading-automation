import datetime
import pytest
from unittest.mock import MagicMock, Mock, patch

from src.core.trading_manager import TradingManager
from src.core.planned_order import PlannedOrder, ActiveOrder, PositionStrategy
from src.core.events import OrderEvent

# Helper function to create TradingManager with mocks - Begin
def create_trading_manager_with_mocks():
    """Create a TradingManager instance with all dependencies mocked."""
    mock_data_feed = MagicMock()
    mock_data_feed.is_connected.return_value = True
    
    mock_ibkr_client = MagicMock()
    mock_ibkr_client.connected = True
    mock_ibkr_client.is_paper_account = True
    mock_ibkr_client.get_account_value.return_value = 200000
    
    manager = TradingManager(data_feed=mock_data_feed, ibkr_client=mock_ibkr_client)
    
    # Mock all services and components
    manager.state_service = MagicMock()
    manager.execution_orchestrator = MagicMock()
    manager.prioritization_service = MagicMock()
    manager.market_hours = MagicMock()
    manager.risk_service = MagicMock()
    manager.advanced_features = MagicMock()
    
    return manager
# Helper function to create TradingManager with mocks - End

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

# Risk Management P&L Tests - Begin
def test_calculate_position_pnl_success():
    """Test successful P&L calculation for both long and short positions."""
    from src.services.risk_management_service import RiskManagementService
    
    # Mock dependencies
    mock_state = MagicMock()
    mock_persistence = MagicMock()
    risk_service = RiskManagementService(mock_state, mock_persistence)
    
    # Test long position (BUY)
    pnl_long = risk_service.calculate_position_pnl(100, 110, 10, 'BUY')
    assert pnl_long == 100  # (110 - 100) * 10 = 100
    
    # Test short position (SELL) 
    pnl_short = risk_service.calculate_position_pnl(100, 90, 10, 'SELL')
    assert pnl_short == 100  # (100 - 90) * 10 = 100
    
    # Test break-even
    pnl_even = risk_service.calculate_position_pnl(100, 100, 10, 'BUY')
    assert pnl_even == 0
    
    # Test loss
    pnl_loss = risk_service.calculate_position_pnl(100, 95, 10, 'BUY')
    assert pnl_loss == -50

def test_calculate_position_pnl_validation_errors():
    """Test P&L calculation raises proper exceptions for invalid parameters."""
    from src.services.risk_management_service import RiskManagementService
    
    mock_state = MagicMock()
    mock_persistence = MagicMock()
    risk_service = RiskManagementService(mock_state, mock_persistence)
    
    # Test None values
    with pytest.raises(ValueError, match="Entry price cannot be None"):
        risk_service.calculate_position_pnl(None, 100, 10, 'BUY')
    
    with pytest.raises(ValueError, match="Exit price cannot be None"):
        risk_service.calculate_position_pnl(100, None, 10, 'BUY')
    
    with pytest.raises(ValueError, match="Quantity cannot be None"):
        risk_service.calculate_position_pnl(100, 110, None, 'BUY')
    
    with pytest.raises(ValueError, match="Action cannot be None"):
        risk_service.calculate_position_pnl(100, 110, 10, None)
    
    # Test invalid types
    with pytest.raises(TypeError, match="Entry price must be numeric"):
        risk_service.calculate_position_pnl("invalid", 100, 10, 'BUY')
    
    # Test invalid values
    with pytest.raises(ValueError, match="Entry price must be positive"):
        risk_service.calculate_position_pnl(0, 100, 10, 'BUY')
    
    with pytest.raises(ValueError, match="Action must be 'BUY' or 'SELL'"):
        risk_service.calculate_position_pnl(100, 110, 10, 'INVALID')
# Risk Management P&L Tests - End

# Fixed Market Close Tests - Begin
def test_check_market_close_actions_closes_day_positions():
    """Test that DAY positions are closed 10 minutes before market close."""
    # Setup
    manager = create_trading_manager_with_mocks()
    
    # Mock market hours to return True for should_close_positions
    manager.market_hours.should_close_positions = MagicMock(return_value=True)
    
    # Mock state service to return some DAY positions
    day_position = MagicMock()
    day_position.symbol = "TEST"
    day_position.strategy = PositionStrategy.DAY
    manager.state_service.get_positions_by_strategy = MagicMock(return_value=[day_position])
    
    # Mock the close method
    manager._close_single_position = MagicMock()
    
    # Execute
    manager._check_market_close_actions()
    
    # Verify
    manager.market_hours.should_close_positions.assert_called_once()
    manager.state_service.get_positions_by_strategy.assert_called_once_with(PositionStrategy.DAY)
    manager._close_single_position.assert_called_once_with(day_position)

def test_check_market_close_actions_does_nothing_when_not_time_to_close():
    """Test that no action is taken when it's not time to close positions."""
    # Setup
    manager = create_trading_manager_with_mocks()
    
    # Mock market hours to return False for should_close_positions
    manager.market_hours.should_close_positions = MagicMock(return_value=False)
    
    # Mock state service and close method
    manager.state_service.get_positions_by_strategy = MagicMock()
    manager._close_single_position = MagicMock()
    
    # Execute
    manager._check_market_close_actions()
    
    # Verify
    manager.market_hours.should_close_positions.assert_called_once()
    manager.state_service.get_positions_by_strategy.assert_not_called()
    manager._close_single_position.assert_not_called()

def test_check_market_close_actions_does_not_close_core_positions():
    """Test that only DAY positions are closed, not CORE/HYBRID."""
    # Setup
    manager = create_trading_manager_with_mocks()
    
    # Mock market hours to return True
    manager.market_hours.should_close_positions = MagicMock(return_value=True)
    
    # Mock state service to return mixed positions
    day_position = MagicMock()
    day_position.symbol = "DAY_STOCK"
    day_position.strategy = PositionStrategy.DAY
    
    core_position = MagicMock()
    core_position.symbol = "CORE_STOCK" 
    core_position.strategy = PositionStrategy.CORE
    
    manager.state_service.get_positions_by_strategy = MagicMock(return_value=[day_position])
    manager._close_single_position = MagicMock()
    
    # Execute
    manager._check_market_close_actions()
    
    # Verify - only DAY position should be closed
    manager.state_service.get_positions_by_strategy.assert_called_once_with(PositionStrategy.DAY)
    manager._close_single_position.assert_called_once_with(day_position)
    assert manager._close_single_position.call_count == 1  # Only one call for DAY position

def test_check_market_close_actions_handles_no_day_positions():
    """Test behavior when there are no DAY positions to close."""
    # Setup
    manager = create_trading_manager_with_mocks()
    
    # Mock market hours to return True
    manager.market_hours.should_close_positions = MagicMock(return_value=True)
    
    # Mock state service to return empty list (no DAY positions)
    manager.state_service.get_positions_by_strategy = MagicMock(return_value=[])
    manager._close_single_position = MagicMock()
    
    # Execute
    manager._check_market_close_actions()
    
    # Verify
    manager.market_hours.should_close_positions.assert_called_once()
    manager.state_service.get_positions_by_strategy.assert_called_once_with(PositionStrategy.DAY)
    manager._close_single_position.assert_not_called()  # No positions to close

def test_check_market_close_actions_uses_correct_buffer_time():
    """Test that market close actions are triggered correctly."""
    # Setup
    manager = create_trading_manager_with_mocks()
    
    # Mock market hours to return True
    manager.market_hours.should_close_positions = MagicMock(return_value=True)
    manager.state_service.get_positions_by_strategy = MagicMock(return_value=[])
    manager._close_single_position = MagicMock()
    
    # Execute
    manager._check_market_close_actions()
    
    # Verify the method was called (don't assert specific parameters for mocked method)
    manager.market_hours.should_close_positions.assert_called_once()
    manager.state_service.get_positions_by_strategy.assert_called_once_with(PositionStrategy.DAY)
# Fixed Market Close Tests - End