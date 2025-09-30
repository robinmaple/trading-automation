import pytest
from unittest.mock import Mock, MagicMock
import datetime

from src.core.order_lifecycle_manager import OrderLifecycleManager
from src.core.models import PlannedOrderDB
from src.core.events import OrderState
from src.core.planned_order import PlannedOrder

from src.services.order_loading_service import OrderLoadingService
from src.services.order_persistence_service import OrderPersistenceService
from src.services.state_service import StateService


# ---------------------------
# Helper factories
# ---------------------------
def make_mock_planned_order(symbol="AAPL", action="BUY", order_type="LIMIT"):
    mock_order = Mock(spec=PlannedOrder)
    mock_order.symbol = symbol
    mock_order.action = Mock()
    mock_order.action.value = action
    mock_order.order_type = Mock()
    mock_order.order_type.value = order_type
    mock_order.entry_price = 100.0
    mock_order.stop_loss = 95.0
    mock_order.risk_per_trade = 0.01
    mock_order.risk_reward_ratio = 2.0
    mock_order.priority = 3
    return mock_order

def make_mock_db_order(symbol="AAPL", action="BUY", order_type="LIMIT", status="FILLED"):
    mock_db = Mock(spec=PlannedOrderDB)
    mock_db.symbol = symbol
    mock_db.action = action
    mock_db.order_type = order_type
    mock_db.entry_price = 100.0
    mock_db.stop_loss = 95.0
    mock_db.status = status
    mock_db.updated_at = datetime.datetime.now()
    mock_db.created_at = datetime.datetime.now()
    mock_db.status_message = None
    return mock_db

def make_mock_planned_order(symbol="AAPL", action="BUY", order_type="LIMIT"):
    mock_order = Mock(spec=PlannedOrder)
    mock_order.symbol = symbol
    mock_order.action = Mock()
    mock_order.action.value = action
    mock_order.order_type = Mock()
    mock_order.order_type.value = order_type
    mock_order.entry_price = 100.0
    mock_order.stop_loss = 95.0
    mock_order.risk_per_trade = 0.01
    mock_order.risk_reward_ratio = 2.0
    mock_order.priority = 3
    
    # Add validate method that throws ValueError for invalid stop loss
    mock_order.validate = Mock()
    
    return mock_order

# ---------------------------
# Fixtures
# ---------------------------
@pytest.fixture
def db_session():
    return Mock()


@pytest.fixture
def loading_service():
    return Mock()


@pytest.fixture
def persistence_service():
    svc = Mock()
    # convert_to_db_model returns a mock DB order
    svc.convert_to_db_model.side_effect = lambda order: make_mock_db_order(
        symbol=order.symbol,
        action=order.action.value,
        order_type=order.order_type.value
    )
    return svc


@pytest.fixture
def state_service():
    svc = Mock()
    svc.has_open_position.return_value = False
    return svc


@pytest.fixture
def manager(loading_service, persistence_service, state_service, db_session):
    return OrderLifecycleManager(
        loading_service=loading_service,
        persistence_service=persistence_service,
        state_service=state_service,
        db_session=db_session
    )


# ---------------------------
# Tests
# ---------------------------

class ActionMock:
    def __init__(self, value):
        self.value = value

class OrderTypeMock:
    def __init__(self, value):
        self.value = value

def test_load_and_persist_orders_success():
    # Mocks
    db_session = Mock()
    loading_service = Mock(spec=OrderLoadingService)
    persistence_service = Mock(spec=OrderPersistenceService)
    state_service = Mock(spec=StateService)

    # Two fake orders
    order1 = Mock(spec=PlannedOrder)
    order1.symbol = "AAPL"
    order1.action = ActionMock("BUY")
    order1.entry_price = 100
    order1.stop_loss = 95
    order1.risk_per_trade = 0.01
    order1.risk_reward_ratio = 2
    order1.priority = 3
    order1.order_type = OrderTypeMock("LIMIT")

    order2 = Mock(spec=PlannedOrder)
    order2.symbol = "GOOG"
    order2.action = ActionMock("SELL")
    order2.entry_price = 200
    order2.stop_loss = 210
    order2.risk_per_trade = 0.01
    order2.risk_reward_ratio = 2
    order2.priority = 2
    order2.order_type = OrderTypeMock("LIMIT")

    valid_orders = [order1, order2]

    # Setup service mocks
    loading_service.load_and_validate_orders.return_value = valid_orders
    # Convert to db_model just returns a Mock
    persistence_service.convert_to_db_model.side_effect = lambda o: Mock()
    
    # Make sure no existing orders
    manager = OrderLifecycleManager(loading_service, persistence_service, state_service, db_session)
    manager.find_existing_order = Mock(return_value=None)

    # Run
    result = manager.load_and_persist_orders("dummy_path.xlsx")

    # Assertions
    assert result == valid_orders
    assert db_session.add.call_count == 2
    assert db_session.commit.call_count == 1
    
def test_load_and_persist_orders_failure_rolls_back(manager, loading_service, db_session):
    order = make_mock_planned_order()
    loading_service.load_and_validate_orders.side_effect = Exception("Load failed")
    
    with pytest.raises(Exception):
        manager.load_and_persist_orders("dummy_path.xlsx")
    
    db_session.rollback.assert_called_once()


def test_is_order_executable(manager):
    order = make_mock_planned_order()
    
    # No existing order in DB, no open position
    manager.find_existing_order = Mock(return_value=None)
    
    can_execute, msg = manager.is_order_executable(order)
    
    assert can_execute
    assert msg is None


def test_update_order_status(manager):
    order = make_mock_planned_order()
    db_order = make_mock_db_order(status="LIVE")
    
    manager.find_existing_order = Mock(return_value=db_order)
    
    success = manager.update_order_status(order, OrderState.FILLED, "Executed successfully")
    
    assert success
    assert db_order.status == OrderState.FILLED
    assert db_order.status_message == "Executed successfully"
    manager.db_session.commit.assert_called_once()

def test_bulk_update_status():
    db_session = Mock()
    manager = OrderLifecycleManager(Mock(), Mock(), Mock(), db_session)

    # Two fake orders
    order1 = Mock(spec=PlannedOrder)
    order1.symbol = "AAPL"
    order2 = Mock(spec=PlannedOrder)
    order2.symbol = "GOOG"

    # Mock update_order_status to always return True
    manager.update_order_status = Mock(return_value=True)

    # Run bulk update
    updates = [
        (order1, "FILLED", "Done"),
        (order2, "CANCELLED", "Cancelled")
    ]
    results = manager.bulk_update_status(updates)

    # Assertions
    assert results == {"AAPL": True, "GOOG": True}
    assert manager.update_order_status.call_count == 2

def test_get_orders_by_status(manager):
    db_order = make_mock_db_order()
    manager.db_session.query().filter_by().all.return_value = [db_order]
    
    result = manager.get_orders_by_status(OrderState.FILLED)
    assert len(result) == 1
    assert result[0].status == "FILLED"


def test_cleanup_old_orders(manager):
    old_order = make_mock_db_order()
    manager.db_session.query().filter().all.return_value = [old_order]
    
    deleted = manager.cleanup_old_orders(days_old=30)
    
    assert deleted == 1
    manager.db_session.delete.assert_called_with(old_order)
    manager.db_session.commit.assert_called_once()


def test_get_order_statistics(manager):
    db_order = make_mock_db_order()
    manager.db_session.query().count.return_value = 1
    manager.db_session.query().filter_by().count.return_value = 1
    
    stats = manager.get_order_statistics()
    assert stats['total_orders'] == 1
    assert isinstance(stats['status_counts'], dict)


def test_find_orders_needing_attention(manager):
    stuck_order = make_mock_db_order(status="EXECUTING")
    failed_order = make_mock_db_order(status="FAILED")
    
    # Mock chained query
    query_mock = Mock()
    query_mock.filter.return_value.all.side_effect = [[stuck_order], [failed_order]]
    manager.db_session.query.return_value = query_mock
    
    result = manager.find_orders_needing_attention()
    assert stuck_order in result
    assert failed_order in result

def test_validate_order_valid(manager):
    order = make_mock_planned_order()
    # <Fix Missing Mock - Begin>
    # Mock that no existing order is found in database
    manager.find_existing_order = Mock(return_value=None)
    # <Fix Missing Mock - End>
    valid, msg = manager.validate_order(order)
    assert valid
    assert msg is None

# Then in the test, configure the mock behavior:
def test_validate_order_invalid_stop_loss(manager):
    order = make_mock_planned_order()
    order.stop_loss = 105.0  # above entry for BUY
    
    # Configure the mock to throw the expected exception
    order.validate.side_effect = ValueError("Stop loss must be below entry price for BUY orders")
    
    valid, msg = manager.validate_order(order)
    assert not valid
    assert "Stop loss must be below entry price" in msg