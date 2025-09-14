import pytest
from unittest.mock import MagicMock, call
from src.core.bracket_order_manager import BracketOrderManager
from src.core.planned_order import PlannedOrder, SecurityType, Action, PositionStrategy, OrderType

# ------------------------------------------------------------------
# Fixture: properly mocks order_service
# ------------------------------------------------------------------
@pytest.fixture
def mock_services():
    manager = BracketOrderManager()
    order_execution = MagicMock()

    # Patch manager's expected service
    manager.order_service = order_execution

    return manager, order_execution

# ------------------------------------------------------------------
# Fixture: a sample planned order
# ------------------------------------------------------------------
@pytest.fixture
def planned_order():
    order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,
        symbol="AAPL",
        order_type=OrderType.LMT,
        entry_price=150.0,
        stop_loss=145.0,
        risk_reward_ratio=2.0,
        position_strategy=PositionStrategy.CORE,
        priority=3
    )
    order.quantity = 5
    order.capital_commitment = 1000
    order.total_capital = 5000
    return order

# ------------------------------------------------------------------
# Test: first order activates immediately
# ------------------------------------------------------------------
def test_add_order_activates_first(mock_services, planned_order):
    manager, order_execution = mock_services

    # Add first order → should activate
    manager.add_order(planned_order)

    order_execution.execute_single_order.assert_called_with(
        planned_order,
        fill_probability=getattr(planned_order, "fill_probability", 0.9),
        effective_priority=getattr(planned_order, "effective_priority", 1.0),
        total_capital=planned_order.total_capital,
        quantity=planned_order.quantity,
        capital_commitment=planned_order.capital_commitment,
        is_live_trading=True
    )

# ------------------------------------------------------------------
# Test: second order goes inactive if first active
# ------------------------------------------------------------------
def test_second_order_goes_inactive_if_first_active(mock_services):
    manager, order_execution = mock_services

    # First order active
    first_order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,
        symbol="AAPL",
        order_type=OrderType.LMT,
        entry_price=150.0,
        stop_loss=145.0,
        risk_reward_ratio=2.0,
        position_strategy=PositionStrategy.CORE,
        priority=3
    )
    first_order.quantity = 5
    first_order.capital_commitment = 1000
    first_order.total_capital = 5000
    # Set fill_probability as an attribute instead of constructor parameter
    first_order.fill_probability = 0.0

    manager.add_order(first_order)

    # Second order should go inactive
    second_order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.SELL,
        symbol="GOOG",
        order_type=OrderType.LMT,
        entry_price=2000.0,
        stop_loss=2050.0,
        risk_reward_ratio=2.0,
        position_strategy=PositionStrategy.CORE,
        priority=3
    )
    second_order.quantity = 1
    second_order.capital_commitment = 5000
    second_order.total_capital = 5000
    # Set fill_probability as an attribute instead of constructor parameter
    second_order.fill_probability = 0.0

    manager.add_order(second_order)

    # Check that second order is in inactive_orders list
    inactive_symbols = [order.symbol for order in manager.inactive_orders]
    active_symbols = [order.symbol for order in manager.active_orders.values()]
    
    assert second_order.symbol in inactive_symbols
    assert second_order.symbol not in active_symbols

# ------------------------------------------------------------------
# Test: handle exit reactivates next inactive order
# ------------------------------------------------------------------
def test_handle_exit_reactivates_next_inactive(mock_services):
    manager, order_execution = mock_services

    # First order active
    first_order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,
        symbol="AAPL",
        order_type=OrderType.LMT,
        entry_price=150.0,
        stop_loss=145.0,
        risk_reward_ratio=2.0,
        position_strategy=PositionStrategy.CORE,
        priority=3
    )
    first_order.quantity = 5
    first_order.capital_commitment = 1000
    first_order.total_capital = 5000
    # Set fill_probability as an attribute instead of constructor parameter
    first_order.fill_probability = 0.0

    manager.add_order(first_order)

    # Second order inactive initially
    second_order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.SELL,
        symbol="GOOG",
        order_type=OrderType.LMT,
        entry_price=2000.0,
        stop_loss=2050.0,
        risk_reward_ratio=2.0,
        position_strategy=PositionStrategy.CORE,
        priority=3
    )
    second_order.quantity = 1
    second_order.capital_commitment = 5000
    second_order.total_capital = 5000
    # Set fill_probability as an attribute instead of constructor parameter
    second_order.fill_probability = 0.0

    manager.add_order(second_order)

    # Exit first order → second should activate
    active_order_id = list(manager.active_orders.keys())[0]
    manager.handle_exit(active_order_id)

    order_execution.execute_single_order.assert_called_with(
        second_order,
        fill_probability=getattr(second_order, "fill_probability", 0.9),
        effective_priority=getattr(second_order, "effective_priority", 1.0),
        total_capital=second_order.total_capital,
        quantity=second_order.quantity,
        capital_commitment=second_order.capital_commitment,
        is_live_trading=True
    )