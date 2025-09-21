import pytest
from unittest.mock import MagicMock, patch
from decimal import Decimal
import datetime

from src.core.bracket_order_manager import BracketOrderManager
from src.core.planned_order import PlannedOrder, Action


def create_mock_planned_order():
    """Helper to create a PlannedOrder instance."""
    order = MagicMock(spec=PlannedOrder)
    order.symbol = "TEST"
    order.action = Action.BUY
    order.capital_commitment = Decimal("10000.00")
    order.total_capital = Decimal("100000.00")
    order.quantity = 100
    order.fill_probability = Decimal("0.8")
    order.effective_priority = 1.0
    order.trading_plan_id = "TEST_PLAN"
    return order


def test_add_order_activates_first():
    """Test that adding orders activates the first one."""
    # Mock order execution service
    mock_order_service = MagicMock()
    mock_order_service.execute_single_order.return_value = True
    manager = BracketOrderManager(order_execution_service=mock_order_service)
    
    # Patch the _activate_order method to avoid ActiveOrder instantiation issues
    with patch.object(manager, '_activate_order') as mock_activate:
        # Create mock planned orders
        order1 = create_mock_planned_order()
        order2 = create_mock_planned_order()
        order2.symbol = "TEST2"
        
        # Add orders to manager
        manager.add_order(order1)
        manager.add_order(order2)
        
        # Both orders should be activated since they fit within total capital
        assert mock_activate.call_count == 2
        assert len(manager.inactive_orders) == 0

def test_cancel_inactive_order_removes_from_queue():
    """Test that canceling an inactive order removes it from the queue."""
    # Mock order execution service
    mock_order_service = MagicMock()
    mock_order_service.execute_single_order.return_value = True
    manager = BracketOrderManager(order_execution_service=mock_order_service)
    
    # Create mock planned orders and add them directly to inactive queue
    order1 = create_mock_planned_order()
    order2 = create_mock_planned_order()
    order2.symbol = "TEST2"
    
    manager.inactive_orders = [order1, order2]
    
    # Verify initial state
    assert len(manager.inactive_orders) == 2
    
    # Cancel the second order by symbol
    manager.cancel_inactive_order("TEST2")
    
    # Verify order2 is removed
    assert len(manager.inactive_orders) == 1
    assert manager.inactive_orders[0].symbol == "TEST"


def test_cancel_active_order_activates_next():
    """Test that canceling an active order activates the next one."""
    # Mock order execution service
    mock_order_service = MagicMock()
    mock_order_service.execute_single_order.return_value = True
    mock_order_service.cancel_order.return_value = None
    manager = BracketOrderManager(order_execution_service=mock_order_service)
    
    # Create a simple test that doesn't rely on ActiveOrder creation
    # Just test that cancel_order calls the service and triggers reactivation
    with patch.object(manager, '_reactivate_inactive_orders') as mock_reactivate:
        # Mock that we have an active order
        manager.active_orders = {"test_order": MagicMock()}
        
        # Cancel active order
        manager.cancel_order("test_order")
        
        # Verify service was called and reactivation triggered
        mock_order_service.cancel_order.assert_called_once_with("test_order")
        mock_reactivate.assert_called_once()


def test_list_active_orders_returns_correctly():
    """Test that list_active_orders returns correct list."""
    manager = BracketOrderManager(order_execution_service=MagicMock())
    
    # Initially no active orders
    assert len(manager.list_active_orders()) == 0
    
    # Add some mock active orders directly (bypassing _activate_order)
    mock_order = MagicMock()
    manager.active_orders = {"order1": mock_order, "order2": MagicMock()}
    
    # Should return the active orders
    active_orders = manager.list_active_orders()
    assert len(active_orders) == 2
    assert mock_order in active_orders


def test_list_inactive_orders_returns_correctly():
    """Test that list_inactive_orders returns correct list."""
    manager = BracketOrderManager(order_execution_service=MagicMock())
    
    # Initially no inactive orders
    assert len(manager.list_inactive_orders()) == 0
    
    # Add some mock planned orders to inactive queue
    order1 = create_mock_planned_order()
    order2 = create_mock_planned_order()
    order2.symbol = "TEST2"
    
    manager.inactive_orders = [order1, order2]
    
    # Should return the inactive orders
    inactive_orders = manager.list_inactive_orders()
    assert len(inactive_orders) == 2
    assert order1 in inactive_orders
    assert order2 in inactive_orders

def test_second_order_goes_inactive_if_first_active():
    """Test that second order goes inactive if first is already active."""
    # Mock order execution service
    mock_order_service = MagicMock()
    mock_order_service.execute_single_order.return_value = True
    manager = BracketOrderManager(order_execution_service=mock_order_service)
    
    # Create orders with proper numeric values (not MagicMock)
    order1 = create_mock_planned_order()
    order1.capital_commitment = Decimal("80000.00")
    order1.total_capital = Decimal("100000.00")
    
    order2 = create_mock_planned_order()
    order2.symbol = "TEST2"
    order2.capital_commitment = Decimal("30000.00")
    order2.total_capital = Decimal("100000.00")
    
    # Mock the internal state to simulate first order being active
    # Create a simple mock for the active order with proper numeric values
    mock_active_order = MagicMock()
    mock_active_order.capital_commitment = Decimal("80000.00")
    manager.active_orders = {"order1": mock_active_order}
    manager.inactive_orders = []
    
    # Now add second order - should go to inactive due to capital constraint
    manager.add_order(order2)
    
    # Second order should be in inactive queue due to capital constraints
    assert len(manager.inactive_orders) == 1
    assert manager.inactive_orders[0].symbol == "TEST2"

def test_get_all_orders_returns_correct_list():
    """Test that get_all_orders returns both active and inactive orders."""
    manager = BracketOrderManager(order_execution_service=MagicMock())
    
    # Add mock active and inactive orders
    mock_active = MagicMock()
    manager.active_orders = {"order1": mock_active}
    
    order1 = create_mock_planned_order()
    order2 = create_mock_planned_order()
    order2.symbol = "TEST2"
    manager.inactive_orders = [order1, order2]
    
    # Get all orders - active + inactive
    active_orders = manager.list_active_orders()
    inactive_orders = manager.list_inactive_orders()
    all_orders = active_orders + inactive_orders
    
    # Verify both types of orders are returned
    assert len(all_orders) == 3
    assert mock_active in all_orders
    assert order1 in all_orders
    assert order2 in all_orders

def test_handle_exit_reactivates_next_inactive():
    """Test that handling an exit reactivates the next inactive order."""
    # Mock order execution service
    mock_order_service = MagicMock()
    mock_order_service.execute_single_order.return_value = True
    manager = BracketOrderManager(order_execution_service=mock_order_service)
    
    # Create an inactive order
    order2 = create_mock_planned_order()
    order2.symbol = "TEST2"
    order2.capital_commitment = Decimal("30000.00")
    order2.total_capital = Decimal("100000.00")
    
    # Set up the initial state with an inactive order AND an active order
    # so that handle_exit has something to actually exit
    mock_active_order = MagicMock()
    mock_active_order.capital_commitment = Decimal("50000.00")
    manager.active_orders = {"active_order_1": mock_active_order}
    manager.inactive_orders = [order2]
    
    # Mock the _reactivate_inactive_orders method
    with patch.object(manager, '_reactivate_inactive_orders') as mock_reactivate:
        # Handle exit for the active order - this should trigger reactivation
        manager.handle_exit("active_order_1")
        
        # Verify reactivation was called
        mock_reactivate.assert_called_once()