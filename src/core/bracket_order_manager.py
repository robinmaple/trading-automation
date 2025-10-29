"""
Manager for handling multiple bracket orders with activation rules.
Phase 2: Supports first-trigger-first-serve, inactive/reactivation, and capital/quantity limits.
"""

import datetime
from typing import Dict, List
from src.trading.orders.planned_order import ActiveOrder, PlannedOrder

class BracketOrderManager:
    """
    Manage multiple bracket orders for a given account.
    Only one order from a set is active at a time if buying power / available quantity constraints apply.
    """

    def __init__(self, order_execution_service=None, trading_manager=None):
        if trading_manager and not order_execution_service:
            order_execution_service = trading_manager
        self.order_service = order_execution_service
        self.active_orders: Dict[str, ActiveOrder] = {}  # key: order_id, value: ActiveOrder
        self.inactive_orders: List[PlannedOrder] = []

    def add_order(self, planned_order: PlannedOrder):
        """
        Add a new planned order to the manager.
        It may become active immediately or go into inactive queue.
        """
        total_active_commitment = sum(o.capital_commitment for o in self.active_orders.values())
        if planned_order.capital_commitment + total_active_commitment <= planned_order.total_capital:
            self._activate_order(planned_order)
        else:
            self.inactive_orders.append(planned_order)
            print(f"⚠️ Order for {planned_order.symbol} added to inactive queue (capital limit reached)")

    def _activate_order(self, planned_order: PlannedOrder):
        """Activate a planned order by executing it via OrderExecutionService"""
        print(f"➡️ Activating order for {planned_order.symbol}")
        success = self.order_service.execute_single_order(
            planned_order,
            fill_probability=getattr(planned_order, 'fill_probability', 0.9),
            effective_priority=getattr(planned_order, 'effective_priority', 1.0),
            total_capital=planned_order.total_capital,
            quantity=planned_order.quantity,
            capital_commitment=planned_order.capital_commitment,
            is_live_trading=True
        )

        if success:
            # Keep backward-compatible db_id linking
            db_id = self.order_service._trading_manager._find_planned_order_db_id(planned_order)
            if db_id:
                active_order = ActiveOrder(
                    planned_order=planned_order,
                    order_ids=[f"ACTIVE-{db_id}"],  # key placeholder
                    db_id=db_id,
                    status='SUBMITTED',
                    capital_commitment=planned_order.capital_commitment,
                    timestamp=datetime.datetime.now(),
                    is_live_trading=True,
                    fill_probability=getattr(planned_order, 'fill_probability', 0.9)
                )
                self.active_orders[active_order.order_ids[0]] = active_order
        else:
            print(f"❌ Failed to activate order for {planned_order.symbol}")

    def handle_exit(self, order_id: str, exit_reason="MANUAL"):
        """
        Called when an ActiveOrder completes or is closed.
        Frees up capital and triggers any eligible inactive orders.
        """
        if order_id not in self.active_orders:
            print(f"⚠️ No active order found for ID {order_id}")
            return

        completed_order = self.active_orders.pop(order_id)
        completed_order.exit_reason = exit_reason
        print(f"✅ Order {order_id} for {completed_order.planned_order.symbol} exited ({exit_reason})")
        self._log_executed_order(completed_order)
        self._reactivate_inactive_orders()

    def _reactivate_inactive_orders(self):
        """
        Go through the inactive queue and activate any orders that now have available capital.
        Follows first-trigger-first-serve.
        """
        if not self.inactive_orders:
            return

        to_remove = []
        total_active_commitment = sum(o.capital_commitment for o in self.active_orders.values())

        for planned_order in self.inactive_orders:
            if planned_order.capital_commitment + total_active_commitment <= planned_order.total_capital:
                self._activate_order(planned_order)
                to_remove.append(planned_order)
                total_active_commitment += planned_order.capital_commitment

        for order in to_remove:
            self.inactive_orders.remove(order)

    def _log_executed_order(self, active_order: ActiveOrder):
        """Log order execution info for tracking and debugging"""
        print(f"💾 Logging executed order: {active_order.planned_order.symbol}, "
              f"Plan ID: {active_order.planned_order.trading_plan_id}, "
              f"Exit Reason: {getattr(active_order, 'exit_reason', 'UNKNOWN')}")

    def list_active_orders(self):
        return list(self.active_orders.values())

    def list_inactive_orders(self):
        return self.inactive_orders

    def cancel_all_orders(self):
        """Cancel all active orders via OrderExecutionService"""
        for order_id, active_order in list(self.active_orders.items()):
            self.order_service.cancel_order(order_id)
            self.active_orders.pop(order_id)

    # --- Selective Cancellation - Begin ---
    def cancel_order(self, order_id: str):
        """
        Cancel a single active order by ID.
        Frees up capital and triggers reactivation of inactive orders if possible.
        """
        if order_id not in self.active_orders:
            print(f"⚠️ No active order found with ID {order_id}")
            return

        active_order = self.active_orders.pop(order_id)
        self.order_service.cancel_order(order_id)
        print(f"🛑 Order {order_id} for {active_order.planned_order.symbol} cancelled manually")
        self._log_executed_order(active_order)
        self._reactivate_inactive_orders()

    def cancel_inactive_order(self, symbol: str):
        """
        Cancel a planned order in the inactive queue by symbol.
        """
        for planned_order in list(self.inactive_orders):
            if planned_order.symbol == symbol:
                self.inactive_orders.remove(planned_order)
                print(f"🛑 Inactive order for {symbol} removed from queue")
                return
        print(f"⚠️ No inactive order found for {symbol}")
    # --- Selective Cancellation - End ---
