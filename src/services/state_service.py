from typing import Dict, List, Optional, Callable
from datetime import datetime
from dataclasses import asdict
import threading

from src.core.events import OrderState, OrderEvent
from src.core.database import get_db_session
from src.core.models import PlannedOrderDB, ExecutedOrderDB
from src.services.order_persistence_service import OrderPersistenceService


class StateService:
    """
    The single source of truth for order state management.
    Handles state transitions, persistence, and event notifications.
    """

    def __init__(self, db_session=None):
        self.db_session = db_session or get_db_session()
        self.persistence_service = OrderPersistenceService(self.db_session)
        
        # Event system for state change notifications
        self._subscribers: Dict[str, List[Callable[[OrderEvent], None]]] = {}
        self._lock = threading.RLock()

    def subscribe(self, event_type: str, callback: Callable[[OrderEvent], None]) -> None:
        """Subscribe to state change events."""
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable[[OrderEvent], None]) -> None:
        """Unsubscribe from state change events."""
        with self._lock:
            if event_type in self._subscribers:
                self._subscribers[event_type] = [cb for cb in self._subscribers[event_type] if cb != callback]

    def _publish_event(self, event: OrderEvent) -> None:
        """Publish a state change event to all subscribers."""
        with self._lock:
            subscribers = self._subscribers.get('order_state_change', [])
            for callback in subscribers:
                try:
                    callback(event)
                except Exception as e:
                    print(f"Error in event subscriber {callback.__name__}: {e}")

    def get_planned_order_state(self, order_id: int) -> Optional[OrderState]:
        """Get the current state of a planned order."""
        order = self.db_session.query(PlannedOrderDB).filter_by(id=order_id).first()
        return self._string_to_order_state(order.status) if order else None

    def update_planned_order_state(self, order_id: int, new_state: OrderState, 
                                 source: str, details: Optional[dict] = None) -> bool:
        """
        Update the state of a planned order and publish an event.
        Returns True if successful, False otherwise.
        """
        with self._lock:
            order = self.db_session.query(PlannedOrderDB).filter_by(id=order_id).first()
            if not order:
                print(f"Order {order_id} not found for state update")
                return False

            # Convert current state from string to OrderState
            old_state = self._string_to_order_state(order.status)
            if old_state == new_state:
                return True  # No change needed

            # Validate state transition
            if not self._is_valid_transition(old_state, new_state):
                print(f"Invalid state transition: {old_state} -> {new_state} for order {order_id}")
                return False

            # Update the state (convert OrderState to string for database)
            order.status = self._order_state_to_string(new_state)
            order.updated_at = datetime.now()

            try:
                self.db_session.commit()
                
                # Publish state change event
                event = OrderEvent(
                    order_id=order_id,
                    symbol=order.symbol,
                    old_state=old_state,
                    new_state=new_state,
                    timestamp=datetime.now(),
                    source=source,
                    details=details
                )
                self._publish_event(event)
                
                print(f"State updated: Order {order_id} ({order.symbol}) {old_state} -> {new_state}")
                return True
                
            except Exception as e:
                self.db_session.rollback()
                print(f"Failed to update order state: {e}")
                return False

    def _string_to_order_state(self, state_str: str) -> OrderState:
        """Convert string from database to OrderState enum."""
        try:
            return OrderState[state_str]
        except (KeyError, TypeError):
            # Handle cases where state_str is already an OrderState or invalid
            if isinstance(state_str, OrderState):
                return state_str
            return OrderState.PENDING  # Default fallback

    def _order_state_to_string(self, state: OrderState) -> str:
        """Convert OrderState enum to string for database storage."""
        return state.name if isinstance(state, OrderState) else str(state)

    def _is_valid_transition(self, old_state: OrderState, new_state: OrderState) -> bool:
        """Validate if a state transition is allowed."""
        # Terminal states cannot be changed
        terminal_states = {OrderState.CANCELLED, OrderState.EXPIRED, 
                          OrderState.LIQUIDATED, OrderState.LIQUIDATED_EXTERNALLY}
        
        if old_state in terminal_states:
            return False
            
        # Add specific transition rules here as needed
        # For now, allow any transition from non-terminal states
        return True

    def get_open_positions(self, symbol: Optional[str] = None) -> List[ExecutedOrderDB]:
        """Get all open positions, optionally filtered by symbol."""
        query = self.db_session.query(ExecutedOrderDB).filter_by(is_open=True)
        if symbol:
            query = query.join(PlannedOrderDB).filter(PlannedOrderDB.symbol == symbol)
        return query.all()

    def has_open_position(self, symbol: str) -> bool:
        """Check if there's an open position for a given symbol."""
        return bool(self.get_open_positions(symbol))

    def close_position(self, executed_order_id: int, close_price: float, 
                      close_quantity: float, commission: float = 0.0) -> bool:
        """
        Close a position and update P&L.
        Returns True if successful, False otherwise.
        """
        with self._lock:
            position = self.db_session.query(ExecutedOrderDB).filter_by(id=executed_order_id).first()
            if not position or not position.is_open:
                return False

            # Calculate P&L (simplified - needs enhancement for different security types)
            pnl = (close_price - position.filled_price) * close_quantity - commission - position.commission
            
            position.pnl = pnl
            position.is_open = False
            position.closed_at = datetime.now()
            position.status = 'CLOSED'

            try:
                self.db_session.commit()
                print(f"Position closed: {position.id}, P&L: ${pnl:,.2f}")
                return True
            except Exception as e:
                self.db_session.rollback()
                print(f"Failed to close position: {e}")
                return False

    def retire_planned_order(self, planned_order_id: int, source: str) -> bool:
        """
        Retire a planned order (set to CANCELLED) and cancel any working orders.
        This is a core function for lifecycle management.
        """
        success = self.update_planned_order_state(
            planned_order_id, 
            OrderState.CANCELLED, 
            source,
            details={'reason': 'retired_via_lifecycle_management'}
        )
        
        # TODO: Integrate with IbkrClient to cancel working orders
        # This will be implemented in Phase 4
        if success:
            print(f"Planned order {planned_order_id} retired by {source}")
            
        return success