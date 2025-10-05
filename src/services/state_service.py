"""
The single source of truth for order and position state management.
Handles state transitions, persistence, validation, and event notifications.
Maintains system consistency and enables reactive behavior through an event-driven architecture.
"""

from typing import Dict, List, Optional, Callable
from datetime import datetime
from dataclasses import asdict
import threading

from src.core.events import OrderState, OrderEvent
from src.core.database import get_db_session
from src.core.models import PlannedOrderDB, ExecutedOrderDB
from src.services.order_persistence_service import OrderPersistenceService

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)


class StateService:
    """Manages the lifecycle and state of all orders and positions."""

    def __init__(self, db_session=None):
        """Initialize the service with a database session and event system."""
        if logger:
            logger.debug("Initializing StateService")
            
        self.db_session = db_session or get_db_session()
        self.persistence_service = OrderPersistenceService(self.db_session)
        self._subscribers: Dict[str, List[Callable[[OrderEvent], None]]] = {}
        self._lock = threading.RLock()
        
        if logger:
            logger.info("StateService initialized successfully")

    def subscribe(self, event_type: str, callback: Callable[[OrderEvent], None]) -> None:
        """Subscribe a callback function to a specific type of state change event."""
        if logger:
            logger.debug(f"Subscribing callback to event type: {event_type}")
            
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(callback)
            
        if logger:
            logger.debug(f"Successfully subscribed callback to {event_type}")

    def unsubscribe(self, event_type: str, callback: Callable[[OrderEvent], None]) -> None:
        """Unsubscribe a callback function from a specific type of state change event."""
        if logger:
            logger.debug(f"Unsubscribing callback from event type: {event_type}")
            
        with self._lock:
            if event_type in self._subscribers:
                self._subscribers[event_type] = [cb for cb in self._subscribers[event_type] if cb != callback]
                
        if logger:
            logger.debug(f"Successfully unsubscribed callback from {event_type}")

    def _publish_event(self, event: OrderEvent) -> None:
        """Publish a state change event to all subscribed callbacks."""
        if logger:
            logger.debug(f"Publishing event: {event.symbol} {event.old_state} -> {event.new_state}")
            
        with self._lock:
            subscribers = self._subscribers.get('order_state_change', [])
            for callback in subscribers:
                try:
                    callback(event)
                except Exception as e:
                    if logger:
                        logger.error(f"Error in event subscriber {callback.__name__}: {e}")

    def get_planned_order_state(self, order_id: int) -> Optional[OrderState]:
        """Get the current state of a planned order by its database ID."""
        if logger:
            logger.debug(f"Getting planned order state for order_id: {order_id}")
            
        order = self.db_session.query(PlannedOrderDB).filter_by(id=order_id).first()
        state = self._string_to_order_state(order.status) if order else None
        
        if logger:
            logger.debug(f"Order {order_id} state: {state}")
            
        return state

    def update_planned_order_state(self, order_id: int, new_state: OrderState,
                                 source: str, details: Optional[dict] = None) -> bool:
        """
        Update the state of a planned order, validate the transition, and publish an event.
        Returns True if the update was successful, False otherwise.
        """
        if logger:
            logger.info(f"Updating order {order_id} state to {new_state}, source: {source}")
            
        with self._lock:
            order = self.db_session.query(PlannedOrderDB).filter_by(id=order_id).first()
            if not order:
                if logger:
                    logger.error(f"Order {order_id} not found for state update")
                return False

            old_state = self._string_to_order_state(order.status)
            if old_state == new_state:
                if logger:
                    logger.debug(f"Order {order_id} already in state {new_state}, no update needed")
                return True

            if not self._is_valid_transition(old_state, new_state):
                if logger:
                    logger.error(f"Invalid state transition: {old_state} -> {new_state} for order {order_id}")
                return False

            order.status = self._order_state_to_string(new_state)
            order.updated_at = datetime.now()

            try:
                self.db_session.commit()

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

                if logger:
                    logger.info(f"State updated: Order {order_id} ({order.symbol}) {old_state} -> {new_state}")
                return True

            except Exception as e:
                self.db_session.rollback()
                if logger:
                    logger.error(f"Failed to update order state for {order_id}: {e}")
                return False

    def _string_to_order_state(self, state_str: str) -> OrderState:
        """Convert a string from the database into an OrderState enum."""
        try:
            return OrderState[state_str]
        except (KeyError, TypeError):
            if isinstance(state_str, OrderState):
                return state_str
            if logger:
                logger.warning(f"Unknown state string '{state_str}', defaulting to PENDING")
            return OrderState.PENDING

    def _order_state_to_string(self, state: OrderState) -> str:
        """Convert an OrderState enum into a string for database storage."""
        return state.name if isinstance(state, OrderState) else str(state)

    def _is_valid_transition(self, old_state: OrderState, new_state: OrderState) -> bool:
        """Validate if a transition from one OrderState to another is allowed."""
        terminal_states = {OrderState.CANCELLED, OrderState.EXPIRED,
                          OrderState.LIQUIDATED, OrderState.LIQUIDATED_EXTERNALLY}

        if old_state in terminal_states:
            if logger:
                logger.warning(f"Cannot transition from terminal state {old_state} to {new_state}")
            return False
            
        if logger:
            logger.debug(f"Valid state transition: {old_state} -> {new_state}")
        return True

    def get_open_positions(self, symbol: Optional[str] = None) -> List[ExecutedOrderDB]:
        """Retrieve all open positions from the database, optionally filtered by symbol."""
        if logger:
            logger.debug(f"Getting open positions, symbol: {symbol}")
            
        query = self.db_session.query(ExecutedOrderDB).filter_by(is_open=True)
        if symbol:
            query = query.join(PlannedOrderDB).filter(PlannedOrderDB.symbol == symbol)
        positions = query.all()
        
        if logger:
            logger.debug(f"Found {len(positions)} open positions")
            
        return positions

    def has_open_position(self, symbol: str) -> bool:
        """Check if an open position exists for a given symbol."""
        if logger:
            logger.debug(f"Checking for open position: {symbol}")
            
        has_position = bool(self.get_open_positions(symbol))
        
        if logger:
            logger.debug(f"Open position exists for {symbol}: {has_position}")
            
        return has_position

    def close_position(self, executed_order_id: int, close_price: float,
                      close_quantity: float, commission: float = 0.0) -> bool:
        """Close a position, calculate its P&L, and update its status in the database."""
        if logger:
            logger.info(f"Closing position {executed_order_id} at price {close_price}")
            
        with self._lock:
            position = self.db_session.query(ExecutedOrderDB).filter_by(id=executed_order_id).first()
            if not position or not position.is_open:
                if logger:
                    logger.warning(f"Position {executed_order_id} not found or already closed")
                return False

            pnl = (close_price - position.filled_price) * close_quantity - commission - position.commission
            position.pnl = pnl
            position.is_open = False
            position.closed_at = datetime.now()
            position.status = 'CLOSED'

            try:
                self.db_session.commit()
                if logger:
                    logger.info(f"Position closed: {position.id}, P&L: ${pnl:,.2f}")
                return True
            except Exception as e:
                self.db_session.rollback()
                if logger:
                    logger.error(f"Failed to close position {executed_order_id}: {e}")
                return False

    def retire_planned_order(self, planned_order_id: int, source: str) -> bool:
        """Retire a planned order by setting its state to CANCELLED."""
        if logger:
            logger.info(f"Retiring planned order {planned_order_id}, source: {source}")
            
        success = self.update_planned_order_state(
            planned_order_id,
            OrderState.CANCELLED,
            source,
            details={'reason': 'retired_via_lifecycle_management'}
        )

        if success:
            if logger:
                logger.info(f"Planned order {planned_order_id} retired by {source}")
        else:
            if logger:
                logger.error(f"Failed to retire planned order {planned_order_id}")

        return success