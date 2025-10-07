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

# Context-aware logging import - Begin
from src.core.context_aware_logger import get_context_logger, TradingEventType
# Context-aware logging import - End


class StateService:
    """Manages the lifecycle and state of all orders and positions."""

    def __init__(self, db_session=None):
        """Initialize the service with a database session and event system."""
        # Context-aware logging initialization - Begin
        self.context_logger = get_context_logger()
        # Context-aware logging initialization - End
        
        self.db_session = db_session or get_db_session()
        self.persistence_service = OrderPersistenceService(self.db_session)
        self._subscribers: Dict[str, List[Callable[[OrderEvent], None]]] = {}
        self._lock = threading.RLock()
        
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="StateService initialized",
            context_provider={
                'has_db_session': lambda: db_session is not None,
                'subscriber_count': lambda: len(self._subscribers)
            },
            decision_reason="Service startup"
        )

    def subscribe(self, event_type: str, callback: Callable[[OrderEvent], None]) -> None:
        """Subscribe a callback function to a specific type of state change event."""
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(callback)
            
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Event subscriber added",
            context_provider={
                'event_type': lambda: event_type,
                'total_subscribers': lambda: len(self._subscribers.get(event_type, [])),
                'callback_name': lambda: getattr(callback, '__name__', 'anonymous')
            },
            decision_reason="Event subscription management"
        )

    def unsubscribe(self, event_type: str, callback: Callable[[OrderEvent], None]) -> None:
        """Unsubscribe a callback function from a specific type of state change event."""
        with self._lock:
            if event_type in self._subscribers:
                self._subscribers[event_type] = [cb for cb in self._subscribers[event_type] if cb != callback]
                
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Event subscriber removed",
            context_provider={
                'event_type': lambda: event_type,
                'remaining_subscribers': lambda: len(self._subscribers.get(event_type, [])),
                'callback_name': lambda: getattr(callback, '__name__', 'anonymous')
            },
            decision_reason="Event subscription management"
        )

    def _publish_event(self, event: OrderEvent) -> None:
        """Publish a state change event to all subscribed callbacks."""
        with self._lock:
            subscribers = self._subscribers.get('order_state_change', [])
            
        self.context_logger.log_event(
            event_type=TradingEventType.STATE_TRANSITION,
            message="Publishing state change event",
            symbol=event.symbol,
            context_provider={
                'order_id': lambda: event.order_id,
                'old_state': lambda: event.old_state.name,
                'new_state': lambda: event.new_state.name,
                'subscriber_count': lambda: len(subscribers),
                'source': lambda: event.source
            },
            decision_reason="Event propagation"
        )
            
        for callback in subscribers:
            try:
                callback(event)
            except Exception as e:
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Event subscriber error",
                    symbol=event.symbol,
                    context_provider={
                        'callback_name': lambda: getattr(callback, '__name__', 'anonymous'),
                        'error_type': lambda: type(e).__name__,
                        'error_message': lambda: str(e)
                    },
                    decision_reason="Subscriber exception handling"
                )

    def get_planned_order_state(self, order_id: int) -> Optional[OrderState]:
        """Get the current state of a planned order by its database ID."""
        order = self.db_session.query(PlannedOrderDB).filter_by(id=order_id).first()
        state = self._string_to_order_state(order.status) if order else None
        
        self.context_logger.log_event(
            event_type=TradingEventType.STATE_TRANSITION,
            message="Retrieved planned order state",
            context_provider={
                'order_id': lambda: order_id,
                'state': lambda: state.name if state else 'NOT_FOUND',
                'symbol': lambda: order.symbol if order else 'UNKNOWN'
            },
            decision_reason="State query operation"
        )
            
        return state

    def update_planned_order_state(self, order_id: int, new_state: OrderState,
                                 source: str, details: Optional[dict] = None) -> bool:
        """
        Update the state of a planned order, validate the transition, and publish an event.
        Returns True if the update was successful, False otherwise.
        """
        self.context_logger.log_event(
            event_type=TradingEventType.STATE_TRANSITION,
            message="Starting state update",
            context_provider={
                'order_id': lambda: order_id,
                'new_state': lambda: new_state.name,
                'source': lambda: source,
                'has_details': lambda: details is not None
            },
            decision_reason="Begin state transition process"
        )
            
        with self._lock:
            order = self.db_session.query(PlannedOrderDB).filter_by(id=order_id).first()
            if not order:
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Order not found for state update",
                    context_provider={
                        'order_id': lambda: order_id
                    },
                    decision_reason="Order lookup failure"
                )
                return False

            old_state = self._string_to_order_state(order.status)
            if old_state == new_state:
                self.context_logger.log_event(
                    event_type=TradingEventType.STATE_TRANSITION,
                    message="No state change needed",
                    symbol=order.symbol,
                    context_provider={
                        'order_id': lambda: order_id,
                        'current_state': lambda: old_state.name
                    },
                    decision_reason="State already matches target"
                )
                return True

            if not self._is_valid_transition(old_state, new_state):
                self.context_logger.log_event(
                    event_type=TradingEventType.STATE_TRANSITION,
                    message="Invalid state transition rejected",
                    symbol=order.symbol,
                    context_provider={
                        'order_id': lambda: order_id,
                        'old_state': lambda: old_state.name,
                        'new_state': lambda: new_state.name
                    },
                    decision_reason="Transition validation failure"
                )
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

                self.context_logger.log_event(
                    event_type=TradingEventType.STATE_TRANSITION,
                    message="State transition completed successfully",
                    symbol=order.symbol,
                    context_provider={
                        'order_id': lambda: order_id,
                        'old_state': lambda: old_state.name,
                        'new_state': lambda: new_state.name,
                        'source': lambda: source
                    },
                    decision_reason="State update committed and event published"
                )
                return True

            except Exception as e:
                self.db_session.rollback()
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Database commit failed during state update",
                    symbol=order.symbol,
                    context_provider={
                        'order_id': lambda: order_id,
                        'error_type': lambda: type(e).__name__,
                        'error_message': lambda: str(e)
                    },
                    decision_reason="Database transaction failure"
                )
                return False

    def _string_to_order_state(self, state_str: str) -> OrderState:
        """Convert a string from the database into an OrderState enum."""
        try:
            return OrderState[state_str]
        except (KeyError, TypeError):
            if isinstance(state_str, OrderState):
                return state_str
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Unknown state string encountered",
                context_provider={
                    'state_string': lambda: str(state_str),
                    'default_state': lambda: 'PENDING'
                },
                decision_reason="State string conversion fallback"
            )
            return OrderState.PENDING

    def _order_state_to_string(self, state: OrderState) -> str:
        """Convert an OrderState enum into a string for database storage."""
        return state.name if isinstance(state, OrderState) else str(state)

    def _is_valid_transition(self, old_state: OrderState, new_state: OrderState) -> bool:
        """Validate if a transition from one OrderState to another is allowed."""
        terminal_states = {OrderState.CANCELLED, OrderState.EXPIRED,
                          OrderState.LIQUIDATED, OrderState.LIQUIDATED_EXTERNALLY}

        if old_state in terminal_states:
            self.context_logger.log_event(
                event_type=TradingEventType.STATE_TRANSITION,
                message="Attempted transition from terminal state",
                context_provider={
                    'old_state': lambda: old_state.name,
                    'new_state': lambda: new_state.name
                },
                decision_reason="Terminal state transition prevention"
            )
            return False
            
        return True

    def get_open_positions(self, symbol: Optional[str] = None) -> List[ExecutedOrderDB]:
        """Retrieve all open positions from the database, optionally filtered by symbol."""
        query = self.db_session.query(ExecutedOrderDB).filter_by(is_open=True)
        if symbol:
            query = query.join(PlannedOrderDB).filter(PlannedOrderDB.symbol == symbol)
        positions = query.all()
        
        # Safe symbol access for logging - Begin
        position_symbols = []
        for position in positions:
            try:
                # Access symbol through the planned_order relationship
                if position.planned_order:
                    position_symbols.append(position.planned_order.symbol)
                else:
                    position_symbols.append('UNKNOWN')
            except Exception:
                position_symbols.append('ERROR')
        # Safe symbol access for logging - End
        
        self.context_logger.log_event(
            event_type=TradingEventType.POSITION_MANAGEMENT,
            message="Retrieved open positions",
            context_provider={
                'symbol_filter': lambda: symbol or 'ALL',
                'position_count': lambda: len(positions),
                'symbols_found': lambda: position_symbols
            },
            decision_reason="Position inventory query"
        )
            
        return positions

    def has_open_position(self, symbol: str) -> bool:
        """Check if an open position exists for a given symbol."""
        has_position = bool(self.get_open_positions(symbol))
        
        self.context_logger.log_event(
            event_type=TradingEventType.POSITION_MANAGEMENT,
            message="Open position check completed",
            symbol=symbol,
            context_provider={
                'has_open_position': lambda: has_position
            },
            decision_reason="Position existence verification"
        )
            
        return has_position

    def close_position(self, executed_order_id: int, close_price: float,
                      close_quantity: float, commission: float = 0.0) -> bool:
        """Close a position, calculate its P&L, and update its status in the database."""
        # Safe symbol access for logging - Begin
        def get_position_symbol():
            try:
                position = self.db_session.query(ExecutedOrderDB).filter_by(id=executed_order_id).first()
                if position and position.planned_order:
                    return position.planned_order.symbol
                return 'UNKNOWN'
            except Exception:
                return 'ERROR'
        # Safe symbol access for logging - End
        
        self.context_logger.log_event(
            event_type=TradingEventType.POSITION_MANAGEMENT,
            message="Starting position closure",
            symbol=get_position_symbol(),
            context_provider={
                'executed_order_id': lambda: executed_order_id,
                'close_price': lambda: close_price,
                'close_quantity': lambda: close_quantity,
                'commission': lambda: commission
            },
            decision_reason="Begin position close process"
        )
            
        with self._lock:
            position = self.db_session.query(ExecutedOrderDB).filter_by(id=executed_order_id).first()
            if not position or not position.is_open:
                # Safe symbol access for logging - Begin
                position_symbol = 'UNKNOWN'
                if position and position.planned_order:
                    position_symbol = position.planned_order.symbol
                # Safe symbol access for logging - End
                
                self.context_logger.log_event(
                    event_type=TradingEventType.POSITION_MANAGEMENT,
                    message="Position not found or already closed",
                    symbol=position_symbol,
                    context_provider={
                        'executed_order_id': lambda: executed_order_id,
                        'position_found': lambda: position is not None,
                        'was_open': lambda: position.is_open if position else False
                    },
                    decision_reason="Position closure validation failure"
                )
                return False

            pnl = (close_price - position.filled_price) * close_quantity - commission - position.commission
            position.pnl = pnl
            position.is_open = False
            position.closed_at = datetime.now()
            position.status = 'CLOSED'

            try:
                self.db_session.commit()
                # Safe symbol access for logging - Begin
                position_symbol = 'UNKNOWN'
                if position.planned_order:
                    position_symbol = position.planned_order.symbol
                # Safe symbol access for logging - End
                
                self.context_logger.log_event(
                    event_type=TradingEventType.POSITION_MANAGEMENT,
                    message="Position closed successfully",
                    symbol=position_symbol,
                    context_provider={
                        'executed_order_id': lambda: executed_order_id,
                        'pnl': lambda: round(pnl, 2),
                        'close_price': lambda: close_price,
                        'filled_price': lambda: position.filled_price,
                        'quantity': lambda: close_quantity
                    },
                    decision_reason="Position closure completed"
                )
                return True
            except Exception as e:
                self.db_session.rollback()
                # Safe symbol access for logging - Begin
                position_symbol = 'UNKNOWN'
                if position.planned_order:
                    position_symbol = position.planned_order.symbol
                # Safe symbol access for logging - End
                
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Database commit failed during position closure",
                    symbol=position_symbol,
                    context_provider={
                        'executed_order_id': lambda: executed_order_id,
                        'error_type': lambda: type(e).__name__,
                        'error_message': lambda: str(e)
                    },
                    decision_reason="Position closure transaction failure"
                )
                return False

    def retire_planned_order(self, planned_order_id: int, source: str) -> bool:
        """Retire a planned order by setting its state to CANCELLED."""
        self.context_logger.log_event(
            event_type=TradingEventType.STATE_TRANSITION,
            message="Starting planned order retirement",
            context_provider={
                'planned_order_id': lambda: planned_order_id,
                'source': lambda: source
            },
            decision_reason="Begin order retirement process"
        )
            
        success = self.update_planned_order_state(
            planned_order_id,
            OrderState.CANCELLED,
            source,
            details={'reason': 'retired_via_lifecycle_management'}
        )

        if success:
            self.context_logger.log_event(
                event_type=TradingEventType.STATE_TRANSITION,
                message="Planned order retired successfully",
                context_provider={
                    'planned_order_id': lambda: planned_order_id,
                    'source': lambda: source
                },
                decision_reason="Order retirement completed"
            )
        else:
            self.context_logger.log_event(
                event_type=TradingEventType.STATE_TRANSITION,
                message="Planned order retirement failed",
                context_provider={
                    'planned_order_id': lambda: planned_order_id,
                    'source': lambda: source
                },
                decision_reason="Order retirement failure"
            )

        return success