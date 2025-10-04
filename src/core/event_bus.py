"""
Event bus implementation for pub/sub architecture.
Provides centralized event distribution with configurable routing.
Maintains backward compatibility with existing OrderEvent system.
"""

import logging
from typing import Dict, List, Callable, Any
from threading import RLock
from src.core.events import EventType, TradingEvent

class EventBus:
    """
    Central event bus for trading system communication.
    Implements publish-subscribe pattern with thread safety.
    This is ADDITIVE only - does not affect existing OrderEvent flow.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self._subscribers: Dict[EventType, List[Callable]] = {}
        self._global_subscribers: List[Callable] = []
        self._lock = RLock()
        self._logger = logging.getLogger(__name__)
        
        # Configurable settings with defaults
        event_bus_config = self.config.get('event_bus', {})
        self.enable_logging = event_bus_config.get('enable_logging', True)
        self.max_subscribers = event_bus_config.get('max_subscribers', 50)
        
        self._logger.info("✅ EventBus initialized - ADDITIVE to existing system")

    def subscribe(self, event_type: EventType, callback: Callable) -> bool:
        """Subscribe to specific event types."""
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            
            if len(self._subscribers[event_type]) >= self.max_subscribers:
                self._logger.warning(f"Max subscribers reached for {event_type}")
                return False
                
            self._subscribers[event_type].append(callback)
            
            if self.enable_logging:
                self._logger.debug(f"Subscribed to {event_type}: {callback.__name__}")
            return True

    def subscribe_all(self, callback: Callable) -> bool:
        """Subscribe to all event types."""
        with self._lock:
            if len(self._global_subscribers) >= self.max_subscribers:
                self._logger.warning("Max global subscribers reached")
                return False
                
            self._global_subscribers.append(callback)
            
            if self.enable_logging:
                self._logger.debug(f"Subscribed to all events: {callback.__name__}")
            return True

    def publish(self, event: TradingEvent) -> None:
        """Publish an event to all subscribers - ADDITIVE only."""
        with self._lock:
            # Notify type-specific subscribers
            if event.event_type in self._subscribers:
                for callback in self._subscribers[event.event_type]:
                    self._safe_execute_callback(callback, event)
            
            # Notify global subscribers
            for callback in self._global_subscribers:
                self._safe_execute_callback(callback, event)
            
            if self.enable_logging:
                self._logger.debug(f"Published {event.event_type.value}: {event.data}")

    def _safe_execute_callback(self, callback: Callable, event: TradingEvent) -> None:
        """Execute callback with error handling - failsafe to protect existing system."""
        try:
            callback(event)
        except Exception as e:
            self._logger.error(f"Callback {callback.__name__} failed: {e}")
            # CRITICAL: Don't propagate errors to protect existing flow

    def unsubscribe(self, event_type: EventType, callback: Callable) -> bool:
        """Unsubscribe from event type."""
        with self._lock:
            if event_type in self._subscribers and callback in self._subscribers[event_type]:
                self._subscribers[event_type].remove(callback)
                return True
            return False

    def get_subscription_stats(self) -> Dict[str, Any]:
        """Get statistics about current subscriptions."""
        with self._lock:
            return {
                'total_event_types': len(self._subscribers),
                'global_subscribers': len(self._global_subscribers),
                'subscriptions_by_type': {
                    event_type.value: len(callbacks)
                    for event_type, callbacks in self._subscribers.items()
                }
            }