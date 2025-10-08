"""
Safe Context-Aware Logging System for Trading Automation.
Provides structured event logging with dead-loop protection and circuit breakers.
Designed to answer debugging questions about order execution, state discrepancies, and system behavior.
"""

import datetime
import threading
import time
from enum import Enum
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass, asdict
import uuid
import inspect

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)


class TradingEventType(Enum):
    """Categories of trading events for structured logging."""
    ORDER_VALIDATION = "order_validation"
    EXECUTION_DECISION = "execution_decision"
    MARKET_CONDITION = "market_condition"
    POSITION_MANAGEMENT = "position_management"
    STATE_TRANSITION = "state_transition"
    RISK_EVALUATION = "risk_evaluation"
    SYSTEM_HEALTH = "system_health"
    DATABASE_STATE = "database_state"


@dataclass
class TradingEvent:
    """Structured event data with safety guarantees."""
    event_id: str
    event_type: str
    timestamp: str
    session_id: str
    symbol: Optional[str]
    message: str
    # Primitive-only context to prevent dead loops
    context: Dict[str, Any]
    decision_reason: Optional[str]
    call_stack_depth: int


class SafeContext:
    """
    Lazy evaluation wrapper to prevent dead loops during context building.
    Only evaluates context providers when actually needed for logging.
    """
    
    def __init__(self, **lazy_fields):
        self._lazy_fields = lazy_fields
        self._evaluated = False
        self._safe_dict = {}
    
    def to_safe_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, evaluating lazy fields only when accessed."""
        if not self._evaluated:
            self._evaluate_lazy_fields()
            self._evaluated = True
        return self._safe_dict
    
    def _evaluate_lazy_fields(self):
        """Evaluate lazy fields and convert to safe types."""
        for key, provider in self._lazy_fields.items():
            try:
                if callable(provider):
                    # Evaluate the provider function
                    value = provider()
                else:
                    # Use the value directly (should be primitive)
                    value = provider
                
                # Convert to primitive types only - NO OBJECT INTROSPECTION
                self._safe_dict[key] = self._make_safe(value)
                
            except Exception as e:
                # If context evaluation fails, log the error but don't break
                self._safe_dict[key] = f"CONTEXT_ERROR: {str(e)}"
    
    def _make_safe(self, value: Any) -> Any:
        """Convert value to safe, primitive types only."""
        if value is None:
            return None
        elif isinstance(value, (str, int, float, bool)):
            return value
        elif isinstance(value, (list, tuple)):
            # Recursively make list items safe
            return [self._make_safe(item) for item in value]
        elif isinstance(value, dict):
            # Recursively make dict values safe
            return {str(k): self._make_safe(v) for k, v in value.items()}
        elif hasattr(value, 'value'):
            # Handle enums - use their value
            return value.value
        elif hasattr(value, 'name'):
            # Handle objects with name attribute
            return value.name
        else:
            # Fallback - string representation, but limit length
            str_repr = str(value)
            return str_repr[:100] + "..." if len(str_repr) > 100 else str_repr


class ContextAwareLogger:
    """
    Safe context-aware logger with multiple layers of dead-loop protection.
    """
    
    def __init__(self, max_events_per_second: int = 50, max_recursion_depth: int = 3):
        """Initialize with safety limits."""
        self.session_id = str(uuid.uuid4())[:8]
        self._active_threads = {}
        self._event_counts = {}
        self._last_reset = time.time()
        
        # Safety limits
        self.max_events_per_second = max_events_per_second
        self.max_recursion_depth = max_recursion_depth
        
        # Statistics
        self._stats = {
            'total_events': 0,
            'dropped_events': 0,
            'recursion_blocks': 0,
            'circuit_breaker_blocks': 0
        }
        
        if logger:
            logger.info(f"ContextAwareLogger initialized (session: {self.session_id})")
    
    def log_event(self, 
                  event_type: TradingEventType,
                  message: str,
                  symbol: Optional[str] = None,
                  context_provider: Optional[Dict[str, Any]] = None,
                  decision_reason: Optional[str] = None) -> bool:
        """
        Safely log a trading event with multiple dead-loop protections.
        
        Returns:
            True if event was logged, False if blocked by safety mechanisms
        """
        thread_id = threading.get_ident()
        current_time = time.time()
        
        # Layer 1: Circuit Breaker - Event Rate Limiting
        if not self._check_circuit_breaker(event_type, current_time):
            self._stats['circuit_breaker_blocks'] += 1
            return False
        
        # Layer 2: Recursion Detection
        recursion_depth = self._check_recursion(thread_id)
        if recursion_depth > self.max_recursion_depth:
            self._stats['recursion_blocks'] += 1
            self._cleanup_thread(thread_id)
            return False
        
        try:
            # Layer 3: Safe Context Evaluation (TRULY LAZY - only when needed for logging)
            # Create SafeContext but don't evaluate it yet
            safe_context_wrapper = SafeContext(**(context_provider or {}))
            
            # Create event with the wrapper, not the evaluated dict
            event = TradingEvent(
                event_id=str(uuid.uuid4())[:8],
                event_type=event_type.value,
                timestamp=datetime.datetime.now().isoformat(),
                session_id=self.session_id,
                symbol=symbol,
                message=message,
                context={},  # Empty for now, will be populated during logging
                decision_reason=decision_reason,
                call_stack_depth=recursion_depth
            )
            
            # Convert to dict for logging, evaluating context only at this point
            event_dict = self._prepare_event_for_logging(event, safe_context_wrapper)
            
            # Log to both structured logger and console for visibility
            self._write_structured_log(event_dict)
            
            self._stats['total_events'] += 1
            return True
            
        except Exception as e:
            # If anything goes wrong during event creation, fail safely
            if logger:
                logger.error(f"ContextAwareLogger error: {e}")
            return False
        finally:
            # Always cleanup recursion tracking
            self._cleanup_thread(thread_id)
    
    def _prepare_event_for_logging(self, event: TradingEvent, context_wrapper: SafeContext) -> Dict[str, Any]:
        """Prepare event for logging by evaluating context only when needed."""
        # Convert event to dict first
        event_dict = asdict(event)
        # Now evaluate the context (this is when lazy evaluation happens)
        event_dict['context'] = context_wrapper.to_safe_dict()
        return event_dict
    
    def _check_circuit_breaker(self, event_type: TradingEventType, current_time: float) -> bool:
        """Circuit breaker to prevent event storms."""
        # Reset counters if more than 1 second has passed
        if current_time - self._last_reset > 1.0:
            self._event_counts.clear()
            self._last_reset = current_time
        
        # Count this event
        event_type_str = event_type.value
        self._event_counts[event_type_str] = self._event_counts.get(event_type_str, 0) + 1
        
        # Check total events per second
        total_events = sum(self._event_counts.values())
        if total_events > self.max_events_per_second:
            return False
        
        return True
    
    def _check_recursion(self, thread_id: int) -> int:
        """Check and track recursion depth for current thread."""
        # Get current call stack to detect actual recursion
        current_frame = inspect.currentframe()
        call_frames = []
        
        # Walk up the call stack to find our own method calls
        frame = current_frame
        while frame:
            # Check if this frame is calling our log_event method
            if (frame.f_code == self.log_event.__code__ or 
                (hasattr(frame, 'f_back') and frame.f_back and 
                 frame.f_back.f_code == self.log_event.__code__)):
                call_frames.append(frame)
            frame = frame.f_back
        
        # Count how many times we're in the call stack
        recursion_count = len(call_frames)
        
        # Also track by thread for simple cases
        if thread_id not in self._active_threads:
            self._active_threads[thread_id] = 1
        else:
            self._active_threads[thread_id] += 1
        
        # Use the maximum of both methods
        return max(recursion_count, self._active_threads[thread_id])
    
    def _cleanup_thread(self, thread_id: int):
        """Clean up recursion tracking for thread."""
        if thread_id in self._active_threads:
            if self._active_threads[thread_id] <= 1:
                del self._active_threads[thread_id]
            else:
                self._active_threads[thread_id] -= 1
    
    def _write_structured_log(self, event_dict: Dict[str, Any]):
        """Write structured event to log with clear formatting."""
        # Create a readable log message
        symbol_str = f" [{event_dict['symbol']}]" if event_dict['symbol'] else ""
        reason_str = f" - {event_dict['decision_reason']}" if event_dict['decision_reason'] else ""
        
        log_message = f"🔍 {event_dict['event_type'].upper()}{symbol_str}: {event_dict['message']}{reason_str}"
        
        # Log to console for immediate visibility
        print(log_message)
        
        # Log structured data to file logger
        if logger:
            logger.info(f"STRUCTURED_EVENT: {event_dict}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get logging statistics for monitoring."""
        return self._stats.copy()
    
    def reset_stats(self):
        """Reset statistics counters."""
        self._stats = {
            'total_events': 0,
            'dropped_events': 0,
            'recursion_blocks': 0,
            'circuit_breaker_blocks': 0
        }


# Global logger instance for easy access
_global_logger: Optional[ContextAwareLogger] = None

def get_context_logger() -> ContextAwareLogger:
    """Get or create the global context-aware logger instance."""
    global _global_logger
    if _global_logger is None:
        _global_logger = ContextAwareLogger()
    return _global_logger