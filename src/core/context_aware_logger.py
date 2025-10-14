"""
Safe Context-Aware Logging System for Trading Automation.
Provides structured event logging with dead-loop protection and circuit breakers.
Designed to answer debugging questions about order execution, state discrepancies, and system behavior.
"""

import datetime
import json
import logging
import os
import sys
import threading
import time
from enum import Enum
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass, asdict
import uuid
import inspect

# <Session Management - Begin>
class SessionLogger:
    """Manages session-based logging with single file per trading session."""
    
    _current_session_file: Optional[str] = None
    _session_start_time: Optional[datetime.datetime] = None
    _session_handlers_configured = False
    
    @classmethod
    def start_new_session(cls) -> str:
        """Start a new logging session and return the session file path."""
        # Create logs directory if it doesn't exist
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Generate session filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        session_file = os.path.join(log_dir, f"trading_session_{timestamp}.log")
        
        cls._current_session_file = session_file
        cls._session_start_time = datetime.datetime.now()
        cls._session_handlers_configured = False
        
        print(f"🔄 Starting new trading session: {session_file}")
        return session_file
    
    @classmethod
    def get_current_session_file(cls) -> Optional[str]:
        """Get the current session log file path."""
        return cls._current_session_file
    
    @classmethod
    def end_current_session(cls) -> None:
        """End the current logging session."""
        if cls._current_session_file:
            print(f"✅ Ending trading session: {cls._current_session_file}")
            cls._current_session_file = None
            cls._session_start_time = None
            cls._session_handlers_configured = False
    
    @classmethod
    def ensure_session_started(cls) -> str:
        """Ensure a session is started and return the session file."""
        if not cls._current_session_file:
            return cls.start_new_session()
        return cls._current_session_file
    
    @classmethod
    def configure_session_handlers(cls, logger: logging.Logger) -> None:
        """Configure logging handlers for the current session."""
        if cls._session_handlers_configured:
            return
            
        session_file = cls.ensure_session_started()
        
        # Remove any existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        simple_formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
        
        # File handler (detailed, session-based)
        file_handler = logging.FileHandler(session_file, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        
        # Console handler (simple)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        
        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.setLevel(logging.DEBUG)
        
        # Prevent propagation to avoid duplicate logs
        logger.propagate = False
        
        cls._session_handlers_configured = True
# <Session Management - End>

class LogImportance(Enum):
    """3-level importance system for log filtering."""
    HIGH = 1      # Errors, order executions, critical state changes
    MEDIUM = 2    # Validations, calculations, major decisions  
    LOW = 3       # System health, progress updates, routine checks

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

# ADD THIS NEW SECTION
# ====================
# Field compression mapping
FIELD_COMPRESSION_MAP = {
    'timestamp': 'ts',
    'event_type': 'et', 
    'symbol': 's',
    'message': 'm',
    'decision_reason': 'r',
    'context': 'c',
    'event_id': 'id',
    'session_id': 'sid',
    'call_stack_depth': 'dep'
}

# Event type numeric coding (0-255)
EVENT_TYPE_CODES = {
    TradingEventType.ORDER_VALIDATION: 1,
    TradingEventType.EXECUTION_DECISION: 2, 
    TradingEventType.MARKET_CONDITION: 3,
    TradingEventType.POSITION_MANAGEMENT: 4,
    TradingEventType.STATE_TRANSITION: 5,
    TradingEventType.RISK_EVALUATION: 6,
    TradingEventType.SYSTEM_HEALTH: 7,
    TradingEventType.DATABASE_STATE: 8,
}

# Context field compression mapping
CONTEXT_FIELD_MAP = {
    'price': 'p', 'quantity': 'q', 'order_id': 'oid', 'symbol': 's',
    'action': 'a', 'filled': 'f', 'remaining': 'rem', 'status': 'st',
    'error': 'e', 'result': 'res', 'count': 'cnt', 'risk': 'r',
    'profit': 'pnl', 'loss': 'l', 'amount': 'amt', 'entry': 'ent',
    'stop': 'stp', 'target': 'tgt', 'capital': 'cap', 'margin': 'mgn'
}
# ====================

@dataclass
class TradingEvent:
    """Structured event data with safety guarantees."""
    event_id: str
    event_type: str
    timestamp: str
    session_id: str
    symbol: Optional[str]
    message: str
    context: Dict[str, Any]
    decision_reason: Optional[str]
    call_stack_depth: int

class SafeContext:
    """
    Lazy evaluation wrapper with moderate compression for key trading fields.
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
        """Evaluate lazy fields and apply moderate compression."""
        for key, provider in self._lazy_fields.items():
            try:
                if callable(provider):
                    value = provider()
                else:
                    value = provider
                self._safe_dict[key] = self._compress_value(key, value)
            except Exception as e:
                self._safe_dict[key] = f"CONTEXT_ERROR: {str(e)}"
    
    def _compress_value(self, key: str, value: Any) -> Any:
        """Apply moderate compression rules based on field type."""
        if value is None:
            return None
            
        # Compress lists - keep only first few items for large lists
        if isinstance(value, (list, tuple)) and len(value) > 5:
            compressed = list(value)[:3]
            return compressed + [f"...+{len(value) - 3} more"]
        
        # Compress very large dictionaries but keep most trading fields
        if isinstance(value, dict) and len(value) > 10:
            compressed = {}
            important_keys = [k for k in value.keys() if self._is_important_key(k)]
            # Keep all important keys, limit others
            for k in important_keys:
                compressed[k] = self._make_safe(value[k])
            other_keys = [k for k in value.keys() if k not in important_keys][:3]
            for k in other_keys:
                compressed[k] = self._make_safe(value[k])
            if len(value) > len(compressed):
                compressed['_other'] = f"{len(value) - len(compressed)} more fields"
            return compressed
            
        # Default safe conversion
        return self._make_safe(value)
    
    def _is_important_key(self, key: str) -> bool:
        """Identify which context keys are important trading fields to keep."""
        important_patterns = [
            'error', 'exception', 'status', 'result', 'count', 
            'price', 'quantity', 'risk', 'profit', 'loss', 'amount',
            'order_id', 'symbol', 'action', 'filled', 'remaining',
            'entry', 'stop', 'target', 'capital', 'margin', 'pnl'
        ]
        key_str = str(key).lower()
        return any(pattern in key_str for pattern in important_patterns)
    
    def _make_safe(self, value: Any) -> Any:
        """Convert value to safe, primitive types only."""
        if value is None:
            return None
        elif isinstance(value, (str, int, float, bool)):
            return value
        elif isinstance(value, (list, tuple)):
            return [self._make_safe(item) for item in value]
        elif isinstance(value, dict):
            return {str(k): self._make_safe(v) for k, v in value.items()}
        elif hasattr(value, 'value'):
            return value.value
        elif hasattr(value, 'name'):
            return value.name
        else:
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
        
        # Importance filtering - default to MEDIUM (filters out LOW importance)
        self.min_importance = LogImportance.MEDIUM
        
        # Statistics
        self._stats = {
            'total_events': 0,
            'dropped_events': 0,
            'recursion_blocks': 0,
            'circuit_breaker_blocks': 0,
            'importance_filtered': 0
        }
        
        # Initialize direct file logging
        self._file_logger = logging.getLogger(f"context_aware_{self.session_id}")
        SessionLogger.configure_session_handlers(self._file_logger)
        self._file_logger.info(f"ContextAwareLogger initialized (session: {self.session_id})")
    
    def log_event(self, 
                  event_type: TradingEventType,
                  message: str,
                  symbol: Optional[str] = None,
                  context_provider: Optional[Dict[str, Any]] = None,
                  decision_reason: Optional[str] = None) -> bool:
        """
        Safely log a trading event with multiple dead-loop protections.
        """
        thread_id = threading.get_ident()
        current_time = time.time()
        
        # Layer 1: Importance Filtering - Skip low importance events
        importance = self._determine_importance(event_type, message, context_provider, decision_reason)
        if importance.value > self.min_importance.value:
            self._stats['importance_filtered'] += 1
            return False
        
        # Layer 2: Circuit Breaker - Event Rate Limiting
        if not self._check_circuit_breaker(event_type, current_time):
            self._stats['circuit_breaker_blocks'] += 1
            return False
        
        # Layer 3: Recursion Detection
        recursion_depth = self._check_recursion(thread_id)
        if recursion_depth > self.max_recursion_depth:
            self._stats['recursion_blocks'] += 1
            self._cleanup_thread(thread_id)
            return False
        
        try:
            # Layer 4: Safe Context Evaluation with Compression
            safe_context_wrapper = SafeContext(**(context_provider or {}))
            
            event = TradingEvent(
                event_id=str(uuid.uuid4())[:8],
                event_type=event_type.value,
                timestamp=datetime.datetime.now().isoformat(),
                session_id=self.session_id,
                symbol=symbol,
                message=message,
                context={},
                decision_reason=decision_reason,
                call_stack_depth=recursion_depth
            )
            
            event_dict = self._prepare_event_for_logging(event, safe_context_wrapper)
            self._write_compressed_log(event_dict, importance)
            
            self._stats['total_events'] += 1
            return True
            
        except Exception as e:
            # Use print as fallback if logger isn't available
            print(f"ContextAwareLogger error: {e}")
            return False
        finally:
            self._cleanup_thread(thread_id)
    
    def _determine_importance(self, 
                            event_type: TradingEventType, 
                            message: str, 
                            context_provider: Optional[Dict[str, Any]],
                            decision_reason: Optional[str]) -> LogImportance:
        """Automatically determine importance based on event type and content."""
        message_lower = message.lower()
        
        # High importance: errors, failures, critical operations
        if any(word in message_lower for word in ['error', 'failed', 'exception', 'critical', 'rejected']):
            return LogImportance.HIGH
        
        # Event type based importance
        event_importance = {
            TradingEventType.SYSTEM_HEALTH: LogImportance.LOW,
            TradingEventType.ORDER_VALIDATION: LogImportance.MEDIUM,
            TradingEventType.EXECUTION_DECISION: LogImportance.HIGH,
            TradingEventType.MARKET_CONDITION: LogImportance.LOW,
            TradingEventType.POSITION_MANAGEMENT: LogImportance.HIGH,
            TradingEventType.STATE_TRANSITION: LogImportance.HIGH,
            TradingEventType.RISK_EVALUATION: LogImportance.MEDIUM,
            TradingEventType.DATABASE_STATE: LogImportance.LOW,
        }
        
        base_importance = event_importance.get(event_type, LogImportance.MEDIUM)
        
        # Upgrade importance for critical context or decision reasons
        if decision_reason and any(word in decision_reason.lower() for word in 
                                 ['execut', 'fill', 'reject', 'error', 'risk']):
            return LogImportance.HIGH
            
        # Check context for high importance indicators
        if context_provider:
            context_str = str(context_provider).lower()
            if any(word in context_str for word in 
                  ['error', 'exception', 'reject', 'fill', 'execute']):
                return LogImportance.HIGH
        
        return base_importance
    
    def _prepare_event_for_logging(self, event: TradingEvent, context_wrapper: SafeContext) -> Dict[str, Any]:
        """Prepare event for logging by evaluating context only when needed."""
        event_dict = asdict(event)
        event_dict['context'] = context_wrapper.to_safe_dict()
        return event_dict
    
    def _write_compressed_log(self, event_dict: Dict[str, Any], importance: LogImportance):
        """Write highly compressed structured event to log."""
        # Convert ISO timestamp to numeric (major space savings)
        try:
            timestamp = datetime.datetime.fromisoformat(event_dict['timestamp']).timestamp()
            timestamp = round(timestamp, 3)  # Millisecond precision
        except:
            timestamp = time.time()
        
        # Get event type code
        event_type_code = 0
        try:
            event_type = TradingEventType(event_dict['event_type'])
            event_type_code = EVENT_TYPE_CODES.get(event_type, 0)
        except:
            pass
        
        # Build compressed core info
        core_info = {
            FIELD_COMPRESSION_MAP['timestamp']: timestamp,
            FIELD_COMPRESSION_MAP['event_type']: event_type_code,
            'i': importance.value,
            FIELD_COMPRESSION_MAP['message']: self._compress_message(event_dict['message']),
        }
        
        # Add symbol if present
        if event_dict['symbol']:
            core_info[FIELD_COMPRESSION_MAP['symbol']] = event_dict['symbol']
        
        # Add compressed reason if present
        if event_dict['decision_reason']:
            reason = event_dict['decision_reason']
            compressed_reason = self._compress_reason(reason)
            core_info[FIELD_COMPRESSION_MAP['decision_reason']] = compressed_reason
        
        # Ultra-compact context with field pruning and compression
        critical_context = self._extract_critical_context(event_dict['context'])
        if critical_context:
            compressed_ctx = self._compress_context_fields(critical_context)
            core_info[FIELD_COMPRESSION_MAP['context']] = compressed_ctx
        
        # Minimal JSON with shortest separators
        compact_json = json.dumps(core_info, separators=(',', ':'))
        
        # Console output remains human readable (unchanged)
        symbol_str = f" [{event_dict['symbol']}]" if event_dict['symbol'] else ""
        reason_str = f" - {event_dict['decision_reason']}" if event_dict['decision_reason'] else ""
        console_message = f"🔍 {event_dict['event_type'].upper()}{symbol_str}: {event_dict['message']}{reason_str}"
        print(console_message)
        
        # File logging with compressed format
        try:
            SessionLogger.configure_session_handlers(self._file_logger)
            self._file_logger.info(f"E:{compact_json}")
        except Exception as e:
            print(f"📊 {compact_json}")
            print(f"File logging error: {e}")
    
    def _compress_message(self, message: str) -> str:
        """More aggressive message compression for common trading patterns."""
        # Common phrase replacements
        phrase_replacements = {
            'completed successfully': '✓',
            'starting': '→',
            'failed': '✗', 
            'validation': 'val',
            'initialization': 'init',
            'calculation': 'calc',
            'successfully': 'ok',
            'received': '←',
            'processing': 'proc',
            'reconciliation': 'recon',
            'evaluation': 'eval',
            'automatic': 'auto',
            'manual': 'man',
            'configuration': 'config',
            'execution': 'exec',
            'transaction': 'tx',
            'position': 'pos',
            'portfolio': 'port',
            'market': 'mkt',
            'order': 'ord',
            'signal': 'sig',
            'strategy': 'strat',
            'algorithm': 'algo',
        }
        
        # Word boundary replacements for better compression
        compressed = message.lower()
        for full, short in phrase_replacements.items():
            compressed = compressed.replace(full, short)
        
        # Remove common filler words
        filler_words = ['the', 'a', 'an', 'for', 'with', 'from', 'this', 'that']
        words = compressed.split()
        filtered_words = [w for w in words if w not in filler_words]
        compressed = ' '.join(filtered_words)
        
        return compressed[:50]
    
    def _compress_reason(self, reason: str) -> str:
        """Specialized compression for decision reasons."""
        reason_compressions = {
            'risk limit exceeded': 'risk_lim',
            'market conditions not favorable': 'mkt_cond',
            'insufficient capital': 'no_cap', 
            'position limit reached': 'pos_lim',
            'validation passed': 'val_ok',
            'validation failed': 'val_fail',
            'price outside acceptable range': 'price_range',
            'quantity too small': 'qty_small',
            'quantity too large': 'qty_large',
            'time in force expired': 'tif_exp',
            'exchange closed': 'exch_closed',
            'connection lost': 'conn_lost',
            'data stale': 'data_old',
        }
        
        compressed = reason.lower()
        for full, short in reason_compressions.items():
            if full in compressed:
                return short
        
        # Fallback: first 3 letters of each word
        words = compressed.split()
        if len(words) > 2:
            return ''.join([w[:3] for w in words[:3]])
        
        return compressed[:30]
    
    def _compress_context_fields(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Compress context field names and values."""
        compressed = {}
        for key, value in context.items():
            short_key = CONTEXT_FIELD_MAP.get(key, key[:3])
            
            # Compress numeric values (round floats, keep ints)
            if isinstance(value, float):
                compressed[short_key] = round(value, 4)
            elif isinstance(value, (int, str, bool)) or value is None:
                compressed[short_key] = value
            else:
                # Convert other types to string with length limit
                str_val = str(value)
                compressed[short_key] = str_val[:40] + '...' if len(str_val) > 40 else str_val
        
        return compressed
    
    def _extract_critical_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract only the most important context fields for compression."""
        critical_fields = {
            'error', 'exception', 'status', 'result', 'count',
            'price', 'quantity', 'risk', 'profit', 'loss', 'amount',
            'order_id', 'symbol', 'action', 'filled', 'remaining',
            'entry', 'stop', 'target', 'capital', 'margin', 'pnl'
        }
        
        # Also include any field with 'error' in the name
        error_fields = [k for k in context.keys() if 'error' in str(k).lower()]
        
        important_keys = set(critical_fields) | set(error_fields)
        return {k: v for k, v in context.items() 
                if k in important_keys and v is not None}
    
    def _check_circuit_breaker(self, event_type: TradingEventType, current_time: float) -> bool:
        """Circuit breaker to prevent event storms."""
        if current_time - self._last_reset > 1.0:
            self._event_counts.clear()
            self._last_reset = current_time
        
        event_type_str = event_type.value
        self._event_counts[event_type_str] = self._event_counts.get(event_type_str, 0) + 1
        
        total_events = sum(self._event_counts.values())
        if total_events > self.max_events_per_second:
            return False
        
        return True
    
    def _check_recursion(self, thread_id: int) -> int:
        """Check and track recursion depth for current thread."""
        current_frame = inspect.currentframe()
        call_frames = []
        
        frame = current_frame
        while frame:
            if (frame.f_code == self.log_event.__code__ or 
                (hasattr(frame, 'f_back') and frame.f_back and 
                 frame.f_back.f_code == self.log_event.__code__)):
                call_frames.append(frame)
            frame = frame.f_back
        
        recursion_count = len(call_frames)
        
        if thread_id not in self._active_threads:
            self._active_threads[thread_id] = 1
        else:
            self._active_threads[thread_id] += 1
        
        return max(recursion_count, self._active_threads[thread_id])
    
    def _cleanup_thread(self, thread_id: int):
        """Clean up recursion tracking for thread."""
        if thread_id in self._active_threads:
            if self._active_threads[thread_id] <= 1:
                del self._active_threads[thread_id]
            else:
                self._active_threads[thread_id] -= 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get logging statistics for monitoring."""
        return self._stats.copy()
    
    def reset_stats(self):
        """Reset statistics counters."""
        self._stats = {
            'total_events': 0,
            'dropped_events': 0,
            'recursion_blocks': 0,
            'circuit_breaker_blocks': 0,
            'importance_filtered': 0
        }# Global logger instance for easy access

_global_logger: Optional[ContextAwareLogger] = None

def get_context_logger() -> ContextAwareLogger:
    """Get or create the global context-aware logger instance."""
    global _global_logger
    if _global_logger is None:
        _global_logger = ContextAwareLogger()
    return _global_logger

# <Session Management Public API - Begin>
def start_trading_session() -> str:
    """Explicitly start a new trading session. Call this at application startup."""
    return SessionLogger.start_new_session()

# MODIFY the end_trading_session function to include compression stats

def end_trading_session() -> None:
    """Explicitly end the current trading session with compression stats."""
    global _global_logger
    session_file = SessionLogger.get_current_session_file()
    
    if _global_logger and session_file and os.path.exists(session_file):
        stats = _global_logger.get_stats()
        file_size = os.path.getsize(session_file)
        
        # Estimate original size (assuming 180 bytes per event uncompressed)
        estimated_original = stats['total_events'] * 180
        compression_ratio = estimated_original / file_size if file_size > 0 else 1
        
        # Log session summary
        logger = get_context_logger()
        logger._file_logger.info(
            f"SESSION_SUMMARY: events={stats['total_events']}, "
            f"size_kb={file_size/1024:.1f}, "
            f"compression={compression_ratio:.1f}x, "
            f"filtered={stats['importance_filtered']}+{stats['circuit_breaker_blocks']}"
        )
    
    SessionLogger.end_current_session()

def get_current_session_file() -> Optional[str]:
    """Get the path to the current session's log file."""
    return SessionLogger.get_current_session_file()
# <Session Management Public API - End>