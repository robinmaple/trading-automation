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

# Field compression mapping - Begin (UPDATED)
FIELD_COMPRESSION_MAP = {
    'timestamp': 'ts',
    'event_type': 'et', 
    'symbol': 's',
    'message': 'm',
    'decision_reason': 'r',
    'context': 'c',
    'event_id': 'id',
    'session_id': 'sid',
    'call_stack_depth': 'dep',
    'importance': 'i'
}

# Event type numeric coding (0-255) - Begin (UPDATED)
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

# Context field compression mapping - Begin (UPDATED)
CONTEXT_FIELD_MAP = {
    'price': 'p', 'quantity': 'q', 'order_id': 'oid', 'symbol': 's',
    'action': 'a', 'filled': 'f', 'remaining': 'rem', 'status': 'st',
    'error': 'e', 'result': 'res', 'count': 'cnt', 'risk': 'r',
    'profit': 'pnl', 'loss': 'l', 'amount': 'amt', 'entry': 'ent',
    'stop': 'stp', 'target': 'tgt', 'capital': 'cap', 'margin': 'mgn',
    'probability': 'prob', 'volatility': 'vol', 'threshold': 'thr',
    'difference': 'diff', 'percent': 'pct', 'available': 'avail',
    'connected': 'conn', 'health': 'hlth', 'success': 'succ',
    'failure': 'fail', 'calculation': 'calc', 'validation': 'val'
}

# Insight patterns for better content selection - Begin (NEW)
INSIGHT_PATTERNS = {
    'critical_actions': ['execut', 'fill', 'reject', 'error', 'fail', 'risk_limit'],
    'important_changes': ['status_change', 'transition', 'position', 'capital'],
    'routine_checks': ['health', 'validation', 'calculation', 'checking', 'starting']
}
# Insight patterns - End

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

# SafeContext class - Begin (UPDATED)
class SafeContext:
    """
    Lazy evaluation wrapper with insight-focused compression.
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
        """Evaluate lazy fields and apply insight-focused compression."""
        for key, provider in self._lazy_fields.items():
            try:
                if callable(provider):
                    value = provider()
                else:
                    value = provider
                self._safe_dict[key] = self._compress_value(key, value)
            except Exception as e:
                self._safe_dict[key] = f"CTX_ERR:{str(e)[:20]}"
    
    def _compress_value(self, key: str, value: Any) -> Any:
        """Apply insight-focused compression rules."""
        if value is None:
            return None
            
        # For lists/tuples - keep only insights, not full data
        if isinstance(value, (list, tuple)):
            return self._compress_list(key, value)
        
        # For dictionaries - focus on insights, prune routine data
        if isinstance(value, dict):
            return self._compress_dict(key, value)
            
        # Default safe conversion with insight focus
        return self._make_safe(value)
    
    def _compress_list(self, key: str, value: list) -> list:
        """Compress lists to show only meaningful insights."""
        if len(value) <= 3:
            return [self._make_safe(item) for item in value]
        
        # For large lists, show only summary if it's routine data
        if any(pattern in key.lower() for pattern in ['history', 'log', 'trace']):
            return [f"items:{len(value)}"]
        
        # For important lists, show first 2 + count
        return [self._make_safe(value[0]), self._make_safe(value[1]), f"+{len(value)-2}"]
    
    def _compress_dict(self, key: str, value: dict) -> dict:
        """Compress dictionaries to focus on insights."""
        if len(value) <= 5:
            return {k: self._make_safe(v) for k, v in value.items()}
        
        # Extract only insight-rich fields
        compressed = {}
        insight_fields = self._get_insight_fields(value)
        
        for field in insight_fields:
            if field in value:
                compressed[field] = self._make_safe(value[field])
        
        # Add count if we filtered significantly
        if len(compressed) < len(value):
            compressed['_filtered'] = f"{len(compressed)}/{len(value)}"
            
        return compressed
    
    def _get_insight_fields(self, context: Dict[str, Any]) -> list:
        """Identify fields that provide meaningful insights."""
        insight_fields = []
        
        for field in context.keys():
            field_lower = str(field).lower()
            
            # Always include error/exception fields
            if any(err in field_lower for err in ['error', 'exception', 'fail']):
                insight_fields.append(field)
                continue
                
            # Include critical trading fields
            if any(trade in field_lower for trade in 
                  ['price', 'quantity', 'risk', 'profit', 'loss', 'capital', 'margin']):
                insight_fields.append(field)
                continue
                
            # Include decision-making fields
            if any(decision in field_lower for decision in
                  ['result', 'status', 'action', 'decision', 'threshold']):
                insight_fields.append(field)
                continue
                
            # Include state change indicators
            if any(state in field_lower for state in
                  ['change', 'transition', 'update', 'new', 'old']):
                insight_fields.append(field)
        
        # Limit to top 8 most important fields
        return insight_fields[:8]
    
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
            # Shorter representation for non-critical values
            return str_repr[:50] + "..." if len(str_repr) > 50 else str_repr
# SafeContext class - End

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
    
    # ContextAwareLogger._determine_importance - Begin (UPDATED)
    def _determine_importance(self, 
                            event_type: TradingEventType, 
                            message: str, 
                            context_provider: Optional[Dict[str, Any]],
                            decision_reason: Optional[str]) -> LogImportance:
        """Intelligently determine importance with insight-based filtering."""
        message_lower = message.lower()
        
        # HIGH importance: Critical actions and errors
        if any(pattern in message_lower for pattern in 
               INSIGHT_PATTERNS['critical_actions']):
            return LogImportance.HIGH
        
        # MEDIUM importance: Important state changes
        if any(pattern in message_lower for pattern in 
               INSIGHT_PATTERNS['important_changes']):
            return LogImportance.MEDIUM
            
        # LOW importance: Routine checks and health updates
        if any(pattern in message_lower for pattern in 
               INSIGHT_PATTERNS['routine_checks']):
            return LogImportance.LOW
        
        # Event type based importance with smarter defaults
        event_importance = {
            TradingEventType.SYSTEM_HEALTH: LogImportance.LOW,      # Reduced frequency
            TradingEventType.ORDER_VALIDATION: LogImportance.MEDIUM,
            TradingEventType.EXECUTION_DECISION: LogImportance.HIGH,
            TradingEventType.MARKET_CONDITION: LogImportance.LOW,   # Reduced frequency
            TradingEventType.POSITION_MANAGEMENT: LogImportance.HIGH,
            TradingEventType.STATE_TRANSITION: LogImportance.HIGH,
            TradingEventType.RISK_EVALUATION: LogImportance.MEDIUM,
            TradingEventType.DATABASE_STATE: LogImportance.LOW,     # Reduced frequency
        }
        
        base_importance = event_importance.get(event_type, LogImportance.MEDIUM)
        
        # Upgrade importance based on decision reason insights
        if decision_reason:
            reason_lower = decision_reason.lower()
            if any(pattern in reason_lower for pattern in 
                   INSIGHT_PATTERNS['critical_actions']):
                return LogImportance.HIGH
            elif any(pattern in reason_lower for pattern in 
                     INSIGHT_PATTERNS['important_changes']):
                return LogImportance.MEDIUM
        
        return base_importance
    # ContextAwareLogger._determine_importance - End

    def _prepare_event_for_logging(self, event: TradingEvent, context_wrapper: SafeContext) -> Dict[str, Any]:
        """Prepare event for logging by evaluating context only when needed."""
        event_dict = asdict(event)
        event_dict['context'] = context_wrapper.to_safe_dict()
        return event_dict
    
    # ContextAwareLogger._write_compressed_log - Begin (UPDATED)
    def _write_compressed_log(self, event_dict: Dict[str, Any], importance: LogImportance):
        """Write insight-focused compressed log with better content selection."""
        # Convert timestamp to numeric for major space savings
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
        
        # Build ultra-compact core info with insight focus
        core_info = {
            FIELD_COMPRESSION_MAP['timestamp']: timestamp,
            FIELD_COMPRESSION_MAP['event_type']: event_type_code,
            FIELD_COMPRESSION_MAP['importance']: importance.value,
            FIELD_COMPRESSION_MAP['message']: self._compress_message(event_dict['message']),
        }
        
        # Add symbol only if it provides context (not for system-wide events)
        if event_dict['symbol'] and event_dict['symbol'] not in ['', 'SYSTEM']:
            core_info[FIELD_COMPRESSION_MAP['symbol']] = event_dict['symbol']
        
        # Add compressed reason only if it provides meaningful insight
        if event_dict['decision_reason'] and self._is_insightful_reason(event_dict['decision_reason']):
            reason = event_dict['decision_reason']
            compressed_reason = self._compress_reason(reason)
            core_info[FIELD_COMPRESSION_MAP['decision_reason']] = compressed_reason
        
        # Extract and compress only insightful context
        insightful_context = self._extract_insightful_context(event_dict['context'], importance)
        if insightful_context:
            compressed_ctx = self._compress_context_fields(insightful_context)
            core_info[FIELD_COMPRESSION_MAP['context']] = compressed_ctx
        
        # Minimal JSON with shortest separators
        compact_json = json.dumps(core_info, separators=(',', ':'))
        
        # Enhanced console output with better insights
        self._write_insightful_console_output(event_dict, importance, core_info)
        
        # File logging with compressed format
        try:
            SessionLogger.configure_session_handlers(self._file_logger)
            self._file_logger.info(f"E:{compact_json}")
        except Exception as e:
            print(f"📊 {compact_json}")
            print(f"File logging error: {e}")
    
    def _is_insightful_reason(self, reason: str) -> bool:
        """Check if reason provides meaningful insight vs routine explanation."""
        routine_patterns = ['completed', 'starting', 'processing', 'checking', 'validating']
        reason_lower = reason.lower()
        return not any(pattern in reason_lower for pattern in routine_patterns)
    
    def _write_insightful_console_output(self, event_dict: Dict[str, Any], 
                                       importance: LogImportance, core_info: Dict[str, Any]):
        """Write enhanced console output that provides better insights than logs."""
        symbol_str = f" [{event_dict['symbol']}]" if event_dict['symbol'] else ""
        
        # Use different emojis based on importance and content
        if importance == LogImportance.HIGH:
            emoji = "🚨" if any(word in event_dict['message'].lower() for word in ['error', 'fail']) else "⚡"
        elif importance == LogImportance.MEDIUM:
            emoji = "🔍" 
        else:
            emoji = "📝"
        
        # Enhanced message with key insights from context
        enhanced_message = event_dict['message']
        context_insights = self._extract_console_insights(event_dict['context'])
        if context_insights:
            enhanced_message = f"{event_dict['message']} | {context_insights}"
        
        reason_str = f" - {event_dict['decision_reason']}" if event_dict['decision_reason'] else ""
        console_message = f"{emoji} {event_dict['event_type'].upper()}{symbol_str}: {enhanced_message}{reason_str}"
        print(console_message)
    
    def _extract_console_insights(self, context: Dict[str, Any]) -> str:
        """Extract key insights for console display."""
        insights = []
        
        # Look for critical numeric values
        numeric_fields = ['price', 'quantity', 'probability', 'risk', 'capital']
        for field in numeric_fields:
            if field in context and context[field] is not None:
                value = context[field]
                if isinstance(value, (int, float)):
                    insights.append(f"{field}:{value}")
        
        # Look for critical status changes
        status_fields = ['status', 'result', 'action']
        for field in status_fields:
            if field in context and context[field] is not None:
                value = str(context[field])
                if len(value) < 20:  # Only short status values
                    insights.append(f"{field}:{value}")
        
        return " | ".join(insights[:3])  # Max 3 insights
    # ContextAwareLogger._write_compressed_log - End

    # ContextAwareLogger._compress_message - Begin (UPDATED)
    def _compress_message(self, message: str) -> str:
        """Smart message compression that preserves insights."""
        # Don't over-compress messages that contain insights
        if any(insight in message.lower() for insight in 
               ['error', 'fail', 'reject', 'execute', 'fill', 'risk']):
            return message[:60]  # Keep more of insightful messages
        
        # Enhanced phrase replacements for common trading patterns
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
            'checking': 'chk',
            'verifying': 'vfy',
            'monitoring': 'mon',
        }
        
        # Apply replacements
        compressed = message.lower()
        for full, short in phrase_replacements.items():
            compressed = compressed.replace(full, short)
        
        # Remove common filler words but preserve numeric and key terms
        filler_words = ['the', 'a', 'an', 'for', 'with', 'from', 'this', 'that', 'and', 'or']
        words = compressed.split()
        filtered_words = []
        
        for word in words:
            # Keep words with numbers or special characters
            if any(char.isdigit() for char in word) or any(char in word for char in ['$', '%', '.']):
                filtered_words.append(word)
            elif word not in filler_words:
                filtered_words.append(word)
                
        compressed = ' '.join(filtered_words)
        
        return compressed[:40]  # Consistent length limit
    # ContextAwareLogger._compress_message - End

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

    # ContextAwareLogger._extract_critical_context - Begin (UPDATED)
    def _extract_insightful_context(self, context: Dict[str, Any], importance: LogImportance) -> Dict[str, Any]:
        """Extract only context that provides meaningful insights."""
        if importance == LogImportance.LOW:
            # For low importance, keep only critical errors/status
            return {k: v for k, v in context.items() 
                    if any(err in str(k).lower() for err in ['error', 'exception', 'status'])}
        
        # For medium/high importance, use insight-based selection
        insightful_fields = set()
        
        # Always include error/exception fields
        error_fields = [k for k in context.keys() if any(err in str(k).lower() 
                       for err in ['error', 'exception', 'fail'])]
        insightful_fields.update(error_fields)
        
        # Include trading-critical fields
        trading_fields = [k for k in context.keys() if any(trade in str(k).lower()
                         for trade in ['price', 'quantity', 'risk', 'capital', 'margin', 'profit', 'loss'])]
        insightful_fields.update(trading_fields)
        
        # Include decision-making fields
        decision_fields = [k for k in context.keys() if any(dec in str(k).lower()
                          for dec in ['result', 'status', 'action', 'decision', 'threshold', 'probability'])]
        insightful_fields.update(decision_fields)
        
        # Include state change indicators
        state_fields = [k for k in context.keys() if any(state in str(k).lower()
                       for state in ['change', 'transition', 'update', 'new', 'difference'])]
        insightful_fields.update(state_fields)
        
        # Limit fields based on importance
        if importance == LogImportance.MEDIUM:
            insightful_fields = list(insightful_fields)[:6]  # Max 6 fields for medium
        else:  # HIGH importance
            insightful_fields = list(insightful_fields)[:10]  # Max 10 fields for high
        
        return {k: v for k, v in context.items() if k in insightful_fields and v is not None}
    # ContextAwareLogger._extract_critical_context - End

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