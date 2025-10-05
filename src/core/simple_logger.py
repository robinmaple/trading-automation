# src/core/simple_logger.py
"""
Minimal, safe logging that won't cause infinite loops or break tests.
Enhanced with session-based file logging for holistic trading session views.
"""
import logging
import sys
import os
from datetime import datetime
from typing import Any, Callable, Optional

def in_test_mode() -> bool:
    """Check if we're running in pytest"""
    return 'pytest' in sys.modules or any('pytest' in arg for arg in sys.argv)

class SessionLogger:
    """Manages session-based logging with single file per trading session."""
    
    _current_session_file: Optional[str] = None
    _session_start_time: Optional[datetime] = None
    _session_handlers_configured = False
    
    @classmethod
    def start_new_session(cls) -> str:
        """Start a new logging session and return the session file path."""
        # Create logs directory if it doesn't exist
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Generate session filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        session_file = os.path.join(log_dir, f"trading_session_{timestamp}.log")
        
        cls._current_session_file = session_file
        cls._session_start_time = datetime.now()
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

class SimpleLogger:
    """Logger that uses session-based file logging for holistic session views."""
    
    def __init__(self, name: str):
        self.name = name
        self._logger = None
        
        # Only create real logger outside of tests
        if not in_test_mode():
            self._logger = logging.getLogger(name)
            SessionLogger.configure_session_handlers(self._logger)
    
    def info(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            # Ensure session is configured before logging
            SessionLogger.configure_session_handlers(self._logger)
            self._logger.info(msg, *args, **kwargs)
    
    def debug(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            SessionLogger.configure_session_handlers(self._logger)
            self._logger.debug(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            SessionLogger.configure_session_handlers(self._logger)
            self._logger.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            SessionLogger.configure_session_handlers(self._logger)
            self._logger.error(msg, *args, **kwargs)
    
    def exception(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            SessionLogger.configure_session_handlers(self._logger)
            self._logger.exception(msg, *args, **kwargs)

def get_simple_logger(name: str) -> SimpleLogger:
    return SimpleLogger(name)

def start_trading_session() -> str:
    """Explicitly start a new trading session. Call this at application startup."""
    return SessionLogger.start_new_session()

def end_trading_session() -> None:
    """Explicitly end the current trading session. Call this at application shutdown."""
    SessionLogger.end_current_session()

def get_current_session_file() -> Optional[str]:
    """Get the path to the current session's log file."""
    return SessionLogger.get_current_session_file()