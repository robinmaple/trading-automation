# src/core/simple_logger.py
"""
Minimal, safe logging that won't cause infinite loops or break tests.
Enhanced with file-based logging for production tracing.
"""
import logging
import sys
import os
from datetime import datetime
from typing import Any, Callable

def in_test_mode() -> bool:
    """Check if we're running in pytest"""
    return 'pytest' in sys.modules or any('pytest' in arg for arg in sys.argv)

class SimpleLogger:
    """Logger that is completely inert during tests, file-based in production"""
    
    def __init__(self, name: str):
        self.name = name
        self._logger = None
        
        # Only create real logger outside of tests
        if not in_test_mode():
            self._logger = logging.getLogger(name)
            if not self._logger.handlers:
                self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Setup file and console logging with proper formatting"""
        try:
            # Create logs directory if it doesn't exist
            log_dir = 'logs'
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            # FIX: Generate log filename with correct timestamp format (YYYYMMDD_HHMMSS.log)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(log_dir, f"{timestamp}.log")
            
            # Set log level
            self._logger.setLevel(logging.DEBUG)
            
            # Create formatters
            detailed_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            simple_formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
            
            # File handler (detailed)
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(detailed_formatter)
            
            # Console handler (simple)
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(simple_formatter)
            
            # Add handlers
            self._logger.addHandler(file_handler)
            self._logger.addHandler(console_handler)
            
            # Prevent propagation to root logger to avoid duplicate logs
            self._logger.propagate = False
            
            print(f"✅ Log file created: {log_file}")
            
        except Exception as e:
            # Fallback to basic console logging if file setup fails
            print(f"❌ File logging setup failed: {e}, falling back to console")
            logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(name)s - %(message)s')
            self._logger = logging.getLogger(self.name)
    
    def info(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            self._logger.info(msg, *args, **kwargs)
    
    def debug(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            self._logger.debug(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            self._logger.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            self._logger.error(msg, *args, **kwargs)
    
    def exception(self, msg: str, *args, **kwargs) -> None:
        if self._logger:
            self._logger.exception(msg, *args, **kwargs)

def get_simple_logger(name: str) -> SimpleLogger:
    return SimpleLogger(name)