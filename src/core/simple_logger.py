# src/core/simple_logger.py
"""
Minimal, safe logging that won't cause infinite loops or break tests.
"""
import logging
import sys
from typing import Any, Callable

def in_test_mode() -> bool:
    """Check if we're running in pytest"""
    return 'pytest' in sys.modules or any('pytest' in arg for arg in sys.argv)

class SimpleLogger:
    """Logger that is completely inert during tests"""
    
    def __init__(self, name: str):
        self.name = name
        self._logger = None
        
        # Only create real logger outside of tests
        if not in_test_mode():
            # Use Python's built-in logging, no fancy config
            self._logger = logging.getLogger(name)
            if not logging.getLogger().handlers:
                # Basic config only if no handlers exist
                logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(name)s - %(message)s')
    
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
    
    # NO method call decorators - they cause recursion
    # NO complex event logging - they can cause loops

def get_simple_logger(name: str) -> SimpleLogger:
    return SimpleLogger(name)