"""
Comprehensive unit tests for ContextAwareLogger safety features and event logging.
Tests circuit breakers, recursion protection, safe context evaluation, and event structure.
"""

import pytest
import time
import threading
from unittest.mock import MagicMock, Mock, patch, call
from datetime import datetime

from src.core.context_aware_logger import (
    ContextAwareLogger, 
    TradingEventType, 
    TradingEvent, 
    SafeContext, 
    get_context_logger
)


class TestContextAwareLogger:
    """Test suite for ContextAwareLogger safety features and functionality."""

    def test_basic_event_logging(self):
        """Test successful event logging with valid parameters."""
        logger = ContextAwareLogger(max_events_per_second=100, max_recursion_depth=5)
        
        success = logger.log_event(
            event_type=TradingEventType.ORDER_VALIDATION,
            message="Test order validation",
            symbol="AAPL",
            context_provider={
                'price': 150.0,
                'quantity': 100,
                'is_valid': True
            },
            decision_reason="Test validation passed"
        )
        
        assert success is True
        stats = logger.get_stats()
        assert stats['total_events'] == 1
        assert stats['dropped_events'] == 0

    def test_circuit_breaker_blocks_excessive_events(self):
        """Test circuit breaker prevents event storms."""
        logger = ContextAwareLogger(max_events_per_second=5, max_recursion_depth=3)
        
        # Log events rapidly to trigger circuit breaker
        successful_logs = 0
        for i in range(10):
            success = logger.log_event(
                event_type=TradingEventType.EXECUTION_DECISION,
                message=f"Test event {i}",
                symbol="SPY"
            )
            if success:
                successful_logs += 1
        
        stats = logger.get_stats()
        assert stats['circuit_breaker_blocks'] > 0
        assert successful_logs <= 5  # Should not exceed max_events_per_second

    def test_circuit_breaker_resets_after_time_window(self):
        """Test circuit breaker resets after time window."""
        logger = ContextAwareLogger(max_events_per_second=2)
        
        # Log events to trigger circuit breaker
        for i in range(5):
            logger.log_event(
                event_type=TradingEventType.MARKET_CONDITION,
                message=f"Event {i}"
            )
        
        stats_before = logger.get_stats()
        assert stats_before['circuit_breaker_blocks'] > 0
        
        # Wait for circuit breaker to reset
        time.sleep(1.1)
        
        # Now events should succeed again
        success = logger.log_event(
            event_type=TradingEventType.MARKET_CONDITION,
            message="Event after reset"
        )
        
        assert success is True

    def test_recursion_protection_blocks_excessive_calls(self):
        """Test recursion protection prevents infinite loops."""
        logger = ContextAwareLogger(max_events_per_second=100, max_recursion_depth=2)
        
        initial_blocks = logger.get_stats()['recursion_blocks']
        
        # Create a scenario that would cause recursion
        def recursive_context_provider():
            # This nested call should trigger recursion protection
            logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Nested call from context",
                context_provider={'nested': True}
            )
            return "recursive_value"
        
        # This call should succeed, but the nested one should be blocked
        success = logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Test recursion protection",
            context_provider={'recursive_field': recursive_context_provider}
        )
        
        stats = logger.get_stats()
        recursion_blocks = stats['recursion_blocks'] - initial_blocks
        
        # The nested call should be blocked by recursion protection
        assert recursion_blocks >= 1
        # The original call should still succeed
        assert success is True

    def test_safe_context_lazy_evaluation(self):
        """Test that context providers are evaluated lazily."""
        evaluation_tracker = []
        
        def lazy_context_provider():
            evaluation_tracker.append("evaluated")
            return "lazy_value"
        
        logger = ContextAwareLogger()
        
        # Create event with lazy context provider
        success = logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Test lazy evaluation",
            context_provider={
                'immediate': "value",
                'lazy': lazy_context_provider
            }
        )
        
        # The provider should be evaluated during logging (not before)
        assert len(evaluation_tracker) == 1
        assert success is True

    def test_safe_context_error_handling(self):
        """Test that context evaluation errors are handled gracefully."""
        def error_context_provider():
            raise ValueError("Context provider error")
        
        logger = ContextAwareLogger()
        
        success = logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Test error handling",
            context_provider={
                'good_field': "works",
                'bad_field': error_context_provider
            }
        )
        
        # Event should still be logged despite context error
        assert success is True

    def test_context_complex_types_sanitization(self):
        """Test that complex types are properly sanitized in context."""
        class ComplexObject:
            def __init__(self, value):
                self.value = value
        
        complex_obj = ComplexObject("test")
        
        logger = ContextAwareLogger()
        
        success = logger.log_event(
            event_type=TradingEventType.ORDER_VALIDATION,
            message="Test complex type sanitization",
            context_provider={
                'string_field': "simple string",
                'int_field': 42,
                'float_field': 3.14,
                'bool_field': True,
                'list_field': [1, 2, 3],
                'dict_field': {'key': 'value'},
                'complex_object': complex_obj  # Should be converted to string
            }
        )
        
        assert success is True

    def test_event_structure_and_required_fields(self):
        """Test that events have correct structure and required fields."""
        logger = ContextAwareLogger()
        
        success = logger.log_event(
            event_type=TradingEventType.EXECUTION_DECISION,
            message="Test event structure",
            symbol="TSLA",
            decision_reason="Test decision"
        )
        
        assert success is True

    def test_thread_safety_concurrent_access(self):
        """Test logger is thread-safe for concurrent access."""
        logger = ContextAwareLogger(max_events_per_second=100, max_recursion_depth=10)
        
        results = []
        errors = []
        
        def worker_logger(thread_id):
            try:
                for i in range(10):
                    success = logger.log_event(
                        event_type=TradingEventType.MARKET_CONDITION,
                        message=f"Thread {thread_id} event {i}",
                        symbol=f"SYMBOL{thread_id}",
                        context_provider={'thread_id': thread_id, 'event_id': i}
                    )
                    results.append(success)
            except Exception as e:
                errors.append(str(e))
        
        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker_logger, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify no errors and all events were processed
        assert len(errors) == 0
        assert all(results)  # All log events should succeed
        stats = logger.get_stats()
        assert stats['total_events'] == 50  # 5 threads * 10 events each

    def test_safe_context_to_dict_evaluation(self):
        """Test SafeContext conversion to dictionary with proper lazy evaluation."""
        evaluation_count = 0
        
        def counting_provider():
            nonlocal evaluation_count
            evaluation_count += 1
            return f"evaluated_{evaluation_count}"
        
        context = SafeContext(
            immediate_value="test",
            lazy_value=counting_provider
        )
        
        # Provider should not be evaluated until to_safe_dict is called
        assert evaluation_count == 0
        
        result = context.to_safe_dict()
        
        # Provider should be evaluated exactly once
        assert evaluation_count == 1
        assert result['immediate_value'] == "test"
        assert result['lazy_value'] == "evaluated_1"

    def test_safe_context_caches_evaluation(self):
        """Test that SafeContext caches evaluation results."""
        evaluation_count = 0
        
        def counting_provider():
            nonlocal evaluation_count
            evaluation_count += 1
            return "cached_value"
        
        context = SafeContext(lazy_field=counting_provider)
        
        # Multiple calls to to_safe_dict should only evaluate once
        result1 = context.to_safe_dict()
        result2 = context.to_safe_dict()
        result3 = context.to_safe_dict()
        
        assert evaluation_count == 1
        assert result1['lazy_field'] == "cached_value"
        assert result2['lazy_field'] == "cached_value"
        assert result3['lazy_field'] == "cached_value"

    def test_trading_event_type_enum_coverage(self):
        """Test TradingEventType enum values and coverage."""
        expected_types = [
            'ORDER_VALIDATION', 'EXECUTION_DECISION', 'MARKET_CONDITION',
            'POSITION_MANAGEMENT', 'STATE_TRANSITION', 'RISK_EVALUATION',
            'SYSTEM_HEALTH', 'DATABASE_STATE'
        ]
        
        for expected_type in expected_types:
            assert hasattr(TradingEventType, expected_type)
            event_type = getattr(TradingEventType, expected_type)
            assert isinstance(event_type, TradingEventType)
            assert event_type.value == expected_type.lower()

    def test_global_logger_singleton_pattern(self):
        """Test that get_context_logger returns singleton instance."""
        logger1 = get_context_logger()
        logger2 = get_context_logger()
        
        assert logger1 is logger2
        assert isinstance(logger1, ContextAwareLogger)

    def test_statistics_tracking_and_reset(self):
        """Test that statistics can be tracked and reset."""
        logger = ContextAwareLogger()
        
        # Log some events
        for i in range(3):
            logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message=f"Event {i}"
            )
        
        stats_before = logger.get_stats()
        assert stats_before['total_events'] == 3
        
        # Reset stats
        logger.reset_stats()
        
        stats_after = logger.get_stats()
        assert stats_after['total_events'] == 0
        assert stats_after['dropped_events'] == 0
        assert stats_after['recursion_blocks'] == 0
        assert stats_after['circuit_breaker_blocks'] == 0

    def test_trading_event_dataclass_structure(self):
        """Test TradingEvent dataclass structure and defaults."""
        event = TradingEvent(
            event_id="test123",
            event_type="test_event",
            timestamp="2023-01-01T00:00:00",
            session_id="session123",
            symbol="AAPL",
            message="Test message",
            context={"key": "value"},
            decision_reason="Test reason",
            call_stack_depth=1
        )
        
        assert event.event_id == "test123"
        assert event.event_type == "test_event"
        assert event.symbol == "AAPL"
        assert event.message == "Test message"
        assert event.context == {"key": "value"}
        assert event.decision_reason == "Test reason"
        assert event.call_stack_depth == 1

    def test_safe_context_make_safe_method(self):
        """Test _make_safe method handles various types correctly."""
        safe_context = SafeContext()
        
        test_data = {
            'string': "simple string",
            'integer': 42,
            'float': 3.14,
            'boolean': True,
            'none': None,
            'list': [1, "string", {'nested': 'dict'}],
            'tuple': (1, 2, 3),
            'dict': {'key': 'value'}
        }
        
        safe_result = safe_context._make_safe(test_data)
        
        # Verify all types are properly converted
        assert isinstance(safe_result, dict)
        assert safe_result['string'] == "simple string"
        assert safe_result['integer'] == 42
        assert safe_result['float'] == 3.14
        assert safe_result['boolean'] is True
        assert safe_result['none'] is None
        assert safe_result['list'] == [1, "string", {'nested': 'dict'}]
        assert safe_result['tuple'] == [1, 2, 3]  # Tuple becomes list
        assert safe_result['dict'] == {'key': 'value'}

    def test_empty_context_provider(self):
        """Test logging with empty or None context provider."""
        logger = ContextAwareLogger()
        
        # Test with None context
        success1 = logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Test None context",
            context_provider=None
        )
        assert success1 is True
        
        # Test with empty context
        success2 = logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Test empty context",
            context_provider={}
        )
        assert success2 is True

    def test_event_without_optional_fields(self):
        """Test logging events without optional fields."""
        logger = ContextAwareLogger()
        
        success = logger.log_event(
            event_type=TradingEventType.MARKET_CONDITION,
            message="Test without optional fields"
            # No symbol, no context_provider, no decision_reason
        )
        
        assert success is True

    def test_high_volume_logging_performance(self):
        """Test logger performance under high volume."""
        logger = ContextAwareLogger(max_events_per_second=1000, max_recursion_depth=10)
        
        start_time = time.time()
        
        # Log large number of events
        for i in range(100):
            logger.log_event(
                event_type=TradingEventType.MARKET_CONDITION,
                message=f"High volume event {i}",
                symbol="SPY",
                context_provider={'index': i, 'timestamp': time.time()}
            )
        
        end_time = time.time()
        
        # Should complete quickly
        assert (end_time - start_time) < 1.0  # Less than 1 second
        
        stats = logger.get_stats()
        assert stats['total_events'] == 100
        assert stats['dropped_events'] == 0

    def test_console_output_format(self):
        """Test that events are properly formatted for console output."""
        from io import StringIO
        import sys
        
        # Capture stdout
        captured_output = StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output
        
        try:
            logger = ContextAwareLogger()
            
            logger.log_event(
                event_type=TradingEventType.ORDER_VALIDATION,
                message="Test console output",
                symbol="MSFT",
                decision_reason="Validation test"
            )
            
            # Get the console output
            console_output = captured_output.getvalue()
            
            # Check console formatting
            assert "🔍" in console_output  # Emoji prefix
            assert "ORDER_VALIDATION" in console_output
            assert "MSFT" in console_output
            assert "Test console output" in console_output
            assert "Validation test" in console_output
            
        finally:
            sys.stdout = original_stdout

    def test_context_aware_logging_with_complex_context(self):
        """Test context-aware logging with complex context data."""
        from io import StringIO
        import sys
        
        captured_output = StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output
        
        try:
            logger = ContextAwareLogger()
            
            # Complex context with nested data
            complex_context = {
                'portfolio': {
                    'total_value': 100000,
                    'positions': ['AAPL', 'MSFT', 'GOOGL'],
                    'cash_balance': 25000.50
                },
                'market_conditions': {
                    'volatility': 'high',
                    'trend': 'bullish',
                    'volume': 'above_average'
                },
                'risk_metrics': {
                    'max_drawdown': -0.02,
                    'sharpe_ratio': 1.5,
                    'var_95': -5000
                }
            }
            
            success = logger.log_event(
                event_type=TradingEventType.RISK_EVALUATION,
                message="Portfolio risk assessment",
                symbol="PORTFOLIO",
                context_provider=complex_context,
                decision_reason="Risk limits within acceptable range"
            )
            
            assert success is True
            
            console_output = captured_output.getvalue()
            # Verify the complex context was processed without errors
            assert "Portfolio risk assessment" in console_output
            assert "PORTFOLIO" in console_output
            
        finally:
            sys.stdout = original_stdout

    def test_context_aware_logging_with_lazy_evaluation(self):
        """Test that lazy context evaluation works correctly."""
        from io import StringIO
        import sys
        
        captured_output = StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output
        
        try:
            logger = ContextAwareLogger()
            
            evaluation_tracker = []
            
            def lazy_portfolio_value():
                evaluation_tracker.append("portfolio_evaluated")
                return 150000.75
            
            def lazy_risk_metrics():
                evaluation_tracker.append("risk_evaluated") 
                return {'var': -7500, 'expected_shortfall': -12000}
            
            success = logger.log_event(
                event_type=TradingEventType.POSITION_MANAGEMENT,
                message="Lazy context evaluation test",
                symbol="TEST",
                context_provider={
                    'immediate_value': "available_immediately",
                    'portfolio_value': lazy_portfolio_value,
                    'risk_data': lazy_risk_metrics
                }
            )
            
            assert success is True
            # Verify lazy evaluation occurred
            assert len(evaluation_tracker) == 2
            assert "portfolio_evaluated" in evaluation_tracker
            assert "risk_evaluated" in evaluation_tracker
            
            console_output = captured_output.getvalue()
            assert "Lazy context evaluation test" in console_output
            
        finally:
            sys.stdout = original_stdout

    def test_context_aware_logging_error_handling(self):
        """Test that context evaluation errors are handled gracefully."""
        from io import StringIO
        import sys
        
        captured_output = StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output
        
        try:
            logger = ContextAwareLogger()
            
            def failing_context_provider():
                raise ValueError("Simulated context provider failure")
            
            success = logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Error handling test",
                context_provider={
                    'good_data': "This works fine",
                    'bad_data': failing_context_provider,
                    'more_good_data': {"nested": "value"}
                }
            )
            
            # Should still succeed despite context errors
            assert success is True
            
            console_output = captured_output.getvalue()
            assert "Error handling test" in console_output
            
        finally:
            sys.stdout = original_stdout

if __name__ == "__main__":
    pytest.main([__file__, "-v"])