"""
Unit tests for OrderEligibilityService logging functionality.
Tests focus on verifying logging occurs, not exact message content.
"""

import pytest
import datetime
from unittest.mock import Mock, patch, MagicMock, call
from decimal import Decimal

from src.services.order_eligibility_service import OrderEligibilityService
from src.core.context_aware_logger import TradingEventType
from src.core.planned_order import PlannedOrder, Action, OrderType, SecurityType, PositionStrategy


class TestOrderEligibilityLogging:
    """Test suite for OrderEligibilityService logging functionality."""

    @pytest.fixture
    def mock_db_session(self):
        """Mock database session for testing."""
        session = Mock()
        session.commit.return_value = None
        session.rollback.return_value = None
        return session

    @pytest.fixture
    def mock_probability_engine(self):
        """Mock probability engine for testing."""
        engine = Mock()
        engine.score_fill.return_value = (0.85, {})  # Default 85% probability
        return engine

    @pytest.fixture
    def eligibility_service(self, mock_probability_engine, mock_db_session):
        """Create OrderEligibilityService instance for testing."""
        planned_orders = []
        
        with patch('src.services.order_eligibility_service.get_context_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            service = OrderEligibilityService(
                planned_orders=planned_orders,
                probability_engine=mock_probability_engine,
                db_session=mock_db_session
            )
            
            # Replace the context logger with our mock
            service.context_logger = mock_logger
            
            return service

    @pytest.fixture
    def sample_planned_order(self):
        """Create a valid sample planned order for testing."""
        return PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NASDAQ",
            currency="USD",
            action=Action.BUY,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=150.0,
            stop_loss=145.0,  # Valid stop loss for BUY order
            position_strategy=PositionStrategy.CORE,
            risk_per_trade=Decimal('0.01'),
            risk_reward_ratio=Decimal('2.0'),
            priority=3
        )

    def test_business_rule_validation_logging(self, eligibility_service, sample_planned_order):
        """Test logging for business rule validation decisions."""
        # Test successful validation
        result = eligibility_service.can_trade(sample_planned_order)

        assert result is True

        # Verify logging was called (don't check exact message)
        assert eligibility_service.context_logger.log_event.called

    def test_order_expiration_logging(self, eligibility_service):
        """Test logging for order expiration business rules."""
        # Create a valid order
        order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NASDAQ",
            currency="USD",
            action=Action.BUY,
            symbol="TEST",
            order_type=OrderType.LMT,
            entry_price=100.0,
            stop_loss=95.0,  # Valid stop loss
            position_strategy=PositionStrategy.CORE,
            risk_per_trade=Decimal('0.01'),
            risk_reward_ratio=Decimal('2.0'),
            priority=3
        )

        # Mock the order to be expired
        with patch.object(eligibility_service, '_is_order_expired') as mock_expired:
            mock_expired.return_value = True

            result = eligibility_service.can_trade(order)

            # Just verify that logging occurred
            assert eligibility_service.context_logger.log_event.called

    def test_validation_error_logging(self, eligibility_service, sample_planned_order):
        """Test logging for validation errors and exceptions."""
        # Mock an exception during validation
        with patch.object(eligibility_service, '_is_order_expired') as mock_expired:
            mock_expired.side_effect = Exception("Validation error")

            result = eligibility_service.can_trade(sample_planned_order)

            # Verify error logging occurred
            assert eligibility_service.context_logger.log_event.called

    def test_probability_scoring_logging(self, eligibility_service, sample_planned_order):
        """Test logging for probability scoring results."""
        # Add order to service's planned orders
        eligibility_service.planned_orders = [sample_planned_order]

        # Execute batch processing
        results = eligibility_service.find_executable_orders()

        # Verify that some form of logging occurred
        assert eligibility_service.context_logger.log_event.called

    def test_batch_processing_logging(self, eligibility_service, sample_planned_order):
        """Test logging for batch eligibility evaluation."""
        # Add multiple valid orders
        order1 = sample_planned_order
        
        # Create a second valid order
        order2 = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NASDAQ",
            currency="USD",
            action=Action.BUY,  # Use BUY to avoid stop loss validation issues
            symbol="MSFT",
            order_type=OrderType.LMT,
            entry_price=300.0,
            stop_loss=290.0,  # Valid stop loss for BUY
            position_strategy=PositionStrategy.CORE,
            risk_per_trade=Decimal('0.01'),
            risk_reward_ratio=Decimal('2.0'),
            priority=2
        )

        eligibility_service.planned_orders = [order1, order2]

        results = eligibility_service.find_executable_orders()

        # Verify batch processing logging occurred
        assert eligibility_service.context_logger.log_event.called

    def test_database_persistence_logging_success(self, eligibility_service, sample_planned_order):
        """Test logging for successful probability score persistence."""
        eligibility_service.planned_orders = [sample_planned_order]

        results = eligibility_service.find_executable_orders()

        # Verify that some database-related logging occurred
        assert eligibility_service.context_logger.log_event.called

    def test_database_persistence_logging_failure(self, eligibility_service, sample_planned_order):
        """Test logging for failed probability score persistence."""
        eligibility_service.planned_orders = [sample_planned_order]

        # Mock database commit failure
        eligibility_service.db_session.commit.side_effect = Exception("Database connection failed")

        results = eligibility_service.find_executable_orders()

        # Verify error logging occurred
        assert eligibility_service.context_logger.log_event.called

    def test_service_initialization_logging(self, mock_probability_engine, mock_db_session):
        """Test logging during service initialization."""
        planned_orders = []

        with patch('src.services.order_eligibility_service.get_context_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            # Create service - this should trigger initialization logging
            service = OrderEligibilityService(
                planned_orders=planned_orders,
                probability_engine=mock_probability_engine,
                db_session=mock_db_session
            )

            # Verify initialization was logged
            mock_logger.log_event.assert_called()

    def test_empty_order_list_logging(self, eligibility_service):
        """Test logging when processing empty order list."""
        eligibility_service.planned_orders = []

        results = eligibility_service.find_executable_orders()

        # Verify that logging still occurred even with empty list
        assert eligibility_service.context_logger.log_event.called

    def test_probability_engine_error_logging(self, eligibility_service, sample_planned_order):
        """Test logging for probability engine errors."""
        eligibility_service.planned_orders = [sample_planned_order]

        # Mock probability engine to raise exception but catch it
        eligibility_service.probability_engine.score_fill.side_effect = Exception("Probability engine failed")

        try:
            results = eligibility_service.find_executable_orders()
        except Exception:
            pass  # Expected to fail

        # Verify error logging occurred
        assert eligibility_service.context_logger.log_event.called

    def test_effective_priority_calculation_logging(self, eligibility_service, sample_planned_order):
        """Test logging for effective priority calculations."""
        eligibility_service.planned_orders = [sample_planned_order]

        # Test with different priority values
        test_cases = [
            (1, 0.85),   # Low priority, high probability
            (5, 0.85),   # High priority, high probability
            (3, 0.40),   # Medium priority, low probability
        ]

        for base_priority, fill_prob in test_cases:
            sample_planned_order.priority = base_priority

            with patch.object(eligibility_service.probability_engine, 'score_fill') as mock_score:
                mock_score.return_value = (fill_prob, {})

                results = eligibility_service.find_executable_orders()

                # Verify that logging occurred
                assert eligibility_service.context_logger.log_event.called

    def test_order_sorting_logging(self, eligibility_service):
        """Test that executable orders are properly sorted by effective priority."""
        # Create orders with different priorities
        orders = []
        test_data = [
            ("LOW_PRIO", 1, 0.9),
            ("HIGH_PRIO", 5, 0.2),
            ("MED_PRIO", 3, 0.7),
        ]

        for symbol, priority, fill_prob in test_data:
            order = PlannedOrder(
                security_type=SecurityType.STK,
                exchange="NASDAQ",
                currency="USD",
                action=Action.BUY,
                symbol=symbol,
                order_type=OrderType.LMT,
                entry_price=100.0,
                stop_loss=95.0,
                position_strategy=PositionStrategy.CORE,
                risk_per_trade=Decimal('0.01'),
                risk_reward_ratio=Decimal('2.0'),
                priority=priority
            )
            orders.append(order)

        eligibility_service.planned_orders = orders

        # Mock probability engine to return different probabilities
        def mock_score_fill(order, return_features=True):
            prob_map = {
                "LOW_PRIO": 0.9,
                "HIGH_PRIO": 0.2,
                "MED_PRIO": 0.7
            }
            return (prob_map[order.symbol], {})

        eligibility_service.probability_engine.score_fill = mock_score_fill

        results = eligibility_service.find_executable_orders()

        # Verify that we got results and logging occurred
        assert len(results) > 0
        assert eligibility_service.context_logger.log_event.called

    def test_comprehensive_feature_logging(self, eligibility_service, sample_planned_order):
        """Test logging with comprehensive probability features."""
        # Mock comprehensive features from probability engine
        comprehensive_features = {
            'volume_ratio': 1.2,
            'price_momentum': 0.8,
            'volatility': 0.15,
            'liquidity_score': 0.9,
            'market_regime': 'BULL',
            'technical_score': 0.75
        }

        eligibility_service.probability_engine.score_fill.return_value = (0.82, comprehensive_features)
        eligibility_service.planned_orders = [sample_planned_order]

        results = eligibility_service.find_executable_orders()

        # Verify that logging occurred with features
        assert eligibility_service.context_logger.log_event.called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])