from decimal import Decimal
import pytest
from unittest.mock import Mock, patch, MagicMock
import datetime
from src.core.planned_order import Action, OrderType, PlannedOrder, SecurityType, PositionStrategy
from src.services.order_eligibility_service import OrderEligibilityService
from src.core.models import ProbabilityScoreDB


class TestOrderEligibilityService:
    """Test suite for OrderEligibilityService"""

    @pytest.fixture
    def mock_db_session(self):
        """Provide a mock database session for testing."""
        session = Mock()
        session.add = Mock()
        session.commit = Mock()
        return session

    @patch("src.services.order_eligibility_service.ProbabilityScoreDB")
    def test_find_executable_orders_filters_by_probability(self, mock_probability_score_db):
        # Arrange
        mock_orders = [
            PlannedOrder(
                symbol="AAPL",
                action=Action.BUY,
                security_type=SecurityType.STK,
                exchange="SMART",
                currency="USD",
                entry_price=100,
                stop_loss=95,
                priority=3
            ),
            PlannedOrder(
                symbol="MSFT",
                action=Action.SELL,
                security_type=SecurityType.STK,
                exchange="SMART",
                currency="USD",
                entry_price=200,
                stop_loss=210,
                priority=2
            ),
        ]
        
        # Mock the probability engine to return score and features
        mock_probability_engine = Mock()
        mock_probability_engine.score_fill.return_value = (0.8, {"feature": 1.0, "symbol": "TEST"})

        # Fix constructor call - remove planned_orders parameter - Begin
        service = OrderEligibilityService(mock_probability_engine)
        # Fix constructor call - remove planned_orders parameter - End

        # Act
        # Fix method call - add planned_orders parameter - Begin
        results = service.find_executable_orders(mock_orders)
        # Fix method call - add planned_orders parameter - End

        # Assert
        assert len(results) == 2
        for result in results:
            assert "fill_probability" in result
            assert result["fill_probability"] == 0.8
            assert "effective_priority" in result
            assert result["effective_priority"] == result["priority"] * 0.8
            assert "features" in result  # Phase B: features should be included

    @patch("src.services.order_eligibility_service.ProbabilityScoreDB")
    def test_find_executable_orders_includes_timestamp(self, mock_probability_score_db):
        # Arrange
        mock_orders = [
            PlannedOrder(
                symbol="TSLA",
                action=Action.BUY,
                security_type=SecurityType.STK,
                exchange="SMART",
                currency="USD",
                entry_price=300,
                stop_loss=290,
                priority=4
            )
        ]
        
        mock_probability_engine = Mock()
        mock_probability_engine.score_fill.return_value = (0.6, {"feature": 2.0, "symbol": "TSLA"})

        # Fix constructor call - remove planned_orders parameter - Begin
        service = OrderEligibilityService(mock_probability_engine)
        # Fix constructor call - remove planned_orders parameter - End

        # Act
        # Fix method call - add planned_orders parameter - Begin
        results = service.find_executable_orders(mock_orders)
        # Fix method call - add planned_orders parameter - End

        # Assert
        assert len(results) == 1
        result = results[0]
        assert "timestamp" in result
        assert isinstance(result["timestamp"], datetime.datetime)
        assert result["fill_probability"] == 0.6
        assert "features" in result  # Phase B: features should be included

    def test_can_trade_returns_true_by_default(self):
        """Test that can_trade method returns True (placeholder implementation)"""
        # Fix constructor call - remove planned_orders parameter - Begin
        mock_probability_engine = Mock()
        service = OrderEligibilityService(mock_probability_engine)
        # Fix constructor call - remove planned_orders parameter - End
        
        # Create a proper PlannedOrder mock with required attributes
        test_order = MagicMock(spec=PlannedOrder)
        test_order.symbol = "TEST"
        test_order.action = Action.BUY
        test_order.order_type = OrderType.LMT
        test_order.entry_price = 100.0
        test_order.stop_loss = 95.0
        test_order.security_type = SecurityType.STK
        test_order.position_strategy = PositionStrategy.DAY
        test_order.risk_per_trade = Decimal('0.01')
        test_order.risk_reward_ratio = Decimal('2.0')
        test_order.priority = 3
        test_order.overall_trend = "Neutral"
        
        result = service.can_trade(test_order)
        
        assert result == True

    def test_find_executable_orders_empty_when_no_orders(self):
        """Test that empty list is returned when no planned orders"""
        # Fix constructor call - remove planned_orders parameter - Begin
        mock_probability_engine = Mock()
        service = OrderEligibilityService(mock_probability_engine)
        # Fix constructor call - remove planned_orders parameter - End
        
        # Fix method call - pass empty list - Begin
        executable = service.find_executable_orders([])
        # Fix method call - pass empty list - End
        
        assert len(executable) == 0
        assert executable == []

    # Phase B Additions - Begin
    def test_persistence_with_db_session(self, mock_db_session):
        """Test that probability scores are persisted when DB session is provided."""
        # Arrange
        mock_orders = [
            PlannedOrder(
                symbol="AAPL",
                action=Action.BUY,
                security_type=SecurityType.STK,
                exchange="SMART",
                currency="USD",
                entry_price=100,
                stop_loss=95,
                priority=3
            )
        ]
        
        mock_probability_engine = Mock()
        mock_features = {
            "symbol": "AAPL",
            "current_price": 100.5,
            "time_of_day_seconds": 34200,  # 9:30 AM
            "feature": "test_value"
        }
        mock_probability_engine.score_fill.return_value = (0.85, mock_features)

        # Fix constructor call - remove planned_orders parameter - Begin
        service = OrderEligibilityService(mock_probability_engine, mock_db_session)
        # Fix constructor call - remove planned_orders parameter - End

        # Act
        # Fix method call - add planned_orders parameter - Begin
        results = service.find_executable_orders(mock_orders)
        # Fix method call - add planned_orders parameter - End

        # Assert
        assert len(results) == 1
        mock_db_session.add.assert_called_once()
        mock_db_session.commit.assert_called_once()
        
        # Verify the added object is a ProbabilityScoreDB with correct data
        added_score = mock_db_session.add.call_args[0][0]
        assert isinstance(added_score, ProbabilityScoreDB)
        assert added_score.symbol == "AAPL"
        assert added_score.fill_probability == 0.85
        assert added_score.features == mock_features
        assert added_score.engine_version == "phaseB_v1"
        assert added_score.source == "eligibility_service"

    def test_no_persistence_without_db_session(self):
        """Test that no DB operations occur when no session is provided."""
        # Arrange
        mock_orders = [
            PlannedOrder(
                symbol="AAPL",
                action=Action.BUY,
                security_type=SecurityType.STK,
                exchange="SMART",
                currency="USD",
                entry_price=100,
                stop_loss=95,
                priority=3
            )
        ]
        
        mock_probability_engine = Mock()
        mock_probability_engine.score_fill.return_value = (0.85, {"feature": "test"})

        # Fix constructor call - remove planned_orders parameter - Begin
        service = OrderEligibilityService(mock_probability_engine, None)
        # Fix constructor call - remove planned_orders parameter - End

        # Act - should not raise any database-related errors
        # Fix method call - add planned_orders parameter - Begin
        results = service.find_executable_orders(mock_orders)
        # Fix method call - add planned_orders parameter - End

        # Assert
        assert len(results) == 1
        # No database operations should occur

    def test_features_included_in_executable_results(self):
        """Test that features are included in the executable orders results."""
        # Arrange
        mock_orders = [
            PlannedOrder(
                symbol="GOOGL",
                action=Action.BUY,
                security_type=SecurityType.STK,
                exchange="SMART", 
                currency="USD",
                entry_price=150,
                stop_loss=145,
                priority=2
            )
        ]
        
        mock_probability_engine = Mock()
        expected_features = {
            "symbol": "GOOGL",
            "current_price": 151.2,
            "spread_absolute": 0.15,
            "time_based_feature": 12345
        }
        mock_probability_engine.score_fill.return_value = (0.75, expected_features)

        # Fix constructor call - remove planned_orders parameter - Begin
        service = OrderEligibilityService(mock_probability_engine)
        # Fix constructor call - remove planned_orders parameter - End

        # Act
        # Fix method call - add planned_orders parameter - Begin
        results = service.find_executable_orders(mock_orders)
        # Fix method call - add planned_orders parameter - End

        # Assert
        assert len(results) == 1
        assert results[0]["features"] == expected_features
        assert "features" in results[0]  # Phase B: features should be present
    # Phase B Additions - End