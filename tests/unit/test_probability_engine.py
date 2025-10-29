"""
Unit tests for the FillProbabilityEngine - focused on core functionality.
"""

import pytest
from pytest import approx
from unittest.mock import Mock, MagicMock, patch
import datetime
from src.core.probability_engine import FillProbabilityEngine
from src.trading.orders.planned_order import PlannedOrder, Action, OrderType, SecurityType


class MockDataFeed:
    """Mock data feed that returns proper market data dictionaries."""
    
    def __init__(self, mock_data=None):
        self.mock_data = mock_data or {
            'price': 100.0,
            'bid': 99.95,
            'ask': 100.05,
            'bid_size': 1000,
            'ask_size': 1500,
            'last': 100.02,
            'volume': 25000
        }
    
    def get_current_price(self, symbol):
        return self.mock_data
    
    def is_connected(self):
        return True


class TestFillProbabilityEngine:
    """Test suite for FillProbabilityEngine core functionality."""
    
    @pytest.fixture
    def mock_data_feed(self):
        """Provide a mock data feed for testing."""
        return MockDataFeed()
    
    @pytest.fixture
    def buy_limit_order(self):
        """Create a sample BUY limit order for testing."""
        return PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            action=Action.BUY,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=100.0,
            stop_loss=98.0,
            risk_per_trade=0.005,
            risk_reward_ratio=2.0,
            priority=3,
            trading_setup="Breakout",
            core_timeframe="15min"
        )
    
    @pytest.fixture
    def sell_limit_order(self):
        """Create a sample SELL limit order for testing."""
        return PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART", 
            currency="USD",
            action=Action.SELL,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=100.0,
            stop_loss=102.0,
            risk_per_trade=0.005,
            risk_reward_ratio=2.0,
            priority=2,
            trading_setup="Reversal",
            core_timeframe="1H"
        )

    def test_score_fill_basic(self, mock_data_feed, buy_limit_order):
        """Test basic score_fill functionality."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        score = engine.score_fill(buy_limit_order)
        
        assert 0.0 <= score <= 1.0

    def test_score_fill_return_features_compatibility(self, mock_data_feed, buy_limit_order):
        """Test that score_fill with return_features returns tuple."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        # Test with return_features=True
        result = engine.score_fill(buy_limit_order, return_features=True)
        
        # Should return a tuple (score, features)
        assert isinstance(result, tuple)
        assert len(result) == 2
        score, features = result
        assert 0.0 <= score <= 1.0
        assert isinstance(features, dict)

    def test_score_fill_buy_order_scenarios(self, mock_data_feed):
        """Test score_fill with different BUY order scenarios."""
        # BUY order with price below entry (favorable)
        order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            action=Action.BUY,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=100.0,
            stop_loss=98.0
        )
        
        # Mock data feed with price below entry
        below_feed = MockDataFeed({'price': 99.5})
        engine_below = FillProbabilityEngine(below_feed)
        score_below = engine_below.score_fill(order)
        
        # Mock data feed with price above entry
        above_feed = MockDataFeed({'price': 100.5})
        engine_above = FillProbabilityEngine(above_feed)
        score_above = engine_above.score_fill(order)
        
        # Price below entry should have higher probability for BUY
        assert score_below > score_above

    def test_score_fill_sell_order_scenarios(self, mock_data_feed):
        """Test score_fill with different SELL order scenarios."""
        # SELL order with price above entry (favorable)
        order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            action=Action.SELL,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=100.0,
            stop_loss=102.0
        )
        
        # Mock data feed with price above entry
        above_feed = MockDataFeed({'price': 100.5})
        engine_above = FillProbabilityEngine(above_feed)
        score_above = engine_above.score_fill(order)
        
        # Mock data feed with price below entry
        below_feed = MockDataFeed({'price': 99.5})
        engine_below = FillProbabilityEngine(below_feed)
        score_below = engine_below.score_fill(order)
        
        # Price above entry should have higher probability for SELL
        assert score_above > score_below

    def test_score_fill_market_orders(self, mock_data_feed):
        """Test score_fill with market orders."""
        order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            action=Action.BUY,
            symbol="AAPL",
            order_type=OrderType.MKT,  # Market order
            entry_price=100.0,
            stop_loss=98.0
        )
        
        engine = FillProbabilityEngine(mock_data_feed)
        score = engine.score_fill(order)
        
        # Market orders should have high fill probability
        assert score >= 0.8

    def test_score_fill_no_market_data(self, mock_data_feed):
        """Test score_fill when no market data is available."""
        # Create a data feed that returns None
        empty_feed = Mock()
        empty_feed.get_current_price.return_value = None
        
        engine = FillProbabilityEngine(empty_feed)
        order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            action=Action.BUY,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=100.0,
            stop_loss=98.0
        )
        
        score = engine.score_fill(order)
        # Should return default probability when no data
        assert score == 0.5

    def test_score_fill_no_market_data_with_features(self, mock_data_feed):
        """Test score_fill with return_features when no market data."""
        empty_feed = Mock()
        empty_feed.get_current_price.return_value = None
        
        engine = FillProbabilityEngine(empty_feed)
        order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD",
            action=Action.BUY,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=100.0,
            stop_loss=98.0
        )
        
        score, features = engine.score_fill(order, return_features=True)
        assert score == 0.5
        assert features == {}

    def test_calculate_fill_probability_method(self, mock_data_feed, buy_limit_order):
        """Test the calculate_fill_probability method."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        # Mock current price and volatility
        current_price = 99.5
        volatility = 0.02
        
        probability = engine.calculate_fill_probability(buy_limit_order, current_price, volatility)
        
        assert 0.0 <= probability <= 1.0

    def test_score_outcome_stub(self, mock_data_feed, buy_limit_order):
        """Test the score_outcome_stub method."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        result = engine.score_outcome_stub(buy_limit_order)
        
        # Stub method should return None
        assert result is None

    def test_engine_initialization(self, mock_data_feed):
        """Test that engine initializes properly."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        assert engine.data_feed == mock_data_feed
        assert hasattr(engine, 'score_fill')
        assert hasattr(engine, 'calculate_fill_probability')
        assert hasattr(engine, 'score_outcome_stub')