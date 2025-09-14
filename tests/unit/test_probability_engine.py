"""
Unit tests for the FillProbabilityEngine and its Phase B feature extraction capabilities.
"""

import pytest
from pytest import approx
from unittest.mock import Mock, MagicMock
import datetime
from src.core.probability_engine import FillProbabilityEngine
from src.core.planned_order import PlannedOrder, Action, OrderType, SecurityType, PositionStrategy


class MockDataFeed:
    """Mock data feed for testing probability engine in isolation."""
    
    def __init__(self, mock_data=None):
        self.mock_data = mock_data or {
            'price': 100.0,
            'bid': 99.95,
            'ask': 100.05,
            'bid_size': 1000,
            'ask_size': 1500,
            'last': 100.02,
            'volume': 25000,
            'history': [99.5, 100.1, 100.0, 99.8, 100.2]
        }
    
    def get_current_price(self, symbol):
        return self.mock_data
    
    def is_connected(self):
        return True


class TestFillProbabilityEngine:
    """Test suite for FillProbabilityEngine Phase B features."""
    
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
    
    def test_extract_features_basic(self, mock_data_feed, buy_limit_order):
        """Test that feature extraction captures basic market data."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        features = engine.extract_features(buy_limit_order, mock_data_feed.mock_data)
        
        # Test basic market data features with floating-point tolerance
        assert features['current_price'] == approx(100.0, rel=1e-6)
        assert features['bid'] == approx(99.95, rel=1e-6)
        assert features['ask'] == approx(100.05, rel=1e-6)
        assert features['spread_absolute'] == approx(0.10, rel=1e-6)
        assert features['spread_relative'] == approx(0.001, rel=1e-6)  # 0.10 / 100.0

    def test_extract_features_time_based(self, mock_data_feed, buy_limit_order):
        """Test that time-based features are captured correctly."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        features = engine.extract_features(buy_limit_order, mock_data_feed.mock_data)
        
        # Test time-based features
        assert 'timestamp' in features
        assert 'time_of_day_seconds' in features
        assert 'day_of_week' in features
        assert 'seconds_since_midnight' in features
        
        # Time features should be reasonable values
        assert 0 <= features['day_of_week'] <= 6
        assert 0 <= features['time_of_day_seconds'] <= 86400
    
    def test_extract_features_order_context(self, mock_data_feed, buy_limit_order):
        """Test that order context features are captured correctly."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        features = engine.extract_features(buy_limit_order, mock_data_feed.mock_data)
        
        # Test order context features
        assert features['symbol'] == "AAPL"
        assert features['order_side'] == "BUY"
        assert features['order_type'] == "LMT"
        assert features['entry_price'] == 100.0
        assert features['stop_loss'] == 98.0
        assert features['priority_manual'] == 3
        assert features['trading_setup'] == "Breakout"
        assert features['core_timeframe'] == "15min"
        assert features['price_diff_absolute'] == 0.0  # current == entry
    
    def test_extract_features_no_data(self, mock_data_feed, buy_limit_order):
        """Test feature extraction with no market data."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        features = engine.extract_features(buy_limit_order, None)
        
        # Should return empty dict when no data
        assert features == {}
    
    def test_extract_features_missing_data_fields(self, mock_data_feed, buy_limit_order):
        """Test feature extraction with incomplete market data."""
        incomplete_data = {'price': 100.0}  # Only price, no bid/ask
        engine = FillProbabilityEngine(mock_data_feed)
        
        features = engine.extract_features(buy_limit_order, incomplete_data)
        
        # Should handle missing fields gracefully
        assert features['current_price'] == 100.0
        assert features['bid'] is None
        assert features['ask'] is None
        assert features['spread_absolute'] is None
    
    def test_score_fill_backward_compatibility(self, mock_data_feed, buy_limit_order):
        """Test that score_fill maintains backward compatibility."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        # Test without return_features (original behavior)
        score = engine.score_fill(buy_limit_order)
        
        assert 0.0 <= score <= 1.0
        # Since current price == entry price for BUY, should be high probability
        assert score >= 0.9
    
    def test_score_fill_with_features(self, mock_data_feed, buy_limit_order):
        """Test that score_fill returns both score and features when requested."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        # Test with return_features=True (Phase B enhancement)
        score, features = engine.score_fill(buy_limit_order, return_features=True)
        
        assert 0.0 <= score <= 1.0
        assert isinstance(features, dict)
        assert 'current_price' in features
        assert 'timestamp' in features
    
    def test_score_fill_different_scenarios(self, mock_data_feed):
        """Test score_fill with different price scenarios."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        # Test BUY order with current price below entry (favorable)
        order_below = PlannedOrder(
            security_type=SecurityType.STK,
            exchange="SMART",
            currency="USD", 
            action=Action.BUY,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=100.0,
            stop_loss=98.0
        )
        
        # Mock data with price below entry
        below_data = mock_data_feed.mock_data.copy()
        below_data['price'] = 99.5
        
        score = engine.score_fill(order_below)
        assert score >= 0.9  # Should be high probability
    
    def test_should_execute_order_compatibility(self, mock_data_feed, buy_limit_order):
        """Test that should_execute_order maintains its original interface."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        should_execute, probability = engine.should_execute_order(buy_limit_order)
        
        assert isinstance(should_execute, bool)
        assert 0.0 <= probability <= 1.0
        # With default threshold of 0.7 and favorable conditions, should execute
        assert should_execute == True
    
    def test_volatility_estimation(self, mock_data_feed, buy_limit_order):
        """Test that volatility estimation works (even as placeholder)."""
        engine = FillProbabilityEngine(mock_data_feed)
        
        volatility = engine.estimate_volatility(
            buy_limit_order.symbol,
            mock_data_feed.mock_data['history'],
            buy_limit_order
        )
        
        # Currently returns fixed value, but should be a float
        assert isinstance(volatility, float)
        assert volatility == 0.001


# Additional test for edge cases
def test_empty_market_data():
    """Test behavior with completely empty market data."""
    empty_feed = MockDataFeed({})
    engine = FillProbabilityEngine(empty_feed)
    
    order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,
        symbol="TEST",
        order_type=OrderType.LMT,
        entry_price=100.0,
        stop_loss=98.0
    )
    
    # Should handle empty data gracefully
    score = engine.score_fill(order)
    assert score == 0.9  # Default neutral score
    
    features = engine.extract_features(order, {})
    assert features == {}  # Empty features for empty data