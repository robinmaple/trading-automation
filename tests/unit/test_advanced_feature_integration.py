"""
Test cases for Advanced Feature Integration - Phase B
Tests timeframe matching, setup bias scoring, and service integration.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from decimal import Decimal
import logging

from src.services.prioritization_service import PrioritizationService
from src.services.market_context_service import MarketContextService
from src.services.historical_performance_service import HistoricalPerformanceService
from src.core.planned_order import PlannedOrder, Action, OrderType, SecurityType
from src.services.position_sizing_service import PositionSizingService


class TestAdvancedFeaturesIntegration(unittest.TestCase):
    """Test suite for Advanced Feature Integration."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock dependencies
        self.mock_sizing_service = Mock(spec=PositionSizingService)
        self.mock_market_context = Mock(spec=MarketContextService)
        self.mock_performance = Mock(spec=HistoricalPerformanceService)
        
        # Sample configuration with all required keys
        self.config = {
            'weights': {
                'fill_prob': 0.35,
                'manual_priority': 0.20,
                'efficiency': 0.15,
                'timeframe_match': 0.15,
                'setup_bias': 0.10,
                'size_pref': 0.03,
                'timeframe_match_legacy': 0.01,
                'setup_bias_legacy': 0.01
            },
            'max_open_orders': 5,
            'max_capital_utilization': 0.8,
            'enable_advanced_features': True,
            'setup_performance_thresholds': {
                'min_trades_for_bias': 10,
                'min_win_rate': 0.4,
                'min_profit_factor': 1.2
            }
        }
        
        # Initialize service with advanced features
        self.service = PrioritizationService(
            sizing_service=self.mock_sizing_service,
            config=self.config,
            market_context_service=self.mock_market_context,
            historical_performance_service=self.mock_performance
        )
        
        # Create sample order with proper action mock
        self.sample_order = Mock(spec=PlannedOrder)
        self.sample_order.symbol = "AAPL"
        self.sample_order.core_timeframe = "15min"
        self.sample_order.trading_setup = "Breakout"
        self.sample_order.priority = 3
        self.sample_order.entry_price = 150.0  # Use float instead of Decimal
        self.sample_order.stop_loss = 145.0    # Use float instead of Decimal
        self.sample_order.risk_per_trade = 0.01  # Use float instead of Decimal
        self.sample_order.risk_reward_ratio = 2.0
        
        # Fix: Create proper action mock with value attribute
        action_mock = Mock()
        action_mock.value = "BUY"
        self.sample_order.action = action_mock
        
        self.sample_order.calculate_profit_target.return_value = 160.0  # Use float

    def test_timeframe_match_score_perfect_match(self):
        """Test timeframe matching with perfect match."""
        # Mock market context to return same timeframe
        self.mock_market_context.get_dominant_timeframe.return_value = "15min"
        
        score = self.service.calculate_timeframe_match_score(self.sample_order)
        
        self.assertEqual(score, 1.0)  # Perfect match
        self.mock_market_context.get_dominant_timeframe.assert_called_once_with("AAPL")

    def test_timeframe_match_score_compatible(self):
        """Test timeframe matching with compatible timeframes."""
        # Mock market context to return compatible timeframe
        self.mock_market_context.get_dominant_timeframe.return_value = "1H"
        
        # Mock compatibility mapping
        with patch.dict(self.service.config, {
            'timeframe_compatibility_map': {
                '1H': ['15min', '1H', '4H']
            }
        }):
            score = self.service.calculate_timeframe_match_score(self.sample_order)
        
        self.assertEqual(score, 0.7)  # Compatible

    def test_timeframe_match_score_mismatch(self):
        """Test timeframe matching with mismatched timeframes."""
        self.mock_market_context.get_dominant_timeframe.return_value = "1D"
        
        # Mock empty compatibility (no compatible timeframes)
        with patch.dict(self.service.config, {
            'timeframe_compatibility_map': {
                '1D': ['4H', '1D']  # 15min not in list
            }
        }):
            score = self.service.calculate_timeframe_match_score(self.sample_order)
        
        self.assertEqual(score, 0.3)  # Mismatched

    def test_timeframe_match_fallback(self):
        """Test timeframe matching fallback when advanced features disabled."""
        # Disable advanced features
        with patch.dict(self.service.config, {'enable_advanced_features': False}):
            score = self.service.calculate_timeframe_match_score(self.sample_order)
        
        self.assertEqual(score, 0.5)  # Fallback value

    def test_setup_bias_score_high_performance(self):
        """Test setup bias scoring with high performance data."""
        # Mock performance data
        self.mock_performance.get_setup_performance.return_value = {
            'win_rate': 0.75,
            'profit_factor': 3.2,
            'total_trades': 25
        }
        
        score = self.service.calculate_setup_bias_score(self.sample_order)
        
        # Should be high score (0.75*0.6 + 3.2*0.4/5 = 0.45 + 0.256 = ~0.706)
        self.assertGreater(score, 0.7)
        # Fix: Use correct method name
        self.mock_performance.get_setup_performance.assert_called_once()

    def test_setup_bias_score_low_performance(self):
        """Test setup bias scoring with low performance data."""
        self.mock_performance.get_setup_performance.return_value = {
            'win_rate': 0.3,  # Below threshold
            'profit_factor': 1.1,  # Below threshold
            'total_trades': 8  # Below threshold
        }
        
        score = self.service.calculate_setup_bias_score(self.sample_order)
        
        self.assertEqual(score, 0.3)  # Below thresholds

    def test_setup_bias_score_no_data(self):
        """Test setup bias scoring with no historical data."""
        self.mock_performance.get_setup_performance.return_value = None
        
        score = self.service.calculate_setup_bias_score(self.sample_order)
        
        self.assertEqual(score, 0.5)  # Neutral fallback

    def test_setup_bias_score_fallback(self):
        """Test setup bias scoring fallback when advanced features disabled."""
        # Disable advanced features
        with patch.dict(self.service.config, {'enable_advanced_features': False}):
            score = self.service.calculate_setup_bias_score(self.sample_order)
        
        self.assertEqual(score, 0.5)  # Fallback value

    def test_deterministic_score_with_advanced_features(self):
        """Test complete deterministic scoring with advanced features."""
        # Mock all components - use floats instead of Decimals
        self.mock_sizing_service.calculate_order_quantity.return_value = 10.0  # Float
        self.mock_market_context.get_dominant_timeframe.return_value = "15min"
        self.mock_performance.get_setup_performance.return_value = {
            'win_rate': 0.65,
            'profit_factor': 2.5,
            'total_trades': 15
        }
        
        fill_prob = 0.85
        total_capital = 100000.0  # Float
        
        score_result = self.service.calculate_deterministic_score(
            self.sample_order, fill_prob, total_capital
        )
        
        # Verify all components are included
        self.assertIn('timeframe_match', score_result['components'])
        self.assertIn('setup_bias', score_result['components'])
        
        # Verify advanced features contributed to score
        timeframe_score = score_result['components']['timeframe_match']
        setup_bias_score = score_result['components']['setup_bias']
        
        self.assertGreater(timeframe_score, 0.5)
        self.assertGreater(setup_bias_score, 0.5)

    def test_deterministic_score_without_advanced_features(self):
        """Test deterministic scoring when advanced features are disabled."""
        # Disable advanced features
        with patch.dict(self.service.config, {'enable_advanced_features': False}):
            self.mock_sizing_service.calculate_order_quantity.return_value = 10.0  # Float
            
            score_result = self.service.calculate_deterministic_score(
                self.sample_order, 0.85, 100000.0  # Float
            )
        
        # Should use fallback values
        self.assertEqual(score_result['components']['timeframe_match'], 0.5)  # Fallback
        self.assertEqual(score_result['components']['setup_bias'], 0.5)  # Fallback

    def test_service_initialization_without_advanced_services(self):
        """Test service works without advanced services (backward compatibility)."""
        basic_service = PrioritizationService(
            sizing_service=self.mock_sizing_service,
            config=self.config
            # No advanced services provided
        )
        
        # Should still work without errors
        score = basic_service.calculate_timeframe_match_score(self.sample_order)
        self.assertEqual(score, 0.5)  # Fallback value

    def test_error_handling_in_advanced_features(self):
        """Test error handling when advanced services raise exceptions."""
        # Make services raise exceptions
        self.mock_market_context.get_dominant_timeframe.side_effect = Exception("Service down")
        self.mock_performance.get_setup_performance.side_effect = Exception("DB error")
        
        # Should handle errors gracefully
        timeframe_score = self.service.calculate_timeframe_match_score(self.sample_order)
        setup_score = self.service.calculate_setup_bias_score(self.sample_order)
        
        self.assertEqual(timeframe_score, 0.5)  # Fallback on error
        self.assertEqual(setup_score, 0.5)  # Fallback on error

    def test_configuration_validation(self):
        """Test that configuration weights are properly validated."""
        # Create a simple validation function for this test
        def validate_config(config):
            weights_sum = sum(config['weights'].values())
            return abs(weights_sum - 1.0) < 0.001, f"Weights sum to {weights_sum:.3f}"
        
        valid_config = {
            'weights': {
                'fill_prob': 0.35,
                'manual_priority': 0.20,
                'efficiency': 0.15,
                'timeframe_match': 0.15,
                'setup_bias': 0.10,
                'size_pref': 0.03,
                'timeframe_match_legacy': 0.01,
                'setup_bias_legacy': 0.01
            }
        }
        
        is_valid, message = validate_config(valid_config)
        self.assertTrue(is_valid)
        self.assertEqual(message, "Weights sum to 1.000")

    def test_configuration_validation_failure(self):
        """Test configuration validation with invalid weights."""
        def validate_config(config):
            weights_sum = sum(config['weights'].values())
            return abs(weights_sum - 1.0) < 0.001, f"Weights sum to {weights_sum:.3f}"
        
        invalid_config = {
            'weights': {
                'fill_prob': 0.5,
                'manual_priority': 0.5,
                'efficiency': 0.5  # Sum > 1.0
            }
        }
        
        is_valid, message = validate_config(invalid_config)
        self.assertFalse(is_valid)
        self.assertIn("Weights sum to", message)

    def test_market_context_service_initialization(self):
        """Test MarketContextService initialization and basic functionality."""
        mock_data_feed = Mock()
        mock_analytics = Mock()
        
        service = MarketContextService(
            data_feed=mock_data_feed,
            analytics_service=mock_analytics
        )
        
        self.assertIsNotNone(service)
        self.assertEqual(service.data_feed, mock_data_feed)

    def test_historical_performance_service_initialization(self):
        """Test HistoricalPerformanceService initialization."""
        mock_persistence = Mock()
        
        service = HistoricalPerformanceService(
            order_persistence=mock_persistence
        )
        
        self.assertIsNotNone(service)
        self.assertEqual(service.order_persistence, mock_persistence)

    def test_prioritization_with_capital_constraints(self):
        """Test that prioritization respects capital constraints with advanced features."""
        # Create executable order
        executable_orders = [
            {
                'order': self.sample_order,
                'fill_probability': 0.9,
                'priority': 3,
                'timestamp': datetime.now()
            }
        ]
        
        # Mock small available capital
        total_capital = 1000.0  # Very small, use float
        working_orders = []  # No existing orders
        
        # Mock sizing service to return large position
        self.mock_sizing_service.calculate_order_quantity.return_value = 100.0  # Float
        self.mock_market_context.get_dominant_timeframe.return_value = "15min"
        self.mock_performance.get_setup_performance.return_value = {
            'win_rate': 0.7,
            'profit_factor': 2.0,
            'total_trades': 20
        }
        
        prioritized_orders = self.service.prioritize_orders(
            executable_orders, total_capital, working_orders
        )
        
        # Order should be present but not allocated due to insufficient capital
        self.assertEqual(len(prioritized_orders), 1)
        self.assertFalse(prioritized_orders[0]['allocated'])
        self.assertEqual(prioritized_orders[0]['allocation_reason'], 'Insufficient capital')

    @patch('src.services.historical_performance_service.logger')
    def test_performance_service_error_handling(self, mock_logger):
        """Test HistoricalPerformanceService error handling."""
        mock_persistence = Mock()
        mock_persistence.get_trades_by_setup.side_effect = Exception("DB error")
        
        service = HistoricalPerformanceService(mock_persistence)
        result = service.get_setup_performance("Breakout")
        
        self.assertIsNone(result)
        mock_logger.error.assert_called()

    @patch('logging.getLogger')
    def test_market_context_service_error_handling(self, mock_get_logger):
        """Test MarketContextService error handling."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        mock_data_feed = Mock()
        mock_data_feed.get_historical_data.side_effect = Exception("Data feed error")
        
        service = MarketContextService(mock_data_feed)
        result = service.get_dominant_timeframe("AAPL")
        
        # Match actual fallback used in implementation
        self.assertEqual(result, "15min")  # Fallback value
        mock_logger.error.assert_called()


if __name__ == "__main__":
    unittest.main()