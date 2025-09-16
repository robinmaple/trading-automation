"""Tests for MarketContextService"""
import unittest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime

from src.services.market_context_service import MarketContextService

class TestMarketContextService(unittest.TestCase):
    def setUp(self):
        self.mock_data_feed = Mock()
        self.service = MarketContextService(self.mock_data_feed)
    
    def test_cache_mechanism(self):
        """Test that caching works correctly."""
        with patch.object(self.service, '_analyze_timeframe_strength') as mock_analyze:
            mock_analyze.return_value = 0.8  # Return a high score

            # First call: should analyze multiple timeframes
            result1 = self.service.get_dominant_timeframe("AAPL")
            first_call_count = mock_analyze.call_count

            # Second call: should use cache (no new calls)
            result2 = self.service.get_dominant_timeframe("AAPL")

            self.assertEqual(result1, result2)
            self.assertEqual(mock_analyze.call_count, first_call_count)  # no increase in calls
