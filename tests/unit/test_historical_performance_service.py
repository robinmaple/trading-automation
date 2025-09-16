"""Tests for HistoricalPerformanceService"""
import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from src.services.historical_performance_service import HistoricalPerformanceService

class TestHistoricalPerformanceService(unittest.TestCase):
    def setUp(self):
        self.mock_persistence = Mock()
        self.service = HistoricalPerformanceService(self.mock_persistence)
    
    def test_performance_calculation(self):
        """Test performance metrics calculation."""
        mock_trades = [
            {'pnl': 100, 'entry_time': datetime.now(), 'exit_time': datetime.now() + timedelta(hours=1)},
            {'pnl': -50, 'entry_time': datetime.now(), 'exit_time': datetime.now() + timedelta(hours=2)}
        ]
        
        performance = self.service._calculate_performance_metrics(mock_trades)
        
        self.assertEqual(performance['win_rate'], 0.5)
        self.assertEqual(performance['profit_factor'], 2.0)
        self.assertEqual(performance['total_trades'], 2)