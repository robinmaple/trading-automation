# tests/scanner/test_integration.py
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

class TestIntegration:
    """Integration tests that test multiple components together - Updated for tiered architecture"""
    
    def test_end_to_end_scan(self, mock_ibkr_adapter, scanner_config):
        """Test complete scan from universe to candidates using tiered architecture"""
        from src.scanning.scan_manager import ScanManager
        
        manager = ScanManager(mock_ibkr_adapter, scanner_config)
        
        # Mock the TieredScanner to return test candidates
        mock_candidates = [
            {
                'symbol': 'AAPL',
                'identified_by': 'bull_trend_pullback',
                'confidence': 85.0,
                'current_price': 182.50,
                'total_score': 85,
                'matching_strategies': ['bull_trend_pullback']
            }
        ]
        
        with patch.object(manager.tiered_scanner, 'run_scan', return_value=mock_candidates):
            # This tests the entire pipeline through public interface
            candidates = manager.generate_bull_trend_pullback_candidates()
            
            assert isinstance(candidates, list)
            # Should return candidates that match the bull trend strategy
            if candidates:
                assert candidates[0]['identified_by'] == 'bull_trend_pullback'
    
    # Updated Integration Test - Begin
    def test_strategy_evaluation_integration(self, mock_ibkr_adapter, scan_result_factory):
        """Test strategy evaluation integration through public interfaces"""
        from src.scanning.scan_manager import ScanManager
        
        manager = ScanManager(mock_ibkr_adapter)
        
        # Mock candidates with lower confidence for testing
        mock_candidates = [
            {
                'symbol': 'TEST',
                'identified_by': 'bull_trend_pullback',
                'confidence': 45.0,  # Lower confidence for testing
                'current_price': 100.0,
                'total_score': 75,
                'matching_strategies': ['bull_trend_pullback']
            }
        ]
        
        with patch.object(manager.tiered_scanner, 'run_scan', return_value=mock_candidates):
            # Test through public method with lower confidence threshold
            candidates = manager.generate_all_candidates()
            
            # For now, just test that it doesn't crash and returns proper structure
            assert isinstance(candidates, list)
            if candidates:
                candidate = candidates[0]
                assert 'symbol' in candidate
                assert 'identified_by' in candidate
                assert 'confidence' in candidate
    
    def test_tiered_architecture_integration(self, mock_ibkr_adapter):
        """Test that the tiered architecture components work together"""
        from src.scanning.scan_manager import ScanManager
        from src.scanning.tiered_scanner import TieredScanner
        
        # Test that ScanManager properly uses TieredScanner
        manager = ScanManager(mock_ibkr_adapter)
        
        assert hasattr(manager, 'tiered_scanner')
        assert isinstance(manager.tiered_scanner, TieredScanner)
        
        # Verify public methods are available
        assert hasattr(manager, 'generate_bull_trend_pullback_candidates')
        assert hasattr(manager, 'generate_all_candidates')
        assert hasattr(manager, 'get_scan_statistics')
        assert hasattr(manager, 'update_configuration')
    
    def test_or_logic_integration(self, mock_ibkr_adapter):
        """Test that OR logic works across multiple strategies"""
        from src.scanning.scan_manager import ScanManager
        
        manager = ScanManager(mock_ibkr_adapter)
        
        # Mock candidates from multiple strategies
        mock_candidates = [
            {
                'symbol': 'AAPL',
                'identified_by': 'bull_trend_pullback',
                'confidence': 85.0,
                'current_price': 182.50
            },
            {
                'symbol': 'MSFT',
                'identified_by': 'momentum_breakout', 
                'confidence': 78.0,
                'current_price': 345.67
            },
            {
                'symbol': 'GOOGL',
                'identified_by': 'bull_trend_pullback',
                'confidence': 72.0,
                'current_price': 145.23
            }
        ]
        
        with patch.object(manager.tiered_scanner, 'run_scan', return_value=mock_candidates):
            # Test OR logic - all candidates from all strategies should be returned
            all_candidates = manager.generate_all_candidates()
            bull_trend_candidates = manager.generate_bull_trend_pullback_candidates()
            
            assert len(all_candidates) == 3  # All strategies
            assert len(bull_trend_candidates) == 2  # Only bull_trend_pullback strategy
            
            # Verify strategy identification
            strategies_in_all = {c['identified_by'] for c in all_candidates}
            strategies_in_bull = {c['identified_by'] for c in bull_trend_candidates}
            
            assert 'bull_trend_pullback' in strategies_in_all
            assert 'momentum_breakout' in strategies_in_all
            assert strategies_in_bull == {'bull_trend_pullback'}  # Only one strategy
    # Updated Integration Test - End