# tests/scanner/test_scan_manager.py
import pytest
from unittest.mock import Mock, patch

class TestScanManager:
    """Test the high-level scan manager - Updated for TieredScanner architecture"""
    
    def test_manager_initialization(self, mock_ibkr_adapter):
        from src.scanner.scan_manager import ScanManager
        
        manager = ScanManager(mock_ibkr_adapter)
        
        # Verify TieredScanner is initialized (not individual components)
        assert hasattr(manager, 'tiered_scanner')
        assert hasattr(manager, 'data_adapter')
        assert hasattr(manager, 'scanner_config')
        
        # Internal components are now encapsulated in TieredScanner
        assert not hasattr(manager, 'scanner')  # No longer direct access
        assert not hasattr(manager, 'candidate_generator')  # No longer direct access
        assert not hasattr(manager, 'strategy_registry')  # No longer direct access
    
    # Updated Test Methods - Begin
    def test_candidate_generation_workflow(self, mock_ibkr_adapter):
        from src.scanner.scan_manager import ScanManager
        
        manager = ScanManager(mock_ibkr_adapter)
        
        # Mock the TieredScanner to return test candidates
        mock_candidates = [
            {
                'symbol': 'AAPL',
                'identified_by': 'bull_trend_pullback',
                'confidence': 85.0,
                'current_price': 182.50,
                'total_score': 85,
                'strategy_type': 'bull_trend',
                'matching_strategies': ['bull_trend_pullback']
            }
        ]
        
        with patch.object(manager.tiered_scanner, 'run_scan', return_value=mock_candidates):
            # Test candidate generation through public interface
            candidates = manager.generate_bull_trend_pullback_candidates()
            
            assert isinstance(candidates, list)
            if candidates:
                candidate = candidates[0]
                assert candidate['symbol'] == 'AAPL'
                assert candidate['identified_by'] == 'bull_trend_pullback'
                assert candidate['confidence'] >= 0
    
    def test_all_candidates_generation(self, mock_ibkr_adapter):
        """Test generating candidates from all strategies with OR logic"""
        from src.scanner.scan_manager import ScanManager
        
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
            }
        ]
        
        with patch.object(manager.tiered_scanner, 'run_scan', return_value=mock_candidates):
            candidates = manager.generate_all_candidates()
            
            assert isinstance(candidates, list)
            assert len(candidates) == 2
            # Verify OR logic - both strategies' candidates are included
            strategies = {candidate['identified_by'] for candidate in candidates}
            assert 'bull_trend_pullback' in strategies
            assert 'momentum_breakout' in strategies
    
    def test_configuration_management(self, mock_ibkr_adapter):
        from src.scanner.scan_manager import ScanManager
        from config.scanner_config import ScannerConfig
        
        manager = ScanManager(mock_ibkr_adapter)
        
        # Test config statistics
        stats = manager.get_scan_statistics()
        assert 'configuration' in stats
        assert 'architecture' in stats
        assert stats['architecture'] == '2-Tier (Basic Screening + Strategy OR Logic)'
        assert 'available_strategies' in stats
        
        # Test config update delegates to TieredScanner
        new_config = ScannerConfig(min_volume=2_000_000, min_confidence_score=70)
        
        with patch.object(manager.tiered_scanner, 'update_config') as mock_update:
            manager.update_configuration(new_config)
            mock_update.assert_called_once_with(new_config)
    
    def test_empty_results_handling(self, mock_ibkr_adapter):
        """Test graceful handling when no candidates are found"""
        from src.scanner.scan_manager import ScanManager
        
        manager = ScanManager(mock_ibkr_adapter)
        
        # Mock empty results
        with patch.object(manager.tiered_scanner, 'run_scan', return_value=[]):
            bull_trend_candidates = manager.generate_bull_trend_pullback_candidates()
            all_candidates = manager.generate_all_candidates()
            
            assert bull_trend_candidates == []
            assert all_candidates == []
    
    def test_strategy_identification_in_candidates(self, mock_ibkr_adapter):
        """Test that candidates include strategy identification"""
        from src.scanner.scan_manager import ScanManager
        
        manager = ScanManager(mock_ibkr_adapter)
        
        mock_candidates = [
            {
                'symbol': 'AAPL',
                'identified_by': 'bull_trend_pullback',
                'confidence': 85.0,
                'current_price': 182.50,
                'matching_strategies': ['bull_trend_pullback'],
                'strategy_type': 'bull_trend'
            }
        ]
        
        with patch.object(manager.tiered_scanner, 'run_scan', return_value=mock_candidates):
            candidates = manager.generate_all_candidates()
            
            if candidates:
                candidate = candidates[0]
                # Verify strategy identification is present
                assert 'identified_by' in candidate
                assert 'matching_strategies' in candidate
                assert 'strategy_type' in candidate
                assert candidate['identified_by'] == 'bull_trend_pullback'
    # Updated Test Methods - End