# tests/scanner/test_scan_manager.py
import pytest
from unittest.mock import Mock

class TestScanManager:
    """Test the high-level scan manager"""
    
    def test_manager_initialization(self, mock_ibkr_adapter):
        from src.scanner.scan_manager import ScanManager
        
        manager = ScanManager(mock_ibkr_adapter)
        
        # Verify all components initialized
        assert hasattr(manager, 'scanner')
        assert hasattr(manager, 'candidate_generator')
        assert hasattr(manager, 'strategy_registry')
        assert hasattr(manager, 'criteria_registry')
    
    def test_candidate_generation_workflow(self, mock_ibkr_adapter):
        from src.scanner.scan_manager import ScanManager
        from config.scanner_config import ScanResult
        from datetime import datetime
        
        manager = ScanManager(mock_ibkr_adapter)
        
        # Mock scan results
        mock_scan_results = [
            ScanResult(
                symbol='AAPL',
                total_score=85,
                bull_trend_score=90,
                bull_pullback_score=80,
                current_price=182.50,
                volume_status='✅',
                market_cap_status='✅',
                price_status='✅',
                ema_values={10: 180, 20: 175, 50: 170, 100: 160},
                last_updated=datetime.now()
            )
        ]
        
        # Test candidate generation
        candidates = manager.candidate_generator.generate_candidates(
            mock_scan_results,
            strategy_names=['bull_trend_pullback'],
            min_confidence=60
        )
        
        assert isinstance(candidates, list)
        # Should handle gracefully even if no strategies match
    
    def test_configuration_management(self, mock_ibkr_adapter):
        from src.scanner.scan_manager import ScanManager
        from config.scanner_config import ScannerConfig
        
        manager = ScanManager(mock_ibkr_adapter)
        
        # Test config statistics
        stats = manager.get_scan_statistics()
        assert 'configuration' in stats
        assert 'available_strategies' in stats
        
        # Test config update
        new_config = ScannerConfig(min_volume=2_000_000, min_confidence_score=70)
        manager.update_configuration(new_config)
        
        updated_stats = manager.get_scan_statistics()
        assert updated_stats['configuration']['min_volume'] == 2_000_000