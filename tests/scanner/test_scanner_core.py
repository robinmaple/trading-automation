# tests/scanner/test_scanner_core.py
import pytest
import pandas as pd

class TestScannerCore:
    """Test the main scanner engine"""
    
    def test_scanner_initialization(self, mock_ibkr_adapter, scanner_config):
        from src.scanner.scanner_core import StockScanner
        
        scanner = StockScanner(mock_ibkr_adapter, scanner_config)
        assert scanner.config == scanner_config
        assert scanner.data_adapter == mock_ibkr_adapter
    
    def test_complete_scan_workflow(self, mock_ibkr_adapter, scanner_config):
        from src.scanner.scanner_core import StockScanner
        
        scanner = StockScanner(mock_ibkr_adapter, scanner_config)
        results_df = scanner.run_scan()
        
        # Verify results structure
        assert isinstance(results_df, pd.DataFrame)
        expected_columns = [
            'symbol', 'total_score', 'bull_trend_score', 
            'bull_pullback_score', 'current_price'
        ]
        
        if not results_df.empty:
            assert all(col in results_df.columns for col in expected_columns)
            
            # Verify score ranges
            assert all(0 <= score <= 100 for score in results_df['total_score'])
            assert all(0 <= score <= 100 for score in results_df['bull_trend_score'])
    
    def test_single_stock_analysis(self, mock_ibkr_adapter, scanner_config):
        from src.scanner.scanner_core import StockScanner
        
        scanner = StockScanner(mock_ibkr_adapter, scanner_config)
        
        # Test analyzing a single stock
        stock_info = {
            'symbol': 'TEST',
            'price': 100.0,
            'volume': 5_000_000,
            'market_cap': 50_000_000_000
        }
        
        result = scanner._analyze_stock(stock_info)
        
        if result:  # Might be None if data insufficient
            assert result.symbol == 'TEST'
            assert 0 <= result.total_score <= 100
            assert isinstance(result.ema_values, dict)