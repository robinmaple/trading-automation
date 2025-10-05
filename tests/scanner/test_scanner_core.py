# tests/scanner/test_scanner_core.py
import pytest
import pandas as pd

class TestScannerCore:
    """Test the main scanner engine - Updated for Tier 1 architecture"""
    
    def test_scanner_initialization(self, mock_ibkr_adapter, scanner_config):
        from src.scanner.scanner_core import StockScanner
        
        scanner = StockScanner(mock_ibkr_adapter, scanner_config)
        assert scanner.config == scanner_config
        assert scanner.data_adapter == mock_ibkr_adapter
    
    # Updated Test Methods - Begin
    def test_complete_scan_workflow(self, mock_ibkr_adapter, scanner_config):
        from src.scanner.scanner_core import StockScanner
        
        scanner = StockScanner(mock_ibkr_adapter, scanner_config)
        scan_results = scanner.run_scan()
        
        # Verify results structure - now returns List[ScanResult]
        assert isinstance(scan_results, list)
        
        if scan_results:
            # Verify ScanResult structure for Tier 1 data
            first_result = scan_results[0]
            assert hasattr(first_result, 'symbol')
            assert hasattr(first_result, 'current_price')
            assert hasattr(first_result, 'volume')
            assert hasattr(first_result, 'market_cap')
            assert hasattr(first_result, 'ema_values')
            assert hasattr(first_result, 'historical_data')
            
            # Verify data types
            assert isinstance(first_result.symbol, str)
            assert isinstance(first_result.current_price, (int, float))
            assert isinstance(first_result.volume, (int, float))
            assert isinstance(first_result.market_cap, (int, float))
            assert isinstance(first_result.ema_values, dict)
            assert isinstance(first_result.historical_data, pd.DataFrame)
            
            # Note: Strategy scores are now handled by StrategyOrchestrator in Tier 2
    
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
            # Verify Tier 1 data structure
            assert result.symbol == 'TEST'
            assert result.current_price == 100.0
            assert result.volume == 5_000_000
            assert result.market_cap == 50_000_000_000
            assert isinstance(result.ema_values, dict)
            assert isinstance(result.historical_data, pd.DataFrame)
            
            # Verify price and volume data for strategy processing
            assert 'current' in result.price_data
            assert 'historical' in result.price_data
            assert 'current' in result.volume_data
    
    def test_scan_dataframe_compatibility(self, mock_ibkr_adapter, scanner_config):
        """Test legacy DataFrame output for backward compatibility"""
        from src.scanner.scanner_core import StockScanner
        
        scanner = StockScanner(mock_ibkr_adapter, scanner_config)
        results_df = scanner.run_scan_dataframe()
        
        # Verify DataFrame structure
        assert isinstance(results_df, pd.DataFrame)
        
        if not results_df.empty:
            expected_columns = [
                'symbol', 'current_price', 'volume', 'market_cap', 'last_updated'
            ]
            assert all(col in results_df.columns for col in expected_columns)
            
            # Verify EMA columns are present
            ema_columns = [col for col in results_df.columns if col.startswith('ema_')]
            assert len(ema_columns) > 0
    
    def test_tier_1_data_quality(self, mock_ibkr_adapter, scanner_config):
        """Test that Tier 1 scanner provides quality data for strategy processing"""
        from src.scanner.scanner_core import StockScanner
        
        scanner = StockScanner(mock_ibkr_adapter, scanner_config)
        scan_results = scanner.run_scan()
        
        if scan_results:
            for result in scan_results:
                # Verify data completeness for strategy evaluation
                assert result.symbol, "Symbol should not be empty"
                assert result.current_price > 0, "Price should be positive"
                assert result.volume >= 0, "Volume should be non-negative"
                assert len(result.ema_values) > 0, "EMA values should be calculated"
                assert not result.historical_data.empty, "Historical data should not be empty"
                
                # Verify price data structure
                assert 'current' in result.price_data
                assert 'historical' in result.price_data
                assert len(result.price_data['historical']) > 0
    # Updated Test Methods - End