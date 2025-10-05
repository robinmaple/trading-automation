# tests/scanner/conftest.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock
import sys
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

@pytest.fixture
def mock_historical_data():
    """Generate realistic mock historical data for tests"""
    dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
    
    # Create price series that will produce good test results
    base_price = 150.0
    trend = np.linspace(0, 0.15, 100)
    noise = np.random.normal(0, 0.008, 100)
    prices = base_price * (1 + trend + noise)
    
    return pd.DataFrame({
        'open': prices * (1 + np.random.normal(0, 0.005, 100)),
        'high': prices * (1 + np.abs(np.random.normal(0, 0.008, 100))),
        'low': prices * (1 - np.abs(np.random.normal(0, 0.008, 100))),
        'close': prices,
        'volume': np.random.randint(2_000_000, 8_000_000, 100)
    }, index=dates)

@pytest.fixture
def scanner_config():
    """Scanner config for tests"""
    from config.scanner_config import ScannerConfig
    return ScannerConfig(max_symbols_to_scan=5)  # Small number for fast tests

@pytest.fixture
def mock_ibkr_adapter(mock_historical_data):
    """Mock IBKR adapter for tests"""
    adapter = Mock()
    
    # Mock universe data
    adapter.get_dynamic_universe.return_value = [
        {
            'symbol': 'AAPL',
            'price': 182.50,
            'volume': 25_000_000,
            'market_cap': 2_800_000_000_000
        },
        {
            'symbol': 'MSFT',
            'price': 330.25, 
            'volume': 18_000_000,
            'market_cap': 2_500_000_000_000
        },
        {
            'symbol': 'GOOGL',
            'price': 135.75,
            'volume': 12_000_000,
            'market_cap': 1_700_000_000_000
        }
    ]
    
    # Mock historical data
    adapter.get_historical_data.return_value = mock_historical_data
    
    return adapter

@pytest.fixture
def scan_result_factory():
    """Factory to create scan results for testing"""
    def create_scan_result(symbol="TEST", trend_score=80, pullback_score=80, 
                         current_price=100.0, ema_values=None):
        from config.scanner_config import ScanResult
        from datetime import datetime
        
        if ema_values is None:
            ema_values = {10: 99, 20: 98, 50: 95, 100: 90}
            
        return ScanResult(
            symbol=symbol,
            total_score=int(0.6 * trend_score + 0.4 * pullback_score),
            bull_trend_score=trend_score,
            bull_pullback_score=pullback_score,
            current_price=current_price,
            volume_status='✅',
            market_cap_status='✅',
            price_status='✅',
            ema_values=ema_values,
            last_updated=datetime.now()
        )
    return create_scan_result