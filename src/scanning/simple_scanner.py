# src/scanner/simple_scanner.py
import sys
import os

# Add project root to path - FIXED PATH
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

# Now use absolute imports
from src.scanning.scan_manager import ScanManager
from config.scanner_config import ScannerConfig

def simple_bull_trend_pullback_scan(ibkr_client, data_feed):
    """
    Simple one-function call to get bull trend pullback candidates
    """
    from src.market_data.managers.market_data_manager import MarketDataManager
    from src.services.market_context_service import MarketContextService
    from src.scanning.integration.ibkr_data_adapter import IBKRDataAdapter
    
    market_data_manager = MarketDataManager(ibkr_client)
    market_context_service = MarketContextService(data_feed)
    data_adapter = IBKRDataAdapter(market_data_manager, market_context_service)
    
    scan_manager = ScanManager(data_adapter)
    
    stats = scan_manager.get_scan_statistics()
    print("🔧 Scanner Configuration:")
    print(f"   Volume: > {stats['configuration']['min_volume']:,}")
    print(f"   Market Cap: > ${stats['configuration']['min_market_cap']:,.0f}")
    print(f"   Price: > ${stats['configuration']['min_price']:.2f}")
    print()
    
    candidates = scan_manager.generate_bull_trend_pullback_candidates()
    
    print(f"🎯 Found {len(candidates)} Bull Trend Pullback Candidates:")
    print("=" * 80)
    
    for i, candidate in enumerate(candidates, 1):
        print(f"{i}. {candidate['symbol']}")
        print(f"   Confidence: {candidate['confidence']:.1f}% | "
              f"Price: ${candidate['current_price']:.2f}")
        print(f"   Trend Score: {candidate['trend_score']} | "
              f"Pullback Score: {candidate['pullback_score']}")
        print()
    
    return candidates

# Test with mock data
if __name__ == "__main__":
    print("🧪 Testing scanner with mock data...")
    from unittest.mock import Mock
    
    mock_client = Mock()
    mock_feed = Mock()
    
    try:
        candidates = simple_bull_trend_pullback_scan(mock_client, mock_feed)
        print(f"✅ Scanner test completed! Found {len(candidates)} candidates")
    except Exception as e:
        print(f"❌ Scanner test failed: {e}")
        import traceback
        traceback.print_exc()