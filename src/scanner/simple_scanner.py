# examples/simple_scanner_usage.py
from src.scanner.scan_manager import ScanManager
from src.scanner.scanner_config import ScannerConfig

def simple_bull_trend_pullback_scan(ibkr_client, data_feed):
    """
    Simple one-function call to get bull trend pullback candidates
    
    Args:
        ibkr_client: Your actual IbkrClient instance from main()
        data_feed: Your actual IBKRDataFeed instance from main()
    """
    # Setup your data adapter using ACTUAL instances from your app
    from src.core.market_data_manager import MarketDataManager
    from src.services.market_context_service import MarketContextService
    from src.scanner.integration.ibkr_data_adapter import IBKRDataAdapter
    
    # Use the ACTUAL instances from your main()
    market_data_manager = MarketDataManager(ibkr_client)  # Use real ibkr_client
    market_context_service = MarketContextService(data_feed)  # Use real data_feed
    data_adapter = IBKRDataAdapter(market_data_manager, market_context_service)
    
    # Create ScanManager with default config
    scan_manager = ScanManager(data_adapter)
    
    # Get configuration info
    stats = scan_manager.get_scan_statistics()
    print("🔧 Scanner Configuration:")
    print(f"   Volume: > {stats['configuration']['min_volume']:,}")
    print(f"   Market Cap: > ${stats['configuration']['min_market_cap']:,.0f}")
    print(f"   Price: > ${stats['configuration']['min_price']:.2f}")
    print(f"   EMA Periods: {stats['configuration']['ema_periods']}")
    print(f"   Pullback Threshold: {stats['configuration']['pullback_threshold']}")
    print()
    
    # Generate candidates with ONE simple call
    candidates = scan_manager.generate_bull_trend_pullback_candidates()
    
    # Display results
    print(f"🎯 Found {len(candidates)} Bull Trend Pullback Candidates:")
    print("=" * 80)
    
    for i, candidate in enumerate(candidates, 1):
        print(f"{i}. {candidate['symbol']}")
        print(f"   Confidence: {candidate['confidence']:.1f}% | "
              f"Price: ${candidate['current_price']:.2f}")
        print(f"   Trend Score: {candidate['trend_score']} | "
              f"Pullback Score: {candidate['pullback_score']}")
        print(f"   Setup: {candidate['metadata'].get('setup_quality', 'N/A')} | "
              f"Risk: {candidate.get('risk_level', 'N/A')}")
        print(f"   Signal: {candidate.get('entry_signal', 'N/A')}")
        print()
    
    return candidates