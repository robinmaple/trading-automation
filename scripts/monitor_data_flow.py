#!/usr/bin/env python3
"""
Real-time monitoring script to validate IBKR data flow
"""
import sys
import os
import time
import datetime

# Add the project root to Python path so we can import src modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_feeds.ibkr_data_feed import IBKRDataFeed
from src.core.ibkr_client import IbkrClient

def monitor_data_flow(symbols=["SPY", "AAPL", "MSFT"], duration_minutes=2):
    """Monitor real-time data flow from IBKR"""
    print("🔍 Starting IBKR Data Flow Monitor")
    print("=" * 50)
    
    # Initialize IBKR client and data feed
    ibkr_client = IbkrClient()
    data_feed = IBKRDataFeed(ibkr_client)
    
    # Connect to IBKR
    if not data_feed.connect():
        print("❌ Failed to connect to IBKR")
        print("💡 Make sure TWS or IB Gateway is running on port 7497")
        return
    
    print("✅ Connected to IBKR")
    
    # Subscribe to symbols
    for symbol in symbols:
        # Create simple contract for testing
        from ibapi.contract import Contract
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        if data_feed.subscribe(symbol, contract):
            print(f"✅ Subscribed to {symbol}")
        else:
            print(f"❌ Failed to subscribe to {symbol}")
    
    # Monitor data for specified duration
    end_time = datetime.datetime.now() + datetime.timedelta(minutes=duration_minutes)
    update_count = {symbol: 0 for symbol in symbols}
    last_prices = {symbol: None for symbol in symbols}
    
    print(f"\n📊 Monitoring data for {duration_minutes} minutes...")
    print("Press Ctrl+C to stop early\n")
    
    try:
        while datetime.datetime.now() < end_time:
            print(f"\n{datetime.datetime.now().strftime('%H:%M:%S')} - Checking prices:")
            print("-" * 40)
            
            for symbol in symbols:
                price_data = data_feed.get_current_price(symbol)
                
                if price_data and price_data.get('price') not in [0, None]:
                    update_count[symbol] += 1
                    current_price = price_data['price']
                    previous_price = last_prices[symbol]
                    
                    # Show price change if we have previous data
                    change_str = ""
                    if previous_price is not None:
                        change = current_price - previous_price
                        change_pct = (change / previous_price) * 100
                        change_str = f" ({change:+.2f} [{change_pct:+.2f}%])"
                    
                    print(f"{symbol}: ${current_price:.2f}{change_str} "
                          f"(updates: {update_count[symbol]})")
                    
                    last_prices[symbol] = current_price
                else:
                    print(f"{symbol}: No data received")
            
            print("-" * 40)
            time.sleep(2)  # Check every 2 seconds
            
    except KeyboardInterrupt:
        print("\n⏹️  Monitoring stopped by user")
    
    # Print summary
    print("\n" + "=" * 50)
    print("📈 MONITORING SUMMARY")
    print("=" * 50)
    total_updates = sum(update_count.values())
    print(f"Total updates received: {total_updates}")
    
    for symbol in symbols:
        status = "✅" if update_count[symbol] > 0 else "❌"
        print(f"{status} {symbol}: {update_count[symbol]} updates")
    
    if total_updates > 0:
        print("\n🎉 SUCCESS: Real IBKR data is flowing!")
        print("Your system is now using live market data instead of mock data.")
    else:
        print("\n⚠️  WARNING: No data received")
        print("Check that:")
        print("1. TWS/IB Gateway is running")
        print("2. You're connected to the correct port (7497 for paper, 7496 for live)")
        print("3. Market data permissions are enabled for your account")
    
    data_feed.disconnect()
    print("✅ Disconnected from IBKR")

if __name__ == "__main__":
    monitor_data_flow(duration_minutes=2)  # Short test run