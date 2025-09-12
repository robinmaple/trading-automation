#!/usr/bin/env python3
"""
SAFE Live Account Data Monitoring - No orders executed!
Connect to live IBKR account to verify real-time data without trading.
"""
import sys
import os
import time
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.ibkr_client import IbkrClient
from src.data_feeds.ibkr_data_feed import IBKRDataFeed

def monitor_live_data_safe(symbols=["SPY", "AAPL", "MSFT"], duration_minutes=5):
    """Monitor live account data WITHOUT executing any orders"""
    print("🔍 SAFE Live Account Data Verification")
    print("=" * 60)
    print("⚠️  WARNING: Connected to LIVE ACCOUNT")
    print("✅ SAFETY: Order execution is DISABLED in this script")
    print("=" * 60)
    
    # Initialize but DON'T use trading manager (to avoid accidental execution)
    ibkr_client = IbkrClient()
    data_feed = IBKRDataFeed(ibkr_client)
    
    # Connect to LIVE ACCOUNT (port 7496)
    print("Connecting to LIVE ACCOUNT (port 7496)...")
    if not data_feed.connect(port=7496):
        print("❌ Failed to connect to live account")
        return
    
    print("✅ Connected to LIVE IBKR account")
    print(f"Account: {ibkr_client.account_number}")
    print(f"Paper Trading: {ibkr_client.is_paper_account}")
    
    # Verify this is a LIVE account (should start with 'U')
    if ibkr_client.account_number.startswith('DU'):
        print("❌ WARNING: Connected to PAPER account instead of LIVE!")
        print("   Check your TWS/IB Gateway is configured for live trading")
        data_feed.disconnect()
        return
    
    # Test account value (read-only)
    account_value = ibkr_client.get_account_value()
    print(f"Account Value: ${account_value:,.2f}")
    
    # Subscribe to symbols for market data only
    for symbol in symbols:
        from ibapi.contract import Contract
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        if data_feed.subscribe(symbol, contract):
            print(f"✅ Subscribed to {symbol} (market data only)")
        else:
            print(f"❌ Failed to subscribe to {symbol}")
    
    # Monitor data - NO ORDER EXECUTION
    end_time = datetime.datetime.now() + datetime.timedelta(minutes=duration_minutes)
    print(f"\n📊 Monitoring LIVE data for {duration_minutes} minutes...")
    print("💡 SAFETY: Order execution is completely disabled")
    print("Press Ctrl+C to stop early\n")
    
    try:
        while datetime.datetime.now() < end_time:
            print(f"\n{datetime.datetime.now().strftime('%H:%M:%S')} - LIVE DATA:")
            print("-" * 40)
            
            any_real_time_data = False
            
            for symbol in symbols:
                price_data = data_feed.get_current_price(symbol)
                
                if price_data and price_data.get('price') not in [0, None]:
                    current_price = price_data['price']
                    data_type = price_data.get('data_type', 'UNKNOWN')
                    
                    print(f"{symbol}: ${current_price:.2f} ({data_type})")
                    
                    if 'DELAYED' not in data_type and 'FROZEN' not in data_type:
                        any_real_time_data = True
                else:
                    print(f"{symbol}: No data")
            
            if any_real_time_data:
                print("🎉 REAL-TIME DATA FLOWING! - Your connection is working!")
            else:
                print("💡 Data received but may be delayed (check market hours)")
            
            print("-" * 40)
            time.sleep(3)  # Check every 3 seconds
            
    except KeyboardInterrupt:
        print("\n⏹️  Monitoring stopped by user")
    
    # Clean disconnect
    data_feed.disconnect()
    print("✅ Disconnected from LIVE account safely")

if __name__ == "__main__":
    # Add confirmation prompt for safety
    print("⚠️  WARNING: This will connect to your LIVE IBKR account")
    response = input("Continue? (yes/no): ").strip().lower()
    
    if response in ['yes', 'y']:
        monitor_live_data_safe(duration_minutes=3)  # Short test
    else:
        print("❌ Script cancelled - no connection made")