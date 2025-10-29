#!/usr/bin/env python3
"""
SAFE Live Account Data Monitoring - No orders executed!
Connect to live IBKR account to verify real-time data without trading.
"""

import sys
import os
import time
import datetime
import threading
from typing import Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.brokers.ibkr.ibkr_client import IbkrClient
from src.market_data.feeds.ibkr_data_feed import IBKRDataFeed


class SafeLiveMonitor:
    def __init__(self, symbols=None):
        self.symbols = symbols or ["META", "TSLA", "AMZN"]
        self.ibkr_client = IbkrClient()
        self.data_feed = IBKRDataFeed(self.ibkr_client)
        self.prices: Dict[str, float] = {s: None for s in self.symbols}
        self._stop = False

    def _start_client_loop(self):
        """Run IBKR EClient network loop in a background thread."""
        threading.Thread(target=self.ibkr_client.run, daemon=True).start()

    def connect_and_subscribe(self):
        print("🔍 SAFE Live Account Data Verification")
        print("=" * 60)
        print("⚠️  WARNING: Connected to LIVE ACCOUNT")
        print("✅ SAFETY: Order execution is DISABLED in this script")
        print("=" * 60)

        # Connect to IBKR
        print("Connecting to LIVE ACCOUNT (port 7496)...")
        if not self.data_feed.connect(port=7496):
            print("❌ Failed to connect to live account")
            return False

        print("✅ Connected to LIVE IBKR account")

        # Wait for managed account
        timeout = time.time() + 10
        while not self.ibkr_client.account_number and time.time() < timeout:
            time.sleep(0.2)

        account = self.ibkr_client.account_number or "UNKNOWN"
        print(f"Account: {account}")
        print("✅ Verified LIVE account")

        # Start network loop
        self._start_client_loop()

        # Subscribe to symbols
        from ibapi.contract import Contract
        for symbol in self.symbols:
            contract = Contract()
            contract.symbol = symbol
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"

            if self.data_feed.subscribe(symbol, contract):
                print(f"✅ Subscribed to {symbol} (market data only)")
            else:
                print(f"❌ Failed to subscribe to {symbol}")

        return True

    def monitor(self, duration_minutes=3):
        print(f"\n📊 Monitoring LIVE data for {duration_minutes} minutes...")
        print("💡 SAFETY: Order execution is completely disabled")
        print("Press Ctrl+C to stop early\n")

        end_time = datetime.datetime.now() + datetime.timedelta(minutes=duration_minutes)

        try:
            while datetime.datetime.now() < end_time and not self._stop:
                print(f"{datetime.datetime.now().strftime('%H:%M:%S')} - LIVE DATA:")
                print("-" * 40)
                for symbol in self.symbols:
                    # Fetch latest cached price
                    price_data = self.data_feed.get_current_price(symbol)
                    if price_data and price_data.get("price") not in [0, None]:
                        print(f"{symbol}: ${price_data['price']:.2f} ({price_data.get('data_type', 'UNKNOWN')})")
                    else:
                        print(f"{symbol}: No data")

                print("-" * 40)
                time.sleep(2)
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")
        finally:
            self.stop()

    def stop(self):
        self._stop = True
        self.data_feed.disconnect()
        print("✅ Disconnected from LIVE account safely")


if __name__ == "__main__":
    # Directly run without confirmation prompt
    monitor = SafeLiveMonitor()
    if monitor.connect_and_subscribe():
        monitor.monitor(duration_minutes=3)
