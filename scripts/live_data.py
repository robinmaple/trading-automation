#!/usr/bin/env python3
"""
SAFE Live Real-Time Market Data Monitoring - NO orders executed.
Fetches real-time IBKR data for given symbols in a low-risk way.
"""

import datetime
import time
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

SYMBOLS = ["META", "TSLA", "AMZN"]
DURATION_MINUTES = 3  # monitoring duration
DATA_TYPE_REALTIME = 1  # 1 = live real-time data


class SafeRealTimeMonitor(EWrapper, EClient):
    def __init__(self, symbols):
        EClient.__init__(self, self)
        self.symbols = symbols
        self.next_req_id = 1
        self.prices = {}
        self.subscribed = set()
        self.end_time = None

    def nextValidId(self, orderId: int):
        print(f"✅ Connected. Next valid order ID: {orderId}")
        self.end_time = datetime.datetime.now() + datetime.timedelta(minutes=DURATION_MINUTES)
        # Request real-time market data type
        self.reqMarketDataType(DATA_TYPE_REALTIME)
        for symbol in self.symbols:
            self.subscribe_symbol(symbol)

    def subscribe_symbol(self, symbol):
        if symbol in self.subscribed:
            return
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        self.reqMktData(self.next_req_id, contract, "", False, False, [])
        print(f"🔍 Subscribed to {symbol} with reqId {self.next_req_id}")
        self.subscribed.add(symbol)
        self.next_req_id += 1

    def tickPrice(self, reqId, tickType, price, attrib):
        if price != 0:
            symbol = list(self.subscribed)[reqId - 1]  # map reqId to symbol
            self.prices[symbol] = price
            print(f"{datetime.datetime.now().strftime('%H:%M:%S')} - {symbol}: ${price:.2f}")

    def run_monitor(self):
        while datetime.datetime.now() < self.end_time:
            time.sleep(1)  # loop delay
        self.disconnect()
        print("✅ Monitoring complete. Disconnected safely.")


if __name__ == "__main__":
    print("⚠️ SAFE: No orders will be executed. Monitoring real-time market data.")
    app = SafeRealTimeMonitor(SYMBOLS)
    app.connect("127.0.0.1", 7496, clientId=999)  # live account
    app.run()
