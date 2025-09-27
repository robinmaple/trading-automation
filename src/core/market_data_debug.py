# src/core/market_data_debug.py

import datetime

class MarketDataDebugger:
    def __init__(self, client):
        self.client = client

    # symbols is now optional
    def run_comprehensive_diagnostic(self, symbols=None):
        if symbols is None:
            symbols = []  # empty list if nothing passed

        print("="*60)
        print("RUNNING MARKET DATA DIAGNOSTIC FOR IBKR SUPPORT")
        print("="*60)

        summary = {
            "connected": self.client.isConnected() if hasattr(self.client, "isConnected") else False,
            "account": getattr(self.client, "account_number", "UNKNOWN"),
            "paper_trading": getattr(self.client, "is_paper", False),
            "results": []
}
        for symbol in symbols:
            try:
                contract = self.client.make_contract(
                    symbol,
                    secType="STK",
                    currency="USD",
                    exchange="SMART"
                )
            except AttributeError:
                print(f"❌ Failed to create contract for {symbol}: "
                      f"'{type(self.client).__name__}' object has no attribute 'make_contract'")
                summary["results"].append({"symbol": symbol, "success": False})
                continue

            try:
                success, price_data = self.client.subscribe_market_data(contract)
                if success:
                    print(f"✅ Subscribed to {symbol} with REAL-TIME data")
                else:
                    print(f"❌ Failed to subscribe to {symbol}")
                summary["results"].append({"symbol": symbol, "success": success, "price_data": price_data})
            except Exception as e:
                print(f"❌ Error testing market data for {symbol}: {e}")
                summary["results"].append({"symbol": symbol, "success": False})

        print("="*60)
        print("DIAGNOSTIC SUMMARY:")
        print(f"Connected: {summary['connected']}")
        print(f"Account: {summary['account']}")
        print(f"Paper Trading: {summary['paper_trading']}")
        print(f"Test Symbols: {len(symbols)}")
        success_count = sum(1 for r in summary["results"] if r.get("success"))
        print(f"Successful Data: {success_count}")
        print(f"Failed Data: {len(symbols) - success_count}")
        print("="*60)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"ibkr_market_data_diagnostic_{timestamp}.log"
        with open(log_file, "w") as f:
            f.write(str(summary))
        print(f"✅ Diagnostic complete! Report saved to: {log_file}")

        return summary
