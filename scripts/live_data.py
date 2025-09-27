from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import datetime
import time

class DataTypeTestApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.data_type = 3  # Start with delayed
        self.test_count = 0
        self.max_tests = 3

    def nextValidId(self, orderId: int):
        if self.test_count >= self.max_tests:
            print("Testing complete.")
            self.disconnect()
            return
            
        data_types = {
            1: "LIVE (Real-time - requires subscription)",
            2: "FROZEN (Last traded when market closed)", 
            3: "DELAYED (15-20 min delayed - free)",
            4: "DELAYED FROZEN"
        }
        
        print(f"\n{'='*50}")
        print(f"TEST {self.test_count + 1}: {data_types[self.data_type]}")
        print(f"{'='*50}")
        
        # Request the market data type
        self.reqMarketDataType(self.data_type)
        
        # Create AAPL contract
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK" 
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        self.reqMktData(orderId, contract, "", False, False, [])
        
    def tickPrice(self, reqId, tickType, price, attrib):
        # Use the correct attribute names for newer API versions
        print(f"Price: ${price}")
        print(f"TickAttrib fields: {dir(attrib)}")  # Show available attributes
        
        # Check available attributes (these vary by API version)
        if hasattr(attrib, 'pastLimit'):
            print(f"Past Limit: {attrib.pastLimit}")
        if hasattr(attrib, 'preOpen'):
            print(f"Pre-open: {attrib.preOpen}")
        if hasattr(attrib, 'canAutoExecute'):
            print(f"Can Auto Execute: {attrib.canAutoExecute}")
        
        # Test next data type after receiving data
        self.cancelMktData(reqId)
        self.test_count += 1
        self.data_type += 1
        if self.data_type <= 4:
            time.sleep(2)
            self.nextValidId(reqId + 1)
        else:
            self.disconnect()

    def tickString(self, reqId, tickType, value):
        if tickType == 45:  # LAST_TIMESTAMP
            timestamp = int(value)
            dt = datetime.datetime.fromtimestamp(timestamp)
            now = datetime.datetime.now()
            time_diff = (now - dt).total_seconds() / 60  # in minutes
            print(f"Last trade was at: {dt}")
            print(f"Current time is:   {now}")
            print(f"Time difference: {time_diff:.1f} minutes")
            
            # Determine data type based on time difference
            if time_diff > 20:
                print("🎯 CONCLUSION: DELAYED DATA (more than 20 minutes old)")
            elif time_diff > 2:
                print("🎯 CONCLUSION: LIKELY DELAYED (2-20 minutes old)")
            else:
                print("🎯 CONCLUSION: REAL-TIME or near real-time data!")

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode == 10167:  # No market data permissions
            print(f"❌ No permissions for data type {self.data_type}")
            # Skip to next test
            self.test_count += 1
            self.data_type += 1
            if self.data_type <= 4:
                time.sleep(1)
                self.nextValidId(reqId + 1)
        elif errorCode not in [2104, 2106, 2158]:
            print(f"Error {errorCode}: {errorString}")

# Run the test
app = DataTypeTestApp()
app.connect("127.0.0.1", 7496, 0)
app.run()