from ibapi.client import *
from ibapi.wrapper import *
import datetime
import time
import threading
from ibapi.ticktype import TickTypeEnum

port = 7497

class TestApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextValidOrderId = None
        self.dataReceived = False
        self.mycontract = None

    def nextValidId(self, orderId: OrderId):
        print(f"Next valid order ID: {orderId}")
        self.nextValidOrderId = orderId

        # Build the contract
        self.mycontract = Contract()
        self.mycontract.symbol = "AAPL"
        self.mycontract.secType = "STK"
        self.mycontract.exchange = "SMART"
        self.mycontract.currency = "USD"

        # Set to delayed data
        self.reqMarketDataType(3)
        print("Requesting market data...")
        
        # Request market data
        self.reqMktData(1, self.mycontract, "", False, False, [])
        
        # Set a timer to place the order anyway if no data arrives
        threading.Timer(5.0, self.placeOrderIfNoData).start()

    def placeOrderIfNoData(self):
        """Safety method to place order if market data doesn't arrive"""
        if not self.dataReceived:
            print("No market data received within timeout. Placing order anyway...")
            self.placeOrder(self.nextValidOrderId, self.mycontract, self.createOrder())

    def error(self, reqId, errorCode, errorString, advancedOrderReject=""):
        print(f"Error. reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}")

    def tickPrice(self, reqId, tickType, price, attrib):
        tick_name = TickTypeEnum.toStr(tickType)
        print(f"Market Data: {tick_name}: {price}")
        
        # DEBUG: Print the tickType number to verify
        print(f"DEBUG: tickType = {tickType}")
        
        # Trigger on LAST_PRICE (4) or if we get any valid price > 0
        if not self.dataReceived and (tickType == 1 or tickType == 2 or price > 0):
            self.dataReceived = True
            print(f"Received market data ({tick_name}). Now placing order...")
            self.placeOrder(self.nextValidOrderId, self.mycontract, self.createOrder())

    def tickSize(self, reqId, tickType, size):
        print(f"Market Data Size: {TickTypeEnum.toStr(tickType)}: {size}")

    def createOrder(self):
        myorder = Order()
        myorder.orderId = self.nextValidOrderId
        myorder.action = "BUY"
        myorder.tif = "GTC"
        myorder.orderType = "LMT"
        myorder.lmtPrice = 232
        myorder.totalQuantity = 1
        return myorder

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        print(f"openOrder. orderId: {orderId}")

    def orderStatus(self, orderId: OrderId, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        print(f"orderStatus. orderId: {orderId}, status: {status}")

def main():
    app = TestApp()
    app.connect("127.0.0.1", port, 0)
    threading.Thread(target=app.run, daemon=True).start()

    # Let the program run longer to ensure order placement
    time.sleep(15)
    print("Program finished.")

if __name__ == "__main__":
    main()