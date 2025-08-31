from ibapi.client import *
from ibapi.wrapper import *
import datetime
import time
import threading
from ibapi.ticktype import TickTypeEnum
from decimal import Decimal


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


        # Build FOREX contract for EUR/USD
        self.mycontract = Contract()
        self.mycontract.symbol = "EUR"
        self.mycontract.secType = "CASH"
        self.mycontract.exchange = "IDEALPRO"  # Specific exchange for forex
        self.mycontract.currency = "USD"
        # Note: For Forex, the contract is defined by "symbol.currency" pair: EUR.USD


        print("Requesting Forex market data for EUR/USD...")
        self.reqMarketDataType(3)  # Delayed data is fine for testing
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
        print(f"Forex Data: {tick_name}: {price}")
       
        # DEBUG: Print the tickType number to verify
        print(f"DEBUG: tickType = {tickType}")
       
        # Trigger on BID (1), ASK (2), or LAST_PRICE (4) for Forex
        if not self.dataReceived and (tickType == 1 or tickType == 2 or tickType == 4):
            self.dataReceived = True
            print(f"Received Forex data ({tick_name}). Now placing order...")
            self.placeOrder(self.nextValidOrderId, self.mycontract, self.createOrder())


    def tickSize(self, reqId, tickType, size):
        print(f"Forex Data Size: {TickTypeEnum.toStr(tickType)}: {size}")


    def createOrder(self):
        myorder = Order()
        myorder.orderId = self.nextValidOrderId
        myorder.action = "BUY"  # Buying EUR, selling USD
        myorder.tif = "GTC"     # Good Till Canceled
        myorder.orderType = "LMT"
        myorder.lmtPrice = 1.0850  # Example limit price for EUR/USD
        myorder.totalQuantity = 10000  # Standard lot size for forex (10,000 units)
        # For forex, quantity is in the base currency (EUR in this case)
        return myorder


    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        print(f"Open Forex Order. orderId: {orderId}, action: {order.action}, "
              f"quantity: {order.totalQuantity}, limitPrice: {order.lmtPrice}")


    def orderStatus(self, orderId: OrderId, status: str, filled: Decimal, remaining: Decimal,
                   avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float,
                   clientId: int, whyHeld: str, mktCapPrice: float):
        print(f"Forex Order Status. orderId: {orderId}, status: {status}, "
              f"filled: {filled}, remaining: {remaining}, avgFillPrice: {avgFillPrice}")


def main():
    app = TestApp()
    app.connect("127.0.0.1", port, 0)
    threading.Thread(target=app.run, daemon=True).start()


    # Let the program run for a while to get data and place the order
    time.sleep(15)
    print("Forex trading test completed.")


if __name__ == "__main__":
    main()