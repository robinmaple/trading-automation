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
        self.mycontract.exchange = "IDEALPRO"
        self.mycontract.currency = "USD"

        print("Requesting Forex market data for EUR/USD...")
        self.reqMarketDataType(3)
        self.reqMktData(1, self.mycontract, "", False, False, [])
        
        threading.Timer(5.0, self.placeOrderIfNoData).start()

    def placeOrderIfNoData(self):
        """Safety method to place order if market data doesn't arrive"""
        if not self.dataReceived:
            print("No market data received within timeout. Placing bracket order anyway...")
            self.placeNativeBracketOrder()

    def error(self, reqId, errorCode, errorString, advancedOrderReject=""):
        print(f"Error. reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}")

    def tickPrice(self, reqId, tickType, price, attrib):
        tick_name = TickTypeEnum.toStr(tickType)
        print(f"Forex Data: {tick_name}: {price}")
        
        # Trigger on BID, ASK, or LAST_PRICE for Forex
        if not self.dataReceived and (tickType == 1 or tickType == 2 or tickType == 4):
            self.dataReceived = True
            print(f"Received Forex data ({tick_name}). Now placing native bracket order...")
            self.placeNativeBracketOrder()

    def tickSize(self, reqId, tickType, size):
        print(f"Forex Data Size: {TickTypeEnum.toStr(tickType)}: {size}")

    def createNativeBracketOrder(self, parentOrderId, entryPrice):
        """
        Create a native bracket order using IB's specific order attributes
        for better system integration and handling.
        """
        # 1. PARENT ORDER (Entry Order)
        parent = Order()
        parent.orderId = parentOrderId
        parent.action = "BUY"
        parent.orderType = "LMT"
        parent.totalQuantity = 20000  # Minimum for IdealPro is 20,000 EUR
        parent.lmtPrice = round(entryPrice, 5)
        parent.transmit = False  # Don't transmit until all children are ready
        
        # 2. TAKE-PROFIT ORDER (Child 1) - Using native bracket features
        takeProfit = Order()
        takeProfit.orderId = parentOrderId + 1
        takeProfit.action = "SELL"  # Opposite of parent for profit taking
        takeProfit.orderType = "LMT"
        takeProfit.totalQuantity = 20000  # Same quantity as parent
        takeProfit.lmtPrice = round(entryPrice * 1.010, 5)  # 1% profit target
        takeProfit.parentId = parentOrderId  # Explicit link to parent
        takeProfit.transmit = False
        takeProfit.openClose = "C"  # ⭐ NATIVE: This is a CLOSING order
        takeProfit.origin = 0       # ⭐ NATIVE: Customer origin (not system-generated)
        
        # 3. STOP-LOSS ORDER (Child 2) - Using native bracket features
        stopLoss = Order()
        stopLoss.orderId = parentOrderId + 2
        stopLoss.action = "SELL"  # Opposite of parent to limit losses
        stopLoss.orderType = "STP"  # Stop order
        stopLoss.auxPrice = round(entryPrice * 0.995, 5)  # 0.5% stop loss
        stopLoss.totalQuantity = 20000
        stopLoss.parentId = parentOrderId  # Explicit link to parent
        stopLoss.transmit = True  # Last order - transmit the entire bracket
        stopLoss.openClose = "C"  # ⭐ NATIVE: This is a CLOSING order
        stopLoss.origin = 0       # ⭐ NATIVE: Customer origin (not system-generated)
        
        return [parent, takeProfit, stopLoss]

    def placeNativeBracketOrder(self):
        """Place the complete native bracket order"""
        # For testing, use a reasonable EUR/USD price
        # In production, you might use the actual market data price
        current_price = 1.16850
        
        bracket_orders = self.createNativeBracketOrder(self.nextValidOrderId, current_price)
        
        print("\n" + "="*50)
        print("PLACING NATIVE BRACKET ORDER")
        print("="*50)
        print(f"Entry (BUY): {bracket_orders[0].totalQuantity} EUR @ {bracket_orders[0].lmtPrice}")
        print(f"Take-Profit (SELL): {bracket_orders[1].totalQuantity} EUR @ {bracket_orders[1].lmtPrice}")
        print(f"Stop-Loss (SELL): {bracket_orders[2].totalQuantity} EUR @ {bracket_orders[2].auxPrice}")
        print(f"Risk/Reward: 0.5% stop loss, 0.5% profit target")
        print("="*50 + "\n")
        
        # Place all three orders as a single bracket
        for order in bracket_orders:
            self.placeOrder(order.orderId, self.mycontract, order)

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        order_type = getattr(order, 'orderType', 'N/A')
        price = getattr(order, 'lmtPrice', getattr(order, 'auxPrice', 'N/A'))
        print(f"Open Order. ID: {orderId}, Type: {order_type}, Action: {order.action}, Price: {price}")
        
        # Show native attributes if present
        if hasattr(order, 'openClose'):
            print(f"  → Native: openClose='{order.openClose}', origin={getattr(order, 'origin', 'N/A')}")
        if hasattr(order, 'parentId') and order.parentId > 0:
            print(f"  → ParentID: {order.parentId}")

    def orderStatus(self, orderId: OrderId, status: str, filled: Decimal, remaining: Decimal, 
                   avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, 
                   clientId: int, whyHeld: str, mktCapPrice: float):
        print(f"Order Status. ID: {orderId}, Status: {status}, "
              f"Filled: {filled}, Remaining: {remaining}, ParentID: {parentId}")
        
        # Log fills with prices
        if status == "Filled" and filled > 0:
            print(f"  → Fill Price: {avgFillPrice if avgFillPrice > 0 else lastFillPrice}")

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        print(f"Execution Details. ID: {execution.orderId}, "
              f"Shares: {execution.shares}, Price: {execution.price}, "
              f"Side: {execution.side}")

def main():
    app = TestApp()
    app.connect("127.0.0.1", port, 0)
    threading.Thread(target=app.run, daemon=True).start()

    # Let the program run longer to monitor order status
    time.sleep(20)
    print("\nNative bracket order test completed.")
    print("Note: Orders remain active on IBKR servers even after program exit.")

if __name__ == "__main__":
    main()