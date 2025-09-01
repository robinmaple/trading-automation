from ibapi.client import *
from ibapi.wrapper import *
from ibapi.ticktype import TickTypeEnum
from ibapi.order_cancel import OrderCancel

from decimal import Decimal
import threading
import time
import datetime

class OrderExecutor(EClient, EWrapper):
    """
    Handles IB connection, market data, and order execution.
    Tightly coupled design for initial phase - connection and execution together.
    """
    
    def __init__(self):
        EClient.__init__(self, self)
        self.next_valid_id = None
        self.connected = False
        self.data_received = False
        self.current_contract = None
        self.order_history = []
        self.connection_event = threading.Event()
        
    def connect_to_ib(self, host='127.0.0.1', port=7497, client_id=0):
        """Establish connection to IB Gateway/TWS"""
        try:
            self.connect(host, port, client_id)
            # Start the message processing thread
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            print(f"Connecting to IB API at {host}:{port}...")
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
    
    def wait_for_connection(self, timeout=10):
        """Wait for connection to be established and valid ID"""
        print("Waiting for connection to be established...")
        return self.connection_event.wait(timeout)
    
    def nextValidId(self, orderId: OrderId):
        """Callback: Connection is ready and we have a valid order ID"""
        print(f"Connection established. Next valid order ID: {orderId}")
        self.next_valid_id = orderId
        self.connected = True
        self.connection_event.set()  # Signal that connection is ready
    
    def error(self, reqId, errorCode, errorString, advancedOrderReject=""):
        """Handle errors and messages"""
        if errorCode in [2104, 2106, 2158]:  # Connection status messages
            print(f"Connection: {errorString}")
        elif errorCode == 399:  # Order warnings (not errors)
            print(f"Order Warning: {errorString}")
        else:
            print(f"Error {errorCode}: {errorString}")
    
    def create_forex_contract(self, symbol='EUR', currency='USD'):
        """Create a Forex contract"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CASH"
        contract.exchange = "IDEALPRO"
        contract.currency = currency
        return contract
    
    def create_native_bracket_order(self, entry_price, quantity=20000, 
                                  profit_pct=0.005, loss_pct=0.005):
        """
        Create a native bracket order for Forex
        Returns: list of [parent_order, take_profit_order, stop_loss_order]
        """
        if not self.connected or self.next_valid_id is None:
            raise Exception("Not connected to IB")
        
        parent_id = self.next_valid_id
        
        # 1. PARENT ORDER (Entry)
        parent = Order()
        parent.orderId = parent_id
        parent.action = "BUY"
        parent.orderType = "LMT"
        parent.totalQuantity = quantity
        parent.lmtPrice = round(entry_price, 5)
        parent.transmit = False
        
        # 2. TAKE-PROFIT ORDER
        take_profit = Order()
        take_profit.orderId = parent_id + 1
        take_profit.action = "SELL"
        take_profit.orderType = "LMT"
        take_profit.totalQuantity = quantity
        take_profit.lmtPrice = round(entry_price * (1 + profit_pct), 5)
        take_profit.parentId = parent_id
        take_profit.transmit = False
        take_profit.openClose = "C"
        take_profit.origin = 0
        
        # 3. STOP-LOSS ORDER
        stop_loss = Order()
        stop_loss.orderId = parent_id + 2
        stop_loss.action = "SELL"
        stop_loss.orderType = "STP"
        stop_loss.auxPrice = round(entry_price * (1 - loss_pct), 5)
        stop_loss.totalQuantity = quantity
        stop_loss.parentId = parent_id
        stop_loss.transmit = True
        stop_loss.openClose = "C"
        stop_loss.origin = 0
        
        # Reserve the next two IDs
        self.next_valid_id += 3
        
        return [parent, take_profit, stop_loss]
    
    def place_bracket_order(self, contract, entry_price, quantity=20000,
                          profit_pct=0.005, loss_pct=0.005):
        """
        Place a complete bracket order
        Returns: order IDs of the placed orders
        """
        try:
            if not self.connected:
                raise Exception("Not connected to IB. Call connect_to_ib() first.")
            
            orders = self.create_native_bracket_order(entry_price, quantity, 
                                                    profit_pct, loss_pct)
            
            print(f"\nPlacing bracket order:")
            print(f"Entry: BUY {quantity} @ {entry_price}")
            print(f"Take Profit: SELL @ {orders[1].lmtPrice}")
            print(f"Stop Loss: SELL @ {orders[2].auxPrice}")
            
            # Place all orders
            order_ids = []
            for order in orders:
                self.placeOrder(order.orderId, contract, order)
                order_ids.append(order.orderId)
                self.order_history.append({
                    'order_id': order.orderId,
                    'type': order.orderType,
                    'action': order.action,
                    'price': getattr(order, 'lmtPrice', getattr(order, 'auxPrice', None)),
                    'quantity': order.totalQuantity,
                    'status': 'PendingSubmit',  # Initial status
                    'parent_id': getattr(order, 'parentId', None),
                    'timestamp': datetime.datetime.now()
                })
            
            return order_ids
            
        except Exception as e:
            print(f"Failed to place bracket order: {e}")
            return None
    
    def get_market_data(self, contract, timeout=10):
        """
        Request market data and return the last price
        Simple implementation for now
        """
        self.data_received = False
        self.reqMarketDataType(3)  # Delayed data
        self.reqMktData(999, contract, "", False, False, [])
        
        # Wait for data or timeout
        start_time = time.time()
        while time.time() - start_time < timeout and not self.data_received:
            time.sleep(0.1)
        
        return self.data_received
    
    def tickPrice(self, reqId, tickType, price, attrib):
        """Handle market data price updates"""
        if tickType in [1, 2, 4]:  # BID, ASK, LAST
            self.data_received = True
    
    # Order status callbacks for monitoring
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        print(f"Order opened: {orderId} - {order.action} {order.orderType}")
    
    def orderStatus(self, orderId: OrderId, status: str, filled: Decimal, remaining: Decimal, 
                avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, 
                clientId: int, whyHeld: str, mktCapPrice: float):
        # Update order history with current status
        for order in self.order_history:
            if order['order_id'] == orderId:
                order['status'] = status
                order['filled'] = float(filled)
                order['remaining'] = float(remaining)
                order['last_update'] = datetime.datetime.now()
                
                if status == 'Filled' and avgFillPrice > 0:
                    order['avg_fill_price'] = avgFillPrice
    
        print(f"Order status: {orderId} - {status}, Filled: {filled}, Remaining: {remaining}")    

    def disconnect(self):
        """Clean disconnect - FIXED: Use super() to avoid recursion"""
        if self.connected:
            super().disconnect()  # ✅ FIX: Call parent class method
            self.connected = False
            print("Disconnected from IB API")

    def cancel_bracket_order(self, parent_order_id):
        """
        Cancel all orders in a bracket (parent + all children)
        Returns: True if successful, False otherwise
        """
        try:
            if not self.connected:
                print("Not connected to IB. Cannot cancel orders.")
                return False
            
            # Cancel parent order and its two children (standard bracket structure)
            orders_to_cancel = [parent_order_id, parent_order_id + 1, parent_order_id + 2]
            
            print(f"Cancelling bracket order: Parent ID {parent_order_id}, Children {orders_to_cancel[1:]}")
            
            success_count = 0
            for order_id in orders_to_cancel:
                try:
                    # ✅ CORRECT: Create OrderCancel object from the right module
                    cancel_obj = OrderCancel()
                    cancel_obj.orderId = order_id
                    self.cancelOrder(cancel_obj)
                    print(f"Cancel request sent for order {order_id}")
                    success_count += 1
                    
                    # Update order history status
                    for order in self.order_history:
                        if order['order_id'] == order_id:
                            order['status'] = 'Cancelling'
                            order['cancel_timestamp'] = datetime.datetime.now()
                            
                except Exception as e:
                    print(f"Failed to send cancel request for order {order_id}: {e}")
            
            return success_count > 0
            
        except Exception as e:
            print(f"Error in cancel_bracket_order: {e}")
            return False
        
    def cancel_all_orders(self):
        """
        Cancel all active orders from order history
        Returns: number of orders cancelled
        """
        try:
            if not self.connected:
                print("Not connected to IB. Cannot cancel orders.")
                return 0
            
            active_orders = [order for order in self.order_history 
                            if order.get('status') in ['PreSubmitted', 'Submitted', 'PendingSubmit']]
            
            if not active_orders:
                print("No active orders to cancel")
                return 0
            
            print(f"Cancelling {len(active_orders)} active orders...")
            
            cancelled_count = 0
            for order in active_orders:
                try:
                    # ✅ CORRECT: Create OrderCancel object from the right module
                    cancel_obj = OrderCancel()
                    cancel_obj.orderId = order['order_id']
                    self.cancelOrder(cancel_obj)
                    print(f"Cancel request sent for order {order['order_id']} ({order['action']} {order['type']})")
                    order['status'] = 'Cancelling'
                    order['cancel_timestamp'] = datetime.datetime.now()
                    cancelled_count += 1
                except Exception as e:
                    print(f"Failed to cancel order {order['order_id']}: {e}")
            
            return cancelled_count
            
        except Exception as e:
            print(f"Error in cancel_all_orders: {e}")
            return 0        
        
    def cancel_order_by_id(self, order_id):
        """
        Cancel a specific order by ID
        Returns: True if successful, False otherwise
        """
        try:
            if not self.connected:
                print("Not connected to IB. Cannot cancel order.")
                return False
            
            # Check if order exists in history
            order_info = next((order for order in self.order_history 
                            if order['order_id'] == order_id), None)
            
            if not order_info:
                print(f"Order {order_id} not found in history")
                return False
            
            # ✅ CORRECT: Create OrderCancel object from the right module
            cancel_obj = OrderCancel()
            cancel_obj.orderId = order_id
            self.cancelOrder(cancel_obj)
            print(f"Cancel request sent for order {order_id} ({order_info['action']} {order_info['type']})")
            
            # Update order history
            for order in self.order_history:
                if order['order_id'] == order_id:
                    order['status'] = 'Cancelling'
                    order['cancel_timestamp'] = datetime.datetime.now()
            
            return True
            
        except Exception as e:
            print(f"Error cancelling order {order_id}: {e}")
            return False