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
        self.market_data_manager = None

        # Use account number to determine if it's a live or paper trading account
        self.account_number = None
        self.is_paper_account = False

        # Add error tracking to prevent duplicate messages
        self.displayed_errors = set()  # Track (reqId, errorCode) to avoid duplicates

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

        """Handle errors and messages with better market data differentiation"""
        super().error(reqId, errorCode, errorString, advancedOrderReject)
        
        # Create a unique key based on error content (not request ID)
        error_key = f"{errorCode}:{errorString.split('.')[0]}"  # First part of message

        # Check if this error has already been displayed
        error_key = (reqId, errorCode)
        if error_key in self.displayed_errors:
            return  # Skip duplicate error
        
        self.displayed_errors.add(error_key)

        # Market data specific errors
        if errorCode in [10089, 10167, 322, 10201, 10202]:
            # Check if this is a snapshot request (different handling)
            is_snapshot = "snapshot" in errorString.lower() or "not subscribed" in errorString.lower()
            
            if is_snapshot:
                print(f"📊 Snapshot Error {errorCode}: {errorString}")
                print("💡 Paper account snapshot data may be limited during off-hours")
            else:
                print(f"📊 Streaming Error {errorCode}: {errorString}")
                
            if errorCode == 10089:  # Subscription required
                if is_snapshot:
                    print("💡 Snapshot data requires basic market data subscription")
                else:
                    print("💡 Streaming data requires additional subscription")
            elif errorCode == 10167:  # Data farm connection
                print("💡 Market data temporarily unavailable - will retry")

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
    
    def create_native_bracket_order(self, action, order_type, security_type, entry_price, stop_loss,
                                  risk_per_trade, risk_reward_ratio):
        """
        Create a native bracket order for Forex
        Returns: list of [parent_order, take_profit_order, stop_loss_order]
        """
        if not self.connected or self.next_valid_id is None:
            raise Exception("Not connected to IB")
        
        parent_id = self.next_valid_id
        
        # Get total capital from account - this needs to be implemented separately
        # For now, we'll use a placeholder - you'll need to implement account value retrieval
        total_capital = self._get_account_value()  # This method needs to be implemented
        
        # Calculate quantity based on security type
        quantity = self._calculate_quantity(security_type, entry_price, stop_loss, 
                                          total_capital, risk_per_trade)
        
        # Calculate profit target based on risk reward ratio
        profit_target = self._calculate_profit_target(action, entry_price, stop_loss, risk_reward_ratio)
        
        # 1. PARENT ORDER (Entry)
        parent = Order()
        parent.orderId = parent_id
        parent.action = action
        parent.orderType = order_type
        parent.totalQuantity = quantity
        parent.lmtPrice = round(entry_price, 5)
        parent.transmit = False
        
        # Determine opposite action for closing orders
        closing_action = "SELL" if action == "BUY" else "BUY"
        
        # 2. TAKE-PROFIT ORDER
        take_profit = Order()
        take_profit.orderId = parent_id + 1
        take_profit.action = closing_action
        take_profit.orderType = "LMT"
        take_profit.totalQuantity = quantity
        
        # Use calculated profit target
        take_profit.lmtPrice = round(profit_target, 5)
        take_profit.parentId = parent_id
        take_profit.transmit = False
        take_profit.openClose = "C"
        take_profit.origin = 0
        
        # 3. STOP-LOSS ORDER
        stop_loss_order = Order()
        stop_loss_order.orderId = parent_id + 2
        stop_loss_order.action = closing_action
        stop_loss_order.orderType = "STP"
        stop_loss_order.totalQuantity = quantity
        
        # Use provided stop loss price
        stop_loss_order.auxPrice = round(stop_loss, 5)
            
        stop_loss_order.parentId = parent_id
        stop_loss_order.transmit = True
        stop_loss_order.openClose = "C"
        stop_loss_order.origin = 0
        
        # Reserve the next two IDs
        self.next_valid_id += 3
        
        return [parent, take_profit, stop_loss_order]

    def _calculate_quantity(self, security_type, entry_price, stop_loss, total_capital, risk_per_trade):
        """Calculate position size based on security type and risk management"""
        if entry_price is None or stop_loss is None:
            raise ValueError("Entry price and stop loss are required for quantity calculation")
            
        # Different risk calculation based on security type
        if security_type == "OPT":
            # For options, risk is the premium difference per contract
            # Options are typically 100 shares per contract, so multiply by 100
            risk_per_unit = abs(entry_price - stop_loss) * 100
        else:
            # For stocks, forex, futures, etc. - risk is price difference per share/unit
            risk_per_unit = abs(entry_price - stop_loss)
        
        if risk_per_unit == 0:
            raise ValueError("Entry price and stop loss cannot be the same")
            
        risk_amount = total_capital * risk_per_trade
        base_quantity = risk_amount / risk_per_unit
        
        # Different rounding and minimums based on security type
        if security_type == "CASH":
            # Forex typically trades in standard lots (100,000) or mini lots (10,000)
            # Round to the nearest 10,000 units for reasonable position sizing
            quantity = round(base_quantity / 10000) * 10000
            quantity = max(quantity, 10000)  # Minimum 10,000 units
        elif security_type == "STK":
            # Stocks trade in whole shares
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 share
        elif security_type == "OPT":
            # Options trade in whole contracts (each typically represents 100 shares)
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
            print(f"Options position: {quantity} contracts (each = 100 shares)")
        elif security_type == "FUT":
            # Futures trade in whole contracts
            quantity = round(base_quantity)
            quantity = max(quantity, 1)  # Minimum 1 contract
        else:
            # Default to whole units for other security types
            quantity = round(base_quantity)
            quantity = max(quantity, 1)
            
        return quantity

    def _calculate_profit_target(self, action, entry_price, stop_loss, risk_reward_ratio):
        """Calculate profit target based on risk reward ratio"""
        risk_amount = abs(entry_price - stop_loss)
        if action == "BUY":
            profit_target = entry_price + (risk_amount * risk_reward_ratio)
        else:  # SELL
            profit_target = entry_price - (risk_amount * risk_reward_ratio)
        return profit_target

    def _get_account_value(self):
        """Get total account value - placeholder implementation"""
        # TODO: Implement actual account value retrieval from IBKR
        # This should query account summary for NetLiquidation value
        print("⚠️  WARNING: Using placeholder account value of $100,000")
        print("💡 Implement _get_account_value() to retrieve actual account value from IBKR")
        return 100000.0  # Placeholder value

    def place_bracket_order(self, contract, action, order_type, security_type, entry_price, stop_loss,
                          risk_per_trade, risk_reward_ratio):
        """
        Place a complete bracket order
        Returns: order IDs of the placed orders
        """
        try:
            if not self.connected:
                raise Exception("Not connected to IB. Call connect_to_ib() first.")
            
            orders = self.create_native_bracket_order(action, order_type, security_type, entry_price, stop_loss,
                                                    risk_per_trade, risk_reward_ratio)
            
            print(f"Entry: {action} {orders[0].totalQuantity} @ {entry_price}")
            print(f"Take Profit: {orders[1].action} @ {orders[1].lmtPrice}")
            print(f"Stop Loss: {orders[2].action} @ {orders[2].auxPrice}")
            
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
        """Handle market data and delegate to manager"""
        super().tickPrice(reqId, tickType, price, attrib)
        if self.market_data_manager:
            self.market_data_manager.on_tick_price(reqId, tickType, price, attrib)    

    # Thread-safe market data access
    def get_current_price(self, symbol):
        with self.lock:
            if symbol in self.prices:
                return self.prices[symbol]['price']
        return None

    def handle_market_data_error(self, req_id, error_code, error_string):
        """Handle market data disconnections and errors"""
        if error_code in [2104, 2106, 2107]:  # Market data farm messages
            print(f"Market data connection: {error_string}")
        elif error_code == 10167:  # Data farm connection is broken
            print("Market data farm disconnected, attempting reconnect...")
            self._reconnect_market_data()

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
        
    def managedAccounts(self, accountsList: str):
        """Callback: Received when connection is established"""
        super().managedAccounts(accountsList)
        print(f"Managed accounts: {accountsList}")
        
        # Extract account number and detect environment
        if accountsList:
            self.account_number = accountsList.split(',')[0]
            self.is_paper_account = self.account_number.startswith('DU')
            
            env = "PAPER" if self.is_paper_account else "PRODUCTION"
            print(f"🎯 Auto-detected environment: {env} (Account: {self.account_number})")