from ibapi.client import *
from ibapi.wrapper import *
from ibapi.order_cancel import OrderCancel
from typing import List, Optional, Any
import threading
import datetime

from core.ibkr_types import IbkrOrder, IbkrPosition

class IbkrClient(EClient, EWrapper):
    """
    A high-level, test-friendly client for IBKR TWS API.
    Handles all IBKR API communication. Phase 2: Full API layer.
    """
    def __init__(self):
        EClient.__init__(self, self)
        self.next_valid_id = None
        self.connected = False
        self.connection_event = threading.Event()
        self.account_values = {}
        self.account_value_received = threading.Event()
        self.order_history = []
        self.account_number = None
        self.is_paper_account = False
        self.displayed_errors = set()

            # Phase 2: Reconciliation data tracking
        self.open_orders: List[IbkrOrder] = []
        self.positions: List[IbkrPosition] = []
        self.orders_received_event = threading.Event()
        self.positions_received_event = threading.Event()
        self.open_orders_end_received = False
        self.positions_end_received = False

    def connect(self, host='127.0.0.1', port=7497, client_id=0) -> bool:
        """Establish connection to IB Gateway/TWS. Returns success status."""
        try:
            # FIXED: Call parent class method instead of self.connect()
            EClient.connect(self, host, port, client_id)
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            print(f"Connecting to IB API at {host}:{port}...")
            # Wait for connection to be fully established
            return self.connection_event.wait(10)
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
        
    def disconnect(self):
        """Cleanly disconnect from TWS."""
        if self.connected:
            super().disconnect()
            self.connected = False

    def get_account_value(self) -> float:
        """
        Request and return the Net Liquidation value of the account.
        Handles all underlying reqAccountUpdates calls and waiting.
        """
        if not self.connected:
            return 100000.0  # Fallback value

        self.account_value_received.clear()
        self.reqAccountUpdates(True, self.account_number)

        if not self.account_value_received.wait(5.0):
            print("⚠️  Timeout waiting for account value data - using fallback value")
            self.reqAccountUpdates(False, self.account_number)
            return 100000.0

        net_liquidation = self.account_values.get("NetLiquidation")
        if net_liquidation is not None:
            print(f"✅ Current account value: ${net_liquidation:,.2f}")
            self.reqAccountUpdates(False, self.account_number)
            return net_liquidation

        cash_value = self.account_values.get("TotalCashValue", 100000.0)
        print(f"⚠️  Using cash value: ${cash_value:,.2f} (NetLiquidation not available)")
        self.reqAccountUpdates(False, self.account_number)
        return cash_value

    def place_bracket_order(self, contract, action, order_type, security_type, entry_price, stop_loss,
                          risk_per_trade, risk_reward_ratio, total_capital):
        """
        Place a complete bracket order.
        Returns: order IDs of the placed orders or None.
        """
        if not self.connected or self.next_valid_id is None:
            print("Not connected to IBKR or no valid order ID")
            return None

        try:
            # Create bracket order using internal method
            orders = self._create_bracket_order(
                action, order_type, security_type, entry_price, stop_loss,
                risk_per_trade, risk_reward_ratio, total_capital, self.next_valid_id
            )

            print(f"Entry: {action} {orders[0].totalQuantity} @ {entry_price}")
            print(f"Take Profit: {orders[1].action} @ {orders[1].lmtPrice}")
            print(f"Stop Loss: {orders[2].action} @ {orders[2].auxPrice}")

            # Place all orders through the API
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
                    'status': 'PendingSubmit',
                    'parent_id': getattr(order, 'parentId', None),
                    'timestamp': datetime.datetime.now()
                })

            # Reserve the next three IDs for the bracket
            self.next_valid_id += 3

            return order_ids

        except Exception as e:
            print(f"Failed to place bracket order: {e}")
            return None

    def _create_bracket_order(self, action, order_type, security_type, entry_price, stop_loss,
                            risk_per_trade, risk_reward_ratio, total_capital, starting_order_id):
        """
        Create a native bracket order for Forex
        Returns: list of [parent_order, take_profit_order, stop_loss_order]
        """
        from ibapi.order import Order
        
        parent_id = starting_order_id
        
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

    # --- IBKR API Callbacks ---
    def nextValidId(self, orderId: int):
        """Callback: Connection is ready and we have a valid order ID"""
        print(f"Connection established. Next valid order ID: {orderId}")
        self.next_valid_id = orderId
        self.connected = True
        self.connection_event.set()

    def error(self, reqId, errorCode, errorString, advancedOrderReject=""):
        """Handle errors and messages"""
        super().error(reqId, errorCode, errorString, advancedOrderReject)

        error_key = (reqId, errorCode)
        if error_key in self.displayed_errors:
            return

        self.displayed_errors.add(error_key)

        # Market data specific errors
        if errorCode in [10089, 10167, 322, 10201, 10202]:
            is_snapshot = "snapshot" in errorString.lower() or "not subscribed" in errorString.lower()
            if is_snapshot:
                print(f"📊 Snapshot Error {errorCode}: {errorString}")
                print("💡 Paper account snapshot data may be limited during off-hours")
            else:
                print(f"📊 Streaming Error {errorCode}: {errorString}")
        elif errorCode in [2104, 2106, 2158]:  # Connection status messages
            print(f"Connection: {errorString}")
        elif errorCode == 399:  # Order warnings
            print(f"Order Warning: {errorString}")
        else:
            print(f"Error {errorCode}: {errorString}")

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        """Callback: Received account value updates"""
        super().updateAccountValue(key, val, currency, accountName)

        if key == "NetLiquidation":
            self.account_values["NetLiquidation"] = float(val)
        elif key == "TotalCashValue":
            self.account_values["TotalCashValue"] = float(val)
        elif key == "UnrealizedPnL":
            self.account_values["UnrealizedPnL"] = float(val)
        elif key == "RealizedPnL":
            self.account_values["RealizedPnL"] = float(val)

        self.account_value_received.set()

    def managedAccounts(self, accountsList: str):
        """Callback: Received when connection is established"""
        super().managedAccounts(accountsList)
        print(f"Managed accounts: {accountsList}")

        if accountsList:
            self.account_number = accountsList.split(',')[0]
            self.is_paper_account = self.account_number.startswith('DU')
            env = "PAPER" if self.is_paper_account else "PRODUCTION"
            print(f"🎯 Auto-detected environment: {env} (Account: {self.account_number})")

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        """Callback: Order status updates"""
        for order in self.order_history:
            if order['order_id'] == orderId:
                order['status'] = status
                order['filled'] = float(filled)
                order['remaining'] = float(remaining)
                order['last_update'] = datetime.datetime.now()

                if status == 'Filled' and avgFillPrice > 0:
                    order['avg_fill_price'] = avgFillPrice

        print(f"Order status: {orderId} - {status}, Filled: {filled}, Remaining: {remaining}")

    def cancel_order(self, order_id: int) -> bool:
        """
        Cancel an order through IBKR API
        Returns: Success status
        """
        if not self.connected:
            print(f"❌ Cannot cancel order {order_id} - not connected to IBKR")
            return False
            
        try:
            self.cancelOrder(order_id)
            print(f"✅ Sent cancel request for order {order_id}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to cancel order {order_id}: {e}")
            return False
        
    # Add new methods for reconciliation:
    def get_open_orders(self) -> List[IbkrOrder]:
        """
        Fetch all open orders from IBKR API synchronously.
        Returns: List of IbkrOrder objects or empty list on failure.
        """
        if not self.connected:
            print("❌ Not connected to IBKR - cannot fetch open orders")
            return []
        
        try:
            self.open_orders.clear()
            self.open_orders_end_received = False
            self.orders_received_event.clear()
            
            print("📋 Requesting open orders from IBKR...")
            self.reqAllOpenOrders()  # IBKR API call to get all open orders
            
            # Wait for data with timeout
            if self.orders_received_event.wait(10.0):
                print(f"✅ Received {len(self.open_orders)} open orders")
                return self.open_orders.copy()
            else:
                print("❌ Timeout waiting for open orders data")
                return []
                
        except Exception as e:
            print(f"❌ Failed to fetch open orders: {e}")
            return []

    def get_positions(self) -> List[IbkrPosition]:
        """
        Fetch all positions from IBKR API synchronously.
        Returns: List of IbkrPosition objects or empty list on failure.
        """
        if not self.connected:
            print("❌ Not connected to IBKR - cannot fetch positions")
            return []
        
        try:
            self.positions.clear()
            self.positions_end_received = False
            self.positions_received_event.clear()
            
            print("📊 Requesting positions from IBKR...")
            self.reqPositions()  # IBKR API call to get all positions
            
            # Wait for data with timeout
            if self.positions_received_event.wait(10.0):
                print(f"✅ Received {len(self.positions)} positions")
                return self.positions.copy()
            else:
                print("❌ Timeout waiting for positions data")
                return []
                
        except Exception as e:
            print(f"❌ Failed to fetch positions: {e}")
            return []

    # Add new callback methods for order and position data:
    def openOrder(self, orderId, contract, order, orderState):
        """Callback: Received open order data"""
        try:
            ibkr_order = IbkrOrder(
                order_id=orderId,
                client_id=order.clientId,
                perm_id=order.permId,
                action=order.action,
                order_type=order.orderType,
                total_quantity=order.totalQuantity,
                filled_quantity=orderState.filled,
                remaining_quantity=orderState.remaining,
                avg_fill_price=orderState.avgFillPrice,
                status=orderState.status,
                lmt_price=getattr(order, 'lmtPrice', None),
                aux_price=getattr(order, 'auxPrice', None),
                parent_id=getattr(order, 'parentId', None),
                why_held=getattr(orderState, 'whyHeld', ''),
                last_update_time=datetime.now()
            )
            self.open_orders.append(ibkr_order)
            
        except Exception as e:
            print(f"Error processing open order {orderId}: {e}")

    def openOrderEnd(self):
        """Callback: Finished receiving open orders"""
        self.open_orders_end_received = True
        self.orders_received_event.set()
        print("📋 Open orders request completed")

    def position(self, account: str, contract, position: float, avgCost: float):
        """Callback: Received position data"""
        try:
            ibkr_position = IbkrPosition(
                account=account,
                contract_id=contract.conId,
                symbol=contract.symbol,
                security_type=contract.secType,
                currency=contract.currency,
                position=position,
                avg_cost=avgCost
            )
            self.positions.append(ibkr_position)
            
        except Exception as e:
            print(f"Error processing position for {contract.symbol}: {e}")

    def positionEnd(self):
        """Callback: Finished receiving positions"""
        self.positions_end_received = True
        self.positions_received_event.set()
        print("📊 Positions request completed")

    # Add error handling for position requests
    def error(self, reqId, errorCode, errorString, advancedOrderReject=""):
        """Handle errors and messages"""
        super().error(reqId, errorCode, errorString, advancedOrderReject)
        
        # Handle position request errors
        if errorCode in [321, 322]:  # Position related errors
            print(f"Position Error {errorCode}: {errorString}")
            self.positions_received_event.set()
        
        # Handle open order request errors  
        if errorCode in [201, 202]:  # Order related errors
            print(f"Open Order Error {errorCode}: {errorString}")
            self.orders_received_event.set()