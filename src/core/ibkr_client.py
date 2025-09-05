from ibapi.client import *
from ibapi.wrapper import *
from ibapi.order_cancel import OrderCancel
from typing import Optional, Any
import threading
import datetime
from src.core.order_executor import OrderExecutor


class IbkrClient(EClient, EWrapper):
    """
    A high-level, test-friendly client for IBKR TWS API.
    Handles all IBKR API communication. Phase 2: Full API layer.
    """
    def __init__(self):
        EClient.__init__(self, self)
        self._order_executor = OrderExecutor()
        self.next_valid_id = None
        self.connected = False
        self.connection_event = threading.Event()
        self.account_values = {}
        self.account_value_received = threading.Event()
        self.order_history = []
        self.account_number = None
        self.is_paper_account = False
        self.displayed_errors = set()

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
                          risk_per_trade, risk_reward_ratio):
        """
        Place a complete bracket order.
        Returns: order IDs of the placed orders or None.
        """
        if not self.connected or self.next_valid_id is None:
            print("Not connected to IBKR or no valid order ID")
            return None

        try:
            # Get account value for risk calculation
            total_capital = self.get_account_value()

            # Use the pure business logic method from OrderExecutor
            orders = self._order_executor.create_native_bracket_order(
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

    # Property to access the underlying executor if needed for specific operations
    @property
    def order_executor(self) -> OrderExecutor:
        return self._order_executor