"""
A high-level, test-friendly client for the Interactive Brokers (IBKR) TWS API.
Handles all low-level communication, including connection management, order placement,
account data retrieval, and real-time updates. Subclasses the official IBKR EClient/EWrapper.
"""

from ibapi.client import *
from ibapi.wrapper import *
from ibapi.order_cancel import OrderCancel
from typing import List, Optional, Any
import threading
import datetime

from src.core.ibkr_types import IbkrOrder, IbkrPosition
from src.core.account_utils import is_paper_account, get_ibkr_port

class IbkrClient(EClient, EWrapper):
    """Manages the connection and all communication with the IBKR trading API."""

    def __init__(self, host='127.0.0.1', port=None, client_id=1, mode='auto'):
        """Initialize the client, connection flags, and data stores."""
        EClient.__init__(self, self)
        self.next_valid_id = None
        self.connected = False
        self.connection_event = threading.Event()
        self.account_values = {}
        self.account_value_received = threading.Event()
        self.order_history = []
        self.account_number = None
        self.account_name = None  # ADD ACCOUNT NAME STORAGE
        self.is_paper_account = False
        self.account_ready_event = threading.Event()
        self.displayed_errors = set()

        # Store connection parameters
        self.host = host
        self.port = port
        self.client_id = client_id

        # Reconciliation data tracking
        self.open_orders: List[IbkrOrder] = []
        self.positions: List[IbkrPosition] = []
        self.orders_received_event = threading.Event()
        self.positions_received_event = threading.Event()
        self.open_orders_end_received = False
        self.positions_end_received = False
        self.market_data_manager = None

        # Add port determination based on mode
        if port is None:
            self.port = self._get_port_from_mode(mode)
        else:
            self.port = port
        self.mode = mode

    def connect(self, host: Optional[str] = None, port: Optional[int] = None, 
                client_id: Optional[int] = None) -> bool:
        """Establish a connection to IB Gateway/TWS. Returns success status."""

        # Use provided parameters or fall back to instance defaults
        connect_host = host or self.host
        connect_port = port or self.port
        connect_client_id = client_id or self.client_id

        EClient.connect(self, connect_host, connect_port, connect_client_id)
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

        print(f"Connecting to IB API at {connect_host}:{connect_port}...")

        # Wait for both: valid ID and account info
        ready = self.connection_event.wait(10) and self.account_ready_event.wait(10)
        if not ready:
            print("❌ Connection timed out waiting for account details")
            return False

        # Validate account ↔ port
        expected_port = get_ibkr_port(self.account_name)
        if connect_port != expected_port:
            print(f"❌ Port mismatch: account {self.account_name} "
                  f"requires port {expected_port}, but tried {connect_port}")
            self.disconnect()
            return False

        print(f"✅ Connected to {self.account_name} "
              f"({ 'PAPER' if self.is_paper_account else 'LIVE' }) on port {connect_port}")
        return True
    
    def disconnect(self) -> None:
        """Cleanly disconnect from TWS."""
        if self.connected:
            super().disconnect()
            self.connected = False

    def get_account_name(self) -> Optional[str]:
        """Get the connected account name."""
        return self.account_name

    def get_account_value(self) -> float:
        """Request and return the Net Liquidation value of the account."""
        if not self.connected:
            return 100000.0

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
                          risk_per_trade, risk_reward_ratio, total_capital) -> Optional[List[int]]:
        """Place a complete bracket order (entry, take-profit, stop-loss). Returns list of order IDs or None."""
        if not self.connected or self.next_valid_id is None:
            print("Not connected to IBKR or no valid order ID")
            return None

        try:
            orders = self._create_bracket_order(
                action, order_type, security_type, entry_price, stop_loss,
                risk_per_trade, risk_reward_ratio, total_capital, self.next_valid_id
            )

            print(f"Entry: {action} {orders[0].totalQuantity} @ {entry_price}")
            print(f"Take Profit: {orders[1].action} @ {orders[1].lmtPrice}")
            print(f"Stop Loss: {orders[2].action} @ {orders[2].auxPrice}")

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

            self.next_valid_id += 3
            return order_ids

        except Exception as e:
            print(f"Failed to place bracket order: {e}")
            return None

    def _create_bracket_order(self, action, order_type, security_type, entry_price, stop_loss,
                            risk_per_trade, risk_reward_ratio, total_capital, starting_order_id) -> List[Any]:
        """Create the IBKR Order objects for a bracket order. Returns [parent, take_profit, stop_loss]."""
        from ibapi.order import Order

        parent_id = starting_order_id
        quantity = self._calculate_quantity(security_type, entry_price, stop_loss,
                                          total_capital, risk_per_trade)
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
        stop_loss_order.auxPrice = round(stop_loss, 5)
        stop_loss_order.parentId = parent_id
        stop_loss_order.transmit = True
        stop_loss_order.openClose = "C"
        stop_loss_order.origin = 0

        return [parent, take_profit, stop_loss_order]

    def _calculate_quantity(self, security_type, entry_price, stop_loss, total_capital, risk_per_trade) -> float:
        """Calculate position size based on security type and risk management."""
        if entry_price is None or stop_loss is None:
            raise ValueError("Entry price and stop loss are required for quantity calculation")

        if security_type == "OPT":
            risk_per_unit = abs(entry_price - stop_loss) * 100
        else:
            risk_per_unit = abs(entry_price - stop_loss)

        if risk_per_unit == 0:
            raise ValueError("Entry price and stop loss cannot be the same")

        risk_amount = total_capital * risk_per_trade
        base_quantity = risk_amount / risk_per_unit

        if security_type == "CASH":
            quantity = round(base_quantity / 10000) * 10000
            quantity = max(quantity, 10000)
        elif security_type == "STK":
            quantity = round(base_quantity)
            quantity = max(quantity, 1)
        elif security_type == "OPT":
            quantity = round(base_quantity)
            quantity = max(quantity, 1)
            print(f"Options position: {quantity} contracts (each = 100 shares)")
        elif security_type == "FUT":
            quantity = round(base_quantity)
            quantity = max(quantity, 1)
        else:
            quantity = round(base_quantity)
            quantity = max(quantity, 1)

        return quantity

    def _calculate_profit_target(self, action, entry_price, stop_loss, risk_reward_ratio) -> float:
        """Calculate profit target price based on risk/reward ratio."""
        risk_amount = abs(entry_price - stop_loss)
        if action == "BUY":
            profit_target = entry_price + (risk_amount * risk_reward_ratio)
        else:
            profit_target = entry_price - (risk_amount * risk_reward_ratio)
        return profit_target

    def cancel_order(self, order_id: int) -> bool:
        """Cancel an order through the IBKR API. Returns success status."""
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

    def get_open_orders(self) -> List[IbkrOrder]:
        """Fetch all open orders from IBKR API synchronously. Returns a list of IbkrOrder objects."""
        if not self.connected:
            print("❌ Not connected to IBKR - cannot fetch open orders")
            return []

        try:
            self.open_orders.clear()
            self.open_orders_end_received = False
            self.orders_received_event.clear()

            print("📋 Requesting open orders from IBKR...")
            self.reqAllOpenOrders()

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
        """Fetch all positions from IBKR API synchronously. Returns a list of IbkrPosition objects."""
        if not self.connected:
            print("❌ Not connected to IBKR - cannot fetch positions")
            return []

        try:
            self.positions.clear()
            self.positions_end_received = False
            self.positions_received_event.clear()

            print("📊 Requesting positions from IBKR...")
            self.reqPositions()

            if self.positions_received_event.wait(10.0):
                print(f"✅ Received {len(self.positions)} positions")
                return self.positions.copy()
            else:
                print("❌ Timeout waiting for positions data")
                return []

        except Exception as e:
            print(f"❌ Failed to fetch positions: {e}")
            return []

    # --- IBKR API Callbacks ---
    def nextValidId(self, orderId: int) -> None:
        """Callback: Connection is ready and we have a valid order ID."""
        print(f"Connection established. Next valid order ID: {orderId}")
        self.next_valid_id = orderId
        self.connected = True
        self.connection_event.set()

    def error(self, reqId, errorCode, errorString, advancedOrderReject="") -> None:
        """Callback: Handle errors and messages from the IBKR API."""
        super().error(reqId, errorCode, errorString, advancedOrderReject)

        error_key = (reqId, errorCode)
        if error_key in self.displayed_errors:
            return

        self.displayed_errors.add(error_key)

        if errorCode in [10089, 10167, 322, 10201, 10202]:
            is_snapshot = "snapshot" in errorString.lower() or "not subscribed" in errorString.lower()
            if is_snapshot:
                print(f"📊 Snapshot Error {errorCode}: {errorString}")
                print("💡 Paper account snapshot data may be limited during off-hours")
            else:
                print(f"📊 Streaming Error {errorCode}: {errorString}")
        elif errorCode in [2104, 2106, 2158]:
            print(f"Connection: {errorString}")
        elif errorCode == 399:
            print(f"Order Warning: {errorString}")
        else:
            print(f"Error {errorCode}: {errorString}")

        if errorCode in [321, 322]:
            self.positions_received_event.set()
        if errorCode in [201, 202]:
            self.orders_received_event.set()

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str) -> None:
        """Callback: Received account value updates."""
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

    def managedAccounts(self, accountsList: str) -> None:
        """Callback: Received managed account list when connection is established."""
        super().managedAccounts(accountsList)
        print(f"Managed accounts: {accountsList}")

        if accountsList:
            self.account_number = accountsList.split(',')[0].strip()
            self.account_name = self.account_number  # Store account name for detection
            self.is_paper_account = is_paper_account(self.account_name)  # Use utility function
            
            env = "PAPER" if self.is_paper_account else "PRODUCTION"
            print(f"🎯 Auto-detected environment: {env} (Account: {self.account_name})")
            self.account_ready_event.set()

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice) -> None:
        """Callback: Order status updates."""
        for order in self.order_history:
            if order['order_id'] == orderId:
                order['status'] = status
                order['filled'] = float(filled)
                order['remaining'] = float(remaining)
                order['last_update'] = datetime.datetime.now()

                if status == 'Filled' and avgFillPrice > 0:
                    order['avg_fill_price'] = avgFillPrice

        print(f"Order status: {orderId} - {status}, Filled: {filled}, Remaining: {remaining}")

    def openOrder(self, orderId, contract, order, orderState) -> None:
        """Callback: Received open order data."""
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
                last_update_time=datetime.datetime.now()
            )
            self.open_orders.append(ibkr_order)
        except Exception as e:
            print(f"Error processing open order {orderId}: {e}")

    def openOrderEnd(self) -> None:
        """Callback: Finished receiving open orders."""
        self.open_orders_end_received = True
        self.orders_received_event.set()
        print("📋 Open orders request completed")

    def position(self, account: str, contract, position: float, avgCost: float) -> None:
        """Callback: Received position data."""
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

    def positionEnd(self) -> None:
        """Callback: Finished receiving positions."""
        self.positions_end_received = True
        self.positions_received_event.set()
        print("📊 Positions request completed")

    def tickPrice(self, reqId: int, tickType: int, price: float, attrib) -> None:
        """Callback: Receive market data price tick and forward to MarketDataManager if set."""
        super().tickPrice(reqId, tickType, price, attrib)

        if self.market_data_manager:
            try:
                self.market_data_manager.on_tick_price(reqId, tickType, price, attrib)
            except Exception as e:
                print(f"⚠️ Error forwarding tickPrice to MarketDataManager: {e}")

    # Add helper method
    def _get_port_from_mode(self, mode: str) -> int:
        """Determine port based on mode parameter."""
        if mode == 'paper':
            return 7497
        elif mode == 'live':
            return 7496
        elif mode == 'auto':
            # Default to paper for auto mode until account is detected
            return 7497
        else:
            raise ValueError(f"Invalid mode: {mode}. Use 'paper', 'live', or 'auto'")

    # Simplify connect method - remove port validation logic