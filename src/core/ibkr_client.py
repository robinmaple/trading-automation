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

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)

# <Context-Aware Logger Integration - Begin>
from src.core.context_aware_logger import get_context_logger, TradingEventType
# <Context-Aware Logger Integration - End>

class IbkrClient(EClient, EWrapper):
    """Manages the connection and all communication with the IBKR trading API."""

    def __init__(self, host='127.0.0.1', port=None, client_id=1, mode='auto'):
        """Initialize the client, connection flags, and data stores."""
        # <Context-Aware Logger Initialization - Begin>
        self.context_logger = get_context_logger()
        # <Context-Aware Logger Initialization - End>
        
        # <Context-Aware Logging - IbkrClient Initialization Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IbkrClient initialization starting",
            context_provider={
                "host": host,
                "port": port,
                "client_id": client_id,
                "mode": mode
            }
        )
        # <Context-Aware Logging - IbkrClient Initialization Start - End>
        
        # Minimal logging
        if logger:
            logger.debug("Initializing IbkrClient")
            
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

        # <Market Data Manager Thread Safety - Begin>
        self._manager_lock = threading.RLock()
        self._tick_errors = 0
        self._last_tick_time = None
        self._total_ticks_processed = 0
        # <Market Data Manager Thread Safety - End>

        # Add port determination based on mode
        if port is None:
            self.port = self._get_port_from_mode(mode)
        else:
            self.port = port
        self.mode = mode

        # <Context-Aware Logging - IbkrClient Initialization Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IbkrClient initialization completed",
            context_provider={
                "resolved_port": self.port,
                "mode": mode,
                "market_data_manager_ready": False
            }
        )
        # <Context-Aware Logging - IbkrClient Initialization Complete - End>

    # <AON Order Methods - Begin>
    def submit_aon_bracket_order(self, contract, action, order_type, security_type, entry_price, stop_loss,
                               risk_per_trade, risk_reward_ratio, total_capital) -> Optional[List[int]]:
        """
        Place a complete bracket order with All-or-Nothing (AON) execution.
        
        Args:
            contract: IBKR Contract object
            action: BUY or SELL
            order_type: Order type (LMT, MKT, etc.)
            security_type: Security type (STK, OPT, etc.)
            entry_price: Entry price for the order
            stop_loss: Stop loss price
            risk_per_trade: Risk percentage per trade
            risk_reward_ratio: Risk/reward ratio for profit target
            total_capital: Total account capital for position sizing
            
        Returns:
            List of order IDs for the bracket order, or None if failed
        """
        if not self.connected or self.next_valid_id is None:
            # <Context-Aware Logging - AON Order Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "AON bracket order failed - not connected or no valid order ID",
                symbol=getattr(contract, 'symbol', 'UNKNOWN'),
                context_provider={
                    'connected': self.connected,
                    'next_valid_id': self.next_valid_id,
                    'action': action,
                    'order_type': order_type,
                    'entry_price': entry_price
                },
                decision_reason="IBKR connection not ready for order placement"
            )
            # <Context-Aware Logging - AON Order Failed - End>
            if logger:
                logger.error("Not connected to IBKR or no valid order ID for AON order")
            return None

        try:
            orders = self._create_aon_bracket_order(
                action, order_type, security_type, entry_price, stop_loss,
                risk_per_trade, risk_reward_ratio, total_capital, self.next_valid_id
            )

            if logger:
                logger.info(f"Placing AON bracket order for {contract.symbol}")

            # <Context-Aware Logging - AON Order Placement Start - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Placing AON bracket order",
                symbol=contract.symbol,
                context_provider={
                    'action': action,
                    'order_type': order_type,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'risk_per_trade': risk_per_trade,
                    'risk_reward_ratio': risk_reward_ratio,
                    'total_capital': total_capital,
                    'starting_order_id': self.next_valid_id,
                    'order_count': len(orders)
                },
                decision_reason="AON bracket order meets placement criteria"
            )
            # <Context-Aware Logging - AON Order Placement Start - End>

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
                    'aon': getattr(order, 'allOrNone', False),
                    'timestamp': datetime.datetime.now()
                })

            self.next_valid_id += 3
            
            if logger:
                logger.info(f"AON bracket order placed successfully: {contract.symbol}")
                
            # <Context-Aware Logging - AON Order Placement Success - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "AON bracket order placed successfully",
                symbol=contract.symbol,
                context_provider={
                    'order_ids': order_ids,
                    'final_order_id': self.next_valid_id,
                    'order_count': len(orders)
                },
                decision_reason="AON bracket order submitted to IBKR API"
            )
            # <Context-Aware Logging - AON Order Placement Success - End>
            
            return order_ids

        except Exception as e:
            # <Context-Aware Logging - AON Order Placement Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "AON bracket order placement failed",
                symbol=getattr(contract, 'symbol', 'UNKNOWN'),
                context_provider={
                    'error': str(e),
                    'action': action,
                    'entry_price': entry_price,
                    'starting_order_id': self.next_valid_id
                },
                decision_reason=f"AON order placement exception: {e}"
            )
            # <Context-Aware Logging - AON Order Placement Error - End>
            if logger:
                logger.error(f"Failed to place AON bracket order: {e}")
            return None

    def _create_aon_bracket_order(self, action, order_type, security_type, entry_price, stop_loss,
                                risk_per_trade, risk_reward_ratio, total_capital, starting_order_id) -> List[Any]:
        """
        Create IBKR Order objects for AON bracket order.
        
        Returns:
            List of [parent, take_profit, stop_loss] orders with AON flag set
        """
        from ibapi.order import Order

        parent_id = starting_order_id
        quantity = self._calculate_quantity(security_type, entry_price, stop_loss,
                                          total_capital, risk_per_trade)
        profit_target = self._calculate_profit_target(action, entry_price, stop_loss, risk_reward_ratio)

        # 1. PARENT ORDER (Entry) - WITH AON FLAG
        parent = Order()
        parent.orderId = parent_id
        parent.action = action
        parent.orderType = order_type
        parent.totalQuantity = quantity
        parent.lmtPrice = round(entry_price, 5)
        parent.transmit = False
        parent.allOrNone = True  # <-- AON FLAG SET HERE

        # Determine opposite action for closing orders
        closing_action = "SELL" if action == "BUY" else "BUY"

        # 2. TAKE-PROFIT ORDER (child - inherits AON characteristics from parent execution)
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
        # Note: Child orders don't need AON flag - they execute based on parent fill

        # 3. STOP-LOSS ORDER (child - inherits AON characteristics from parent execution)
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
        # Note: Child orders don't need AON flag - they execute based on parent fill

        return [parent, take_profit, stop_loss_order]
    # <AON Order Methods - End>

    # <Market Data Manager Connection Methods - Begin>
    def set_market_data_manager(self, manager) -> None:
        """
        Thread-safe method to set the MarketDataManager instance.
        
        Args:
            manager: MarketDataManager instance to receive price ticks
        """
        with self._manager_lock:
            self.market_data_manager = manager
            if logger:
                logger.info(f"MarketDataManager connected to IbkrClient")
                
            # <Context-Aware Logging - Market Data Manager Connected - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "MarketDataManager connected to IbkrClient",
                context_provider={
                    'manager_type': type(manager).__name__,
                    'connected': True
                }
            )
            # <Context-Aware Logging - Market Data Manager Connected - End>

    def get_market_data_health(self) -> dict:
        """
        Get health metrics for market data flow monitoring.
        
        Returns:
            Dictionary with health metrics for troubleshooting
        """
        with self._manager_lock:
            health = {
                'manager_connected': self.market_data_manager is not None,
                'total_ticks_processed': self._total_ticks_processed,
                'tick_errors': self._tick_errors,
                'last_tick_time': self._last_tick_time,
                'connection_status': 'Connected' if self.connected else 'Disconnected',
                'manager_type': type(self.market_data_manager).__name__ if self.market_data_manager else 'None'
            }
            
            # Calculate error rate if we have processed ticks
            if self._total_ticks_processed > 0:
                health['error_rate_percent'] = round(
                    (self._tick_errors / self._total_ticks_processed) * 100, 2
                )
            else:
                health['error_rate_percent'] = 0.0
                
            return health
    # <Market Data Manager Connection Methods - End>

    def connect(self, host: Optional[str] = None, port: Optional[int] = None, 
                client_id: Optional[int] = None) -> bool:
        """Establish a connection to IB Gateway/TWS. Returns success status."""

        # Use provided parameters or fall back to instance defaults
        connect_host = host or self.host
        connect_port = port or self.port
        connect_client_id = client_id or self.client_id

        # <Context-Aware Logging - Connection Attempt Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Attempting IBKR connection",
            context_provider={
                'host': connect_host,
                'port': connect_port,
                'client_id': connect_client_id,
                'mode': self.mode
            }
        )
        # <Context-Aware Logging - Connection Attempt Start - End>
        
        if logger:
            logger.info(f"Connecting to IB API at {connect_host}:{connect_port}")

        EClient.connect(self, connect_host, connect_port, connect_client_id)
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

        # Wait for both: valid ID and account info
        ready = self.connection_event.wait(10) and self.account_ready_event.wait(10)
        if not ready:
            # <Context-Aware Logging - Connection Timeout - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR connection timeout",
                context_provider={
                    'host': connect_host,
                    'port': connect_port,
                    'timeout_seconds': 10,
                    'connection_event_set': self.connection_event.is_set(),
                    'account_ready_event_set': self.account_ready_event.is_set()
                },
                decision_reason="Connection timeout waiting for account details"
            )
            # <Context-Aware Logging - Connection Timeout - End>
            if logger:
                logger.error("Connection timed out waiting for account details")
            return False

        # Validate account ↔ port
        expected_port = get_ibkr_port(self.account_name)
        if connect_port != expected_port:
            # <Context-Aware Logging - Port Mismatch - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR port mismatch detected",
                context_provider={
                    'account_name': self.account_name,
                    'expected_port': expected_port,
                    'actual_port': connect_port,
                    'is_paper_account': self.is_paper_account
                },
                decision_reason=f"Account {self.account_name} requires port {expected_port}, but connected to {connect_port}"
            )
            # <Context-Aware Logging - Port Mismatch - End>
            if logger:
                logger.error(f"Port mismatch: account {self.account_name} requires port {expected_port}, but tried {connect_port}")
            self.disconnect()
            return False

        # <Context-Aware Logging - Connection Success - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKR connection established successfully",
            context_provider={
                'account_name': self.account_name,
                'account_number': self.account_number,
                'is_paper_account': self.is_paper_account,
                'port': connect_port,
                'next_valid_id': self.next_valid_id
            },
            decision_reason=f"Connected to {self.account_name} ({'PAPER' if self.is_paper_account else 'LIVE'})"
        )
        # <Context-Aware Logging - Connection Success - End>
        
        if logger:
            logger.info(f"Connected to {self.account_name} ({'PAPER' if self.is_paper_account else 'LIVE'}) on port {connect_port}")
        return True
    
    def disconnect(self) -> None:
        """Cleanly disconnect from TWS."""
        if self.connected:
            # <Context-Aware Logging - Disconnection Start - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Disconnecting from IBKR TWS",
                context_provider={
                    'account_name': self.account_name,
                    'next_valid_id': self.next_valid_id,
                    'open_orders_count': len(self.open_orders),
                    'positions_count': len(self.positions)
                }
            )
            # <Context-Aware Logging - Disconnection Start - End>
            
            if logger:
                logger.info("Disconnecting from IBKR TWS")
            super().disconnect()
            self.connected = False
            
            # <Context-Aware Logging - Disconnection Complete - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Disconnected from IBKR TWS",
                context_provider={
                    'connected': False,
                    'connection_event_cleared': True
                }
            )
            # <Context-Aware Logging - Disconnection Complete - End>

    def get_account_name(self) -> Optional[str]:
        """Get the connected account name."""
        return self.account_name

    def place_bracket_order(self, contract, action, order_type, security_type, entry_price, stop_loss,
                          risk_per_trade, risk_reward_ratio, total_capital) -> Optional[List[int]]:
        """Place a complete bracket order (entry, take-profit, stop-loss). Returns list of order IDs or None."""
        if not self.connected or self.next_valid_id is None:
            # <Context-Aware Logging - Bracket Order Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Bracket order failed - not connected or no valid order ID",
                symbol=getattr(contract, 'symbol', 'UNKNOWN'),
                context_provider={
                    'connected': self.connected,
                    'next_valid_id': self.next_valid_id,
                    'action': action,
                    'order_type': order_type
                },
                decision_reason="IBKR connection not ready for bracket order"
            )
            # <Context-Aware Logging - Bracket Order Failed - End>
            if logger:
                logger.error("Not connected to IBKR or no valid order ID")
            return None

        try:
            orders = self._create_bracket_order(
                action, order_type, security_type, entry_price, stop_loss,
                risk_per_trade, risk_reward_ratio, total_capital, self.next_valid_id
            )

            if logger:
                logger.info(f"Placing bracket order for {contract.symbol}")

            # <Context-Aware Logging - Bracket Order Placement Start - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Placing bracket order",
                symbol=contract.symbol,
                context_provider={
                    'action': action,
                    'order_type': order_type,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'risk_per_trade': risk_per_trade,
                    'risk_reward_ratio': risk_reward_ratio,
                    'total_capital': total_capital,
                    'starting_order_id': self.next_valid_id,
                    'order_count': len(orders)
                },
                decision_reason="Bracket order meets placement criteria"
            )
            # <Context-Aware Logging - Bracket Order Placement Start - End>

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
            
            if logger:
                logger.info(f"Bracket order placed successfully: {contract.symbol}")
                
            # <Context-Aware Logging - Bracket Order Placement Success - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Bracket order placed successfully",
                symbol=contract.symbol,
                context_provider={
                    'order_ids': order_ids,
                    'final_order_id': self.next_valid_id,
                    'order_count': len(orders)
                },
                decision_reason="Bracket order submitted to IBKR API"
            )
            # <Context-Aware Logging - Bracket Order Placement Success - End>
            
            return order_ids

        except Exception as e:
            # <Context-Aware Logging - Bracket Order Placement Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket order placement failed",
                symbol=getattr(contract, 'symbol', 'UNKNOWN'),
                context_provider={
                    'error': str(e),
                    'action': action,
                    'entry_price': entry_price,
                    'starting_order_id': self.next_valid_id
                },
                decision_reason=f"Bracket order placement exception: {e}"
            )
            # <Context-Aware Logging - Bracket Order Placement Error - End>
            if logger:
                logger.error(f"Failed to place bracket order: {e}")
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
            if logger:
                logger.debug(f"Options position: {quantity} contracts (each = 100 shares)")
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
            # <Context-Aware Logging - Cancel Order Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order cancellation failed - not connected to IBKR",
                context_provider={
                    'order_id': order_id,
                    'connected': False
                },
                decision_reason="IBKR not connected, cannot cancel order"
            )
            # <Context-Aware Logging - Cancel Order Failed - End>
            if logger:
                logger.error(f"Cannot cancel order {order_id} - not connected to IBKR")
            return False

        try:
            self.cancelOrder(order_id)
            # <Context-Aware Logging - Cancel Order Requested - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order cancellation requested",
                context_provider={
                    'order_id': order_id,
                    'connected': True
                },
                decision_reason="Cancel order request sent to IBKR API"
            )
            # <Context-Aware Logging - Cancel Order Requested - End>
            if logger:
                logger.info(f"Sent cancel request for order {order_id}")
            return True
        except Exception as e:
            # <Context-Aware Logging - Cancel Order Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Order cancellation failed with exception",
                context_provider={
                    'order_id': order_id,
                    'error': str(e)
                },
                decision_reason=f"Cancel order exception: {e}"
            )
            # <Context-Aware Logging - Cancel Order Error - End>
            if logger:
                logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    def get_open_orders(self) -> List[IbkrOrder]:
        """Fetch all open orders from IBKR API synchronously. Returns a list of IbkrOrder objects."""
        if not self.connected:
            # <Context-Aware Logging - Open Orders Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Open orders request failed - not connected to IBKR",
                context_provider={
                    'connected': False
                },
                decision_reason="IBKR not connected, cannot fetch open orders"
            )
            # <Context-Aware Logging - Open Orders Failed - End>
            if logger:
                logger.error("Not connected to IBKR - cannot fetch open orders")
            return []

        try:
            self.open_orders.clear()
            self.open_orders_end_received = False
            self.orders_received_event.clear()

            # <Context-Aware Logging - Open Orders Request Start - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Requesting open orders from IBKR",
                context_provider={
                    'timeout_seconds': 10.0
                }
            )
            # <Context-Aware Logging - Open Orders Request Start - End>
            
            if logger:
                logger.debug("Requesting open orders from IBKR...")
            self.reqAllOpenOrders()

            if self.orders_received_event.wait(10.0):
                # <Context-Aware Logging - Open Orders Success - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Open orders retrieved successfully",
                    context_provider={
                        'open_orders_count': len(self.open_orders)
                    }
                )
                # <Context-Aware Logging - Open Orders Success - End>
                if logger:
                    logger.info(f"Received {len(self.open_orders)} open orders")
                return self.open_orders.copy()
            else:
                # <Context-Aware Logging - Open Orders Timeout - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Timeout waiting for open orders data",
                    context_provider={
                        'timeout_seconds': 10.0
                    },
                    decision_reason="Open orders request timeout"
                )
                # <Context-Aware Logging - Open Orders Timeout - End>
                if logger:
                    logger.error("Timeout waiting for open orders data")
                return []

        except Exception as e:
            # <Context-Aware Logging - Open Orders Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Open orders request failed with exception",
                context_provider={
                    'error': str(e)
                },
                decision_reason=f"Open orders exception: {e}"
            )
            # <Context-Aware Logging - Open Orders Error - End>
            if logger:
                logger.error(f"Failed to fetch open orders: {e}")
            return []

    def get_positions(self) -> List[IbkrPosition]:
        """Fetch all positions from IBKR API synchronously. Returns a list of IbkrPosition objects."""
        if not self.connected:
            # <Context-Aware Logging - Positions Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Positions request failed - not connected to IBKR",
                context_provider={
                    'connected': False
                },
                decision_reason="IBKR not connected, cannot fetch positions"
            )
            # <Context-Aware Logging - Positions Failed - End>
            if logger:
                logger.error("Not connected to IBKR - cannot fetch positions")
            return []

        try:
            self.positions.clear()
            self.positions_end_received = False
            self.positions_received_event.clear()

            # <Context-Aware Logging - Positions Request Start - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Requesting positions from IBKR",
                context_provider={
                    'timeout_seconds': 10.0
                }
            )
            # <Context-Aware Logging - Positions Request Start - End>
            
            if logger:
                logger.debug("Requesting positions from IBKR...")
            self.reqPositions()

            if self.positions_received_event.wait(10.0):
                # <Context-Aware Logging - Positions Success - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Positions retrieved successfully",
                    context_provider={
                        'positions_count': len(self.positions)
                    }
                )
                # <Context-Aware Logging - Positions Success - End>
                if logger:
                    logger.info(f"Received {len(self.positions)} positions")
                return self.positions.copy()
            else:
                # <Context-Aware Logging - Positions Timeout - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Timeout waiting for positions data",
                    context_provider={
                        'timeout_seconds': 10.0
                    },
                    decision_reason="Positions request timeout"
                )
                # <Context-Aware Logging - Positions Timeout - End>
                if logger:
                    logger.error("Timeout waiting for positions data")
                return []

        except Exception as e:
            # <Context-Aware Logging - Positions Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Positions request failed with exception",
                context_provider={
                    'error': str(e)
                },
                decision_reason=f"Positions exception: {e}"
            )
            # <Context-Aware Logging - Positions Error - End>
            if logger:
                logger.error(f"Failed to fetch positions: {e}")
            return []

    # --- IBKR API Callbacks ---
    def nextValidId(self, orderId: int) -> None:
        """Callback: Connection is ready and we have a valid order ID."""
        # <Context-Aware Logging - Next Valid ID Received - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKR connection ready - next valid order ID received",
            context_provider={
                'next_valid_id': orderId,
                'connection_ready': True
            },
            decision_reason="IBKR connection established and ready for order placement"
        )
        # <Context-Aware Logging - Next Valid ID Received - End>
        if logger:
            logger.info(f"Connection established. Next valid order ID: {orderId}")
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

        # <Context-Aware Logging - IBKR Error - Begin>
        error_context = {
            'req_id': reqId,
            'error_code': errorCode,
            'error_string': errorString,
            'advanced_order_reject': advancedOrderReject,
            'total_unique_errors': len(self.displayed_errors)
        }
        
        # Categorize error severity for structured logging
        if errorCode in [10089, 10167, 322, 10201, 10202]:
            is_snapshot = "snapshot" in errorString.lower() or "not subscribed" in errorString.lower()
            if is_snapshot:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"IBKR Snapshot Error {errorCode}",
                    context_provider=error_context,
                    decision_reason=f"Snapshot error: {errorString}"
                )
                if logger:
                    logger.warning(f"Snapshot Error {errorCode}: {errorString}")
            else:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"IBKR Streaming Error {errorCode}",
                    context_provider=error_context,
                    decision_reason=f"Streaming error: {errorString}"
                )
                if logger:
                    logger.warning(f"Streaming Error {errorCode}: {errorString}")
        elif errorCode in [2104, 2106, 2158]:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"IBKR Connection Message",
                context_provider=error_context,
                decision_reason=f"Connection message: {errorString}"
            )
            if logger:
                logger.info(f"Connection: {errorString}")
        elif errorCode == 399:
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                f"IBKR Order Warning {errorCode}",
                context_provider=error_context,
                decision_reason=f"Order warning: {errorString}"
            )
            if logger:
                logger.warning(f"Order Warning: {errorString}")
        else:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"IBKR Error {errorCode}",
                context_provider=error_context,
                decision_reason=f"API error: {errorString}"
            )
            if logger:
                logger.error(f"Error {errorCode}: {errorString}")
        # <Context-Aware Logging - IBKR Error - End>

        if errorCode in [321, 322]:
            self.positions_received_event.set()
        if errorCode in [201, 202]:
            self.orders_received_event.set()

    def managedAccounts(self, accountsList: str) -> None:
        """Callback: Received managed account list when connection is established."""
        super().managedAccounts(accountsList)
        if logger:
            logger.info(f"Managed accounts: {accountsList}")

        if accountsList:
            self.account_number = accountsList.split(',')[0].strip()
            self.account_name = self.account_number  # Store account name for detection
            self.is_paper_account = is_paper_account(self.account_name)  # Use utility function
            
            env = "PAPER" if self.is_paper_account else "PRODUCTION"
            
            # <Context-Aware Logging - Account Detection - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR account detected",
                context_provider={
                    'account_name': self.account_name,
                    'account_number': self.account_number,
                    'is_paper_account': self.is_paper_account,
                    'environment': env,
                    'all_accounts': accountsList
                },
                decision_reason=f"Auto-detected {env} trading environment"
            )
            # <Context-Aware Logging - Account Detection - End>
            
            if logger:
                logger.info(f"Auto-detected environment: {env} (Account: {self.account_name})")
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

        # <Context-Aware Logging - Order Status Update - Begin>
        if status in ['Filled', 'Cancelled', 'Submitted']:
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                f"Order status update: {status}",
                context_provider={
                    'order_id': orderId,
                    'status': status,
                    'filled_quantity': float(filled),
                    'remaining_quantity': float(remaining),
                    'avg_fill_price': avgFillPrice,
                    'perm_id': permId,
                    'parent_id': parentId
                },
                decision_reason=f"Order {orderId} status changed to {status}"
            )
        # <Context-Aware Logging - Order Status Update - End>

        if logger:
            logger.debug(f"Order status: {orderId} - {status}, Filled: {filled}, Remaining: {remaining}")

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
            # <Context-Aware Logging - Open Order Processing Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error processing open order data",
                context_provider={
                    'order_id': orderId,
                    'error': str(e),
                    'symbol': getattr(contract, 'symbol', 'UNKNOWN')
                },
                decision_reason=f"Open order processing error: {e}"
            )
            # <Context-Aware Logging - Open Order Processing Error - End>
            if logger:
                logger.error(f"Error processing open order {orderId}: {e}")

    def openOrderEnd(self) -> None:
        """Callback: Finished receiving open orders."""
        self.open_orders_end_received = True
        self.orders_received_event.set()
        # <Context-Aware Logging - Open Orders Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Open orders request completed",
            context_provider={
                'open_orders_count': len(self.open_orders),
                'end_received': True
            }
        )
        # <Context-Aware Logging - Open Orders Complete - End>
        if logger:
            logger.debug("Open orders request completed")

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
            # <Context-Aware Logging - Position Processing Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error processing position data",
                context_provider={
                    'symbol': getattr(contract, 'symbol', 'UNKNOWN'),
                    'error': str(e),
                    'position_value': position
                },
                decision_reason=f"Position processing error: {e}"
            )
            # <Context-Aware Logging - Position Processing Error - End>
            if logger:
                logger.error(f"Error processing position for {contract.symbol}: {e}")

    def positionEnd(self) -> None:
        """Callback: Finished receiving positions."""
        self.positions_end_received = True
        self.positions_received_event.set()
        # <Context-Aware Logging - Positions Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Positions request completed",
            context_provider={
                'positions_count': len(self.positions),
                'end_received': True
            }
        )
        # <Context-Aware Logging - Positions Complete - End>
        if logger:
            logger.debug("Positions request completed")

    # <Enhanced tickPrice with Thread Safety and Error Handling - Begin>
    def tickPrice(self, reqId: int, tickType: int, price: float, attrib) -> None:
        """
        Callback: Receive market data price tick and forward to MarketDataManager if set.
        Enhanced with thread safety and comprehensive error handling.
        """
        super().tickPrice(reqId, tickType, price, attrib)

        # Update health metrics
        self._last_tick_time = datetime.datetime.now()
        self._total_ticks_processed += 1

        # Thread-safe access to market data manager
        with self._manager_lock:
            if self.market_data_manager:
                try:
                    self.market_data_manager.on_tick_price(reqId, tickType, price, attrib)
                    
                    # Log first successful tick for debugging
                    if self._total_ticks_processed == 1:
                        tick_type_name = {1: 'BID', 2: 'ASK', 4: 'LAST'}.get(tickType, f'UNKNOWN({tickType})')
                        # <Context-Aware Logging - First Tick Processed - Begin>
                        self.context_logger.log_event(
                            TradingEventType.MARKET_CONDITION,
                            "First market data tick processed",
                            context_provider={
                                'tick_type': tick_type_name,
                                'price': price,
                                'req_id': reqId,
                                'total_ticks_processed': 1
                            }
                        )
                        # <Context-Aware Logging - First Tick Processed - End>
                        if logger:
                            logger.info(f"FIRST TICK PROCESSED: Type {tick_type_name}, Price ${price}")
                        
                except Exception as e:
                    self._tick_errors += 1
                    error_count = self._tick_errors
                    
                    # Only log periodic errors to avoid spam
                    if error_count <= 3 or error_count % 10 == 0:
                        # <Context-Aware Logging - Tick Processing Error - Begin>
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            "Market data tick processing error",
                            context_provider={
                                'error_count': error_count,
                                'total_ticks_processed': self._total_ticks_processed,
                                'error': str(e),
                                'req_id': reqId,
                                'tick_type': tickType
                            },
                            decision_reason=f"Tick processing error #{error_count}: {e}"
                        )
                        # <Context-Aware Logging - Tick Processing Error - End>
                        if logger:
                            logger.warning(f"Error in market data processing (Error #{error_count}): {e}")
            else:
                # Only log missing manager occasionally to avoid spam
                if self._total_ticks_processed <= 5 or self._total_ticks_processed % 50 == 0:
                    # <Context-Aware Logging - Missing Market Data Manager - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Market data tick received but no manager connected",
                        context_provider={
                            'total_ticks_processed': self._total_ticks_processed,
                            'req_id': reqId,
                            'tick_type': tickType,
                            'price': price
                        },
                        decision_reason=f"No MarketDataManager for tick processing (total: {self._total_ticks_processed})"
                    )
                    # <Context-Aware Logging - Missing Market Data Manager - End>
                    if logger:
                        logger.debug(f"Tick received but no MarketDataManager connected (Total ticks: {self._total_ticks_processed})")
    # <Enhanced tickPrice with Thread Safety and Error Handling - End>

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

    def get_account_number(self) -> Optional[str]:
        """Get the current account number for order placement."""
        return self.account_number
    
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str) -> None:
        """Callback: Received account value updates with comprehensive debugging."""
        super().updateAccountValue(key, val, currency, accountName)

        # <Context-Aware Logging - Account Value Update Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Account value update received",
            context_provider={
                'key': key,
                'value': val,
                'currency': currency,
                'account_name': accountName,
                'value_type': type(val).__name__,
                'value_length': len(val) if val else 0
            }
        )
        # <Context-Aware Logging - Account Value Update Start - End>

        # DEBUG: Log ALL account values received to console
        print(f"🔍 DEBUG Account Value: key='{key}', value='{val}', currency='{currency}', account='{accountName}'")
        
        try:
            # Store all values for debugging with currency context
            if key not in self.account_values:
                self.account_values[key] = {}
            
            # Store the value with currency context
            self.account_values[key][currency] = val

            # Also store the raw values for specific currencies
            if currency in ["CAD", "USD", "BASE"]:
                currency_key = f"{key}_{currency}"
                try:
                    numeric_value = float(val) if val and val.strip() else 0.0
                    self.account_values[currency_key] = numeric_value
                    
                    # <Context-Aware Logging - Currency Value Stored - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Stored currency-specific account value",
                        context_provider={
                            'currency_key': currency_key,
                            'numeric_value': numeric_value,
                            'original_value': val,
                            'currency': currency
                        }
                    )
                    # <Context-Aware Logging - Currency Value Stored - End>
                    
                except (ValueError, TypeError) as e:
                    # <Context-Aware Logging - Value Conversion Error - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Failed to convert account value to numeric",
                        context_provider={
                            'key': key,
                            'value': val,
                            'currency': currency,
                            'error': str(e)
                        },
                        decision_reason=f"Account value conversion failed: {e}"
                    )
                    # <Context-Aware Logging - Value Conversion Error - End>
                    print(f"❌ ERROR converting value: key='{key}', value='{val}', error={e}")

            # Store specific important values for easy access
            important_keys = ["NetLiquidation", "BuyingPower", "AvailableFunds", "TotalCashValue", "CashBalance"]
            if key in important_keys and currency == "CAD":
                try:
                    self.account_values[key] = float(val) if val and val.strip() else 0.0
                    
                    # <Context-Aware Logging - Important Value Stored - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Stored important account value",
                        context_provider={
                            'key': key,
                            'value': self.account_values[key],
                            'currency': currency,
                            'category': 'primary_capital_field'
                        }
                    )
                    # <Context-Aware Logging - Important Value Stored - End>
                    
                except (ValueError, TypeError) as e:
                    # <Context-Aware Logging - Important Value Error - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Failed to store important account value",
                        context_provider={
                            'key': key,
                            'value': val,
                            'currency': currency,
                            'error': str(e)
                        },
                        decision_reason=f"Important account value storage failed: {e}"
                    )
                    # <Context-Aware Logging - Important Value Error - End>

            self.account_value_received.set()
            
            # <Context-Aware Logging - Account Value Update Complete - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Account value update processing completed",
                context_provider={
                    'total_keys_stored': len(self.account_values),
                    'key_processed': key,
                    'currency_processed': currency
                }
            )
            # <Context-Aware Logging - Account Value Update Complete - End>

        except Exception as e:
            # <Context-Aware Logging - Account Value Processing Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Account value update processing failed",
                context_provider={
                    'key': key,
                    'value': val,
                    'currency': currency,
                    'error_type': type(e).__name__,
                    'error_message': str(e)
                },
                decision_reason=f"Account value processing exception: {e}"
            )
            # <Context-Aware Logging - Account Value Processing Error - End>
            print(f"❌ CRITICAL ERROR in updateAccountValue: {e}")

    def get_account_value(self) -> float:
        """Request and return the Net Liquidation value of the account with enhanced financial data retrieval."""
        print("🎯 DEBUG: ENHANCED get_account_value() METHOD CALLED!")

        if not self.connected:
            # <Context-Aware Logging - Account Value Fallback - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Using fallback account value - not connected to IBKR",
                context_provider={
                    'connected': False,
                    'fallback_value': 100000.0
                },
                decision_reason="IBKR not connected, using simulation account value"
            )
            # <Context-Aware Logging - Account Value Fallback - End>
            if logger:
                logger.warning("Not connected to IBKR - using fallback account value")
            return 100000.0

        # <Context-Aware Logging - Enhanced Account Value Request - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting enhanced account value retrieval with multiple strategies",
            context_provider={
                'account_number': self.account_number,
                'timeout_seconds': 10.0
            }
        )
        # <Context-Aware Logging - Enhanced Account Value Request - End>

        print("🔄 DEBUG: Starting enhanced capital detection...")
        
        # Strategy 1: Try Account Summary API first (most reliable for financial data)
        capital = self._get_account_value_via_summary()
        if capital != 100000.0:
            return capital
            
        # Strategy 2: Fall back to traditional Account Updates
        capital = self._get_account_value_via_updates()  
        if capital != 100000.0:
            return capital
            
        # Strategy 3: Final fallback
        print("❌ DEBUG: All capital detection strategies failed, using fallback $100,000")
        return 100000.0

    def _get_account_value_via_summary(self) -> float:
        """Use Account Summary API to get financial data (primary strategy)."""
        try:
            self.account_values.clear()
            self.account_value_received.clear()
            
            # Key financial fields to request
            financial_fields = [
                "NetLiquidation", "BuyingPower", "AvailableFunds", 
                "TotalCashValue", "CashBalance", "EquityWithLoanValue",
                "GrossPositionValue", "MaintMarginReq", "FullInitMarginReq"
            ]
            
            print(f"🔍 DEBUG [Strategy 1]: Requesting Account Summary for {len(financial_fields)} financial fields")
            print(f"🔍 DEBUG [Strategy 1]: Fields: {financial_fields}")
            
            # Request account summary with specific financial fields
            self.reqAccountSummary(9001, "All", ",".join(financial_fields))
            
            # Wait for data with timeout
            if not self.account_value_received.wait(8.0):
                print("❌ DEBUG [Strategy 1]: Account Summary timeout - no financial data received")
                self.cancelAccountSummary(9001)
                return 100000.0

            # Cancel the summary request
            self.cancelAccountSummary(9001)
            
            # Analyze received data
            print(f"📊 DEBUG [Strategy 1]: Account Summary received {len(self.account_values)} values")
            
            # Log all received values for debugging
            financial_values_found = {}
            for key, value in self.account_values.items():
                if isinstance(value, dict):
                    for currency, val in value.items():
                        composite_key = f"{key}_{currency}"
                        financial_values_found[composite_key] = str(val)
                else:
                    financial_values_found[key] = str(value)
                    
            print("🔍 DEBUG [Strategy 1]: Financial values received:")
            for k, v in financial_values_found.items():
                print(f"   {k}: {v}")

            # Try different capital fields in priority order
            capital = None
            capital_source = "unknown"
            
            # Priority 1: CAD-specific values
            cad_priority_fields = [
                "NetLiquidation_CAD", "AvailableFunds_CAD", "BuyingPower_CAD",
                "TotalCashValue_CAD", "CashBalance_CAD", "EquityWithLoanValue_CAD"
            ]
            
            for field in cad_priority_fields:
                if field in self.account_values:
                    capital = self.account_values[field]
                    capital_source = field
                    print(f"✅ DEBUG [Strategy 1]: Using {field}: ${capital:,.2f}")
                    break
                    
            # Priority 2: BASE currency values
            if capital is None:
                base_priority_fields = [
                    "NetLiquidation_BASE", "AvailableFunds_BASE", "BuyingPower_BASE",
                    "TotalCashValue_BASE", "CashBalance_BASE", "EquityWithLoanValue_BASE"
                ]
                for field in base_priority_fields:
                    if field in self.account_values:
                        capital = self.account_values[field]
                        capital_source = field
                        print(f"✅ DEBUG [Strategy 1]: Using {field}: ${capital:,.2f}")
                        break
            
            # Priority 3: Generic values (any currency)
            if capital is None:
                generic_priority_fields = [
                    "NetLiquidation", "AvailableFunds", "BuyingPower",
                    "TotalCashValue", "CashBalance", "EquityWithLoanValue"
                ]
                for field in generic_priority_fields:
                    if field in self.account_values:
                        capital = self.account_values[field]
                        capital_source = field
                        print(f"✅ DEBUG [Strategy 1]: Using {field}: ${capital:,.2f}")
                        break

            if capital is not None and capital > 0:
                # <Context-Aware Logging - Account Summary Success - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Capital determined via Account Summary",
                    context_provider={
                        'final_capital': capital,
                        'capital_source': capital_source,
                        'strategy': 'account_summary',
                        'values_found_count': len(financial_values_found)
                    },
                    decision_reason=f"Successfully determined capital: ${capital:,.2f} from {capital_source}"
                )
                # <Context-Aware Logging - Account Summary Success - End>
                return capital
                
            print("❌ DEBUG [Strategy 1]: No valid financial data found in Account Summary")
            return 100000.0
            
        except Exception as e:
            print(f"❌ DEBUG [Strategy 1]: Account Summary exception: {e}")
            # Cancel summary request on error
            try:
                self.cancelAccountSummary(9001)
            except:
                pass
            return 100000.0

    def _get_account_value_via_updates(self) -> float:
        """Fallback strategy using traditional Account Updates."""
        try:
            self.account_value_received.clear()
            previous_values_count = len(self.account_values)
            self.account_values.clear()

            print("🔄 DEBUG [Strategy 2]: Trying traditional Account Updates...")
            self.reqAccountUpdates(True, self.account_number)

            if not self.account_value_received.wait(5.0):
                print("❌ DEBUG [Strategy 2]: Account Updates timeout")
                self.reqAccountUpdates(False, self.account_number)
                return 100000.0

            # Analyze received data
            print(f"📊 DEBUG [Strategy 2]: Account Updates received {len(self.account_values)} values")
            
            # Log all received values
            all_values_summary = {}
            for key, value in self.account_values.items():
                if isinstance(value, dict):
                    for currency, val in value.items():
                        composite_key = f"{key}_{currency}"
                        all_values_summary[composite_key] = str(val)
                else:
                    all_values_summary[key] = str(value)
                    
            print("🔍 DEBUG [Strategy 2]: All values received:")
            for k, v in all_values_summary.items():
                print(f"   {k}: {v}")

            # Try to find capital in received data (same priority logic as before)
            capital = None
            capital_source = "unknown"
            
            # Priority order for capital fields
            capital_priority = [
                "NetLiquidation_CAD", "NetLiquidation", "BuyingPower_CAD", "BuyingPower",
                "AvailableFunds_CAD", "AvailableFunds", "TotalCashValue_CAD", "TotalCashValue",
                "CashBalance_CAD", "CashBalance", "EquityWithLoanValue_CAD", "EquityWithLoanValue"
            ]
            
            for field in capital_priority:
                if field in self.account_values:
                    capital = self.account_values[field]
                    capital_source = field
                    print(f"✅ DEBUG [Strategy 2]: Using {field}: ${capital:,.2f}")
                    break

            if capital is not None and capital > 0:
                # <Context-Aware Logging - Account Updates Success - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Capital determined via Account Updates",
                    context_provider={
                        'final_capital': capital,
                        'capital_source': capital_source,
                        'strategy': 'account_updates',
                        'values_found_count': len(all_values_summary)
                    },
                    decision_reason=f"Successfully determined capital: ${capital:,.2f} from {capital_source}"
                )
                # <Context-Aware Logging - Account Updates Success - End>
                self.reqAccountUpdates(False, self.account_number)
                return capital
                
            print("❌ DEBUG [Strategy 2]: No valid capital found in Account Updates")
            self.reqAccountUpdates(False, self.account_number)
            return 100000.0
            
        except Exception as e:
            print(f"❌ DEBUG [Strategy 2]: Account Updates exception: {e}")
            try:
                self.reqAccountUpdates(False, self.account_number)
            except:
                pass
            return 100000.0