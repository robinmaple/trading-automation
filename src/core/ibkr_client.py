"""
A high-level, test-friendly client for the Interactive Brokers (IBKR) TWS API.
Handles all low-level communication, including connection management, order placement,
account data retrieval, and real-time updates. Subclasses the official IBKR EClient/EWrapper.
"""

import math
import time
from ibapi.client import *
from ibapi.wrapper import *
from ibapi.order_cancel import OrderCancel
from typing import List, Optional, Any, Dict
import threading
import datetime
import queue

from src.core.ibkr_types import IbkrOrder, IbkrPosition
from src.core.account_utils import is_paper_account, get_ibkr_port

# <Context-Aware Logger Integration - Begin>
from src.core.context_aware_logger import get_context_logger, TradingEventType
# <Context-Aware Logger Integration - End>

# Valid numeric account value fields - Begin (NEW)
VALID_NUMERIC_ACCOUNT_FIELDS = {
    "NetLiquidation", "BuyingPower", "AvailableFunds", "TotalCashValue", 
    "CashBalance", "EquityWithLoanValue", "GrossPositionValue", 
    "MaintMarginReq", "FullInitMarginReq", "FullAvailableFunds",
    "FullExcessLiquidity", "Cushion", "LookAheadNextChange"
}
# Valid numeric account value fields - End

# Bracket Order Constants - Begin (NEW)
BRACKET_ORDER_TIMEOUT = 10.0  # seconds to wait for all bracket components
BRACKET_TRANSMISSION_CHECK_INTERVAL = 0.5  # seconds between transmission checks
# Bracket Order Constants - End

class IbkrClient(EClient, EWrapper):
    """Manages the connection and all communication with the IBKR trading API."""

    # __init__ - Begin (UPDATED - Add EOD provider reference)
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

        # <Early Tick Queue for Market Data Callbacks - Begin>
        self._early_tick_queue = queue.Queue(maxsize=1000)  # Buffer for ticks before manager ready
        self._max_early_ticks_logged = 10  # Limit log spam for early ticks
        self._early_ticks_logged = 0
        self._manager_connection_time = None
        # <Early Tick Queue for Market Data Callbacks - End>

        # <Historical Data Manager Integration - Begin>
        self.historical_data_manager = None
        self._historical_manager_lock = threading.RLock()
        # <Historical Data Manager Integration - End>

        # <Historical EOD Provider Integration - Begin>
        self.historical_eod_provider = None  # Direct reference for scanner callbacks
        # <Historical EOD Provider Integration - End>

        # <Bracket Order Tracking - Begin (NEW)>
        self._active_bracket_orders: Dict[int, Dict] = {}  # parent_id -> bracket order info
        self._bracket_order_lock = threading.RLock()
        self._bracket_transmission_events: Dict[int, threading.Event] = {}  # parent_id -> transmission event
        # <Bracket Order Tracking - End>

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
                "market_data_manager_ready": False,
                "historical_data_manager_ready": False,
                "historical_eod_provider_ready": False,
                "early_tick_queue_initialized": True
            }
        )
        # <Context-Aware Logging - IbkrClient Initialization Complete - End>
    # __init__ - End
    
    # <Historical Data Manager Integration Methods - Begin>
    def set_historical_data_manager(self, manager) -> None:
        """
        Thread-safe method to set the HistoricalDataManager instance.
        
        Args:
            manager: HistoricalDataManager instance to receive historical data callbacks
        """
        with self._historical_manager_lock:
            self.historical_data_manager = manager
            if logger:
                logger.info(f"HistoricalDataManager connected to IbkrClient")
                
            # <Context-Aware Logging - Historical Data Manager Connected - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "HistoricalDataManager connected to IbkrClient",
                context_provider={
                    'manager_type': type(manager).__name__,
                    'connected': True
                }
            )
            # <Context-Aware Logging - Historical Data Manager Connected - End>

    def historicalData(self, reqId: int, bar) -> None:
        """
        Callback: Receive historical data bar and forward to HistoricalDataManager if set.
        """
        super().historicalData(reqId, bar)

        # Thread-safe access to historical data manager
        with self._historical_manager_lock:
            if self.historical_data_manager:
                try:
                    self.historical_data_manager.historical_data(reqId, bar)
                    
                    # Log first successful historical data for debugging
                    if hasattr(self, '_first_historical_received') and not self._first_historical_received:
                        self._first_historical_received = True
                        # <Context-Aware Logging - First Historical Data - Begin>
                        self.context_logger.log_event(
                            TradingEventType.MARKET_CONDITION,
                            "First historical data bar processed",
                            context_provider={
                                'req_id': reqId,
                                'bar_date': bar.date,
                                'close_price': bar.close,
                                'volume': bar.volume
                            }
                        )
                        # <Context-Aware Logging - First Historical Data - End>
                        if logger:
                            logger.info(f"FIRST HISTORICAL DATA: Req {reqId}, Date {bar.date}, Close ${bar.close}")
                        
                except Exception as e:
                    # <Context-Aware Logging - Historical Data Processing Error - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Historical data processing error",
                        context_provider={
                            'req_id': reqId,
                            'error': str(e),
                            'bar_date': bar.date
                        },
                        decision_reason=f"Historical data processing error: {e}"
                    )
                    # <Context-Aware Logging - Historical Data Processing Error - End>
                    if logger:
                        logger.warning(f"Error in historical data processing: {e}")
            else:
                # Only log missing manager occasionally to avoid spam
                if not hasattr(self, '_historical_manager_warned') or not self._historical_manager_warned:
                    self._historical_manager_warned = True
                    # <Context-Aware Logging - Missing Historical Data Manager - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Historical data received but no manager connected",
                        context_provider={
                            'req_id': reqId,
                            'bar_date': bar.date,
                            'close_price': bar.close
                        },
                        decision_reason="No HistoricalDataManager for historical data processing"
                    )
                    # <Context-Aware Logging - Missing Historical Data Manager - End>
                    if logger:
                        logger.debug(f"Historical data received but no HistoricalDataManager connected")

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        """
        Callback: Historical data request ended - forward to HistoricalDataManager if set.
        """
        super().historicalDataEnd(reqId, start, end)

        # Thread-safe access to historical data manager
        with self._historical_manager_lock:
            if self.historical_data_manager:
                try:
                    self.historical_data_manager.historical_data_end(reqId, start, end)
                    
                    # <Context-Aware Logging - Historical Data End Processed - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Historical data end callback processed",
                        context_provider={
                            'req_id': reqId,
                            'start_date': start,
                            'end_date': end
                        }
                    )
                    # <Context-Aware Logging - Historical Data End Processed - End>
                    
                except Exception as e:
                    # <Context-Aware Logging - Historical Data End Error - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Historical data end processing error",
                        context_provider={
                            'req_id': reqId,
                            'error': str(e),
                            'start_date': start,
                            'end_date': end
                        },
                        decision_reason=f"Historical data end processing error: {e}"
                    )
                    # <Context-Aware Logging - Historical Data End Error - End>
                    if logger:
                        logger.warning(f"Error in historical data end processing: {e}")
    # <Historical Data Manager Integration Methods - End>

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

    # set_market_data_manager - UPDATED (Enhanced with queue processing)
    def set_market_data_manager(self, manager) -> None:
        """
        Thread-safe method to set the MarketDataManager instance.
        Processes any queued ticks that arrived before manager was available.
        
        Args:
            manager: MarketDataManager instance to receive price ticks
        """
        with self._manager_lock:
            self.market_data_manager = manager
            self._manager_connection_time = datetime.datetime.now()
            
            # Process any ticks that arrived before manager was ready
            processed_ticks = 0
            dropped_ticks = 0
            
            try:
                while not self._early_tick_queue.empty():
                    try:
                        tick_data = self._early_tick_queue.get_nowait()
                        reqId, tickType, price, attrib = tick_data
                        
                        if self.market_data_manager:
                            self.market_data_manager.on_tick_price(reqId, tickType, price, attrib)
                            processed_ticks += 1
                        else:
                            dropped_ticks += 1
                            
                        self._early_tick_queue.task_done()
                    except queue.Empty:
                        break
            except Exception as e:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Error processing early tick queue",
                    context_provider={
                        'error': str(e),
                        'processed_ticks': processed_ticks,
                        'dropped_ticks': dropped_ticks
                    }
                )
            
            # Log connection and queue processing results
            connection_context = {
                'manager_type': type(manager).__name__,
                'connected': True,
                'early_ticks_processed': processed_ticks,
                'early_ticks_dropped': dropped_ticks,
                'remaining_queue_size': self._early_tick_queue.qsize(),
                'manager_connection_time': self._manager_connection_time.isoformat()
            }
            
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "MarketDataManager connected to IbkrClient with early tick processing",
                context_provider=connection_context
            )
            
            if logger:
                logger.info(f"MarketDataManager connected to IbkrClient - Processed {processed_ticks} early ticks")
                
            # Reset early tick logging counter
            self._early_ticks_logged = 0
    # set_market_data_manager - End

    # get_market_data_health - UPDATED (Add queue metrics)
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
                'manager_type': type(self.market_data_manager).__name__ if self.market_data_manager else 'None',
                'early_tick_queue_size': self._early_tick_queue.qsize(),
                'early_ticks_logged': self._early_ticks_logged,
                'manager_connection_time': self._manager_connection_time
            }
            
            # Calculate error rate if we have processed ticks
            if self._total_ticks_processed > 0:
                health['error_rate_percent'] = round(
                    (self._tick_errors / self._total_ticks_processed) * 100, 2
                )
            else:
                health['error_rate_percent'] = 0.0
                
            return health
    # get_market_data_health - End
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

    # _calculate_profit_target - Begin (UPDATED - Enhanced validation)
    def _calculate_profit_target(self, action, entry_price, stop_loss, risk_reward_ratio) -> float:
        """Calculate profit target price based on risk/reward ratio with enhanced validation."""
        if entry_price is None or entry_price <= 0:
            raise ValueError(f"Invalid entry_price for profit target: {entry_price}")
        if stop_loss is None or stop_loss <= 0:
            raise ValueError(f"Invalid stop_loss for profit target: {stop_loss}")
        if risk_reward_ratio is None or risk_reward_ratio <= 0:
            raise ValueError(f"Invalid risk_reward_ratio for profit target: {risk_reward_ratio}")

        risk_amount = abs(entry_price - stop_loss)
        if risk_amount == 0:
            raise ValueError("Entry price and stop loss cannot be the same")

        try:
            if action == "BUY":
                profit_target = entry_price + (risk_amount * risk_reward_ratio)
            else:
                profit_target = entry_price - (risk_amount * risk_reward_ratio)
                
            # Validate the calculated profit target
            if profit_target <= 0:
                raise ValueError(f"Calculated profit target is invalid: {profit_target}")
                
            # Ensure profit target is meaningfully different from entry price
            if abs(profit_target - entry_price) < (risk_amount * 0.1):  # At least 10% of risk amount
                raise ValueError(f"Profit target too close to entry price: {profit_target} vs {entry_price}")
                
            return profit_target
            
        except Exception as e:
            # <Context-Aware Logging - Profit Target Calculation Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Profit target calculation failed",
                context_provider={
                    'action': action,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'risk_reward_ratio': risk_reward_ratio,
                    'error': str(e)
                },
                decision_reason=f"Profit target calculation error: {e}"
            )
            # <Context-Aware Logging - Profit Target Calculation Error - End>
            raise
    # _calculate_profit_target - End

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

    # error - Begin (UPDATED - Fix method signature for IBKR API compatibility)
    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="", *args) -> None:
        """Callback: Handle errors and messages from the IBKR API.
        
        Updated signature to handle both old and new IBKR API versions.
        *args captures any additional arguments that might be passed.
        """
        # Use the standard error handling logic but ignore extra args
        super().error(reqId, errorCode, errorString, advancedOrderRejectJson)

        error_key = (reqId, errorCode)
        if error_key in self.displayed_errors:
            return

        self.displayed_errors.add(error_key)

        # <Context-Aware Logging - IBKR Error - Begin>
        error_context = {
            'req_id': reqId,
            'error_code': errorCode,
            'error_string': errorString,
            'advanced_order_reject': advancedOrderRejectJson,
            'total_unique_errors': len(self.displayed_errors),
            'api_version_compatibility': 'extended_signature_used'
        }
        
        # [Rest of your existing error handling logic remains the same...]
    # error - End

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

    # tickPrice - UPDATED (Enhanced with early tick queuing)
    def tickPrice(self, reqId: int, tickType: int, price: float, attrib) -> None:
        """
        Callback: Receive market data price tick and forward to MarketDataManager if set.
        Enhanced with early tick queuing to prevent data loss during manager connection.
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
                        self.context_logger.log_event(
                            TradingEventType.MARKET_CONDITION,
                            "First market data tick processed",
                            context_provider={
                                'tick_type': tick_type_name,
                                'price': price,
                                'req_id': reqId,
                                'total_ticks_processed': 1,
                                'manager_available': True
                            }
                        )
                        if logger:
                            logger.info(f"FIRST TICK PROCESSED: Type {tick_type_name}, Price ${price}")
                        
                except Exception as e:
                    self._tick_errors += 1
                    error_count = self._tick_errors
                    
                    # Only log periodic errors to avoid spam
                    if error_count <= 3 or error_count % 10 == 0:
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            "Market data tick processing error",
                            context_provider={
                                'error_count': error_count,
                                'total_ticks_processed': self._total_ticks_processed,
                                'error': str(e),
                                'req_id': reqId,
                                'tick_type': tickType,
                                'manager_available': True
                            },
                            decision_reason=f"Tick processing error #{error_count}: {e}"
                        )
                        if logger:
                            logger.warning(f"Error in market data processing (Error #{error_count}): {e}")
            else:
                # Manager not available - queue the tick for later processing
                try:
                    # Only log first few queued ticks to avoid spam
                    if self._early_ticks_logged < self._max_early_ticks_logged:
                        self._early_ticks_logged += 1
                        tick_type_name = {1: 'BID', 2: 'ASK', 4: 'LAST'}.get(tickType, f'UNKNOWN({tickType})')
                        
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            "Market data tick queued - no manager available",
                            context_provider={
                                'req_id': reqId,
                                'tick_type': tick_type_name,
                                'price': price,
                                'queued_ticks_count': self._early_tick_queue.qsize() + 1,
                                'early_ticks_logged': self._early_ticks_logged,
                                'total_ticks_processed': self._total_ticks_processed
                            },
                            decision_reason=f"Tick queued (No MarketDataManager) - Total queued: {self._early_tick_queue.qsize() + 1}"
                        )
                    
                    # Queue the tick for later processing
                    self._early_tick_queue.put((reqId, tickType, price, attrib))
                    
                except queue.Full:
                    # Queue is full - log the drop but don't spam
                    if self._early_ticks_logged < self._max_early_ticks_logged:
                        self._early_ticks_logged += 1
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            "Early tick queue full - tick dropped",
                            context_provider={
                                'req_id': reqId,
                                'tick_type': tickType,
                                'price': price,
                                'queue_size': self._early_tick_queue.qsize(),
                                'max_queue_size': self._early_tick_queue.maxsize,
                                'total_ticks_dropped': self._total_ticks_processed - self._early_tick_queue.qsize()
                            },
                            decision_reason="Early tick queue full - data loss occurred"
                        )
    # tickPrice - End    

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

    # updateAccountValue - Begin (UPDATED)
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str) -> None:
        """Callback: Received account value updates with comprehensive debugging and numeric field filtering."""
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
                'value_length': len(val) if val else 0,
                'is_numeric_field': key in VALID_NUMERIC_ACCOUNT_FIELDS
            }
        )
        # <Context-Aware Logging - Account Value Update Start - End>

        # DEBUG: Log ALL account values received to console
        print(f"🔍 DEBUG Account Value: key='{key}', value='{val}', currency='{currency}', account='{accountName}'")
        
        try:
            # Store all raw values for debugging with currency context
            if key not in self.account_values:
                self.account_values[key] = {}
            
            # Store the raw string value with currency context (for debugging)
            self.account_values[key][currency] = val

            # ONLY process known numeric financial fields - ignore metadata
            if key in VALID_NUMERIC_ACCOUNT_FIELDS:
                # Store currency-specific numeric values SAFELY
                if currency in ["CAD", "USD", "BASE"]:
                    currency_key = f"{key}_{currency}"
                    try:
                        # Convert string to float - THIS IS THE CRITICAL FIX
                        numeric_value = float(val) if val and val.strip() else 0.0
                        
                        # Store as dictionary with numeric value
                        self.account_values[currency_key] = {
                            'value': numeric_value,
                            'currency': currency,
                            'key': key,
                            'timestamp': time.time()
                        }

                        # <Context-Aware Logging - Currency Value Stored - Begin>
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            f"Stored currency-specific numeric account value",
                            context_provider={
                                'currency_key': currency_key,
                                'numeric_value': numeric_value,
                                'original_value': val,
                                'currency': currency,
                                'category': 'valid_numeric_field'
                            }
                        )
                        # <Context-Aware Logging - Currency Value Stored - End>
                        
                        print(f"✅ NUMERIC CONVERSION: {currency_key} = {numeric_value}")
                        
                    except (ValueError, TypeError) as e:
                        # <Context-Aware Logging - Value Conversion Error - Begin>
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            f"Failed to convert valid numeric account value",
                            context_provider={
                                'key': key,
                                'value': val,
                                'currency': currency,
                                'error': str(e)
                            },
                            decision_reason=f"Valid numeric field conversion failed: {e}"
                        )
                        # <Context-Aware Logging - Value Conversion Error - End>
                        print(f"❌ ERROR converting valid numeric field: key='{key}', value='{val}', error={e}")

                # Store important CAD values safely for easy retrieval
                important_keys = ["NetLiquidation", "BuyingPower", "AvailableFunds", "TotalCashValue", "CashBalance"]
                if key in important_keys and currency == "CAD":
                    try:
                        # Create dedicated key for important CAD values
                        important_cad_key = f"{key}_CAD_PRIMARY"
                        numeric_value = float(val) if val and val.strip() else 0.0
                        
                        self.account_values[important_cad_key] = {
                            'value': numeric_value,
                            'currency': currency,
                            'key': key,
                            'timestamp': time.time(),
                            'priority': 'high'
                        }
                        
                        # <Context-Aware Logging - Important Value Stored - Begin>
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            f"Stored important numeric account value",
                            context_provider={
                                'key': important_cad_key,
                                'value': numeric_value,
                                'currency': currency,
                                'category': 'primary_capital_field'
                            }
                        )
                        # <Context-Aware Logging - Important Value Stored - End>
                        
                        print(f"✅ IMPORTANT CAD VALUE: {important_cad_key} = {numeric_value}")
                        
                    except (ValueError, TypeError) as e:
                        # <Context-Aware Logging - Important Value Error - Begin>
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            f"Failed to store important numeric account value",
                            context_provider={
                                'key': key,
                                'value': val,
                                'currency': currency,
                                'error': str(e)
                            },
                            decision_reason=f"Important numeric field storage failed: {e}"
                        )
                        # <Context-Aware Logging - Important Value Error - End>
            else:
                # Log ignored metadata fields (first occurrence only to avoid spam)
                if not hasattr(self, '_ignored_metadata_warned'):
                    self._ignored_metadata_warned = set()
                
                if key not in self._ignored_metadata_warned:
                    self._ignored_metadata_warned.add(key)
                    print(f"🔍 INFO: Ignoring metadata field: '{key}' = '{val}' (currency: {currency})")
                    # <Context-Aware Logging - Metadata Ignored - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Ignoring non-numeric metadata field",
                        context_provider={
                            'key': key,
                            'value': val,
                            'currency': currency,
                            'reason': 'not_a_valid_numeric_field'
                        }
                    )
                    # <Context-Aware Logging - Metadata Ignored - End>

            self.account_value_received.set()
            
            # <Context-Aware Logging - Account Value Update Complete - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Account value update processing completed",
                context_provider={
                    'total_keys_stored': len(self.account_values),
                    'key_processed': key,
                    'currency_processed': currency,
                    'is_numeric_field': key in VALID_NUMERIC_ACCOUNT_FIELDS
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
    # updateAccountValue - End

    # get_account_value - Begin (UPDATED)
    def get_account_value(self) -> float:
        """Request and return the Net Liquidation value using the proven single strategy.
        Raises ValueError if no valid numeric account values are found - NO MOCK/DEFAULT DATA.
        """
        print("🎯 DEBUG: SINGLE-STRATEGY get_account_value() METHOD CALLED!")

        if not self.connected:
            # <Context-Aware Logging - Account Value Connection Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Cannot retrieve account value - not connected to IBKR",
                context_provider={
                    'connected': False
                },
                decision_reason="IBKR not connected, cannot retrieve account value"
            )
            # <Context-Aware Logging - Account Value Connection Error - End>
            raise ValueError("Not connected to IBKR - cannot retrieve account value")

        # <Context-Aware Logging - Single Strategy Account Value Request - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting single-strategy account value retrieval - NO MOCK DATA",
            context_provider={
                'account_number': self.account_number,
                'timeout_seconds': 15.0,
                'strategy': 'direct_account_updates_empty_string',
                'safety_feature': 'no_mock_defaults'
            }
        )
        # <Context-Aware Logging - Single Strategy Account Value Request - End>

        print("🔄 DEBUG: Using proven single strategy - reqAccountUpdates(True, '')...")
        
        # SINGLE PROVEN STRATEGY: Direct Account Updates with empty string
        capital = self._get_account_value_direct_updates()
        if capital is not None and capital > 0:
            print(f"✅ DEBUG: SINGLE STRATEGY SUCCESS - Real capital: ${capital:,.2f}")
            return capital

        # 🚨 CRITICAL SAFETY: No valid account values found - STOP TRADING
        error_msg = "CRITICAL: No valid numeric account values found using proven strategy - TRADING HALTED for safety"
        print(f"❌ {error_msg}")
        
        # Log detailed diagnostic information
        received_keys = list(self.account_values.keys()) if self.account_values else []
        numeric_values_found = []
        
        # Check what values we actually received - WITH FIXED ITERATION
        account_values_copy = dict(self.account_values.items()) if self.account_values else {}
        for key, value in account_values_copy.items():
            if isinstance(value, (int, float)) and value > 0:
                numeric_values_found.append((key, value))
            elif isinstance(value, dict):
                # Handle nested dictionary structure properly
                if 'value' in value and isinstance(value['value'], (int, float)) and value['value'] > 0:
                    numeric_values_found.append((key, value['value']))
                else:
                    # Also check currency sub-values
                    for subkey, subval in value.items():
                        if isinstance(subval, (int, float)) and subval > 0:
                            numeric_values_found.append((f"{key}.{subkey}", subval))

        # <Context-Aware Logging - Trading Halted Safety - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "TRADING HALTED - No valid account capital available using proven strategy",
            context_provider={
                'account_values_total_received': len(self.account_values) if self.account_values else 0,
                'received_account_keys': received_keys,
                'numeric_values_found': numeric_values_found,
                'valid_numeric_fields_checked': list(VALID_NUMERIC_ACCOUNT_FIELDS),
                'strategy_used': 'direct_account_updates_empty_string',
                'strategy_success': False,
                'safety_action': 'trading_halted'
            },
            decision_reason="Single strategy failed: No real account data for safe position sizing"
        )
        # <Context-Aware Logging - Trading Halted Safety - End>
        
        raise ValueError(error_msg)
    # get_account_value - End

    # _get_account_value_direct_updates - Begin (ROBUST VERSION)
    def _get_account_value_direct_updates(self) -> Optional[float]:
        """
        ROBUST SINGLE STRATEGY: Wait specifically for numeric account values to arrive.
        """
        try:
            self.account_values.clear()
            self.account_value_received.clear()
            
            print("📞 [ROBUST STRATEGY] Calling reqAccountUpdates(True, '')...")
            self.reqAccountUpdates(True, "")
            
            # Wait for ANY data first
            print("⏳ [ROBUST STRATEGY] Waiting for initial account data...")
            if not self.account_value_received.wait(10.0):
                print("❌ [ROBUST STRATEGY] Timeout waiting for initial account data")
                self.reqAccountUpdates(False, "")
                return None

            # Now wait specifically for numeric values
            print("⏳ [ROBUST STRATEGY] Waiting for numeric account values...")
            start_time = time.time()
            while time.time() - start_time < 10.0:  # Wait up to 10 more seconds
                # Check if we have any numeric values
                has_numeric = any(
                    any(field in key for field in VALID_NUMERIC_ACCOUNT_FIELDS)
                    for key in self.account_values.keys()
                )
                
                if has_numeric:
                    print("✅ [ROBUST STRATEGY] Numeric values detected!")
                    break
                    
                time.sleep(0.5)  # Check every 500ms
            
            # Cleanup
            self.reqAccountUpdates(False, "")
            
            # Final extraction attempt
            capital = self._extract_capital_from_values()
            
            if capital is not None and capital > 0:
                print(f"✅ [ROBUST STRATEGY] SUCCESS: Capital = ${capital:,.2f}")
                return capital
            else:
                print(f"❌ [ROBUST STRATEGY] FAILED: No capital found after {time.time() - start_time:.1f}s")
                print(f"❌ [ROBUST STRATEGY] Available keys: {list(self.account_values.keys())}")
                return None
                
        except Exception as e:
            print(f"❌ [ROBUST STRATEGY] Exception: {e}")
            try:
                self.reqAccountUpdates(False, "")
            except:
                pass
            return None
    # _get_account_value_direct_updates - End    # _get_account_value_direct_updates - End

    '''
    # _get_account_value_via_summary - Begin (UPDATED)
    def _get_account_value_via_summary_fixed(self) -> float:
        """Use Account Summary API to get financial data (primary strategy) - FIXED VERSION"""
        try:
            self.account_values.clear()
            self.account_value_received.clear()
            
            # Request only valid numeric financial fields
            financial_fields = list(VALID_NUMERIC_ACCOUNT_FIELDS)
            
            print(f"🔍 DEBUG [Strategy 1]: Requesting Account Summary for {len(financial_fields)} numeric financial fields")
            print(f"🔍 DEBUG [Strategy 1]: Valid Fields: {financial_fields}")
            
            # Request account summary with specific numeric financial fields only
            self.reqAccountSummary(9001, "All", ",".join(financial_fields))
            
            # Wait for data with timeout
            if not self.account_value_received.wait(8.0):
                print("❌ DEBUG [Strategy 1]: Account Summary timeout - no financial data received")
                self.cancelAccountSummary(9001)
                return None

            # Cancel the summary request
            self.cancelAccountSummary(9001)
            
            # FIX: Create copy for safe iteration
            account_values_copy = dict(self.account_values.items())
            
            # Analyze received data - only consider valid numeric fields
            valid_values_found = {}
            for key in VALID_NUMERIC_ACCOUNT_FIELDS:
                if key in account_values_copy:
                    if isinstance(account_values_copy[key], dict):
                        for currency, val in account_values_copy[key].items():
                            composite_key = f"{key}_{currency}"
                            valid_values_found[composite_key] = str(val)
                    else:
                        valid_values_found[key] = str(account_values_copy[key])
            
            print(f"📊 DEBUG [Strategy 1]: Found {len(valid_values_found)} valid numeric account values")
            
            # Log valid values for debugging
            print("🔍 DEBUG [Strategy 1]: Valid numeric values received:")
            for k, v in valid_values_found.items():
                print(f"   {k}: {v}")

            # Try different capital fields in priority order - WITH FIXED ACCESS
            capital = None
            capital_source = "unknown"
            
            # Priority 1: CAD-specific values from valid numeric fields
            cad_priority_fields = [
                "NetLiquidation_CAD", "AvailableFunds_CAD", "BuyingPower_CAD",
                "TotalCashValue_CAD", "CashBalance_CAD", "EquityWithLoanValue_CAD"
            ]
            
            for field in cad_priority_fields:
                capital = self._extract_numeric_value_safe(field)
                if capital is not None:
                    capital_source = field
                    print(f"✅ DEBUG [Strategy 1]: Using {field}: ${capital:,.2f}")
                    break
                    
            # Priority 2: BASE currency values from valid numeric fields
            if capital is None:
                base_priority_fields = [
                    "NetLiquidation_BASE", "AvailableFunds_BASE", "BuyingPower_BASE",
                    "TotalCashValue_BASE", "CashBalance_BASE", "EquityWithLoanValue_BASE"
                ]
                for field in base_priority_fields:
                    capital = self._extract_numeric_value_safe(field)
                    if capital is not None:
                        capital_source = field
                        print(f"✅ DEBUG [Strategy 1]: Using {field}: ${capital:,.2f}")
                        break
            
            # Priority 3: Generic values (any currency) from valid numeric fields
            if capital is None:
                generic_priority_fields = list(VALID_NUMERIC_ACCOUNT_FIELDS)
                for field in generic_priority_fields:
                    capital = self._extract_numeric_value_safe(field)
                    if capital is not None:
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
                        'valid_values_found_count': len(valid_values_found)
                    },
                    decision_reason=f"Successfully determined capital: ${capital:,.2f} from {capital_source}"
                )
                # <Context-Aware Logging - Account Summary Success - End>
                return capital
                
            print("❌ DEBUG [Strategy 1]: No valid numeric financial data found in Account Summary")
            return None
                
        except Exception as e:
            print(f"❌ DEBUG [Strategy 1]: Account Summary exception: {e}")
            # Cancel summary request on error
            try:
                self.cancelAccountSummary(9001)
            except:
                pass
            return None
    # _get_account_value_via_summary - End

    # _get_account_value_via_updates - Begin (UPDATED)
    def _get_account_value_via_updates(self) -> float:
        """Fallback strategy using traditional Account Updates. Returns None if no valid values found."""
        try:
            self.account_value_received.clear()
            self.account_values.clear()

            print("🔄 DEBUG [Strategy 2]: Trying traditional Account Updates with numeric field filtering...")
            self.reqAccountUpdates(True, self.account_number)

            if not self.account_value_received.wait(5.0):
                print("❌ DEBUG [Strategy 2]: Account Updates timeout")
                self.reqAccountUpdates(False, self.account_number)
                return None

            # Analyze received data - only consider valid numeric fields
            valid_values_summary = {}
            for key in VALID_NUMERIC_ACCOUNT_FIELDS:
                if key in self.account_values:
                    if isinstance(self.account_values[key], dict):
                        for currency, val in self.account_values[key].items():
                            composite_key = f"{key}_{currency}"
                            valid_values_summary[composite_key] = str(val)
                    else:
                        valid_values_summary[key] = str(self.account_values[key])
            
            print(f"📊 DEBUG [Strategy 2]: Found {len(valid_values_summary)} valid numeric account values")
            
            # Log valid values
            print("🔍 DEBUG [Strategy 2]: Valid numeric values received:")
            for k, v in valid_values_summary.items():
                print(f"   {k}: {v}")

            # Try to find capital in valid numeric data only
            capital = None
            capital_source = "unknown"
            
            # Priority order for capital fields (only valid numeric fields)
            capital_priority = [
                "NetLiquidation_CAD", "NetLiquidation", "BuyingPower_CAD", "BuyingPower",
                "AvailableFunds_CAD", "AvailableFunds", "TotalCashValue_CAD", "TotalCashValue",
                "CashBalance_CAD", "CashBalance", "EquityWithLoanValue_CAD", "EquityWithLoanValue"
            ]
            
            for field in capital_priority:
                if field in self.account_values and isinstance(self.account_values[field], (int, float)):
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
                        'valid_values_found_count': len(valid_values_summary)
                    },
                    decision_reason=f"Successfully determined capital: ${capital:,.2f} from {capital_source}"
                )
                # <Context-Aware Logging - Account Updates Success - End>
                self.reqAccountUpdates(False, self.account_number)
                return capital
                
            print("❌ DEBUG [Strategy 2]: No valid numeric capital found in Account Updates")
            self.reqAccountUpdates(False, self.account_number)
            return None
            
        except Exception as e:
            print(f"❌ DEBUG [Strategy 2]: Account Updates exception: {e}")
            try:
                self.reqAccountUpdates(False, self.account_number)
            except:
                pass
            return None
    # _get_account_value_via_updates - End
    '''

    def _extract_numeric_value_safe(self, key: str) -> Optional[float]:
        """Safe method to extract numeric value from account_values (handles nested structures)"""
        if key not in self.account_values:
            return None
            
        value = self.account_values[key]
        
        # Handle nested dictionary structure (your current format)
        if isinstance(value, dict) and 'value' in value:
            numeric_val = value['value']
            if isinstance(numeric_val, (int, float)) and numeric_val > 0:
                return numeric_val
        
        # Handle direct numeric value
        elif isinstance(value, (int, float)) and value > 0:
            return value
            
        # Handle string values that can be converted
        elif isinstance(value, str):
            try:
                numeric_val = float(value)
                return numeric_val if numeric_val > 0 else None
            except (ValueError, TypeError):
                return None
                
        return None

    # set_historical_eod_provider - Begin (NEW)
    def set_historical_eod_provider(self, eod_provider) -> None:
        """
        Thread-safe method to set the HistoricalEODProvider instance for direct scanner callbacks.
        
        Args:
            eod_provider: HistoricalEODProvider instance to receive scanner data callbacks
        """
        with self._historical_manager_lock:
            self.historical_eod_provider = eod_provider
            
            # <Context-Aware Logging - Historical EOD Provider Connected - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "HistoricalEODProvider connected to IbkrClient for scanner callbacks",
                context_provider={
                    'eod_provider_type': type(eod_provider).__name__,
                    'connected': True,
                    'purpose': 'direct_scanner_callback_routing'
                }
            )
            # <Context-Aware Logging - Historical EOD Provider Connected - End>
            
            print("🔗 IbkrClient: HistoricalEODProvider connected for direct scanner routing")
    # set_historical_eod_provider - End

    # scannerData - Begin (UPDATED)
    def scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        """Callback: Receive scanner data results and route DIRECTLY to HistoricalEODProvider"""
        super().scannerData(reqId, rank, contractDetails, distance, benchmark, projection, legsStr)
        
        # Extract the contract from contractDetails
        contract = contractDetails.contract
        symbol = contract.symbol if contract and hasattr(contract, 'symbol') else "UNKNOWN"
        
        print(f"🎯 IBKR CLIENT: Scanner data received - {symbol} (Rank: {rank})")
        
        # PRIMARY: Route directly to HistoricalEODProvider if available
        with self._historical_manager_lock:
            if hasattr(self, 'historical_eod_provider') and self.historical_eod_provider:
                try:
                    self.historical_eod_provider.scanner_data_callback(
                        reqId, rank, contract, distance, benchmark, projection, legsStr
                    )
                    print(f"✅ IBKR CLIENT: Routed scanner data directly to HistoricalEODProvider")
                    return  # Success - direct routing complete
                except Exception as e:
                    print(f"❌ IBKR CLIENT: Direct EOD provider routing failed: {e}")
                    # Fall through to backup routing
        
        # SECONDARY: Try to route through HistoricalDataManager's EOD provider reference
        if hasattr(self, 'historical_data_manager') and self.historical_data_manager:
            if hasattr(self.historical_data_manager, 'eod_provider') and self.historical_data_manager.eod_provider:
                try:
                    self.historical_data_manager.eod_provider.scanner_data_callback(
                        reqId, rank, contract, distance, benchmark, projection, legsStr
                    )
                    print(f"✅ IBKR CLIENT: Routed scanner data via HistoricalDataManager EOD reference")
                    return  # Success - indirect routing complete
                except Exception as e:
                    print(f"❌ IBKR CLIENT: Indirect EOD provider routing failed: {e}")
        
        # FALLBACK: Log that scanner data was received but no provider available
        print(f"⚠️ IBKR CLIENT: Scanner data received for {symbol} but no EOD provider available")
        
        # <Context-Aware Logging - Scanner Data No Provider - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner data received but no EOD provider available",
            symbol=symbol,
            context_provider={
                'req_id': reqId,
                'rank': rank,
                'symbol': symbol,
                'routing_attempted': True,
                'direct_provider_available': hasattr(self, 'historical_eod_provider') and bool(self.historical_eod_provider),
                'indirect_provider_available': hasattr(self, 'historical_data_manager') and 
                                            hasattr(self.historical_data_manager, 'eod_provider') and 
                                            bool(self.historical_data_manager.eod_provider) if hasattr(self, 'historical_data_manager') else False
            },
            decision_reason="Scanner data received but no provider available for processing"
        )
        # <Context-Aware Logging - Scanner Data No Provider - End>
    # scannerData - End

    # scannerDataEnd - Begin (UPDATED)
    def scannerDataEnd(self, reqId):
        """Callback: Scanner data request ended - route DIRECTLY to HistoricalEODProvider"""
        super().scannerDataEnd(reqId)
        
        print(f"🎯 IBKR CLIENT: Scanner data ended for reqId: {reqId}")
        
        # PRIMARY: Route directly to HistoricalEODProvider if available
        with self._historical_manager_lock:
            if hasattr(self, 'historical_eod_provider') and self.historical_eod_provider:
                try:
                    if hasattr(self.historical_eod_provider, 'scanner_data_end_callback'):
                        self.historical_eod_provider.scanner_data_end_callback(reqId)
                        print(f"✅ IBKR CLIENT: Routed scanner end directly to HistoricalEODProvider")
                        return  # Success - direct routing complete
                    else:
                        print(f"❌ IBKR CLIENT: HistoricalEODProvider missing scanner_data_end_callback method")
                except Exception as e:
                    print(f"❌ IBKR CLIENT: Direct EOD provider end routing failed: {e}")
                    # Fall through to backup routing
        
        # SECONDARY: Try to route through HistoricalDataManager's EOD provider reference
        if hasattr(self, 'historical_data_manager') and self.historical_data_manager:
            if hasattr(self.historical_data_manager, 'eod_provider') and self.historical_data_manager.eod_provider:
                try:
                    if hasattr(self.historical_data_manager.eod_provider, 'scanner_data_end_callback'):
                        self.historical_data_manager.eod_provider.scanner_data_end_callback(reqId)
                        print(f"✅ IBKR CLIENT: Routed scanner end via HistoricalDataManager EOD reference")
                        return  # Success - indirect routing complete
                    else:
                        print(f"❌ IBKR CLIENT: HistoricalDataManager EOD provider missing scanner_data_end_callback")
                except Exception as e:
                    print(f"❌ IBKR CLIENT: Indirect EOD provider end routing failed: {e}")
        
        # FALLBACK: Log that scanner ended but no provider available
        print(f"⚠️ IBKR CLIENT: Scanner ended but no EOD provider available for completion callback")
        
        # <Context-Aware Logging - Scanner End No Provider - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner data ended but no EOD provider available for completion",
            context_provider={
                'req_id': reqId,
                'routing_attempted': True,
                'direct_provider_available': hasattr(self, 'historical_eod_provider') and bool(self.historical_eod_provider),
                'indirect_provider_available': hasattr(self, 'historical_data_manager') and 
                                            hasattr(self.historical_data_manager, 'eod_provider') and 
                                            bool(self.historical_data_manager.eod_provider) if hasattr(self, 'historical_data_manager') else False,
                'completion_callback_missing': True
            },
            decision_reason="Scanner ended but no provider available for completion processing"
        )
        # <Context-Aware Logging - Scanner End No Provider - End>
    # scannerDataEnd - End

    # _extract_capital_from_values - Begin (UPDATED)
    def _extract_capital_from_values(self) -> Optional[float]:
        """Extract capital from account_values with enhanced nested structure handling"""
        print("🔍 DEBUG: Starting capital extraction from account values...")
        
        # Show what's actually in account_values for debugging
        print(f"📊 DEBUG: Total keys in account_values: {len(self.account_values)}")
        for key, value in list(self.account_values.items())[:10]:  # Show first 10
            print(f"   🔑 {key}: {type(value)} = {value}")
        
        capital_priority = [
            "NetLiquidation_CAD_PRIMARY", "NetLiquidation_CAD", 
            "AvailableFunds_CAD_PRIMARY", "AvailableFunds_CAD",
            "BuyingPower_CAD_PRIMARY", "BuyingPower_CAD",
            "TotalCashValue_CAD_PRIMARY", "TotalCashValue_CAD",
            "NetLiquidation", "AvailableFunds", "BuyingPower", "TotalCashValue"
        ]
        
        for field in capital_priority:
            if field in self.account_values:
                value = self.account_values[field]
                print(f"🔍 DEBUG: Checking field '{field}': {value}")
                
                # Handle nested dictionary structure
                if isinstance(value, dict) and 'value' in value:
                    capital = value['value']
                    if isinstance(capital, (int, float)) and capital > 0:
                        print(f"✅ DEBUG: Found capital in '{field}': ${capital:,.2f}")
                        return capital
                # Handle direct numeric value
                elif isinstance(value, (int, float)) and value > 0:
                    print(f"✅ DEBUG: Found direct capital in '{field}': ${value:,.2f}")
                    return value
                # Handle string values that can be converted
                elif isinstance(value, str):
                    try:
                        capital = float(value)
                        if capital > 0:
                            print(f"✅ DEBUG: Converted string capital in '{field}': ${capital:,.2f}")
                            return capital
                    except (ValueError, TypeError):
                        continue
                        
        print("❌ DEBUG: No capital found in any priority fields")
        return None
    # _extract_capital_from_values - End

    def get_simple_account_value(self, key: str) -> Optional[float]:
        """
        Safe method to extract numeric account values from complex nested structure
        Returns: Numeric value or None if not found/invalid
        """
        if key not in self.account_values:
            return None
            
        value = self.account_values[key]
        
        # Handle nested dictionary structure
        if isinstance(value, dict) and 'value' in value:
            numeric_val = value['value']
            if isinstance(numeric_val, (int, float)) and numeric_val > 0:
                return numeric_val
        
        # Handle direct numeric value  
        elif isinstance(value, (int, float)) and value > 0:
            return value
            
        return None
    
    # Debug/tests for account value retrival
    # Add to ibkr_client.py - Enhanced Debug Methods

    def debug_account_retrieval(self):
        """Comprehensive diagnostic for account value retrieval"""
        print("🔍 DEBUG ACCOUNT RETRIEVAL DIAGNOSTIC")
        print(f"  Connected: {self.connected}")
        print(f"  Account Number: {self.account_number}")
        print(f"  Account Name: {self.account_name}")
        print(f"  Is Paper: {self.is_paper_account}")
        
        # Test direct account updates (like working example)
        print("🔄 Testing direct account updates...")
        try:
            self.account_values.clear()
            self.account_value_received.clear()
            
            # Use empty string like working example
            self.reqAccountUpdates(True, "")
            print("✅ reqAccountUpdates(True, '') called successfully")
            
            # Wait for data
            if self.account_value_received.wait(10.0):
                print("✅ Account updates received successfully")
                print(f"📊 Account values received: {len(self.account_values)}")
                for key, value in self.account_values.items():
                    print(f"   {key}: {value}")
            else:
                print("❌ Timeout waiting for account updates")
                
            self.reqAccountUpdates(False, "")
            
        except Exception as e:
            print(f"❌ Error in account updates: {e}")

    def get_account_value_with_debug(self) -> float:
        """Debug version of get_account_value with comprehensive logging"""
        print("🎯 DEBUG: get_account_value_with_debug() CALLED")
        
        if not self.connected:
            print("❌ Not connected to IBKR")
            raise ValueError("Not connected to IBKR")
        
        # Test Strategy 1: Direct Account Updates (like working example)
        print("🔄 Strategy 1: Testing direct account updates...")
        try:
            self.account_values.clear()
            self.account_value_received.clear()
            
            # Use empty string account like working example
            self.reqAccountUpdates(True, "")
            
            if self.account_value_received.wait(8.0):
                print(f"✅ Account updates received: {len(self.account_values)} values")
                
                # Log all received values
                for key, value in self.account_values.items():
                    if isinstance(value, dict):
                        for currency, val in value.items():
                            print(f"   {key}_{currency}: {val} (type: {type(val)})")
                    else:
                        print(f"   {key}: {value} (type: {type(value)})")
                
                # Try to extract capital
                capital = self._extract_capital_from_values()
                if capital:
                    print(f"✅ Capital found: ${capital:,.2f}")
                    self.reqAccountUpdates(False, "")
                    return capital
                else:
                    print("❌ No capital found in account updates")
            else:
                print("❌ Timeout waiting for account updates")
                
            self.reqAccountUpdates(False, "")
            
        except Exception as e:
            print(f"❌ Error in direct account updates: {e}")
            try:
                self.reqAccountUpdates(False, "")
            except:
                pass
        
        # Fall back to original strategies
        print("🔄 Falling back to original strategies...")
        return self.get_account_value()

    # orderStatus - Begin (UPDATED - Enhanced bracket order tracking)
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice) -> None:
        """Callback: Order status updates with enhanced bracket order tracking."""
        # Find the order in history
        order_found = False
        for order in self.order_history:
            if order['order_id'] == orderId:
                order['status'] = status
                order['filled'] = float(filled)
                order['remaining'] = float(remaining)
                order['last_update'] = datetime.datetime.now()

                if status == 'Filled' and avgFillPrice > 0:
                    order['avg_fill_price'] = avgFillPrice
                    
                order_found = True
                
                # DIAGNOSTIC: Log bracket order component status changes
                if order.get('is_bracket_component', False):
                    component_type = order.get('component_type', 'UNKNOWN')
                    bracket_parent_id = order.get('bracket_parent_id')
                    print(f"🔔 BRACKET ORDER STATUS: {component_type} Order {orderId} -> {status} "
                        f"(Parent: {bracket_parent_id}, Filled: {filled}/{remaining})")

        # Update bracket order transmission tracking
        self._update_bracket_transmission_status(orderId, status)

        # <Context-Aware Logging - Order Status Update - Begin>
        if status in ['Filled', 'Cancelled', 'Submitted', 'ApiCancelled']:
            # Find bracket context for this order
            bracket_context = None
            for order in self.order_history:
                if order['order_id'] == orderId and order.get('is_bracket_component', False):
                    bracket_context = {
                        'component_type': order.get('component_type', 'UNKNOWN'),
                        'bracket_parent_id': order.get('bracket_parent_id'),
                        'parent_id': parentId
                    }
                    break

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
                    'parent_id': parentId,
                    'why_held': whyHeld,
                    'is_bracket_component': bracket_context is not None,
                    'bracket_context': bracket_context,
                    'order_found_in_history': order_found
                },
                decision_reason=f"Order {orderId} status changed to {status}" + 
                            (f" (Bracket: {bracket_context['component_type']})" if bracket_context else "")
            )
        # <Context-Aware Logging - Order Status Update - End>

        if logger:
            logger.debug(f"Order status: {orderId} - {status}, Filled: {filled}, Remaining: {remaining}")
    # orderStatus - End

    # submit_aon_bracket_order - Begin (UPDATED - Added account_number parameter)
    def submit_aon_bracket_order(self, contract, action, order_type, security_type, entry_price, stop_loss,
                            risk_per_trade, risk_reward_ratio, total_capital, account_number: Optional[str] = None) -> Optional[List[int]]:
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
            account_number: Specific account to place order against (optional)
            
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
                    'entry_price': entry_price,
                    'account_number': account_number
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
                    'order_count': len(orders),
                    'account_number': account_number or self.account_number
                },
                decision_reason="AON bracket order meets placement criteria"
            )
            # <Context-Aware Logging - AON Order Placement Start - End>

            order_ids = []
            for order in orders:
                # Use provided account_number or fall back to connected account
                target_account = account_number or self.account_number
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
                    'account_number': target_account,
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
                    'order_count': len(orders),
                    'account_number': account_number or self.account_number
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
                    'starting_order_id': self.next_valid_id,
                    'account_number': account_number or self.account_number
                },
                decision_reason=f"AON order placement exception: {e}"
            )
            # <Context-Aware Logging - AON Order Placement Error - End>
            if logger:
                logger.error(f"Failed to place AON bracket order: {e}")
            return None
    # submit_aon_bracket_order - End

    # _should_adjust_bracket_prices - Begin (NEW)
    def _should_adjust_bracket_prices(self, action: str, order_type: str, planned_entry: float, current_market_price: float) -> bool:
        """
        Determine if bracket order prices should be adjusted based on market conditions.
        
        Adjustment occurs when:
        - Order type is LIMIT
        - Market price is better than planned entry (lower for BUY, higher for SELL)
        - Price difference is significant (> 0.5% to avoid micro-adjustments)
        
        Args:
            action: BUY or SELL
            order_type: Order type (LMT, MKT, etc.)
            planned_entry: Planned entry price
            current_market_price: Current market price
            
        Returns:
            bool: True if prices should be adjusted
        """
        if order_type.upper() != "LMT":
            return False
            
        if current_market_price is None or planned_entry is None:
            return False
            
        # Calculate price difference percentage
        price_diff_pct = abs(current_market_price - planned_entry) / planned_entry
        
        # Only adjust for significant differences (> 0.5%)
        if price_diff_pct < 0.005:
            return False
            
        if action.upper() == "BUY":
            # For BUY orders, adjust if market price is lower than planned entry
            should_adjust = current_market_price < planned_entry
        else:  # SELL
            # For SELL orders, adjust if market price is higher than planned entry  
            should_adjust = current_market_price > planned_entry
            
        # <Context-Aware Logging - Price Adjustment Check - Begin>
        if should_adjust:
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Price adjustment condition met",
                context_provider={
                    'action': action,
                    'order_type': order_type,
                    'planned_entry': planned_entry,
                    'current_market_price': current_market_price,
                    'price_difference_percent': price_diff_pct * 100,
                    'adjustment_threshold': 0.5,
                    'adjustment_reason': 'favorable_market_condition'
                },
                decision_reason=f"Market price {'below' if action.upper() == 'BUY' else 'above'} planned entry by {price_diff_pct:.2%}"
            )
        # <Context-Aware Logging - Price Adjustment Check - End>
        
        return should_adjust
    # _should_adjust_bracket_prices - End

    # _calculate_adjusted_bracket_prices - Begin (NEW)
    def _calculate_adjusted_bracket_prices(self, action: str, planned_entry: float, planned_stop: float, 
                                        risk_reward_ratio: float, current_market_price: float) -> tuple[float, float, float]:
        """
        Calculate adjusted bracket order prices maintaining same absolute risk and risk/reward ratio.
        
        Args:
            action: BUY or SELL
            planned_entry: Originally planned entry price
            planned_stop: Originally planned stop loss
            risk_reward_ratio: Risk/reward ratio for profit target
            current_market_price: Current market price to use as new entry
            
        Returns:
            tuple: (adjusted_entry, adjusted_stop, adjusted_profit_target)
        """
        try:
            # Calculate original risk amount (absolute dollar amount)
            original_risk_amount = abs(planned_entry - planned_stop)
            
            if action.upper() == "BUY":
                # For BUY orders, new entry is current market price (lower than planned)
                adjusted_entry = current_market_price
                # Stop loss maintains same absolute risk amount below new entry
                adjusted_stop = adjusted_entry - original_risk_amount
                # Profit target maintains same risk/reward ratio
                adjusted_profit_target = self._calculate_profit_target(action, adjusted_entry, adjusted_stop, risk_reward_ratio)
            else:  # SELL
                # For SELL orders, new entry is current market price (higher than planned)
                adjusted_entry = current_market_price
                # Stop loss maintains same absolute risk amount above new entry
                adjusted_stop = adjusted_entry + original_risk_amount
                # Profit target maintains same risk/reward ratio
                adjusted_profit_target = self._calculate_profit_target(action, adjusted_entry, adjusted_stop, risk_reward_ratio)
            
            # Validate adjusted prices are reasonable
            if adjusted_stop <= 0:
                raise ValueError(f"Adjusted stop loss is invalid: {adjusted_stop}")
            if adjusted_profit_target <= 0:
                raise ValueError(f"Adjusted profit target is invalid: {adjusted_profit_target}")
                
            # Ensure stop loss is meaningfully different from entry
            if abs(adjusted_entry - adjusted_stop) / adjusted_entry < 0.001:
                raise ValueError(f"Adjusted stop loss too close to entry: {adjusted_stop} vs {adjusted_entry}")
                
            # <Context-Aware Logging - Price Adjustment Calculation - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Bracket order prices adjusted for favorable market conditions",
                context_provider={
                    'action': action,
                    'original_entry': planned_entry,
                    'original_stop': planned_stop,
                    'original_risk_amount': original_risk_amount,
                    'adjusted_entry': adjusted_entry,
                    'adjusted_stop': adjusted_stop,
                    'adjusted_profit_target': adjusted_profit_target,
                    'risk_reward_ratio': risk_reward_ratio,
                    'improvement_percent': abs(adjusted_entry - planned_entry) / planned_entry * 100,
                    'risk_amount_maintained': True,
                    'risk_reward_maintained': True
                },
                decision_reason=f"Prices adjusted: Entry ${planned_entry:.2f} → ${adjusted_entry:.2f}, Stop ${planned_stop:.2f} → ${adjusted_stop:.2f}"
            )
            # <Context-Aware Logging - Price Adjustment Calculation - End>
            
            return adjusted_entry, adjusted_stop, adjusted_profit_target
            
        except Exception as e:
            # <Context-Aware Logging - Price Adjustment Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Price adjustment calculation failed",
                context_provider={
                    'action': action,
                    'planned_entry': planned_entry,
                    'planned_stop': planned_stop,
                    'current_market_price': current_market_price,
                    'error': str(e)
                },
                decision_reason=f"Price adjustment calculation error: {e}"
            )
            # <Context-Aware Logging - Price Adjustment Error - End>
            raise
    # _calculate_adjusted_bracket_prices - End

    # _get_current_market_price - Begin (NEW)
    def _get_current_market_price(self, symbol: str) -> Optional[float]:
        """
        Get current market price for a symbol from market data manager.
        
        Args:
            symbol: Symbol to get price for
            
        Returns:
            float or None: Current market price if available
        """
        try:
            if self.market_data_manager and hasattr(self.market_data_manager, 'get_current_price'):
                price_data = self.market_data_manager.get_current_price(symbol)
                if price_data and 'price' in price_data and price_data['price'] > 0:
                    return float(price_data['price'])
                    
            # Fallback: Check if we have recent tick data
            if hasattr(self, '_last_tick_price') and self._last_tick_price:
                return self._last_tick_price
                
            return None
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Failed to get current market price",
                symbol=symbol,
                context_provider={'error': str(e)}
            )
            return None
    # _get_current_market_price - End

    # _verify_bracket_order_transmission - Begin (NEW)
    def _verify_bracket_order_transmission(self, parent_order_id: int, order_ids: List[int], symbol: str) -> bool:
        """
        Verify that all three bracket order components were successfully transmitted to IBKR.
        
        Args:
            parent_order_id: The parent order ID for this bracket
            order_ids: List of all three order IDs [parent, take_profit, stop_loss]
            symbol: Symbol for logging
            
        Returns:
            bool: True if all three orders transmitted successfully, False otherwise
        """
        try:
            # Initialize tracking for this bracket order
            with self._bracket_order_lock:
                self._active_bracket_orders[parent_order_id] = {
                    'order_ids': order_ids.copy(),
                    'symbol': symbol,
                    'components_transmitted': {
                        order_ids[0]: False,  # parent/entry
                        order_ids[1]: False,  # take_profit
                        order_ids[2]: False   # stop_loss
                    },
                    'transmission_time': datetime.datetime.now(),
                    'verified': False
                }
                self._bracket_transmission_events[parent_order_id] = threading.Event()

            # Wait for all three orders to appear in open orders or timeout
            start_time = time.time()
            while time.time() - start_time < BRACKET_ORDER_TIMEOUT:
                # Check if all components have been transmitted
                with self._bracket_order_lock:
                    bracket_info = self._active_bracket_orders.get(parent_order_id)
                    if not bracket_info:
                        break
                    
                    transmitted_count = sum(bracket_info['components_transmitted'].values())
                    if transmitted_count == 3:
                        bracket_info['verified'] = True
                        self._bracket_transmission_events[parent_order_id].set()
                        
                        # <Context-Aware Logging - Bracket Transmission Success - Begin>
                        self.context_logger.log_event(
                            TradingEventType.ORDER_VALIDATION,
                            "All bracket order components transmitted successfully",
                            symbol=symbol,
                            context_provider={
                                'parent_order_id': parent_order_id,
                                'order_ids': order_ids,
                                'transmission_time_seconds': time.time() - start_time,
                                'verified': True
                            },
                            decision_reason="All three bracket order components confirmed transmitted to IBKR"
                        )
                        # <Context-Aware Logging - Bracket Transmission Success - End>
                        
                        print(f"✅ BRACKET VERIFICATION: All 3 orders transmitted for {symbol} - IDs: {order_ids}")
                        return True

                time.sleep(BRACKET_TRANSMISSION_CHECK_INTERVAL)

            # Timeout or incomplete transmission
            with self._bracket_order_lock:
                bracket_info = self._active_bracket_orders.get(parent_order_id)
                if bracket_info:
                    transmitted_components = [oid for oid, transmitted in bracket_info['components_transmitted'].items() if transmitted]
                    missing_components = [oid for oid, transmitted in bracket_info['components_transmitted'].items() if not transmitted]
                    
                    # <Context-Aware Logging - Bracket Transmission Failure - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Bracket order transmission verification failed",
                        symbol=symbol,
                        context_provider={
                            'parent_order_id': parent_order_id,
                            'total_orders_expected': 3,
                            'orders_transmitted': len(transmitted_components),
                            'orders_missing': len(missing_components),
                            'transmitted_order_ids': transmitted_components,
                            'missing_order_ids': missing_components,
                            'timeout_seconds': BRACKET_ORDER_TIMEOUT,
                            'time_elapsed': time.time() - start_time
                        },
                        decision_reason=f"Only {len(transmitted_components)} of 3 bracket orders transmitted within timeout"
                    )
                    # <Context-Aware Logging - Bracket Transmission Failure - End>
                    
                    print(f"❌ BRACKET VERIFICATION: Only {len(transmitted_components)} of 3 orders transmitted for {symbol}")
                    print(f"   Transmitted: {transmitted_components}")
                    print(f"   Missing: {missing_components}")
                    
                    # Attempt to handle partial bracket execution
                    self._handle_partial_bracket_execution(parent_order_id, transmitted_components, missing_components, symbol)

            return False

        except Exception as e:
            # <Context-Aware Logging - Bracket Verification Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket order transmission verification error",
                symbol=symbol,
                context_provider={
                    'parent_order_id': parent_order_id,
                    'error': str(e),
                    'error_type': type(e).__name__
                },
                decision_reason=f"Bracket verification exception: {e}"
            )
            # <Context-Aware Logging - Bracket Verification Error - End>
            print(f"❌ BRACKET VERIFICATION ERROR for {symbol}: {e}")
            return False
    # _verify_bracket_order_transmission - End

        # _handle_partial_bracket_execution - Begin (NEW)
    def _handle_partial_bracket_execution(self, parent_order_id: int, transmitted_orders: List[int], 
                                        missing_orders: List[int], symbol: str) -> None:
        """
        Handle cases where only partial bracket orders were transmitted.
        Attempts to cancel the partial bracket to maintain system consistency.
        
        Args:
            parent_order_id: The parent order ID
            transmitted_orders: List of order IDs that were transmitted
            missing_orders: List of order IDs that failed to transmit
            symbol: Symbol for logging
        """
        try:
            # Check if profit target is among the missing orders
            profit_target_missing = any(oid == parent_order_id + 1 for oid in missing_orders)
            
            # <Context-Aware Logging - Partial Bracket Handling - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Handling partial bracket order execution",
                symbol=symbol,
                context_provider={
                    'parent_order_id': parent_order_id,
                    'transmitted_orders_count': len(transmitted_orders),
                    'missing_orders_count': len(missing_orders),
                    'transmitted_order_ids': transmitted_orders,
                    'missing_order_ids': missing_orders,
                    'profit_target_missing': profit_target_missing,
                    'critical_issue': profit_target_missing  # Profit target missing is critical
                },
                decision_reason=f"Partial bracket detected: {len(transmitted_orders)} transmitted, {len(missing_orders)} missing" +
                              (" - CRITICAL: Profit target missing" if profit_target_missing else "")
            )
            # <Context-Aware Logging - Partial Bracket Handling - End>

            print(f"🔄 HANDLING PARTIAL BRACKET for {symbol}:")
            print(f"   Transmitted: {transmitted_orders}")
            print(f"   Missing: {missing_orders}")
            print(f"   Profit Target Missing: {profit_target_missing}")

            # Cancel all transmitted orders to clean up partial bracket
            for order_id in transmitted_orders:
                try:
                    self.cancelOrder(order_id)
                    print(f"   📤 Sent cancel for order {order_id}")
                except Exception as cancel_error:
                    print(f"   ❌ Failed to cancel order {order_id}: {cancel_error}")

            # Clean up tracking
            with self._bracket_order_lock:
                if parent_order_id in self._active_bracket_orders:
                    del self._active_bracket_orders[parent_order_id]
                if parent_order_id in self._bracket_transmission_events:
                    del self._bracket_transmission_events[parent_order_id]

        except Exception as e:
            # <Context-Aware Logging - Partial Bracket Handling Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Partial bracket order handling failed",
                symbol=symbol,
                context_provider={
                    'parent_order_id': parent_order_id,
                    'error': str(e),
                    'transmitted_orders': transmitted_orders,
                    'missing_orders': missing_orders
                },
                decision_reason=f"Partial bracket handling exception: {e}"
            )
            # <Context-Aware Logging - Partial Bracket Handling Error - End>
            print(f"❌ PARTIAL BRACKET HANDLING ERROR for {symbol}: {e}")
    # _handle_partial_bracket_execution - End

    # _update_bracket_transmission_status - Begin (NEW)
    def _update_bracket_transmission_status(self, order_id: int, status: str) -> None:
        """
        Update bracket order transmission tracking when order status changes.
        
        Args:
            order_id: The order ID that changed status
            status: New order status
        """
        try:
            # Only track submission status for transmission verification
            if status not in ['Submitted', 'Filled', 'Cancelled']:
                return

            with self._bracket_order_lock:
                # Find which bracket this order belongs to
                for parent_id, bracket_info in self._active_bracket_orders.items():
                    if order_id in bracket_info['order_ids']:
                        # Mark this order as transmitted
                        if status == 'Submitted':
                            bracket_info['components_transmitted'][order_id] = True
                            print(f"📨 BRACKET TRANSMISSION: Order {order_id} submitted for bracket {parent_id}")
                        
                        # Check if this is the profit target order
                        if order_id == parent_id + 1 and status == 'Submitted':  # Take profit order
                            print(f"✅ PROFIT TARGET TRANSMITTED: Order {order_id} for bracket {parent_id}")
                        
                        break

        except Exception as e:
            print(f"❌ BRACKET TRANSMISSION TRACKING ERROR for order {order_id}: {e}")
    # _update_bracket_transmission_status - End

    # _validate_and_round_price - Begin (NEW - Price rounding for IBKR compliance)
    def _validate_and_round_price(self, price: float, security_type: str, symbol: str = 'UNKNOWN', 
                                is_profit_target: bool = False) -> float:
        """
        Validate and round prices to conform to IBKR minimum price variation rules.
        For profit targets, round UP to the next valid price increment for better R/R.
        
        Args:
            price: Original price to validate
            security_type: Security type (STK, OPT, etc.)
            symbol: Symbol for logging
            is_profit_target: Whether this is a profit target (round UP if True)
            
        Returns:
            float: Rounded price that conforms to IBKR rules
        """
        try:
            if security_type.upper() == "STK":
                # Determine the appropriate price increment based on price tier
                if price < 1.0:
                    increment = 0.0001  # Penny stocks: $0.0001 increments
                elif price < 10.0:
                    increment = 0.005   # Low-price stocks: $0.005 increments  
                else:
                    increment = 0.01    # Regular stocks: $0.01 increments
                
                # For profit targets, round UP to the next valid increment for better R/R
                if is_profit_target:
                    # Round UP to the next valid increment
                    rounded_price = math.ceil(price / increment) * increment
                    rounding_direction = "UP"
                    improvement = rounded_price - price
                else:
                    # For entry and stop prices, use normal rounding
                    rounded_price = round(price / increment) * increment
                    rounding_direction = "NEAREST"
                    improvement = 0
                
                # Log the rounding operation if significant
                if abs(rounded_price - price) > 0.0001:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Price rounded {rounding_direction} for IBKR compliance",
                        symbol=symbol,
                        context_provider={
                            'original_price': price,
                            'rounded_price': rounded_price,
                            'security_type': security_type,
                            'price_increment': increment,
                            'rounding_direction': rounding_direction,
                            'is_profit_target': is_profit_target,
                            'improvement': improvement,
                            'price_tier': 'PENNY' if price < 1.0 else 'LOW' if price < 10.0 else 'REGULAR'
                        },
                        decision_reason=f"Price rounded {rounding_direction} from {price:.4f} to {rounded_price:.4f} for IBKR compliance"
                    )
                    print(f"🔧 PRICE ROUNDING {rounding_direction}: {symbol} - {price:.4f} → {rounded_price:.4f} (increment: {increment})")
                    
                return rounded_price
            else:
                # For other security types, use original rounding logic
                return round(price, 5)
                
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Price rounding error",
                symbol=symbol,
                context_provider={
                    'original_price': price,
                    'security_type': security_type,
                    'is_profit_target': is_profit_target,
                    'error': str(e)
                }
            )
            # Fallback to safe rounding
            return round(price, 2)
    # _validate_and_round_price - End    
    
    # _create_bracket_order - Begin (UPDATED - Add price rounding while preserving all existing logic)
    def _create_bracket_order(self, action, order_type, security_type, entry_price, stop_loss,
                            risk_per_trade, risk_reward_ratio, total_capital, starting_order_id) -> List[Any]:
        """Create the IBKR Order objects for a bracket order. Returns [parent, take_profit, stop_loss].
        
        Enhanced with comprehensive profit target validation and transmission chain verification.
        """
        from ibapi.order import Order

        parent_id = starting_order_id
        
        # Validate critical inputs before calculation
        if entry_price is None or entry_price <= 0:
            raise ValueError(f"Invalid entry_price: {entry_price}")
        if stop_loss is None or stop_loss <= 0:
            raise ValueError(f"Invalid stop_loss: {stop_loss}")
        if risk_reward_ratio is None or risk_reward_ratio <= 0:
            raise ValueError(f"Invalid risk_reward_ratio: {risk_reward_ratio}")

        quantity = self._calculate_quantity(security_type, entry_price, stop_loss,
                                        total_capital, risk_per_trade)
        
        # Enhanced profit target calculation with validation
        profit_target = self._calculate_profit_target(action, entry_price, stop_loss, risk_reward_ratio)
        
        # ✅ NEW: Apply price rounding for IBKR compliance (profit targets rounded UP)
        symbol = getattr(self, '_current_symbol', 'UNKNOWN')
        profit_target = self._validate_and_round_price(
            profit_target, security_type, 
            symbol=symbol,
            is_profit_target=True  # Round UP for profit targets
        )
        
        # ✅ NEW: Also round entry and stop prices (to nearest increment)
        entry_price = self._validate_and_round_price(
            entry_price, security_type,
            symbol=symbol,
            is_profit_target=False  # Round to nearest for entry
        )
        
        stop_loss = self._validate_and_round_price(
            stop_loss, security_type,
            symbol=symbol, 
            is_profit_target=False  # Round to nearest for stop loss
        )
        
        # Validate profit target is reasonable
        if profit_target is None or profit_target <= 0:
            raise ValueError(f"Invalid profit_target calculated: {profit_target}")
        
        # Ensure profit target is meaningfully different from entry price
        price_tolerance = 0.001  # 0.1% tolerance
        if abs(profit_target - entry_price) / entry_price < price_tolerance:
            raise ValueError(f"Profit target ${profit_target:.2f} too close to entry price ${entry_price:.2f}")

        # DIAGNOSTIC: Log bracket calculation details
        print(f"🔧 BRACKET CALCULATION: Qty={quantity}, Entry=${entry_price:.2f}, "
            f"Stop=${stop_loss:.2f}, Target=${profit_target:.2f}, R/R={risk_reward_ratio:.1f}")

        # 1. PARENT ORDER (Entry)
        parent = Order()
        parent.orderId = parent_id
        parent.action = action
        parent.orderType = order_type
        parent.totalQuantity = quantity
        parent.lmtPrice = round(entry_price, 5)
        parent.transmit = False  # Will be transmitted by last child

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
        take_profit.transmit = False  # Will be transmitted by last child
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
        stop_loss_order.transmit = True  # This transmits the entire bracket
        stop_loss_order.openClose = "C"
        stop_loss_order.origin = 0

        # VALIDATION: Verify transmission chain is correct
        transmission_chain = [parent.transmit, take_profit.transmit, stop_loss_order.transmit]
        expected_chain = [False, False, True]
        
        if transmission_chain != expected_chain:
            error_msg = f"Invalid transmission chain: {transmission_chain}, expected: {expected_chain}"
            # <Context-Aware Logging - Transmission Chain Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket order transmission chain error",
                context_provider={
                    'actual_chain': transmission_chain,
                    'expected_chain': expected_chain,
                    'parent_id': parent_id,
                    'profit_target': profit_target,
                    'entry_price': entry_price
                },
                decision_reason=error_msg
            )
            # <Context-Aware Logging - Transmission Chain Error - End>
            raise ValueError(error_msg)

        # Validate all three orders are properly configured
        orders = [parent, take_profit, stop_loss_order]
        for i, order in enumerate(orders):
            if order.orderId is None:
                raise ValueError(f"Order {i} has no orderId")
            if order.totalQuantity <= 0:
                raise ValueError(f"Order {i} has invalid quantity: {order.totalQuantity}")

        return orders
    # _create_bracket_order - End

    # place_bracket_order - Begin (UPDATED - Add symbol tracking for price validation)
    def place_bracket_order(self, contract, action, order_type, security_type, entry_price, stop_loss,
                        risk_per_trade, risk_reward_ratio, total_capital, account_number: Optional[str] = None) -> Optional[List[int]]:
        """Place a complete bracket order (entry, take-profit, stop-loss). Returns list of order IDs or None.
        
        ENHANCED: Now includes comprehensive bracket order transmission verification.
        """
        # Store current symbol for price validation context
        symbol = getattr(contract, 'symbol', 'UNKNOWN')
        self._current_symbol = symbol
        
        if not self.connected or self.next_valid_id is None:
            # <Context-Aware Logging - Bracket Order Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Bracket order failed - not connected or no valid order ID",
                symbol=symbol,
                context_provider={
                    'connected': self.connected,
                    'next_valid_id': self.next_valid_id,
                    'action': action,
                    'order_type': order_type,
                    'account_number': account_number
                },
                decision_reason="IBKR connection not ready for bracket order"
            )
            # <Context-Aware Logging - Bracket Order Failed - End>
            if logger:
                logger.error("Not connected to IBKR or no valid order ID")
            return None

        try:
            # Get current market price for potential adjustment
            current_market_price = self._get_current_market_price(symbol)
            
            # Check if we should adjust bracket order prices
            adjusted_entry = entry_price
            adjusted_stop = stop_loss
            adjusted_profit_target = None
            prices_adjusted = False
            
            if (current_market_price and 
                self._should_adjust_bracket_prices(action, order_type, entry_price, current_market_price)):
                
                try:
                    adjusted_entry, adjusted_stop, adjusted_profit_target = self._calculate_adjusted_bracket_prices(
                        action, entry_price, stop_loss, risk_reward_ratio, current_market_price
                    )
                    prices_adjusted = True
                    
                    # <Context-Aware Logging - Price Adjustment Applied - Begin>
                    self.context_logger.log_event(
                        TradingEventType.EXECUTION_DECISION,
                        "Dynamic price adjustment applied to bracket order",
                        symbol=symbol,
                        context_provider={
                            'original_entry': entry_price,
                            'adjusted_entry': adjusted_entry,
                            'original_stop': stop_loss,
                            'adjusted_stop': adjusted_stop,
                            'adjusted_profit_target': adjusted_profit_target,
                            'current_market_price': current_market_price,
                            'improvement_amount': entry_price - adjusted_entry if action == 'BUY' else adjusted_entry - entry_price,
                            'risk_amount_maintained': True
                        },
                        decision_reason="Bracket order prices dynamically adjusted for favorable market conditions"
                    )
                    # <Context-Aware Logging - Price Adjustment Applied - End>
                    
                except Exception as adjustment_error:
                    # If adjustment fails, proceed with original prices but log the error
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Price adjustment failed, using original prices",
                        symbol=symbol,
                        context_provider={
                            'error': str(adjustment_error),
                            'original_entry': entry_price,
                            'current_market_price': current_market_price
                        },
                        decision_reason="Price adjustment failed, using original bracket order prices"
                    )
                    # Continue with original prices
                    adjusted_entry = entry_price
                    adjusted_stop = stop_loss
                    prices_adjusted = False

            # Pre-validate profit target before creating orders
            # Use adjusted profit target if available, otherwise calculate from adjusted prices
            if adjusted_profit_target is None:
                profit_target = self._calculate_profit_target(action, adjusted_entry, adjusted_stop, risk_reward_ratio)
            else:
                profit_target = adjusted_profit_target
            
            # <Context-Aware Logging - Profit Target Pre-Validation - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Profit target pre-validation passed" + (" with adjusted prices" if prices_adjusted else ""),
                symbol=symbol,
                context_provider={
                    'entry_price': adjusted_entry,
                    'stop_loss': adjusted_stop,
                    'profit_target': profit_target,
                    'risk_reward_ratio': risk_reward_ratio,
                    'action': action,
                    'prices_adjusted': prices_adjusted,
                    'original_entry_used': not prices_adjusted
                },
                decision_reason="Profit target calculated and validated successfully" + (" with price adjustment" if prices_adjusted else "")
            )
            # <Context-Aware Logging - Profit Target Pre-Validation - End>

            orders = self._create_bracket_order(
                action, order_type, security_type, adjusted_entry, adjusted_stop,
                risk_per_trade, risk_reward_ratio, total_capital, self.next_valid_id
            )

            if logger:
                logger.info(f"Placing bracket order for {symbol}" + (" with adjusted prices" if prices_adjusted else ""))

            # <Context-Aware Logging - Bracket Order Diagnostics - Begin>
            order_details = []
            for i, order in enumerate(orders):
                order_details.append({
                    'order_id': order.orderId,
                    'type': order.orderType,
                    'action': order.action,
                    'price': getattr(order, 'lmtPrice', getattr(order, 'auxPrice', None)),
                    'transmit': getattr(order, 'transmit', None),
                    'parent_id': getattr(order, 'parentId', None)
                })

            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "BRACKET ORDER DIAGNOSTICS - All orders created" + (" with price adjustment" if prices_adjusted else ""),
                symbol=symbol,
                context_provider={
                    'action': action,
                    'order_type': order_type,
                    'entry_price': adjusted_entry,
                    'stop_loss': adjusted_stop,
                    'profit_target': profit_target,
                    'risk_per_trade': risk_per_trade,
                    'risk_reward_ratio': risk_reward_ratio,
                    'total_capital': total_capital,
                    'starting_order_id': self.next_valid_id,
                    'order_count': len(orders),
                    'order_details': order_details,
                    'account_number': account_number or self.account_number,
                    'all_orders_created': True,
                    'profit_target_valid': profit_target > 0,
                    'prices_adjusted': prices_adjusted,
                    'original_entry': entry_price if prices_adjusted else None,
                    'original_stop': stop_loss if prices_adjusted else None,
                    'current_market_price': current_market_price
                },
                decision_reason="All three bracket orders created and validated" + (" with price adjustment" if prices_adjusted else "")
            )
            # <Context-Aware Logging - Bracket Order Diagnostics - End>

            # DIAGNOSTIC: Log each order details before placement
            adjustment_note = " WITH PRICE ADJUSTMENT" if prices_adjusted else ""
            print(f"🎯 BRACKET ORDER PLACEMENT for {symbol}{adjustment_note}:")
            for i, order in enumerate(orders):
                component_type = "ENTRY" if i == 0 else "TAKE_PROFIT" if i == 1 else "STOP_LOSS"
                print(f"   {component_type}: ID={order.orderId}, Type={order.orderType}, Action={order.action}, "
                    f"Price={getattr(order, 'lmtPrice', getattr(order, 'auxPrice', 'N/A'))}, "
                    f"Transmit={getattr(order, 'transmit', 'N/A')}, Parent={getattr(order, 'parentId', 'None')}")

            order_ids = []
            for i, order in enumerate(orders):
                component_type = "ENTRY" if i == 0 else "TAKE_PROFIT" if i == 1 else "STOP_LOSS"
                
                # DIAGNOSTIC: Log individual order placement
                print(f"📤 Placing {component_type} order: ID={order.orderId}, Account={account_number or self.account_number}")
                
                # Use provided account_number or fall back to connected account
                target_account = account_number or self.account_number
                self.placeOrder(order.orderId, contract, order)
                order_ids.append(order.orderId)
                
                # Enhanced order tracking with bracket context and adjustment info
                self.order_history.append({
                    'order_id': order.orderId,
                    'type': order.orderType,
                    'action': order.action,
                    'price': getattr(order, 'lmtPrice', getattr(order, 'auxPrice', None)),
                    'quantity': order.totalQuantity,
                    'status': 'PendingSubmit',
                    'parent_id': getattr(order, 'parentId', None),
                    'transmit': getattr(order, 'transmit', None),
                    'is_bracket_component': True,
                    'bracket_parent_id': order.orderId if getattr(order, 'parentId', None) is None else getattr(order, 'parentId', None),
                    'component_type': component_type,
                    'account_number': target_account,
                    'timestamp': datetime.datetime.now(),
                    'prices_adjusted': prices_adjusted,
                    'original_entry_price': entry_price if prices_adjusted else None,
                    'adjusted_entry_price': adjusted_entry if prices_adjusted else None
                })

            self.next_valid_id += 3
            
            # DIAGNOSTIC: Log successful bracket placement
            adjustment_success_note = " WITH PRICE ADJUSTMENT" if prices_adjusted else ""
            print(f"✅ BRACKET ORDER PLACED SUCCESSFULLY{adjustment_success_note}: {symbol} - Order IDs: {order_ids}, Account: {account_number or self.account_number}")
            
            # VERIFY TRANSMISSION OF ALL THREE ORDERS
            parent_order_id = order_ids[0]  # First order is the parent
            transmission_verified = self._verify_bracket_order_transmission(parent_order_id, order_ids, symbol)
            
            if not transmission_verified:
                # <Context-Aware Logging - Bracket Transmission Failed - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Bracket order transmission verification failed - partial execution likely",
                    symbol=symbol,
                    context_provider={
                        'order_ids': order_ids,
                        'parent_order_id': parent_order_id,
                        'transmission_verified': False,
                        'likely_issue': 'missing_profit_target_or_stop_loss',
                        'action_required': 'bracket_cancelled_automatically'
                    },
                    decision_reason="Bracket order failed transmission verification - automatic cleanup initiated"
                )
                # <Context-Aware Logging - Bracket Transmission Failed - End>
                
                print(f"❌ BRACKET ORDER FAILED: Transmission verification failed for {symbol}")
                return None  # Return None to indicate failure

            if logger:
                logger.info(f"Bracket order placed successfully: {symbol}" + (" with adjusted prices" if prices_adjusted else ""))
                
            # <Context-Aware Logging - Bracket Order Placement Success - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Bracket order placed successfully - ALL THREE COMPONENTS" + (" with price adjustment" if prices_adjusted else ""),
                symbol=symbol,
                context_provider={
                    'order_ids': order_ids,
                    'final_order_id': self.next_valid_id,
                    'order_count': len(orders),
                    'bracket_components_placed': len([o for o in orders if getattr(o, 'parentId', None) is None]) + len([o for o in orders if getattr(o, 'parentId', None) is not None]),
                    'transmission_chain_complete': any(getattr(o, 'transmit', False) for o in orders),
                    'account_number': account_number or self.account_number,
                    'all_three_orders_placed': len(order_ids) == 3,
                    'transmission_verified': transmission_verified,
                    'prices_adjusted': prices_adjusted,
                    'improvement_amount': (entry_price - adjusted_entry) if prices_adjusted and action == 'BUY' else (adjusted_entry - entry_price) if prices_adjusted else 0,
                    'risk_amount_maintained': prices_adjusted
                },
                decision_reason="All three bracket order components submitted to IBKR API" + (" with price adjustment" if prices_adjusted else "") + " and transmission verified"
            )
            # <Context-Aware Logging - Bracket Order Placement Success - End>
            
            return order_ids

        except Exception as e:
            # <Context-Aware Logging - Bracket Order Placement Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket order placement failed" + (" during price adjustment" if 'adjust' in str(e).lower() else ""),
                symbol=symbol,
                context_provider={
                    'error': str(e),
                    'action': action,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'risk_reward_ratio': risk_reward_ratio,
                    'starting_order_id': self.next_valid_id,
                    'error_type': type(e).__name__,
                    'account_number': account_number or self.account_number,
                    'likely_cause': 'price_adjustment' if 'adjust' in str(e).lower() else 'order_validation'
                },
                decision_reason=f"Bracket order placement exception: {e}"
            )
            # <Context-Aware Logging - Bracket Order Placement Error - End>
            if logger:
                logger.error(f"Failed to place bracket order: {e}")
            return None
    # place_bracket_order - End