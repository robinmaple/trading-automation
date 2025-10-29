# src/brokers/ibkr/core/order_manager.py

"""
Manages all order-related operations for IBKR including placement, 
cancellation, tracking, and bracket order management.
"""

import math
import threading
import time
import datetime
from typing import List, Optional, Any, Dict
from ibapi.contract import Contract
from ibapi.order import Order

from src.core.context_aware_logger import get_context_logger, TradingEventType
from src.brokers.ibkr.types.ibkr_types import IbkrOrder, IbkrPosition


class OrderManager:
    """Manages all order operations including bracket orders and order tracking."""
    
    def __init__(self, connection_manager):
        """Initialize order manager with connection reference."""
        self.context_logger = get_context_logger()
        self.connection_manager = connection_manager
        
        # Order tracking
        self.order_history = []
        self.open_orders: List[IbkrOrder] = []
        self.positions: List[IbkrPosition] = []
        
        # Bracket order tracking
        self._active_bracket_orders: Dict[int, Dict] = {}
        self._bracket_order_lock = threading.RLock()
        self._bracket_transmission_events: Dict[int, threading.Event] = {}
        
        # Order request tracking
        self.orders_received_event = threading.Event()
        self.positions_received_event = threading.Event()
        self.open_orders_end_received = False
        self.positions_end_received = False
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "OrderManager initialized",
            context_provider={
                "connection_manager_available": connection_manager is not None
            }
        )
    
    def place_bracket_order(self, contract: Contract, action: str, order_type: str, 
                          security_type: str, entry_price: float, stop_loss: float,
                          risk_per_trade: float, risk_reward_ratio: float, 
                          total_capital: float, account_number: Optional[str] = None) -> Optional[List[int]]:
        """Place a complete bracket order (entry, take-profit, stop-loss)."""
        if not self.connection_manager.connected or self.connection_manager.next_valid_id is None:
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Bracket order failed - not connected or no valid order ID",
                symbol=getattr(contract, 'symbol', 'UNKNOWN'),
                context_provider={
                    'connected': self.connection_manager.connected,
                    'next_valid_id': self.connection_manager.next_valid_id
                }
            )
            return None
        
        try:
            orders = self._create_bracket_order(
                action, order_type, security_type, entry_price, stop_loss,
                risk_per_trade, risk_reward_ratio, total_capital, 
                self.connection_manager.next_valid_id
            )
            
            order_ids = []
            for order in orders:
                target_account = account_number or self.connection_manager.account_number
                self.connection_manager.placeOrder(order.orderId, contract, order)
                order_ids.append(order.orderId)
                
                # Track order in history
                self.order_history.append({
                    'order_id': order.orderId,
                    'type': order.orderType,
                    'action': order.action,
                    'price': getattr(order, 'lmtPrice', getattr(order, 'auxPrice', None)),
                    'quantity': order.totalQuantity,
                    'status': 'PendingSubmit',
                    'parent_id': getattr(order, 'parentId', None),
                    'account_number': target_account,
                    'timestamp': datetime.datetime.now()
                })
            
            # Update next valid ID
            self.connection_manager.next_valid_id += 3
            
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Bracket order placed successfully",
                symbol=contract.symbol,
                context_provider={
                    'order_ids': order_ids,
                    'action': action,
                    'order_type': order_type,
                    'entry_price': entry_price
                }
            )
            
            return order_ids
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket order placement failed",
                symbol=getattr(contract, 'symbol', 'UNKNOWN'),
                context_provider={'error': str(e)}
            )
            return None
    
    def _create_bracket_order(self, action, order_type, security_type, entry_price, stop_loss,
                            risk_per_trade, risk_reward_ratio, total_capital, starting_order_id) -> List[Any]:
        """Create IBKR Order objects for a bracket order."""
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
        elif security_type == "FUT":
            quantity = round(base_quantity)
            quantity = max(quantity, 1)
        else:
            quantity = round(base_quantity)
            quantity = max(quantity, 1)

        return quantity
    
    def _calculate_profit_target(self, action, entry_price, stop_loss, risk_reward_ratio) -> float:
        """Calculate profit target price based on risk/reward ratio."""
        if entry_price is None or entry_price <= 0:
            raise ValueError(f"Invalid entry_price for profit target: {entry_price}")
        if stop_loss is None or stop_loss <= 0:
            raise ValueError(f"Invalid stop_loss for profit target: {stop_loss}")
        if risk_reward_ratio is None or risk_reward_ratio <= 0:
            raise ValueError(f"Invalid risk_reward_ratio for profit target: {risk_reward_ratio}")

        risk_amount = abs(entry_price - stop_loss)
        if risk_amount == 0:
            raise ValueError("Entry price and stop loss cannot be the same")

        if action == "BUY":
            profit_target = entry_price + (risk_amount * risk_reward_ratio)
        else:
            profit_target = entry_price - (risk_amount * risk_reward_ratio)

        if profit_target <= 0:
            raise ValueError(f"Calculated profit target is invalid: {profit_target}")

        return profit_target
    
    def cancel_order(self, order_id: int) -> bool:
        """Cancel an order through the IBKR API."""
        if not self.connection_manager.connected:
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order cancellation failed - not connected to IBKR",
                context_provider={'order_id': order_id}
            )
            return False

        try:
            self.connection_manager.cancelOrder(order_id)
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order cancellation requested",
                context_provider={'order_id': order_id}
            )
            return True
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Order cancellation failed with exception",
                context_provider={
                    'order_id': order_id,
                    'error': str(e)
                }
            )
            return False
    
    def get_open_orders(self) -> List[IbkrOrder]:
        """Fetch all open orders from IBKR API synchronously."""
        if not self.connection_manager.connected:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Open orders request failed - not connected to IBKR"
            )
            return []

        try:
            self.open_orders.clear()
            self.open_orders_end_received = False
            self.orders_received_event.clear()

            self.connection_manager.reqAllOpenOrders()

            if self.orders_received_event.wait(10.0):
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Open orders retrieved successfully",
                    context_provider={'open_orders_count': len(self.open_orders)}
                )
                return self.open_orders.copy()
            else:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Timeout waiting for open orders data"
                )
                return []

        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Open orders request failed with exception",
                context_provider={'error': str(e)}
            )
            return []
    
    # Order callbacks that will be called from main client
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
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error processing open order data",
                context_provider={
                    'order_id': orderId,
                    'error': str(e)
                }
            )

    def openOrderEnd(self) -> None:
        """Callback: Finished receiving open orders."""
        self.open_orders_end_received = True
        self.orders_received_event.set()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Open orders request completed",
            context_provider={'open_orders_count': len(self.open_orders)}
        )