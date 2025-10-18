"""
Service responsible for all order execution interactions with the brokerage client.
Handles placement, cancellation, monitoring of orders, validation, live/simulation mode,
and persistence of execution results, including Phase B: fill probability and unified execution tracking.
"""

import datetime
from typing import Any, Dict, Optional, List
from ibapi.contract import Contract
from ibapi.order import Order

from src.core.planned_order import ActiveOrder
from src.core.models import OrderAttemptDB
from src.core.context_aware_logger import get_context_logger, TradingEventType


class OrderExecutionService:
    """Encapsulates all logic for executing orders and interacting with the broker."""

    def __init__(self, trading_manager, ibkr_client):
        """Initialize the service with references to the trading manager and IBKR client."""
        self.context_logger = get_context_logger()
        
        self._trading_manager = trading_manager
        self._ibkr_client = ibkr_client
        self.order_persistence = None
        self.active_orders = None
        
        # Minimal initialization logging
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Order execution service ready",
            context_provider={
                "ibkr_connected": ibkr_client.connected if ibkr_client else False
            }
        )

    def set_dependencies(self, order_persistence, active_orders) -> None:
        """Inject dependencies for order execution and tracking."""
        self.order_persistence = order_persistence
        self.active_orders = active_orders

    def _validate_order_basic(self, order) -> tuple[bool, str]:
        """Layer 3a: Basic field validation as safety net before execution."""
        try:
            # Symbol validation
            symbol_str = ""
            try:
                symbol_str = str(order.symbol).strip()
            except Exception:
                symbol_str = ""
            if not symbol_str or symbol_str in ['', '0', 'nan', 'None', 'null']:
                return False, f"Invalid symbol: '{order.symbol}'"

            # Price validation
            if not hasattr(order, "entry_price") or order.entry_price is None or order.entry_price <= 0:
                return False, f"Invalid entry price: {getattr(order, 'entry_price', None)}"

            # Stop loss validation (basic syntax)
            if getattr(order, "stop_loss", None) is not None and order.stop_loss <= 0:
                return False, f"Invalid stop loss price: {order.stop_loss}"

            # Action validation - accept enums or strings
            action_val = None
            try:
                action_val = getattr(order.action, "value", None) or getattr(order.action, "name", None)
            except Exception:
                action_val = None

            if action_val is None:
                try:
                    action_val = str(order.action)
                except Exception:
                    action_val = ""

            action_str = str(action_val).upper().strip()
            if action_str not in ("BUY", "SELL"):
                return False, f"Invalid action: {order.action}"

            return True, "Basic validation passed"

        except Exception as e:
            return False, f"Basic validation error: {e}"

    def _validate_market_data_available(self, order) -> tuple[bool, str]:
        """Layer 3b: Validate market data availability for the symbol."""
        try:
            if not hasattr(self._trading_manager, 'data_feed'):
                return False, "Data feed not available"
                
            if not self._trading_manager.data_feed.is_connected():
                return False, "Data feed not connected"
                
            current_price = self._trading_manager.data_feed.get_current_price(order.symbol)
            if current_price is None or current_price <= 0:
                return False, f"No market data available for {order.symbol}"
                
            return True, "Market data available"
            
        except Exception as e:
            return False, f"Market data validation error: {e}"

    def _validate_broker_connection(self) -> tuple[bool, str]:
        """Layer 3c: Validate broker connection status."""
        if self._ibkr_client and self._ibkr_client.connected:
            return True, "Broker connected"
        
        return False, "Broker not connected"

    def _validate_execution_conditions(self, order, quantity, total_capital) -> tuple[bool, str]:
        """Layer 3: Comprehensive pre-execution validation."""
        try:
            # Basic field validation (safety net)
            basic_valid, basic_message = self._validate_order_basic(order)
            if not basic_valid:
                return False, f"Basic validation failed: {basic_message}"
                
            # Market data availability
            market_valid, market_message = self._validate_market_data_available(order)
            if not market_valid:
                return False, f"Market data issue: {market_message}"
                
            # Broker connection
            broker_valid, broker_message = self._validate_broker_connection()
            if not broker_valid:
                return False, f"Broker issue: {broker_message}"
                
            # Margin validation (existing)
            margin_valid, margin_message = self._validate_order_margin(order, quantity, total_capital)
            if not margin_valid:
                return False, f"Margin validation failed: {margin_message}"
                
            return True, "All execution conditions met"
            
        except Exception as e:
            return False, f"Execution validation error: {e}"

# OrderExecutionService.execute_single_order - Begin (UPDATED - fixed IBKR client call)
    def execute_single_order(
        self,
        order,
        fill_probability=0.0,
        effective_priority=0.0,
        total_capital=None,
        quantity=None,
        capital_commitment=None,
        is_live_trading=False,
        account_number: Optional[str] = None
    ) -> bool:
        """Execute a single order while incorporating fill probability into ActiveOrder tracking."""
        # Log execution start for critical orders
        if fill_probability > 0.7 or effective_priority > 5:
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Executing high priority order",
                symbol=order.symbol,
                context_provider={
                    "probability": fill_probability,
                    "priority": effective_priority,
                    "live_trading": is_live_trading,
                    "account_number_provided": account_number is not None
                }
            )

        # Execution conditions validation
        exec_valid, exec_message = self._validate_execution_conditions(order, quantity, total_capital)
        if not exec_valid:
            # Log validation failures
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Execution rejected - validation failed",
                symbol=order.symbol,
                context_provider={
                    "reason": exec_message,
                    "account_number": account_number
                }
            )
            
            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                self.order_persistence.handle_order_rejection(db_id, exec_message)
            
            # Record failed attempt due to validation
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'FAILED', None, exec_message,
                account_number
            )
            return False

        margin_valid, margin_message = self._validate_order_margin(order, quantity, total_capital)
        if not margin_valid:
            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                self.order_persistence.handle_order_rejection(db_id, margin_message)
            
            # Record failed attempt due to margin validation
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'FAILED', None, margin_message,
                account_number
            )
            return False

        ibkr_connected = self._ibkr_client and self._ibkr_client.connected

        # === LIVE ORDER PATH ===
        if ibkr_connected:
            # Record placement attempt before execution
            attempt_id = self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'SUBMITTING', None, None,
                account_number
            )
            
            contract = order.to_ib_contract()
            
            # FIXED: Add account_number parameter to IBKR client call
            order_ids = self._ibkr_client.place_bracket_order(
                contract,
                order.action.value,
                order.order_type.value,
                order.security_type.value,
                order.entry_price,
                order.stop_loss,
                order.risk_per_trade,
                order.risk_reward_ratio,
                total_capital,
                account_number  # CRITICAL: Added account_number parameter
            )

            if not order_ids:
                rejection_reason = "IBKR order placement failed - no order IDs returned"
                db_id = self._trading_manager._find_planned_order_db_id(order)
                if db_id:
                    self.order_persistence.handle_order_rejection(db_id, rejection_reason)
                
                # Update attempt with failure
                self._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'FAILED', None, rejection_reason,
                    account_number
                )
                return False

            # Pass account number to persistence service
            execution_id = self.order_persistence.record_order_execution(
                order,
                order.entry_price,
                quantity,
                account_number,
                status='SUBMITTED',
                is_live_trading=is_live_trading
            )

            # Create ActiveOrder with unified tracking
            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                active_order = ActiveOrder(
                    planned_order=order,
                    order_ids=order_ids,
                    db_id=db_id,
                    status='SUBMITTED',
                    capital_commitment=capital_commitment,
                    timestamp=datetime.datetime.now(),
                    is_live_trading=is_live_trading,
                    fill_probability=fill_probability,
                    account_number=account_number  # Store account number in ActiveOrder
                )
                self.active_orders[order_ids[0]] = active_order
            
            # Update attempt with success and order IDs
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'SUBMITTED', order_ids, None,
                account_number
            )
            
            # Log successful live execution
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Live order placed",
                symbol=order.symbol,
                context_provider={
                    "order_ids": order_ids,
                    "account": account_number,
                    "ibkr_connected": True
                },
                decision_reason="Order successfully submitted to IBKR"
            )
            
            return True

        # === SIMULATION PATH ===
        else:
            # Record simulation attempt
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'SIMULATION', None, None,
                account_number
            )
            
            update_success = self.order_persistence.update_order_status(order, 'FILLED')
            
            # Pass account number to persistence service
            execution_id = self.order_persistence.record_order_execution(
                order,
                order.entry_price,
                quantity,
                account_number,
                status='FILLED',
                is_live_trading=is_live_trading
            )

            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                active_order = ActiveOrder(
                    planned_order=order,
                    order_ids=[f"SIM-{db_id}"],
                    db_id=db_id,
                    status='FILLED',
                    capital_commitment=capital_commitment,
                    timestamp=datetime.datetime.now(),
                    is_live_trading=is_live_trading,
                    fill_probability=fill_probability,
                    account_number=account_number  # Store account number in ActiveOrder
                )
                self.active_orders[active_order.order_ids[0]] = active_order
            
            # Log simulation execution
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Simulation order executed",
                symbol=order.symbol,
                context_provider={
                    "account": account_number,
                    "ibkr_connected": False
                },
                decision_reason="Simulation mode - no actual IBKR order"
            )
            
            return True
# OrderExecutionService.execute_single_order - End

# OrderExecutionService._record_order_attempt - Begin (UPDATED - enhanced account tracking)
    def _record_order_attempt(self, planned_order, attempt_type, fill_probability=None,
                            effective_priority=None, quantity=None, capital_commitment=None,
                            status=None, ib_order_ids=None, details=None,
                            account_number: Optional[str] = None):
        """Record an order attempt to the database for Phase B tracking."""
        if not self.order_persistence or not hasattr(self.order_persistence, 'db_session'):
            return None
            
        try:
            db_id = self._trading_manager._find_planned_order_db_id(planned_order)
            
            attempt = OrderAttemptDB(
                planned_order_id=db_id,
                attempt_ts=datetime.datetime.now(),
                attempt_type=attempt_type,
                fill_probability=fill_probability,
                effective_priority=effective_priority,
                quantity=quantity,
                capital_commitment=capital_commitment,
                status=status,
                ib_order_ids=ib_order_ids,
                details=details,
                account_number=account_number  # Ensure account_number is stored
            )
            
            self.order_persistence.db_session.add(attempt)
            self.order_persistence.db_session.commit()
            
            # Log attempt recording for debugging
            if attempt_type == 'PLACEMENT' and status == 'SUBMITTED':
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Order attempt recorded",
                    symbol=planned_order.symbol,
                    context_provider={
                        "attempt_id": attempt.id,
                        "account_number": account_number,
                        "order_ids": ib_order_ids
                    }
                )
            
            return attempt.id
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Order attempt recording failed",
                symbol=planned_order.symbol,
                context_provider={
                    "error": str(e),
                    "account_number": account_number
                }
            )
            return None
# OrderExecutionService._record_order_attempt - End

    def place_order(
        self,
        planned_order,
        fill_probability=0.0,
        effective_priority=0.0,
        total_capital=None,
        quantity=None,
        capital_commitment=None,
        is_live_trading=False,
        account_number: Optional[str] = None
    ) -> bool:
        """Place an order for a PlannedOrder, tracking fill probability (Phase B)."""
        return self.execute_single_order(
            planned_order,
            fill_probability,
            effective_priority,
            total_capital,
            quantity,
            capital_commitment,
            is_live_trading,
            account_number
        )

    def _validate_order_margin(self, order, quantity, total_capital) -> tuple[bool, str]:
        """Validate if the order has sufficient margin before execution."""
        try:
            is_valid, message = self.order_persistence.validate_sufficient_margin(
                order.symbol, quantity, order.entry_price
            )
            if not is_valid:
                return False, message
            
            return True, "Margin validation passed"
        except Exception as e:
            return False, f"Margin validation error: {e}"

    def cancel_order(self, order_id) -> bool:
        """Cancel a working order by delegating to the trading manager's logic."""
        # Find the active order to get details for tracking
        active_order = None
        for ao in self.active_orders.values():
            if order_id in ao.order_ids:
                active_order = ao
                break
        
        # Record cancellation attempt
        if active_order:
            self._record_order_attempt(
                active_order.planned_order, 'CANCELLATION',
                active_order.fill_probability, None, None, None,
                'ATTEMPTING', [order_id], None,
                active_order.account_number if hasattr(active_order, 'account_number') else None
            )
        
        success = self._trading_manager._cancel_single_order(order_id)
        
        # Update attempt with result
        if active_order:
            status = 'SUCCESS' if success else 'FAILED'
            details = f"Cancellation {'succeeded' if success else 'failed'}"
            self._record_order_attempt(
                active_order.planned_order, 'CANCELLATION',
                active_order.fill_probability, None, None, None,
                status, [order_id], details,
                active_order.account_number if hasattr(active_order, 'account_number') else None
            )
        
        return success

    def close_position(self, position_data: Dict, account_number: Optional[str] = None) -> Optional[int]:
        """Close an open position by placing a market order through IBKR."""
        if not self._ibkr_client or not self._ibkr_client.connected:
            return None
            
        try:
            contract = Contract()
            contract.symbol = position_data['symbol']
            contract.secType = position_data['security_type']
            contract.exchange = position_data.get('exchange', 'SMART')
            contract.currency = position_data.get('currency', 'USD')

            order = Order()
            order.action = position_data['action']
            order.orderType = "MKT"
            order.totalQuantity = position_data['quantity']
            order.tif = "DAY"

            order_id = self._ibkr_client.next_valid_id
            self._ibkr_client.placeOrder(order_id, contract, order)
            self._ibkr_client.next_valid_id += 1

            # Log position close
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                "Position closed",
                symbol=position_data['symbol'],
                context_provider={
                    "order_id": order_id,
                    "account": account_number
                }
            )
            
            return order_id
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Position close failed",
                symbol=position_data['symbol'],
                context_provider={
                    "error": str(e)
                }
            )
            return None

    def cancel_orders_for_symbol(self, symbol: str) -> bool:
        """Cancel all active open orders for a specific symbol."""
        if not self._ibkr_client or not self._ibkr_client.connected:
            return True
            
        try:
            orders = self._ibkr_client.get_open_orders()
            symbol_orders = [
                o for o in orders
                if o.symbol == symbol and o.status in ['Submitted', 'PreSubmitted', 'PendingSubmit']
            ]
            if not symbol_orders:
                return True

            success = True
            for order in symbol_orders:
                if not self._ibkr_client.cancel_order(order.order_id):
                    success = False
            
            # Log cancellation result
            if symbol_orders:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Symbol orders cancelled",
                    symbol=symbol,
                    context_provider={
                        "count": len(symbol_orders),
                        "success": success
                    }
                )
            
            return success
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Symbol cancellation failed",
                symbol=symbol,
                context_provider={
                    "error": str(e)
                }
            )
            return False

    def find_orders_by_symbol(self, symbol: str) -> List[Any]:
        """Find all open orders for a specific symbol from IBKR."""
        if not self._ibkr_client or not self._ibkr_client.connected:
            return []
            
        try:
            orders = self._ibkr_client.get_open_orders()
            found_orders = [o for o in orders if o.symbol == symbol]
            return found_orders
        except Exception as e:
            return []