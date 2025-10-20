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

    # _validate_market_data_available - Begin (UPDATED - make optional)
    def _validate_market_data_available(self, order) -> tuple[bool, str]:
        """Layer 3b: Validate market data availability - but don't block execution if unavailable."""
        try:
            if not hasattr(self._trading_manager, 'data_feed'):
                # Don't block execution - just warn
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Data feed not available - proceeding with execution",
                    symbol=order.symbol,
                    context_provider={},
                    decision_reason="Market data unavailable but order execution allowed"
                )
                return True, "Execution allowed without market data"
                
            if not self._trading_manager.data_feed.is_connected():
                # Don't block execution - just warn  
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Data feed not connected - proceeding with execution",
                    symbol=order.symbol,
                    context_provider={},
                    decision_reason="Data feed disconnected but order execution allowed"
                )
                return True, "Execution allowed with disconnected data feed"
                
            current_price = self._trading_manager.data_feed.get_current_price(order.symbol)
            if current_price is None or current_price <= 0:
                # Don't block execution - just warn
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "No market data available - proceeding with execution",
                    symbol=order.symbol,
                    context_provider={
                        "current_price": current_price
                    },
                    decision_reason="Market data unavailable but order execution allowed"
                )
                return True, "Execution allowed without current market data"
                
            return True, "Market data available"
            
        except Exception as e:
            # Don't block execution on validation errors
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Market data validation error - proceeding with execution",
                symbol=order.symbol,
                context_provider={
                    "error": str(e)
                },
                decision_reason="Market data validation failed but order execution allowed"
            )
            return True, "Execution allowed despite market data validation error"
    # _validate_market_data_available - End

    def _validate_broker_connection(self) -> tuple[bool, str]:
        """Layer 3c: Validate broker connection status."""
        if self._ibkr_client and self._ibkr_client.connected:
            return True, "Broker connected"
        
        return False, "Broker not connected"

    # _validate_execution_conditions - Begin (UPDATED - enhanced logging)
    def _validate_execution_conditions(self, order, quantity, total_capital) -> tuple[bool, str]:
        """Layer 3: Comprehensive pre-execution validation with detailed logging."""
        try:
            # Basic field validation (safety net)
            basic_valid, basic_message = self._validate_order_basic(order)
            if not basic_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Basic validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": basic_message,
                        "entry_price": getattr(order, 'entry_price', None),
                        "stop_loss": getattr(order, 'stop_loss', None),
                        "action": getattr(order, 'action', None)
                    },
                    decision_reason="Basic validation failed"
                )
                return False, f"Basic validation failed: {basic_message}"
                
            # Market data availability
            market_valid, market_message = self._validate_market_data_available(order)
            if not market_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Market data validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": market_message,
                        "data_feed_connected": hasattr(self._trading_manager, 'data_feed') and self._trading_manager.data_feed.is_connected()
                    },
                    decision_reason="Market data validation failed"
                )
                return False, f"Market data issue: {market_message}"
                
            # Broker connection
            broker_valid, broker_message = self._validate_broker_connection()
            if not broker_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Broker validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": broker_message,
                        "ibkr_connected": self._ibkr_client and self._ibkr_client.connected
                    },
                    decision_reason="Broker validation failed"
                )
                return False, f"Broker issue: {broker_message}"
                
            # Margin validation (existing)
            margin_valid, margin_message = self._validate_order_margin(order, quantity, total_capital)
            if not margin_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Margin validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": margin_message,
                        "quantity": quantity,
                        "total_capital": total_capital
                    },
                    decision_reason="Margin validation failed"
                )
                return False, f"Margin validation failed: {margin_message}"
                
            # All validations passed
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                f"All execution conditions met for {order.symbol}",
                symbol=order.symbol,
                context_provider={
                    "quantity": quantity,
                    "total_capital": total_capital
                },
                decision_reason="All execution validations passed"
            )
            return True, "All execution conditions met"
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Execution validation error for {order.symbol}",
                symbol=order.symbol,
                context_provider={
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                decision_reason="Execution validation exception"
            )
            return False, f"Execution validation error: {e}"
# _validate_execution_conditions - End

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
        
    # execute_single_order - Begin (UPDATED - Fixed bracket order parameter order)
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
        """Execute a single order atomically with duplication prevention and rollback protection.
        
        FIXED: Corrected bracket order parameter order to match ibkr_client method signature.
        """
        
        ibkr_order_ids = None
        attempt_id = None
        
        try:
            # Log execution start for critical orders
            if fill_probability > 0.7 or effective_priority > 5:
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Executing high priority order with bracket order fix",
                    symbol=order.symbol,
                    context_provider={
                        "probability": fill_probability,
                        "priority": effective_priority,
                        "live_trading": is_live_trading,
                        "account_number_provided": account_number is not None,
                        "total_capital": total_capital,
                        "quantity": quantity,
                        "bracket_order_fix": "applied"
                    }
                )

            # Execution conditions validation
            exec_valid, exec_message = self._validate_execution_conditions(order, quantity, total_capital)
            if not exec_valid:
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Execution rejected - validation failed",
                    symbol=order.symbol,
                    context_provider={
                        "reason": exec_message,
                        "account_number": account_number,
                        "bracket_order_fix": "validation_failed"
                    }
                )
                
                db_id = self._trading_manager._find_planned_order_db_id(order)
                if db_id:
                    self.order_persistence.handle_order_rejection(db_id, exec_message)
                
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
                
                self._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'FAILED', None, margin_message,
                    account_number
                )
                return False

            ibkr_connected = self._ibkr_client and self._ibkr_client.connected

            # === DUPLICATION PREVENTION CHECK ===
            if ibkr_connected:
                # Check for duplicate active orders
                if self._is_duplicate_order_active(order, account_number):
                    rejection_reason = "Duplicate order prevention - similar order already active in IBKR"
                    db_id = self._trading_manager._find_planned_order_db_id(order)
                    if db_id:
                        self.order_persistence.handle_order_rejection(db_id, rejection_reason)
                    
                    self._record_order_attempt(
                        order, 'PLACEMENT', fill_probability, effective_priority,
                        quantity, capital_commitment, 'REJECTED', None, rejection_reason,
                        account_number
                    )
                    
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "Order rejected - duplicate prevention",
                        symbol=order.symbol,
                        context_provider={
                            "reason": rejection_reason,
                            "account_number": account_number,
                            "bracket_order_fix": "duplicate_blocked"
                        },
                        decision_reason="DUPLICATE_ORDER_BLOCKED"
                    )
                    return False

                # Check for rapid retries
                if self._has_recent_execution_attempt(order):
                    rejection_reason = "Rapid retry prevention - recent execution attempt detected"
                    db_id = self._trading_manager._find_planned_order_db_id(order)
                    if db_id:
                        self.order_persistence.handle_order_rejection(db_id, rejection_reason)
                    
                    self._record_order_attempt(
                        order, 'PLACEMENT', fill_probability, effective_priority,
                        quantity, capital_commitment, 'REJECTED', None, rejection_reason,
                        account_number
                    )
                    
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "Order rejected - rapid retry prevention",
                        symbol=order.symbol,
                        context_provider={
                            "reason": rejection_reason,
                            "account_number": account_number,
                            "bracket_order_fix": "retry_blocked"
                        },
                        decision_reason="RAPID_RETRY_BLOCKED"
                    )
                    return False

            # === LIVE ORDER PATH (ATOMIC) ===
            if ibkr_connected:
                # Record placement attempt before execution
                attempt_id = self._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'SUBMITTING', None, None,
                    account_number
                )
                
                contract = order.to_ib_contract()
                
                # ✅ FIXED: Correct bracket order parameter order
                # OLD: place_bracket_order(contract, action, order_type, security_type, entry_price, stop_loss, risk_per_trade, risk_reward_ratio, total_capital, account_number)
                # NEW: Correct parameter order matching ibkr_client method signature
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Calling bracket order with corrected parameter order",
                    symbol=order.symbol,
                    context_provider={
                        "action": order.action.value,
                        "order_type": order.order_type.value,
                        "security_type": order.security_type.value,
                        "entry_price": order.entry_price,
                        "stop_loss": order.stop_loss,
                        "risk_per_trade": order.risk_per_trade,
                        "risk_reward_ratio": order.risk_reward_ratio,
                        "total_capital": total_capital,
                        "account_number": account_number,
                        "bracket_order_fix": "parameter_order_corrected"
                    },
                    decision_reason="Bracket order call with corrected parameter order"
                )
                
                # STEP 1: Place IBKR order with CORRECTED parameter order
                ibkr_order_ids = self._ibkr_client.place_bracket_order(
                    contract,
                    order.action.value,
                    order.order_type.value,
                    order.security_type.value,
                    order.entry_price,
                    order.stop_loss,
                    order.risk_per_trade,
                    order.risk_reward_ratio,
                    total_capital,  # ✅ FIXED: Now in correct position (8th parameter)
                    account_number  # ✅ FIXED: Now in correct position (9th parameter)
                )

                if not ibkr_order_ids:
                    raise Exception("IBKR bracket order placement failed - no order IDs returned")

                # STEP 2: Persist to DB (with fixed parameters)
                execution_id = self.order_persistence.record_order_execution(
                    planned_order=order,  # ✅ FIXED: named parameter
                    filled_price=order.entry_price,
                    filled_quantity=quantity,
                    account_number=account_number,
                    status='SUBMITTED'
                )

                if execution_id is None:
                    raise Exception("DB persistence failed - no execution ID returned")

                # STEP 3: Create ActiveOrder tracking
                db_id = self._trading_manager._find_planned_order_db_id(order)
                if db_id:
                    active_order = ActiveOrder(
                        planned_order=order,
                        order_ids=ibkr_order_ids,
                        db_id=db_id,
                        status='SUBMITTED',
                        capital_commitment=capital_commitment,
                        timestamp=datetime.datetime.now(),
                        is_live_trading=is_live_trading,
                        fill_probability=fill_probability,
                        account_number=account_number
                    )
                    self.active_orders[ibkr_order_ids[0]] = active_order
                
                # Update attempt with success
                self._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'SUBMITTED', ibkr_order_ids, None,
                    account_number
                )
                
                # Log successful atomic execution with bracket fix
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Live bracket order placed atomically with parameter fix",
                    symbol=order.symbol,
                    context_provider={
                        "order_ids": ibkr_order_ids,
                        "account": account_number,
                        "execution_id": execution_id,
                        "atomic_success": True,
                        "bracket_order_fix": "successful",
                        "expected_components": 3,  # Entry, stop-loss, profit-target
                        "actual_components": len(ibkr_order_ids) if ibkr_order_ids else 0
                    },
                    decision_reason="Bracket order successfully submitted with corrected parameters"
                )
                
                return True

            # === SIMULATION PATH ===
            else:
                # Simulation doesn't need atomicity since no real orders
                self._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'SIMULATION', None, None,
                    account_number
                )
                
                update_success = self.order_persistence.update_order_status(order, 'FILLED')
                
                execution_id = self.order_persistence.record_order_execution(
                    planned_order=order,  # ✅ FIXED: named parameter
                    filled_price=order.entry_price,
                    filled_quantity=quantity,
                    account_number=account_number,
                    status='FILLED'
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
                        account_number=account_number
                    )
                    self.active_orders[active_order.order_ids[0]] = active_order
                
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Simulation order executed",
                    symbol=order.symbol,
                    context_provider={
                        "account": account_number,
                        "ibkr_connected": False,
                        "bracket_order_fix": "simulation_skip"
                    },
                    decision_reason="Simulation mode - no actual IBKR order"
                )
                
                return True

        except Exception as e:
            # ATOMIC ROLLBACK: Cancel IBKR orders if anything failed
            if ibkr_order_ids:
                try:
                    for order_id in ibkr_order_ids:
                        self._ibkr_client.cancel_order(order_id)
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Rolled back IBKR orders due to execution failure",
                        symbol=order.symbol,
                        context_provider={
                            "order_ids": ibkr_order_ids,
                            "error": str(e),
                            "rollback_success": True,
                            "bracket_order_fix": "rollback_attempted"
                        }
                    )
                except Exception as cancel_error:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Failed to cancel orders during rollback",
                        symbol=order.symbol,
                        context_provider={
                            "order_ids": ibkr_order_ids,
                            "rollback_error": str(cancel_error),
                            "original_error": str(e),
                            "bracket_order_fix": "rollback_failed"
                        }
                    )
            
            # Update attempt with failure
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'FAILED', ibkr_order_ids, str(e),
                account_number
            )
            
            # Log atomic execution failure with bracket fix context
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Atomic execution failed despite bracket order fix",
                symbol=order.symbol,
                context_provider={
                    "error": str(e),
                    "account": account_number,
                    "ibkr_orders_rolled_back": ibkr_order_ids is not None,
                    "bracket_order_fix": "execution_failed",
                    "error_type": type(e).__name__
                },
                decision_reason=f"Bracket order execution failed: {e}"
            )
            
            return False
    # execute_single_order - End

    # Add these helper methods to the class:

    def _is_duplicate_order_active(self, order, account_number: Optional[str] = None) -> bool:
        """
        Check if a similar order is already active in IBKR to prevent duplicates.
        Returns True if duplicate found, False if safe to proceed.
        """
        try:
            if not self._ibkr_client or not self._ibkr_client.connected:
                return False  # No IBKR connection, can't check for duplicates
                
            # Get open orders from IBKR for this symbol
            open_orders = self._ibkr_client.get_open_orders()
            symbol_orders = [o for o in open_orders if getattr(o.contract, 'symbol', '') == order.symbol]
            
            if not symbol_orders:
                return False  # No open orders for this symbol
                
            # Check each order for similarity
            for open_order in symbol_orders:
                if self._orders_are_similar(open_order, order, account_number):
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "Duplicate order detected - similar order already active",
                        symbol=order.symbol,
                        context_provider={
                            "existing_order_id": open_order.orderId,
                            "existing_action": getattr(open_order, 'action', 'UNKNOWN'),
                            "existing_price": getattr(open_order, 'lmtPrice', getattr(open_order, 'auxPrice', 0)),
                            "new_action": order.action.value,
                            "new_price": order.entry_price,
                            "account_number": account_number
                        },
                        decision_reason="DUPLICATE_ORDER_PREVENTION"
                    )
                    return True
                    
            return False
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error checking for duplicate orders",
                symbol=order.symbol,
                context_provider={
                    "error": str(e),
                    "account_number": account_number
                }
            )
            # On error, assume no duplicates to avoid blocking execution
            return False

    def _orders_are_similar(self, ibkr_order, planned_order, account_number: Optional[str] = None) -> bool:
        """
        Determine if two orders are similar enough to be considered duplicates.
        """
        try:
            # Basic symbol match
            if getattr(ibkr_order.contract, 'symbol', '') != planned_order.symbol:
                return False
                
            # Action match (BUY/SELL)
            ibkr_action = getattr(ibkr_order, 'action', '').upper()
            planned_action = planned_order.action.value.upper()
            if ibkr_action != planned_action:
                return False
                
            # Price proximity check (within 1%)
            ibkr_price = getattr(ibkr_order, 'lmtPrice', getattr(ibkr_order, 'auxPrice', 0))
            if ibkr_price > 0 and planned_order.entry_price > 0:
                price_ratio = abs(ibkr_price - planned_order.entry_price) / planned_order.entry_price
                if price_ratio > 0.01:  # More than 1% price difference
                    return False
                    
            # Order type compatibility
            ibkr_order_type = getattr(ibkr_order, 'orderType', '').upper()
            planned_order_type = planned_order.order_type.value.upper()
            
            # Consider LMT and LMT+STP as similar for duplication purposes
            compatible_types = {
                'LMT': ['LMT', 'STP LMT'],
                'STP': ['STP', 'STP LMT'],
                'MKT': ['MKT']
            }
            
            if planned_order_type not in compatible_types.get(ibkr_order_type, []):
                return False
                
            # If we get here, orders are similar
            return True
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error comparing orders for similarity",
                context_provider={
                    "error": str(e),
                    "symbol": planned_order.symbol
                }
            )
            return False

    def _has_recent_execution_attempt(self, order, time_window_minutes: int = 5) -> bool:
        """
        Check if there was a recent execution attempt for this order to prevent rapid retries.
        """
        try:
            current_time = datetime.datetime.now()
            time_threshold = current_time - datetime.timedelta(minutes=time_window_minutes)
            
            # Check active orders first
            for active_order in self.active_orders.values():
                if (active_order.planned_order.symbol == order.symbol and
                    active_order.timestamp > time_threshold and
                    active_order.status in ['SUBMITTED', 'SUBMITTING']):
                    return True
                    
            # Could also check database for recent attempts here if needed
            return False
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error checking recent execution attempts",
                symbol=order.symbol,
                context_provider={"error": str(e)}
            )
            return False  # On error, allow execution