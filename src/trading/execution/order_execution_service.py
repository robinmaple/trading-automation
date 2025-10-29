"""
Service responsible for all order execution interactions with the brokerage client.
Handles placement, cancellation, monitoring of orders, validation, live/simulation mode,
and persistence of execution results, including Phase B: fill probability and unified execution tracking.
"""

import math
import datetime
import time
from decimal import Decimal
from typing import Any, Dict, Optional, List
from ibapi.contract import Contract
from ibapi.order import Order

from src.trading.orders.planned_order import ActiveOrder
from src.core.models import OrderAttemptDB
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Bracket Order Verification Constants - Begin (NEW)
BRACKET_VERIFICATION_TIMEOUT = 15.0  # seconds to wait for bracket verification
BRACKET_RETRY_DELAY = 2.0  # seconds between retry attempts
MAX_BRACKET_RETRIES = 2  # maximum number of retry attempts
# Bracket Order Verification Constants - End

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

    # _validate_bracket_order_result - Begin (NEW)
    def _validate_bracket_order_result(self, ibkr_order_ids: Optional[List[int]], symbol: str, 
                                     account_number: Optional[str] = None) -> tuple[bool, str, Optional[List[int]]]:
        """
        Validate that bracket order placement returned valid results with all 3 components.
        
        Args:
            ibkr_order_ids: List of order IDs returned from place_bracket_order
            symbol: Symbol for logging
            account_number: Account number for logging
            
        Returns:
            tuple: (success: bool, message: str, valid_order_ids: Optional[List[int]])
        """
        try:
            # Case 1: No order IDs returned (complete failure)
            if not ibkr_order_ids:
                return False, "Bracket order placement failed - no order IDs returned", None
            
            # Case 2: Wrong number of order IDs (partial failure)
            if len(ibkr_order_ids) != 3:
                error_msg = f"Bracket order returned {len(ibkr_order_ids)} orders instead of 3: {ibkr_order_ids}"
                
                # Check if profit target is missing (most common issue)
                if len(ibkr_order_ids) == 2:
                    # Try to identify which component is missing
                    parent_id = ibkr_order_ids[0] if ibkr_order_ids else None
                    if parent_id:
                        expected_take_profit = parent_id + 1
                        expected_stop_loss = parent_id + 2
                        
                        missing_components = []
                        if expected_take_profit not in ibkr_order_ids:
                            missing_components.append("TAKE_PROFIT")
                        if expected_stop_loss not in ibkr_order_ids:
                            missing_components.append("STOP_LOSS")
                        
                        if missing_components:
                            error_msg += f" - Missing: {', '.join(missing_components)}"
                
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Partial bracket order detected during validation",
                    symbol=symbol,
                    context_provider={
                        "expected_orders": 3,
                        "actual_orders": len(ibkr_order_ids),
                        "returned_order_ids": ibkr_order_ids,
                        "account_number": account_number,
                        "missing_components_identified": len(ibkr_order_ids) == 2,
                        "critical_issue": "profit_target_missing" if len(ibkr_order_ids) == 2 and "TAKE_PROFIT" in error_msg else "general_partial_bracket"
                    },
                    decision_reason=error_msg
                )
                return False, error_msg, None
            
            # Case 3: Valid 3-order bracket
            # Verify the order IDs form a proper bracket sequence
            parent_id = ibkr_order_ids[0]
            expected_take_profit = parent_id + 1
            expected_stop_loss = parent_id + 2
            
            if ibkr_order_ids[1] != expected_take_profit or ibkr_order_ids[2] != expected_stop_loss:
                error_msg = f"Bracket order ID sequence invalid: {ibkr_order_ids}, expected: [{parent_id}, {expected_take_profit}, {expected_stop_loss}]"
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Bracket order ID sequence validation failed",
                    symbol=symbol,
                    context_provider={
                        "actual_sequence": ibkr_order_ids,
                        "expected_sequence": [parent_id, expected_take_profit, expected_stop_loss],
                        "account_number": account_number
                    },
                    decision_reason=error_msg
                )
                return False, error_msg, None
            
            # All validations passed
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Bracket order result validation passed",
                symbol=symbol,
                context_provider={
                    "order_ids": ibkr_order_ids,
                    "account_number": account_number,
                    "parent_id": parent_id,
                    "take_profit_id": expected_take_profit,
                    "stop_loss_id": expected_stop_loss,
                    "sequence_valid": True
                },
                decision_reason="Bracket order IDs validated successfully"
            )
            return True, "Bracket order validation passed", ibkr_order_ids
            
        except Exception as e:
            error_msg = f"Bracket order validation error: {e}"
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket order validation exception",
                symbol=symbol,
                context_provider={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "account_number": account_number
                },
                decision_reason=error_msg
            )
            return False, error_msg, None
    # _validate_bracket_order_result - End

    # _handle_bracket_order_failure - Begin (NEW)
    def _handle_bracket_order_failure(self, ibkr_order_ids: Optional[List[int]], symbol: str, 
                                    error_message: str, account_number: Optional[str] = None) -> bool:
        """
        Handle bracket order placement failure with comprehensive cleanup.
        
        Args:
            ibkr_order_ids: Order IDs that were attempted (may be partial)
            symbol: Symbol for logging
            error_message: Error description
            account_number: Account number for logging
            
        Returns:
            bool: True if cleanup was successful, False otherwise
        """
        try:
            cleanup_success = True
            
            # Attempt to cancel any orders that were placed
            if ibkr_order_ids:
                cancelled_count = 0
                for order_id in ibkr_order_ids:
                    try:
                        if self._ibkr_client and self._ibkr_client.connected:
                            self._ibkr_client.cancel_order(order_id)
                            cancelled_count += 1
                            print(f"📤 Sent cancel for failed bracket order {order_id}")
                    except Exception as cancel_error:
                        print(f"❌ Failed to cancel order {order_id}: {cancel_error}")
                        cleanup_success = False
                
                # Log cleanup results
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Bracket order failure cleanup completed",
                    symbol=symbol,
                    context_provider={
                        "total_orders_attempted": len(ibkr_order_ids),
                        "orders_cancelled": cancelled_count,
                        "cleanup_success": cleanup_success and (cancelled_count == len(ibkr_order_ids)),
                        "account_number": account_number,
                        "original_error": error_message
                    },
                    decision_reason=f"Cleaned up {cancelled_count} of {len(ibkr_order_ids)} failed bracket orders"
                )
            else:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Bracket order failed - no orders to clean up",
                    symbol=symbol,
                    context_provider={
                        "account_number": account_number,
                        "error_message": error_message
                    },
                    decision_reason="No IBKR orders placed, no cleanup needed"
                )
            
            return cleanup_success
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket order failure handling error",
                symbol=symbol,
                context_provider={
                    "error": str(e),
                    "original_error": error_message,
                    "account_number": account_number
                },
                decision_reason=f"Bracket failure handling exception: {e}"
            )
            return False
    # _handle_bracket_order_failure - End

    # _wait_for_bracket_transmission - Begin (NEW)
    def _wait_for_bracket_transmission(self, parent_order_id: int, symbol: str, 
                                     account_number: Optional[str] = None) -> bool:
        """
        Wait for bracket order transmission verification from IBKR client.
        
        Args:
            parent_order_id: Parent order ID for the bracket
            symbol: Symbol for logging
            account_number: Account number for logging
            
        Returns:
            bool: True if transmission verified, False if timeout or failure
        """
        try:
            if not hasattr(self._ibkr_client, '_bracket_transmission_events'):
                # IBKR client doesn't have transmission tracking - skip verification
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Bracket transmission verification skipped - client lacks tracking",
                    symbol=symbol,
                    context_provider={
                        "parent_order_id": parent_order_id,
                        "account_number": account_number,
                        "reason": "ibkr_client_missing_tracking"
                    }
                )
                return True  # Assume success if no tracking available
            
            start_time = time.time()
            
            # Wait for transmission event with timeout
            with self._ibkr_client._bracket_order_lock:
                transmission_event = self._ibkr_client._bracket_transmission_events.get(parent_order_id)
                bracket_info = self._ibkr_client._active_bracket_orders.get(parent_order_id)
            
            if not transmission_event:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Bracket transmission event not found",
                    symbol=symbol,
                    context_provider={
                        "parent_order_id": parent_order_id,
                        "account_number": account_number,
                        "reason": "transmission_event_missing"
                    }
                )
                return False
            
            # Wait for transmission confirmation
            if transmission_event.wait(BRACKET_VERIFICATION_TIMEOUT):
                # Transmission successful
                transmission_time = time.time() - start_time
                
                with self._ibkr_client._bracket_order_lock:
                    bracket_info = self._ibkr_client._active_bracket_orders.get(parent_order_id)
                    verified = bracket_info.get('verified', False) if bracket_info else False
                    components_transmitted = bracket_info.get('components_transmitted', {}) if bracket_info else {}
                
                if verified:
                    transmitted_count = sum(components_transmitted.values())
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "Bracket order transmission verified",
                        symbol=symbol,
                        context_provider={
                            "parent_order_id": parent_order_id,
                            "transmission_time_seconds": transmission_time,
                            "components_transmitted": transmitted_count,
                            "total_components": len(components_transmitted),
                            "account_number": account_number,
                            "all_components_verified": transmitted_count == 3
                        },
                        decision_reason=f"All {transmitted_count} bracket components transmitted successfully"
                    )
                    return True
                else:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Bracket order transmission event set but not verified",
                        symbol=symbol,
                        context_provider={
                            "parent_order_id": parent_order_id,
                            "transmission_time_seconds": transmission_time,
                            "account_number": account_number,
                            "bracket_info_available": bracket_info is not None
                        }
                    )
                    return False
            else:
                # Timeout waiting for transmission
                transmission_time = time.time() - start_time
                
                with self._ibkr_client._bracket_order_lock:
                    bracket_info = self._ibkr_client._active_bracket_orders.get(parent_order_id)
                    components_transmitted = bracket_info.get('components_transmitted', {}) if bracket_info else {}
                
                transmitted_count = sum(components_transmitted.values())
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Bracket order transmission timeout",
                    symbol=symbol,
                    context_provider={
                        "parent_order_id": parent_order_id,
                        "timeout_seconds": BRACKET_VERIFICATION_TIMEOUT,
                        "actual_wait_seconds": transmission_time,
                        "components_transmitted": transmitted_count,
                        "total_components": len(components_transmitted),
                        "account_number": account_number,
                        "missing_components": 3 - transmitted_count
                    },
                    decision_reason=f"Timeout waiting for bracket transmission - only {transmitted_count} of 3 components transmitted"
                )
                return False
                
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket transmission waiting error",
                symbol=symbol,
                context_provider={
                    "parent_order_id": parent_order_id,
                    "error": str(e),
                    "account_number": account_number
                },
                decision_reason=f"Bracket transmission wait exception: {e}"
            )
            return False
    # _wait_for_bracket_transmission - End

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
        
    # Add these helper methods to the class:

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
    
    # _validate_profit_target_parameters - Begin (NEW)
    def _validate_profit_target_parameters(self, order) -> tuple[bool, str]:
        """Validate parameters specifically required for profit target calculation in bracket orders."""
        try:
            # Check if risk_reward_ratio is present and valid
            risk_reward_ratio = getattr(order, 'risk_reward_ratio', None)
            if risk_reward_ratio is None:
                return False, "Missing risk_reward_ratio parameter"
            
            if not isinstance(risk_reward_ratio, (int, float, Decimal)):
                return False, f"Invalid risk_reward_ratio type: {type(risk_reward_ratio)}"
                
            if risk_reward_ratio <= 0:
                return False, f"Invalid risk_reward_ratio value: {risk_reward_ratio}"
                
            # Check if entry_price and stop_loss are valid for profit target calculation
            entry_price = getattr(order, 'entry_price', None)
            stop_loss = getattr(order, 'stop_loss', None)
            
            if entry_price is None or entry_price <= 0:
                return False, f"Invalid entry_price for profit target: {entry_price}"
                
            if stop_loss is None or stop_loss <= 0:
                return False, f"Invalid stop_loss for profit target: {stop_loss}"
                
            # Check if entry_price and stop_loss are meaningfully different
            if abs(entry_price - stop_loss) / entry_price < 0.001:  # 0.1% tolerance
                return False, f"Entry price and stop loss too close: {entry_price} vs {stop_loss}"
                
            # Validate that profit target can be reasonably calculated
            try:
                # Test profit target calculation
                if order.action.value == "BUY":
                    test_profit_target = entry_price + (abs(entry_price - stop_loss) * risk_reward_ratio)
                else:
                    test_profit_target = entry_price - (abs(entry_price - stop_loss) * risk_reward_ratio)
                    
                if test_profit_target <= 0:
                    return False, f"Calculated profit target is invalid: {test_profit_target}"
                    
                if abs(test_profit_target - entry_price) / entry_price < 0.001:
                    return False, f"Profit target too close to entry price: {test_profit_target}"
                    
            except Exception as calc_error:
                return False, f"Profit target calculation test failed: {calc_error}"
                
            return True, "Profit target parameters validated successfully"
            
        except Exception as e:
            return False, f"Profit target parameter validation error: {e}"
    # _validate_profit_target_parameters - End

    # _get_current_market_price_for_order - Begin (NEW)
    def _get_current_market_price_for_order(self, order) -> Optional[float]:
        """
        Get current market price for an order, supporting dynamic price adjustment decisions.
        
        Args:
            order: PlannedOrder to get market price for
            
        Returns:
            float or None: Current market price if available
        """
        try:
            # First try to get price from market data manager via trading manager
            if (hasattr(self._trading_manager, 'data_feed') and 
                self._trading_manager.data_feed and 
                hasattr(self._trading_manager.data_feed, 'get_current_price')):
                
                price_data = self._trading_manager.data_feed.get_current_price(order.symbol)
                if price_data and 'price' in price_data and price_data['price'] > 0:
                    return float(price_data['price'])
            
            # Fallback: Try market data manager directly if available
            if (hasattr(self._trading_manager, 'market_data_manager') and 
                self._trading_manager.market_data_manager and
                hasattr(self._trading_manager.market_data_manager, 'get_current_price')):
                
                price_data = self._trading_manager.market_data_manager.get_current_price(order.symbol)
                if price_data and 'price' in price_data and price_data['price'] > 0:
                    return float(price_data['price'])
                    
            # Final fallback: Check if monitoring service has price
            if (hasattr(self._trading_manager, 'monitoring_service') and 
                self._trading_manager.monitoring_service and
                hasattr(self._trading_manager.monitoring_service, 'get_current_price')):
                
                current_price = self._trading_manager.monitoring_service.get_current_price(order.symbol)
                if current_price and current_price > 0:
                    return float(current_price)
            
            return None
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Failed to get market price for order",
                symbol=order.symbol,
                context_provider={'error': str(e)}
            )
            return None
    # _get_current_market_price_for_order - End

    # _validate_execution_conditions - Begin (UPDATED - Enhanced for price adjustment support)
    def _validate_execution_conditions(self, order, quantity, total_capital) -> tuple[bool, str]:
        """Layer 3: Comprehensive pre-execution validation with detailed logging including price adjustment support."""
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
                
            # Enhanced market data availability check for price adjustment
            market_valid, market_message, current_market_price = self._validate_market_data_available_with_price(order)
            if not market_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Market data validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": market_message,
                        "data_feed_connected": hasattr(self._trading_manager, 'data_feed') and self._trading_manager.data_feed.is_connected(),
                        "current_market_price_available": current_market_price is not None
                    },
                    decision_reason="Market data validation failed"
                )
                # For LIMIT orders, we need market price for potential adjustment
                if hasattr(order, 'order_type') and getattr(order, 'order_type') is not None:
                    order_type = getattr(order, 'order_type').value.upper()
                    if order_type == 'LMT' and current_market_price is None:
                        return False, f"Market data issue: {market_message} - price required for LIMIT order adjustment"
                
            # Check if price adjustment might be beneficial
            if (current_market_price and 
                hasattr(order, 'order_type') and 
                getattr(order, 'order_type') is not None and
                getattr(order, 'order_type').value.upper() == 'LMT'):
                
                price_diff_pct = abs(current_market_price - order.entry_price) / order.entry_price
                adjustment_possible = False
                
                if order.action.value.upper() == "BUY" and current_market_price < order.entry_price:
                    adjustment_possible = True
                elif order.action.value.upper() == "SELL" and current_market_price > order.entry_price:
                    adjustment_possible = True
                    
                if adjustment_possible and price_diff_pct >= 0.005:  # 0.5% threshold
                    self.context_logger.log_event(
                        TradingEventType.EXECUTION_DECISION,
                        "Price adjustment opportunity detected",
                        symbol=order.symbol,
                        context_provider={
                            "current_market_price": current_market_price,
                            "planned_entry_price": order.entry_price,
                            "price_difference_percent": price_diff_pct * 100,
                            "adjustment_threshold_met": True,
                            "potential_improvement": order.entry_price - current_market_price if order.action.value.upper() == "BUY" else current_market_price - order.entry_price
                        },
                        decision_reason=f"Market price favorable for {'BUY' if order.action.value.upper() == 'BUY' else 'SELL'} order adjustment"
                    )
                
            # Profit target specific parameters
            profit_target_valid, profit_target_message = self._validate_profit_target_parameters(order)
            if not profit_target_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Profit target validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": profit_target_message,
                        "risk_reward_ratio": getattr(order, 'risk_reward_ratio', None),
                        "entry_price": getattr(order, 'entry_price', None),
                        "stop_loss": getattr(order, 'stop_loss', None)
                    },
                    decision_reason="Profit target parameter validation failed"
                )
                return False, f"Profit target validation failed: {profit_target_message}"
                
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
                f"All execution conditions met for {order.symbol} including price adjustment readiness",
                symbol=order.symbol,
                context_provider={
                    "quantity": quantity,
                    "total_capital": total_capital,
                    "risk_reward_ratio": getattr(order, 'risk_reward_ratio', None),
                    "profit_target_parameters_valid": True,
                    "current_market_price_available": current_market_price is not None,
                    "price_adjustment_supported": True
                },
                decision_reason="All execution validations passed including price adjustment readiness"
            )
            return True, "All execution conditions met including price adjustment readiness"
            
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

    # _validate_market_data_available_with_price - Begin (NEW)
    def _validate_market_data_available_with_price(self, order) -> tuple[bool, str, Optional[float]]:
        """Enhanced market data validation that returns current market price if available."""
        try:
            if not hasattr(self._trading_manager, 'data_feed'):
                current_price = self._get_current_market_price_for_order(order)
                return True, "Execution allowed without data feed", current_price
                
            if not self._trading_manager.data_feed.is_connected():
                current_price = self._get_current_market_price_for_order(order)
                return True, "Execution allowed with disconnected data feed", current_price
                
            current_price = self._get_current_market_price_for_order(order)
            if current_price is None or current_price <= 0:
                return True, "Execution allowed without current market data", None
                
            return True, "Market data available", current_price
            
        except Exception as e:
            current_price = self._get_current_market_price_for_order(order)
            return True, f"Execution allowed despite market data error: {e}", current_price
    # _validate_market_data_available_with_price - End

    # execute_single_order - Begin (UPDATED - Enhanced with bracket verification)
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
        
        ENHANCED: Now includes comprehensive bracket order transmission verification.
        """
        
        ibkr_order_ids = None
        attempt_id = None
        
        try:
            # Enhanced diagnostic logging for bracket orders with price adjustment support
            is_bracket_order = hasattr(order, 'order_type') and getattr(order, 'order_type') is not None
            current_market_price = self._get_current_market_price_for_order(order)
            
            # Log execution start with price adjustment context
            if is_bracket_order or fill_probability > 0.7 or effective_priority > 5:
                adjustment_context = {
                    "probability": fill_probability,
                    "priority": effective_priority,
                    "live_trading": is_live_trading,
                    "account_number_provided": account_number is not None,
                    "total_capital": total_capital,
                    "quantity": quantity,
                    "is_bracket_order": is_bracket_order,
                    "risk_reward_ratio": getattr(order, 'risk_reward_ratio', 'MISSING'),
                    "bracket_order_fix": "enhanced_diagnostics_v3",
                    "current_market_price_available": current_market_price is not None,
                    "price_adjustment_supported": True,
                    "transmission_verification_enabled": True
                }
                
                if current_market_price and hasattr(order, 'entry_price'):
                    price_diff = current_market_price - order.entry_price
                    price_diff_pct = abs(price_diff) / order.entry_price * 100
                    adjustment_context.update({
                        "current_market_price": current_market_price,
                        "planned_entry_price": order.entry_price,
                        "price_difference": price_diff,
                        "price_difference_percent": price_diff_pct,
                        "adjustment_opportunity": (
                            (order.action.value.upper() == "BUY" and price_diff < 0) or
                            (order.action.value.upper() == "SELL" and price_diff > 0)
                        ) and price_diff_pct >= 0.5  # 0.5% threshold
                    })

                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Executing order with transmission verification",
                    symbol=order.symbol,
                    context_provider=adjustment_context
                )

            # Enhanced execution conditions validation with price adjustment support
            exec_valid, exec_message = self._validate_execution_conditions(order, quantity, total_capital)
            if not exec_valid:
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Execution rejected - validation failed with bracket verification checks",
                    symbol=order.symbol,
                    context_provider={
                        "reason": exec_message,
                        "account_number": account_number,
                        "bracket_order_fix": "validation_failed_v3",
                        "risk_reward_ratio": getattr(order, 'risk_reward_ratio', 'MISSING'),
                        "price_adjustment_impacted": "validation" in exec_message.lower(),
                        "transmission_verification_skipped": True
                    },
                    decision_reason=f"Execution validation failed: {exec_message}"
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
                            "bracket_order_fix": "duplicate_blocked_v3",
                            "price_adjustment_impacted": False,
                            "transmission_verification_skipped": True
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
                            "bracket_order_fix": "retry_blocked_v3",
                            "price_adjustment_impacted": False,
                            "transmission_verification_skipped": True
                        },
                        decision_reason="RAPID_RETRY_BLOCKED"
                    )
                    return False

            # === LIVE ORDER PATH (ATOMIC) ===
            if ibkr_connected:
                # Enhanced diagnostic logging with price adjustment context
                adjustment_diagnostics = {
                    "action": order.action.value,
                    "order_type": order.order_type.value,
                    "entry_price": order.entry_price,
                    "stop_loss": order.stop_loss,
                    "risk_reward_ratio": order.risk_reward_ratio,
                    "risk_per_trade": order.risk_per_trade,
                    "total_capital": total_capital,
                    "quantity": quantity,
                    "account_number": account_number,
                    "all_parameters_present": all([
                        order.entry_price is not None,
                        order.stop_loss is not None, 
                        order.risk_reward_ratio is not None,
                        order.risk_per_trade is not None
                    ]),
                    "current_market_price_available": current_market_price is not None,
                    "price_adjustment_ready": True,
                    "transmission_verification_enabled": True
                }
                
                if current_market_price:
                    adjustment_diagnostics.update({
                        "current_market_price": current_market_price,
                        "price_difference": current_market_price - order.entry_price,
                        "price_difference_percent": abs(current_market_price - order.entry_price) / order.entry_price * 100,
                        "adjustment_possible": (
                            (order.action.value.upper() == "BUY" and current_market_price < order.entry_price) or
                            (order.action.value.upper() == "SELL" and current_market_price > order.entry_price)
                        )
                    })

                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "BRACKET ORDER PRE-PLACEMENT DIAGNOSTICS with Transmission Verification",
                    symbol=order.symbol,
                    context_provider=adjustment_diagnostics,
                    decision_reason="Bracket order parameters validated with transmission verification"
                )

                # Record placement attempt before execution
                attempt_id = self._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'SUBMITTING', None, None,
                    account_number
                )
                
                contract = order.to_ib_contract()
                
                # Enhanced bracket order call with transmission verification
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Calling bracket order with transmission verification",
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
                        "bracket_order_fix": "parameter_validation_complete_v3",
                        "expected_components": 3,
                        "price_adjustment_enabled": True,
                        "transmission_verification_enabled": True,
                        "current_market_price_provided": current_market_price is not None
                    },
                    decision_reason="Bracket order call with transmission verification"
                )
                
                # STEP 1: Place IBKR bracket order - IBKR client now handles price adjustment internally
                ibkr_order_ids = self._ibkr_client.place_bracket_order(
                    contract,
                    order.action.value,
                    order.order_type.value,
                    order.security_type.value,
                    order.entry_price,
                    order.stop_loss,
                    order.risk_per_trade,
                    order.risk_reward_ratio,
                    total_capital,
                    account_number
                )

                # STEP 1a: Validate bracket order result
                bracket_valid, bracket_message, valid_order_ids = self._validate_bracket_order_result(
                    ibkr_order_ids, order.symbol, account_number
                )
                
                if not bracket_valid:
                    # Handle bracket validation failure
                    self._handle_bracket_order_failure(ibkr_order_ids, order.symbol, bracket_message, account_number)
                    raise Exception(bracket_message)
                
                # Use validated order IDs
                ibkr_order_ids = valid_order_ids
                parent_order_id = ibkr_order_ids[0] if ibkr_order_ids else None
                
                # STEP 1b: Wait for bracket transmission verification
                if parent_order_id:
                    transmission_verified = self._wait_for_bracket_transmission(parent_order_id, order.symbol, account_number)
                    if not transmission_verified:
                        error_msg = "Bracket order transmission verification failed - not all components transmitted"
                        self._handle_bracket_order_failure(ibkr_order_ids, order.symbol, error_msg, account_number)
                        raise Exception(error_msg)
                
                # STEP 2: Persist to DB (with fixed parameters)
                execution_id = self.order_persistence.record_order_execution(
                    planned_order=order,
                    filled_price=order.entry_price,  # Note: This might be adjusted by IBKR client
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
                
                # Enhanced success logging with transmission verification context
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Bracket order placed successfully - Transmission Verified",
                    symbol=order.symbol,
                    context_provider={
                        "order_ids": ibkr_order_ids,
                        "account": account_number,
                        "execution_id": execution_id,
                        "atomic_success": True,
                        "bracket_order_fix": "success_v3",
                        "expected_components": 3,
                        "actual_components": len(ibkr_order_ids),
                        "all_components_present": len(ibkr_order_ids) == 3,
                        "risk_reward_ratio_used": order.risk_reward_ratio,
                        "price_adjustment_capable": True,
                        "transmission_verified": True,
                        "market_price_available_at_execution": current_market_price is not None
                    },
                    decision_reason="Bracket order successfully submitted with transmission verification"
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
                    planned_order=order,
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
                    "Simulation order executed with transmission verification awareness",
                    symbol=order.symbol,
                    context_provider={
                        "account": account_number,
                        "ibkr_connected": False,
                        "bracket_order_fix": "simulation_skip_v3",
                        "price_adjustment_simulated": current_market_price is not None,
                        "transmission_verification_skipped": True
                    },
                    decision_reason="Simulation mode - no actual IBKR order"
                )
                
                return True

        except Exception as e:
            # Enhanced error logging with transmission verification context
            error_context = {
                "error": str(e),
                "account": account_number,
                "ibkr_orders_attempted": ibkr_order_ids is not None,
                "bracket_order_fix": "execution_failed_v3",
                "error_type": type(e).__name__,
                "risk_reward_ratio": getattr(order, 'risk_reward_ratio', 'MISSING'),
                "price_adjustment_related": any(keyword in str(e).lower() for keyword in ['adjust', 'price', 'market']),
                "transmission_verification_related": any(keyword in str(e).lower() for keyword in ['transmission', 'verify', 'component']),
                "current_market_price_available": current_market_price is not None
            }
            
            # Add specific context for partial bracket orders
            if ibkr_order_ids and len(ibkr_order_ids) != 3:
                error_context["partial_bracket_detected"] = True
                error_context["expected_components"] = 3
                error_context["actual_components"] = len(ibkr_order_ids)
                error_context["missing_profit_target"] = len(ibkr_order_ids) == 2
                error_context["transmission_verification_failed"] = True
            
            # ATOMIC ROLLBACK: Cancel IBKR orders if anything failed
            if ibkr_order_ids:
                try:
                    for order_id in ibkr_order_ids:
                        self._ibkr_client.cancel_order(order_id)
                    error_context["rollback_success"] = True
                    error_context["orders_cancelled"] = len(ibkr_order_ids)
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Rolled back IBKR orders due to execution failure",
                        symbol=order.symbol,
                        context_provider=error_context
                    )
                except Exception as cancel_error:
                    error_context["rollback_error"] = str(cancel_error)
                    error_context["rollback_success"] = False
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Failed to cancel orders during rollback",
                        symbol=order.symbol,
                        context_provider=error_context
                    )
            
            # Update attempt with failure
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'FAILED', ibkr_order_ids, str(e),
                account_number
            )
            
            # Log atomic execution failure with enhanced transmission verification context
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket order execution failed with transmission verification diagnostics",
                symbol=order.symbol,
                context_provider=error_context,
                decision_reason=f"Bracket order execution failed: {e}"
            )
            
            return False
    # execute_single_order - End

        # _get_order_key_parameters - Begin (NEW)
    def _get_order_key_parameters(self, order) -> Dict[str, Any]:
        """
        Extract key parameters from an order for duplicate detection.
        
        Args:
            order: PlannedOrder or IBKR order
            
        Returns:
            Dict with key parameters for comparison
        """
        try:
            # Extract symbol
            symbol = getattr(order, 'symbol', '') if hasattr(order, 'symbol') else getattr(getattr(order, 'contract', None), 'symbol', '')
            
            # Extract action
            action = None
            if hasattr(order, 'action'):
                action_val = getattr(order.action, "value", None) or getattr(order.action, "name", None) or str(order.action)
                action = str(action_val).upper().strip()
            else:
                action = getattr(order, 'action', '').upper()
            
            # Extract prices and quantity
            entry_price = getattr(order, 'entry_price', None) or getattr(order, 'lmtPrice', None)
            stop_loss = getattr(order, 'stop_loss', None) or getattr(order, 'auxPrice', None)
            quantity = getattr(order, 'totalQuantity', None)
            
            # For planned orders, get additional bracket parameters
            risk_reward_ratio = getattr(order, 'risk_reward_ratio', None)
            risk_per_trade = getattr(order, 'risk_per_trade', None)
            
            return {
                'symbol': symbol,
                'action': action,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'quantity': quantity,
                'risk_reward_ratio': risk_reward_ratio,
                'risk_per_trade': risk_per_trade,
                'order_type': getattr(order, 'orderType', '') if hasattr(order, 'orderType') else getattr(getattr(order, 'order_type', None), 'value', '')
            }
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error extracting order key parameters",
                context_provider={'error': str(e)}
            )
            return {}
    # _get_order_key_parameters - End

    # _detect_partial_bracket_duplicates - Begin (NEW)
    def _detect_partial_bracket_duplicates(self, planned_order, open_orders, account_number: Optional[str] = None) -> bool:
        """
        Detect if there are partial bracket orders that would make this new order a duplicate.
        
        Args:
            planned_order: New planned order to check
            open_orders: List of open orders from IBKR
            account_number: Account number for logging
            
        Returns:
            bool: True if partial bracket duplicate detected, False otherwise
        """
        try:
            planned_params = self._get_order_key_parameters(planned_order)
            planned_symbol = planned_params.get('symbol', '')
            
            if not planned_symbol:
                return False
                
            # Filter orders for the same symbol
            symbol_orders = [o for o in open_orders if getattr(o.contract, 'symbol', '') == planned_symbol]
            
            if not symbol_orders:
                return False
                
            # Look for bracket components (orders with parentId or orders that could be bracket parts)
            bracket_candidates = []
            for open_order in symbol_orders:
                order_params = self._get_order_key_parameters(open_order)
                
                # Check if this open order could be part of a bracket similar to planned order
                if self._orders_match_core_parameters(planned_params, order_params):
                    bracket_candidates.append({
                        'order': open_order,
                        'params': order_params,
                        'parent_id': getattr(open_order, 'parentId', None)
                    })
            
            # Analyze bracket candidates
            if len(bracket_candidates) >= 2:  # At least entry + one child order
                # Check if we have a potential partial bracket
                parent_orders = [c for c in bracket_candidates if c['parent_id'] is None or c['parent_id'] == 0]
                child_orders = [c for c in bracket_candidates if c['parent_id'] is not None and c['parent_id'] != 0]
                
                # If we have a parent order and at least one child, it's likely a partial bracket
                if parent_orders and child_orders:
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "Partial bracket duplicate detected",
                        symbol=planned_symbol,
                        context_provider={
                            "parent_orders_count": len(parent_orders),
                            "child_orders_count": len(child_orders),
                            "planned_action": planned_params.get('action', ''),
                            "planned_entry_price": planned_params.get('entry_price'),
                            "planned_quantity": planned_params.get('quantity'),
                            "account_number": account_number,
                            "duplicate_type": "partial_bracket"
                        },
                        decision_reason="Partial bracket exists - preventing duplicate order"
                    )
                    return True
                    
            return False
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error detecting partial bracket duplicates",
                symbol=getattr(planned_order, 'symbol', 'UNKNOWN'),
                context_provider={
                    "error": str(e),
                    "account_number": account_number
                }
            )
            return False
    # _detect_partial_bracket_duplicates - End

    # _orders_match_core_parameters - Begin (NEW)
    def _orders_match_core_parameters(self, planned_params: Dict, open_order_params: Dict) -> bool:
        """
        Check if two orders match on core parameters for duplicate detection.
        
        Args:
            planned_params: Parameters from planned order
            open_order_params: Parameters from open order
            
        Returns:
            bool: True if orders match core parameters
        """
        try:
            # Symbol must match
            if planned_params.get('symbol') != open_order_params.get('symbol'):
                return False
                
            # Action must match
            if planned_params.get('action') != open_order_params.get('action'):
                return False
                
            # Quantity must be similar (within 5%)
            planned_qty = planned_params.get('quantity')
            open_qty = open_order_params.get('quantity')
            if planned_qty and open_qty:
                qty_ratio = abs(planned_qty - open_qty) / max(planned_qty, open_qty)
                if qty_ratio > 0.05:  # More than 5% quantity difference
                    return False
                    
            # Entry price must be similar (within 1%)
            planned_entry = planned_params.get('entry_price')
            open_entry = open_order_params.get('entry_price')
            if planned_entry and open_entry and planned_entry > 0 and open_entry > 0:
                price_ratio = abs(planned_entry - open_entry) / planned_entry
                if price_ratio > 0.01:  # More than 1% price difference
                    return False
                    
            # Stop loss must be similar (within 1%) if both exist
            planned_stop = planned_params.get('stop_loss')
            open_stop = open_order_params.get('stop_loss')
            if planned_stop and open_stop and planned_stop > 0 and open_stop > 0:
                stop_ratio = abs(planned_stop - open_stop) / planned_stop
                if stop_ratio > 0.01:  # More than 1% stop difference
                    return False
                    
            # If we get here, core parameters match
            return True
            
        except Exception as e:
            # On error, assume no match to be safe
            return False
    # _orders_match_core_parameters - End

    # _is_duplicate_order_active - Begin (UPDATED - Enhanced with partial bracket detection)
    def _is_duplicate_order_active(self, order, account_number: Optional[str] = None) -> bool:
        """
        Enhanced duplicate order detection that checks for active bracket orders including partial brackets.
        
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
                
            # Enhanced: Check for partial bracket duplicates
            if self._detect_partial_bracket_duplicates(order, symbol_orders, account_number):
                return True
                
            # Enhanced: Check for bracket order components
            bracket_parent_orders = [o for o in symbol_orders if getattr(o, 'parentId', 0) == 0]
            
            # If there's already a bracket parent order for this symbol, check if it's similar
            if bracket_parent_orders:
                planned_params = self._get_order_key_parameters(order)
                for parent_order in bracket_parent_orders:
                    parent_params = self._get_order_key_parameters(parent_order)
                    if self._orders_match_core_parameters(planned_params, parent_params):
                        self.context_logger.log_event(
                            TradingEventType.ORDER_VALIDATION,
                            "Duplicate bracket order detected - active bracket already exists",
                            symbol=order.symbol,
                            context_provider={
                                "existing_parent_order_id": parent_order.orderId,
                                "existing_order_status": getattr(parent_order, 'status', 'UNKNOWN'),
                                "new_action": order.action.value,
                                "existing_entry_price": parent_params.get('entry_price'),
                                "planned_entry_price": planned_params.get('entry_price'),
                                "account_number": account_number,
                                "duplicate_type": "complete_bracket"
                            },
                            decision_reason="DUPLICATE_BRACKET_ORDER_PREVENTION"
                        )
                        return True
                
            # Original similarity check as fallback
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
                            "account_number": account_number,
                            "duplicate_type": "similar_order"
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
    # _is_duplicate_order_active - End

    # _orders_are_similar - Begin (UPDATED - Enhanced with parameter matching)
    def _orders_are_similar(self, ibkr_order, planned_order, account_number: Optional[str] = None) -> bool:
        """
        Determine if two orders are similar enough to be considered duplicates.
        Enhanced with core parameter matching.
        """
        try:
            # Use the new parameter matching system
            planned_params = self._get_order_key_parameters(planned_order)
            ibkr_params = self._get_order_key_parameters(ibkr_order)
            
            return self._orders_match_core_parameters(planned_params, ibkr_params)
            
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
    # _orders_are_similar - End

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