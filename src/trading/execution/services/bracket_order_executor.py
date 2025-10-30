"""
Bracket order execution service handling individual bracket order placement, verification, and transmission.
"""

import time
from typing import List, Optional, Tuple
from src.core.context_aware_logger import get_context_logger, TradingEventType


class BracketOrderExecutor:
    """Handles individual bracket order execution, verification, and transmission monitoring."""
    
    # Bracket Order Verification Constants
    BRACKET_VERIFICATION_TIMEOUT = 15.0  # seconds to wait for bracket verification
    BRACKET_RETRY_DELAY = 2.0  # seconds between retry attempts
    MAX_BRACKET_RETRIES = 2  # maximum number of retry attempts

    def __init__(self, ibkr_client):
        self.context_logger = get_context_logger()
        self._ibkr_client = ibkr_client

    def _validate_bracket_order_result(self, ibkr_order_ids: Optional[List[int]], symbol: str, 
                                     account_number: Optional[str] = None) -> Tuple[bool, str, Optional[List[int]]]:
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
            if transmission_event.wait(self.BRACKET_VERIFICATION_TIMEOUT):
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
                        "timeout_seconds": self.BRACKET_VERIFICATION_TIMEOUT,
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