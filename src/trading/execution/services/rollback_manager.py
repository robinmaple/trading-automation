"""
Rollback and error handling service for order execution failures.
"""

from typing import List, Optional
from src.core.context_aware_logger import get_context_logger, TradingEventType


class RollbackManager:
    """Handles rollback and cleanup operations for failed order executions."""
    
    def __init__(self, ibkr_client):
        self.context_logger = get_context_logger()
        self._ibkr_client = ibkr_client

    def execute_rollback(self, ibkr_order_ids: Optional[List[int]], order, error_message: str, account_number: Optional[str] = None) -> bool:
        """
        Execute rollback for failed order execution.
        
        Args:
            ibkr_order_ids: List of IBKR order IDs to cancel
            order: The order that failed
            error_message: Original error message
            account_number: Account number for logging
            
        Returns:
            bool: True if rollback successful, False otherwise
        """
        try:
            rollback_success = True
            
            # ATOMIC ROLLBACK: Cancel IBKR orders if anything failed
            if ibkr_order_ids:
                cancelled_count = 0
                for order_id in ibkr_order_ids:
                    try:
                        if self._ibkr_client and self._ibkr_client.connected:
                            self._ibkr_client.cancel_order(order_id)
                            cancelled_count += 1
                            self.context_logger.log_event(
                                TradingEventType.SYSTEM_HEALTH,
                                f"Rolled back IBKR order {order_id}",
                                symbol=order.symbol,
                                context_provider={
                                    "order_id": order_id,
                                    "account_number": account_number
                                }
                            )
                    except Exception as cancel_error:
                        self.context_logger.log_event(
                            TradingEventType.SYSTEM_HEALTH,
                            f"Failed to cancel order {order_id} during rollback",
                            symbol=order.symbol,
                            context_provider={
                                "order_id": order_id,
                                "cancel_error": str(cancel_error),
                                "account_number": account_number
                            }
                        )
                        rollback_success = False
                
                # Log rollback results
                rollback_context = {
                    "total_orders_attempted": len(ibkr_order_ids),
                    "orders_cancelled": cancelled_count,
                    "rollback_success": rollback_success and (cancelled_count == len(ibkr_order_ids)),
                    "account_number": account_number,
                    "original_error": error_message
                }
                
                # Add specific context for partial bracket orders
                if ibkr_order_ids and len(ibkr_order_ids) != 3:
                    rollback_context["partial_bracket_detected"] = True
                    rollback_context["expected_components"] = 3
                    rollback_context["actual_components"] = len(ibkr_order_ids)
                    rollback_context["missing_profit_target"] = len(ibkr_order_ids) == 2
                
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Order execution rollback completed",
                    symbol=order.symbol,
                    context_provider=rollback_context,
                    decision_reason=f"Rolled back {cancelled_count} of {len(ibkr_order_ids)} orders"
                )
            else:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "No orders to rollback - no IBKR orders were placed",
                    symbol=order.symbol,
                    context_provider={
                        "account_number": account_number,
                        "error_message": error_message
                    },
                    decision_reason="No IBKR orders placed, no rollback needed"
                )
            
            return rollback_success
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Rollback execution error",
                symbol=order.symbol,
                context_provider={
                    "error": str(e),
                    "original_error": error_message,
                    "account_number": account_number
                },
                decision_reason=f"Rollback execution exception: {e}"
            )
            return False