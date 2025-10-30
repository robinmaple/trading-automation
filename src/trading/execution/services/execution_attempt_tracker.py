"""
Order execution attempt tracking and recording service.
"""

import datetime
from typing import Optional
from src.core.models import OrderAttemptDB
from src.core.context_aware_logger import get_context_logger, TradingEventType


class ExecutionAttemptTracker:
    """Tracks and records order execution attempts for Phase B monitoring."""
    
    def __init__(self, order_persistence, trading_manager):
        self.context_logger = get_context_logger()
        self.order_persistence = order_persistence
        self._trading_manager = trading_manager

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