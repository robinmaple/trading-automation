"""
End of Day (EOD) Service for automated position management.
Handles closing DAY positions and expired HYBRID positions before market close.
Follows configurable timing and strategy-based rules.
"""

import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from src.core.context_aware_logger import get_context_logger, TradingEventType
from src.core.models import PlannedOrderDB, ExecutedOrderDB
from src.core.events import OrderState
# Fix PositionStrategy import - Begin
from src.core.planned_order import PositionStrategy
# Fix PositionStrategy import - End
from src.services.market_hours_service import MarketHoursService
from src.services.state_service import StateService


@dataclass
class EODConfig:
    """Configuration for End of Day processing."""
    enabled: bool = True
    close_buffer_minutes: int = 15  # Minutes before market close to start closing
    pre_market_start_minutes: int = 30  # Minutes before market open to start program
    post_market_end_minutes: int = 30  # Minutes after market close to stop program
    max_close_attempts: int = 3  # Maximum attempts to close a position


class EndOfDayService:
    """
    Service responsible for managing end-of-day position closures
    and strategy-based expiration handling.
    """

    def __init__(self, state_service: StateService, market_hours_service: MarketHoursService,
                 config: Optional[EODConfig] = None):
        """Initialize with required services and configuration."""
        self.context_logger = get_context_logger()
        self.state_service = state_service
        self.market_hours = market_hours_service
        self.config = config or EODConfig()
        
        # Track close attempts to prevent infinite loops
        self._close_attempts: Dict[int, int] = {}  # executed_order_id -> attempt_count
        
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="EndOfDayService initialized",
            context_provider={
                'enabled': lambda: self.config.enabled,
                'close_buffer_minutes': lambda: self.config.close_buffer_minutes,
                'pre_market_start_minutes': lambda: self.config.pre_market_start_minutes,
                'post_market_end_minutes': lambda: self.config.post_market_end_minutes
            },
            decision_reason="Service startup"
        )

    def should_run_eod_process(self) -> bool:
        """
        Check if EOD process should run based on market hours and configuration.
        Returns True if within the configured pre/post market windows.
        """
        if not self.config.enabled:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="EOD process disabled by configuration",
                decision_reason="EOD service disabled"
            )
            return False

        # Check if we're in the EOD closing window (last X minutes of trading)
        if self.market_hours.should_close_positions(self.config.close_buffer_minutes):
            time_until_close = self._get_time_until_close()
            self.context_logger.log_event(
                event_type=TradingEventType.MARKET_CONDITION,
                message="EOD process triggered - within closing window",
                context_provider={
                    'buffer_minutes': lambda: self.config.close_buffer_minutes,
                    'time_until_close_minutes': lambda: time_until_close,
                    'market_status': lambda: self.market_hours.get_market_status()
                },
                decision_reason=f"EOD closing window active - {time_until_close:.1f} minutes until close"
            )
            return True

        # Check if we're in pre/post market operational window
        if self._is_in_operational_window():
            window_type = self._get_operational_window_type()
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message=f"EOD service in {window_type} operational window",
                context_provider={
                    'window_type': lambda: window_type,
                    'pre_market_start': lambda: self.config.pre_market_start_minutes,
                    'post_market_end': lambda: self.config.post_market_end_minutes
                },
                decision_reason=f"Operational window check - {window_type}"
            )
            return True

        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="EOD process not triggered - outside operational windows",
            context_provider={
                'market_open': lambda: self.market_hours.is_market_open(),
                'time_until_close_minutes': lambda: self._get_time_until_close(),
                'in_operational_window': lambda: self._is_in_operational_window()
            },
            decision_reason="Outside EOD operational boundaries"
        )
        return False

    def _get_time_until_close(self) -> Optional[float]:
        """Get minutes until market close, or None if closed."""
        time_until_close = self.market_hours.time_until_market_close()
        if time_until_close:
            return time_until_close.total_seconds() / 60
        return None

    def _is_in_operational_window(self) -> bool:
        """Check if current time is within pre/post market operational windows."""
        now_et = datetime.datetime.now(self.market_hours.et_timezone)
        current_time = now_et.time()
        current_weekday = now_et.weekday()
        
        # Only run on weekdays
        if current_weekday >= 5:  # Saturday, Sunday
            return False

        # Pre-market window (X minutes before market open)
        pre_market_start = (
            datetime.datetime.combine(now_et.date(), self.market_hours.MARKET_OPEN) - 
            datetime.timedelta(minutes=self.config.pre_market_start_minutes)
        ).time()

        # Post-market window (up to X minutes after market close)
        post_market_end = (
            datetime.datetime.combine(now_et.date(), self.market_hours.MARKET_CLOSE) + 
            datetime.timedelta(minutes=self.config.post_market_end_minutes)
        ).time()

        # Check if in pre-market window
        if pre_market_start <= current_time < self.market_hours.MARKET_OPEN:
            return True

        # Check if in post-market window
        if self.market_hours.MARKET_CLOSE <= current_time <= post_market_end:
            return True

        return False

    def _get_operational_window_type(self) -> str:
        """Get the type of operational window currently active."""
        now_et = datetime.datetime.now(self.market_hours.et_timezone)
        current_time = now_et.time()

        if current_time < self.market_hours.MARKET_OPEN:
            return "PRE_MARKET"
        elif current_time > self.market_hours.MARKET_CLOSE:
            return "POST_MARKET"
        else:
            return "MARKET_HOURS"

    def run_eod_process(self) -> Dict[str, Any]:
        """
        Execute the complete EOD process:
        - Close DAY positions at market price
        - Close expired HYBRID positions
        - Expire corresponding PlannedOrders
        - Cancel bracket orders for closed positions
        """
        if not self.should_run_eod_process():
            return {"status": "skipped", "reason": "Not in EOD window"}

        self.context_logger.log_event(
            event_type=TradingEventType.POSITION_MANAGEMENT,
            message="Starting EOD process execution",
            context_provider={
                'close_buffer_minutes': lambda: self.config.close_buffer_minutes,
                'time_until_close_minutes': lambda: self._get_time_until_close(),
                'operational_window': lambda: self._get_operational_window_type()
            },
            decision_reason="Begin EOD position management cycle"
        )

        results = {
            "status": "completed",
            "day_positions_closed": 0,
            "hybrid_positions_closed": 0,
            "orders_expired": 0,
            "errors": []
        }

        try:
            # Get all open positions
            open_positions = self.state_service.get_open_positions()
            
            self.context_logger.log_event(
                event_type=TradingEventType.POSITION_MANAGEMENT,
                message="Retrieved open positions for EOD processing",
                context_provider={
                    'total_open_positions': lambda: len(open_positions),
                    'position_symbols': lambda: [self._get_position_symbol(p) for p in open_positions]
                },
                decision_reason="Position inventory loaded for EOD analysis"
            )

            # Process DAY positions
            day_results = self._close_day_positions(open_positions)
            results["day_positions_closed"] = day_results["closed"]
            results["errors"].extend(day_results["errors"])

            # Process HYBRID positions
            hybrid_results = self._close_expired_hybrid_positions(open_positions)
            results["hybrid_positions_closed"] = hybrid_results["closed"]
            results["errors"].extend(hybrid_results["errors"])

            # Expire corresponding PlannedOrders
            expire_results = self._expire_planned_orders()
            results["orders_expired"] = expire_results["expired"]
            results["errors"].extend(expire_results["errors"])

            # Log comprehensive EOD results
            self.context_logger.log_event(
                event_type=TradingEventType.POSITION_MANAGEMENT,
                message="EOD process execution completed",
                context_provider={
                    'day_positions_closed': lambda: results["day_positions_closed"],
                    'hybrid_positions_closed': lambda: results["hybrid_positions_closed"],
                    'orders_expired': lambda: results["orders_expired"],
                    'error_count': lambda: len(results["errors"]),
                    'total_processed_positions': lambda: len(open_positions)
                },
                decision_reason=f"EOD processing finished: {results['day_positions_closed']} DAY, {results['hybrid_positions_closed']} HYBRID closed"
            )

        except Exception as e:
            results["status"] = "failed"
            results["errors"].append(f"EOD process failed: {str(e)}")
            
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="EOD process execution failed with exception",
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e),
                    'results_so_far': lambda: {
                        'day_closed': results["day_positions_closed"],
                        'hybrid_closed': results["hybrid_positions_closed"],
                        'orders_expired': results["orders_expired"]
                    }
                },
                decision_reason=f"EOD process exception: {e}"
            )

        return results

    def _close_day_positions(self, open_positions: List[ExecutedOrderDB]) -> Dict[str, Any]:
        """Close all DAY strategy positions at market price."""
        day_positions = [
            pos for pos in open_positions 
            if self._is_day_position(pos)
        ]

        self.context_logger.log_event(
            event_type=TradingEventType.POSITION_MANAGEMENT,
            message="Processing DAY positions for EOD closure",
            context_provider={
                'day_positions_found': lambda: len(day_positions),
                'day_position_symbols': lambda: [self._get_position_symbol(p) for p in day_positions]
            },
            decision_reason=f"DAY position analysis: {len(day_positions)} found"
        )

        results = {"closed": 0, "errors": []}
        
        for position in day_positions:
            symbol = self._get_position_symbol(position)
            try:
                self.context_logger.log_event(
                    event_type=TradingEventType.POSITION_MANAGEMENT,
                    message="Closing DAY position for EOD",
                    symbol=symbol,
                    context_provider={
                        'executed_order_id': lambda: position.id,
                        'strategy': lambda: "DAY",
                        'position_action': lambda: getattr(position.planned_order, 'action', 'UNKNOWN') if position.planned_order else 'UNKNOWN',
                        'position_quantity': lambda: position.filled_quantity
                    },
                    decision_reason="DAY strategy requires EOD closure"
                )
                
                success = self._close_single_position(position, "DAY_EOD_CLOSE")
                if success:
                    results["closed"] += 1
                    self.context_logger.log_event(
                        event_type=TradingEventType.POSITION_MANAGEMENT,
                        message="DAY position closed successfully for EOD",
                        symbol=symbol,
                        context_provider={
                            'executed_order_id': lambda: position.id,
                            'close_reason': lambda: "DAY_EOD_CLOSE"
                        },
                        decision_reason="DAY position EOD closure completed"
                    )
                else:
                    error_msg = f"Failed to close DAY position {position.id}"
                    results["errors"].append(error_msg)
                    self.context_logger.log_event(
                        event_type=TradingEventType.SYSTEM_HEALTH,
                        message="DAY position closure failed",
                        symbol=symbol,
                        context_provider={
                            'executed_order_id': lambda: position.id,
                            'close_attempts': lambda: self._close_attempts.get(position.id, 0)
                        },
                        decision_reason="DAY position closure attempt failed"
                    )
            except Exception as e:
                error_msg = f"Error closing DAY position {position.id}: {str(e)}"
                results["errors"].append(error_msg)
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Exception during DAY position closure",
                    symbol=symbol,
                    context_provider={
                        'executed_order_id': lambda: position.id,
                        'error_type': lambda: type(e).__name__,
                        'error_message': lambda: str(e)
                    },
                    decision_reason=f"DAY position closure exception: {e}"
                )

        return results

    def _close_expired_hybrid_positions(self, open_positions: List[ExecutedOrderDB]) -> Dict[str, Any]:
        """Close expired HYBRID strategy positions."""
        hybrid_positions = [
            pos for pos in open_positions 
            if self._is_hybrid_position(pos)
        ]

        expired_hybrid_positions = [
            pos for pos in hybrid_positions 
            if self._is_position_expired(pos)
        ]

        self.context_logger.log_event(
            event_type=TradingEventType.POSITION_MANAGEMENT,
            message="Processing HYBRID positions for expiration check",
            context_provider={
                'total_hybrid_positions': lambda: len(hybrid_positions),
                'expired_hybrid_positions': lambda: len(expired_hybrid_positions),
                'expired_symbols': lambda: [self._get_position_symbol(p) for p in expired_hybrid_positions]
            },
            decision_reason=f"HYBRID expiration analysis: {len(expired_hybrid_positions)}/{len(hybrid_positions)} expired"
        )

        results = {"closed": 0, "errors": []}
        
        for position in expired_hybrid_positions:
            symbol = self._get_position_symbol(position)
            try:
                expiration_date = getattr(position.planned_order, 'expiration_date', None) if position.planned_order else None
                
                self.context_logger.log_event(
                    event_type=TradingEventType.POSITION_MANAGEMENT,
                    message="Closing expired HYBRID position",
                    symbol=symbol,
                    context_provider={
                        'executed_order_id': lambda: position.id,
                        'strategy': lambda: "HYBRID",
                        'expiration_date': lambda: expiration_date.isoformat() if expiration_date else 'UNKNOWN',
                        'current_time': lambda: datetime.datetime.now().isoformat(),
                        'is_expired': lambda: True
                    },
                    decision_reason="HYBRID position expired - requires closure"
                )
                
                success = self._close_single_position(position, "HYBRID_EXPIRED")
                if success:
                    results["closed"] += 1
                    self.context_logger.log_event(
                        event_type=TradingEventType.POSITION_MANAGEMENT,
                        message="Expired HYBRID position closed successfully",
                        symbol=symbol,
                        context_provider={
                            'executed_order_id': lambda: position.id,
                            'close_reason': lambda: "HYBRID_EXPIRED"
                        },
                        decision_reason="Expired HYBRID position closure completed"
                    )
                else:
                    error_msg = f"Failed to close expired HYBRID position {position.id}"
                    results["errors"].append(error_msg)
                    self.context_logger.log_event(
                        event_type=TradingEventType.SYSTEM_HEALTH,
                        message="Expired HYBRID position closure failed",
                        symbol=symbol,
                        context_provider={
                            'executed_order_id': lambda: position.id,
                            'close_attempts': lambda: self._close_attempts.get(position.id, 0)
                        },
                        decision_reason="Expired HYBRID position closure attempt failed"
                    )
            except Exception as e:
                error_msg = f"Error closing expired HYBRID position {position.id}: {str(e)}"
                results["errors"].append(error_msg)
                self.context_logger.log_event(
                    event_type=TradingEventType.SYSTEM_HEALTH,
                    message="Exception during expired HYBRID position closure",
                    symbol=symbol,
                    context_provider={
                        'executed_order_id': lambda: position.id,
                        'error_type': lambda: type(e).__name__,
                        'error_message': lambda: str(e)
                    },
                    decision_reason=f"Expired HYBRID position closure exception: {e}"
                )

        # Log non-expired HYBRID positions that are being left open
        non_expired_hybrid = [p for p in hybrid_positions if not self._is_position_expired(p)]
        if non_expired_hybrid:
            self.context_logger.log_event(
                event_type=TradingEventType.POSITION_MANAGEMENT,
                message="Non-expired HYBRID positions left open",
                context_provider={
                    'non_expired_count': lambda: len(non_expired_hybrid),
                    'non_expired_symbols': lambda: [self._get_position_symbol(p) for p in non_expired_hybrid]
                },
                decision_reason=f"HYBRID positions not expired: {len(non_expired_hybrid)} kept open"
            )

        return results

    def _expire_planned_orders(self) -> Dict[str, Any]:
        """Expire PlannedOrders for DAY and expired HYBRID strategies."""
        self.context_logger.log_event(
            event_type=TradingEventType.STATE_TRANSITION,
            message="Starting PlannedOrder expiration process",
            decision_reason="Begin EOD PlannedOrder expiration"
        )
            
        try:
            # Get all planned orders that should be expired
            orders_to_expire = self._get_orders_to_expire()
            
            self.context_logger.log_event(
                event_type=TradingEventType.STATE_TRANSITION,
                message="Found PlannedOrders for expiration",
                context_provider={
                    'orders_to_expire_count': lambda: len(orders_to_expire),
                    'order_symbols': lambda: [order.symbol for order in orders_to_expire]
                },
                decision_reason=f"PlannedOrder expiration analysis: {len(orders_to_expire)} to expire"
            )
            
            expired_count = 0
            
            for order in orders_to_expire:
                success = self.state_service.update_planned_order_state(
                    order.id, OrderState.EXPIRED, "EOD_SERVICE",
                    details={'reason': 'auto_expired_eod'}
                )
                if success:
                    expired_count += 1
                    self.context_logger.log_event(
                        event_type=TradingEventType.STATE_TRANSITION,
                        message="PlannedOrder expired by EOD service",
                        symbol=order.symbol,
                        context_provider={
                            'order_id': lambda: order.id,
                            'strategy': lambda: order.position_strategy,
                            'previous_status': lambda: order.status
                        },
                        decision_reason="PlannedOrder EOD expiration completed"
                    )
                else:
                    self.context_logger.log_event(
                        event_type=TradingEventType.SYSTEM_HEALTH,
                        message="Failed to expire PlannedOrder",
                        symbol=order.symbol,
                        context_provider={
                            'order_id': lambda: order.id,
                            'strategy': lambda: order.position_strategy
                        },
                        decision_reason="PlannedOrder expiration failed"
                    )

            self.context_logger.log_event(
                event_type=TradingEventType.STATE_TRANSITION,
                message="PlannedOrder expiration process completed",
                context_provider={
                    'expired_count': lambda: expired_count,
                    'total_considered': lambda: len(orders_to_expire),
                    'success_rate': lambda: round(expired_count / len(orders_to_expire) * 100, 2) if orders_to_expire else 0
                },
                decision_reason=f"PlannedOrder expiration finished: {expired_count}/{len(orders_to_expire)} expired"
            )

            return {"expired": expired_count, "errors": []}
            
        except Exception as e:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="PlannedOrder expiration process failed",
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason=f"PlannedOrder expiration exception: {e}"
            )
            return {"expired": 0, "errors": [f"Error expiring orders: {str(e)}"]}

    def _get_orders_to_expire(self) -> List[PlannedOrderDB]:
        """Get PlannedOrders that should be expired (DAY and expired HYBRID)."""
        # This would query for orders based on strategy and expiration logic
        # For now, return empty list - implementation depends on specific business rules
        return []

    def _is_day_position(self, position: ExecutedOrderDB) -> bool:
        """Check if position uses DAY strategy."""
        try:
            return (position.planned_order and 
                   position.planned_order.position_strategy == PositionStrategy.DAY.value)
        except Exception:
            return False

    def _is_hybrid_position(self, position: ExecutedOrderDB) -> bool:
        """Check if position uses HYBRID strategy."""
        try:
            return (position.planned_order and 
                   position.planned_order.position_strategy == PositionStrategy.HYBRID.value)
        except Exception:
            return False

    def _is_position_expired(self, position: ExecutedOrderDB) -> bool:
        """Check if HYBRID position has expired."""
        try:
            if not position.planned_order or not position.planned_order.expiration_date:
                return False
                
            now = datetime.datetime.now()
            is_expired = now >= position.planned_order.expiration_date.replace(tzinfo=None)
            return is_expired
        except Exception:
            return False

    def _close_single_position(self, position: ExecutedOrderDB, reason: str) -> bool:
        """Close a single position with market order logic."""
        symbol = self._get_position_symbol(position)
        
        # Check if we've exceeded max close attempts
        attempt_count = self._close_attempts.get(position.id, 0)
        if attempt_count >= self.config.max_close_attempts:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Max close attempts exceeded - skipping position",
                symbol=symbol,
                context_provider={
                    'executed_order_id': lambda: position.id,
                    'attempt_count': lambda: attempt_count,
                    'max_attempts': lambda: self.config.max_close_attempts,
                    'close_reason': lambda: reason
                },
                decision_reason="Close attempt limit reached - position closure skipped"
            )
            return False

        # Increment attempt counter
        self._close_attempts[position.id] = attempt_count + 1

        self.context_logger.log_event(
            event_type=TradingEventType.POSITION_MANAGEMENT,
            message="Attempting position closure",
            symbol=symbol,
            context_provider={
                'executed_order_id': lambda: position.id,
                'close_reason': lambda: reason,
                'attempt_number': lambda: attempt_count + 1,
                'max_attempts': lambda: self.config.max_close_attempts,
                'position_quantity': lambda: position.filled_quantity,
                'position_action': lambda: getattr(position.planned_order, 'action', 'UNKNOWN') if position.planned_order else 'UNKNOWN'
            },
            decision_reason=f"Position closure attempt {attempt_count + 1}/{self.config.max_close_attempts}"
        )

        # TODO: Implement actual market order placement
        # For now, use StateService to mark position as closed
        # In production, this would place a market order and update on fill
        
        # Placeholder: Use current price (would come from market data)
        current_price = self._get_current_market_price(position)
        if current_price is None:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Cannot close position - market price unavailable",
                symbol=symbol,
                context_provider={
                    'executed_order_id': lambda: position.id
                },
                decision_reason="Market price unavailable for position closure"
            )
            return False

        success = self.state_service.close_position(
            position.id, current_price, position.filled_quantity, position.commission
        )

        if success:
            # Clear attempt counter on success
            self._close_attempts.pop(position.id, None)
            self.context_logger.log_event(
                event_type=TradingEventType.POSITION_MANAGEMENT,
                message="Position closed successfully",
                symbol=symbol,
                context_provider={
                    'executed_order_id': lambda: position.id,
                    'close_price': lambda: current_price,
                    'close_reason': lambda: reason,
                    'attempts_used': lambda: attempt_count + 1
                },
                decision_reason="Position closure completed successfully"
            )
        else:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Position closure failed",
                symbol=symbol,
                context_provider={
                    'executed_order_id': lambda: position.id,
                    'close_reason': lambda: reason,
                    'attempt_count': lambda: attempt_count + 1,
                    'current_price': lambda: current_price
                },
                decision_reason="Position closure attempt failed"
            )

        return success

    def _get_current_market_price(self, position: ExecutedOrderDB) -> Optional[float]:
        """Get current market price for position symbol."""
        # TODO: Integrate with market data service
        # For now, return a placeholder price
        try:
            # Use filled price as fallback
            return position.filled_price
        except Exception:
            return None

    def _get_position_symbol(self, position: ExecutedOrderDB) -> str:
        """Safely get symbol from position."""
        try:
            if position.planned_order:
                return position.planned_order.symbol
            return "UNKNOWN"
        except Exception:
            return "UNKNOWN"

    def reset_close_attempts(self):
        """Reset close attempt counters (call at start of each trading day)."""
        previous_count = len(self._close_attempts)
        self._close_attempts.clear()
        
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="Close attempt counters reset",
            context_provider={
                'previous_attempt_count': lambda: previous_count
            },
            decision_reason="Daily close attempt counter reset"
        )