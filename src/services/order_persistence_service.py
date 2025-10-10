"""
Service for handling all order-related database persistence operations.
Consolidates logic for creating, updating, and querying PlannedOrderDB and ExecutedOrderDB records.
Acts as the gateway between business logic and the persistence layer.
"""

from decimal import Decimal
import datetime
import threading
from typing import Optional, Tuple, List, Dict
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.events import OrderState
from src.core.database import get_db_session
from src.core.models import ExecutedOrderDB, PlannedOrderDB, PositionStrategy
from src.core.shared_enums import OrderState as SharedOrderState
from src.core.planned_order import PlannedOrder, Action, OrderType, SecurityType, PositionStrategy as PositionStrategyEnum

# Context-aware logging import - replacing simple_logger
from src.core.context_aware_logger import get_context_logger, TradingEventType, SafeContext

# Initialize context-aware logger
context_logger = get_context_logger()


class OrderPersistenceService:
    """Encapsulates all database operations for order persistence and validation."""

    def __init__(self, db_session: Optional[Session] = None):
        """Initialize the service with an optional database session."""
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing OrderPersistenceService",
            context_provider={
                "db_session_provided": db_session is not None,
                "thread_id": str(threading.get_ident()) if 'threading' in globals() else "unknown"
            }
        )
            
        self.db_session = db_session or get_db_session()
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "OrderPersistenceService initialized successfully",
            context_provider={
                "session_id": context_logger.session_id,
                "db_session_type": type(self.db_session).__name__
            }
        )

    def get_active_orders(self) -> List[PlannedOrderDB]:
        """
        Get all active orders from database that should be resumed.
        
        Returns:
            List of PlannedOrderDB objects with active status
        """
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            "Querying database for active orders",
            context_provider={
                "query_type": "active_orders",
                "target_statuses": ["PENDING", "LIVE", "LIVE_WORKING"]
            }
        )
            
        try:
            active_db_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.status.in_([
                    SharedOrderState.PENDING.value,
                    SharedOrderState.LIVE.value, 
                    SharedOrderState.LIVE_WORKING.value
                ])
            ).all()
            
            context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                f"Found {len(active_db_orders)} active orders in database",
                context_provider={
                    "orders_count": len(active_db_orders),
                    "order_symbols": [order.symbol for order in active_db_orders],
                    "query_execution_time": datetime.datetime.now().isoformat()
                }
            )
            return active_db_orders
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Failed to get active orders from database: {e}",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "get_active_orders"
                },
                decision_reason="DATABASE_QUERY_FAILED"
            )
            return []
    # <Active Orders Query - End>

    # Account Tracking Implementation - Begin
    def record_order_execution(self, planned_order, filled_price: float,
                             filled_quantity: float, account_number: str,
                             commission: float = 0.0, status: str = 'FILLED') -> Optional[int]:
        """Record an order execution in the database with account context. Returns the ID of the new record or None."""
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            f"Recording order execution for {planned_order.symbol}",
            symbol=planned_order.symbol,
            context_provider={
                "account_number": account_number,
                "filled_price": filled_price,
                "filled_quantity": filled_quantity,
                "commission": commission,
                "status": status,
                "is_live_trading": account_number.startswith('U'),
                "order_action": planned_order.action.value,
                "order_type": planned_order.order_type.value
            }
        )
            
        try:
            planned_order_id = self._find_planned_order_id(planned_order)
            if planned_order_id is None:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Cannot record execution: Planned order not found in database for {planned_order.symbol}",
                    symbol=planned_order.symbol,
                    context_provider={
                        "symbol": planned_order.symbol,
                        "entry_price": planned_order.entry_price,
                        "stop_loss": planned_order.stop_loss,
                        "action": planned_order.action.value,
                        "order_type": planned_order.order_type.value
                    },
                    decision_reason="PLANNED_ORDER_NOT_FOUND"
                )
                    
                existing_orders = self.db_session.query(PlannedOrderDB).filter_by(symbol=planned_order.symbol).all()
                context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    f"Found {len(existing_orders)} existing orders for symbol {planned_order.symbol}",
                    symbol=planned_order.symbol,
                    context_provider={
                        "existing_orders_count": len(existing_orders),
                        "existing_order_details": [
                            {
                                "symbol": order.symbol,
                                "entry_price": order.entry_price,
                                "stop_loss": order.stop_loss,
                                "status": order.status
                            } for order in existing_orders
                        ]
                    }
                )
                return None

            executed_order = ExecutedOrderDB(
                planned_order_id=planned_order_id,
                filled_price=filled_price,
                filled_quantity=filled_quantity,
                commission=commission,
                status=status,
                executed_at=datetime.datetime.now(),
                is_live_trading=account_number.startswith('U'),  # Live accounts start with 'U'
                account_number=account_number  # Store which account executed this
            )

            if planned_order.position_strategy.value == 'HYBRID':
                expiration_date = datetime.datetime.now() + datetime.timedelta(days=10)
                executed_order.expiration_date = expiration_date
                context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"HYBRID order expiration set",
                    symbol=planned_order.symbol,
                    context_provider={
                        "expiration_date": expiration_date.strftime('%Y-%m-%d %H:%M'),
                        "position_strategy": planned_order.position_strategy.value
                    }
                )

            self.db_session.add(executed_order)
            self.db_session.commit()

            context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Execution recorded successfully",
                symbol=planned_order.symbol,
                context_provider={
                    "executed_order_id": executed_order.id,
                    "account_number": account_number,
                    "filled_quantity": filled_quantity,
                    "filled_price": filled_price,
                    "status": status,
                    "is_live_trading": account_number.startswith('U'),
                    "commission": commission
                },
                decision_reason="ORDER_EXECUTION_RECORDED"
            )

            return executed_order.id

        except Exception as e:
            self.db_session.rollback()
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Failed to record order execution: {e}",
                symbol=planned_order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "account_number": account_number,
                    "operation": "record_order_execution"
                },
                decision_reason="EXECUTION_RECORDING_FAILED"
            )
            return None

    def create_executed_order(self, planned_order, fill_info, account_number: str) -> Optional[ExecutedOrderDB]:
        """Create an ExecutedOrderDB record from a PlannedOrder and fill information with account context."""
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            f"Creating executed order record",
            symbol=planned_order.symbol,
            context_provider={
                "account_number": account_number,
                "fill_info": fill_info,
                "planned_order_symbol": planned_order.symbol,
                "is_live_trading": account_number.startswith('U')
            }
        )
            
        try:
            planned_order_id = self._find_planned_order_id(planned_order)
            if not planned_order_id:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Cannot create executed order: Planned order not found",
                    symbol=planned_order.symbol,
                    context_provider={
                        "symbol": planned_order.symbol,
                        "account_number": account_number
                    },
                    decision_reason="PLANNED_ORDER_NOT_FOUND"
                )
                return None

            executed_order = ExecutedOrderDB(
                planned_order_id=planned_order_id,
                filled_price=fill_info.get('price', 0),
                filled_quantity=fill_info.get('quantity', 0),
                commission=fill_info.get('commission', 0),
                pnl=fill_info.get('pnl', 0),
                status=fill_info.get('status', 'FILLED'),
                executed_at=datetime.datetime.now(),
                account_number=account_number,  # Store which account executed this
                is_live_trading=account_number.startswith('U')  # Live accounts start with 'U'
            )

            self.db_session.add(executed_order)
            self.db_session.commit()

            context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Executed order created successfully",
                symbol=planned_order.symbol,
                context_provider={
                    "executed_order_id": executed_order.id,
                    "account_number": account_number,
                    "filled_price": fill_info.get('price', 0),
                    "filled_quantity": fill_info.get('quantity', 0),
                    "commission": fill_info.get('commission', 0),
                    "pnl": fill_info.get('pnl', 0)
                },
                decision_reason="EXECUTED_ORDER_CREATED"
            )

            return executed_order

        except Exception as e:
            self.db_session.rollback()
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Failed to create executed order: {e}",
                symbol=planned_order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "account_number": account_number,
                    "operation": "create_executed_order"
                },
                decision_reason="EXECUTED_ORDER_CREATION_FAILED"
            )
            return None

    def get_realized_pnl_period(self, account_number: str, days: int) -> Decimal:
        """Get realized P&L for specific account for the last N calendar days."""
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            f"Querying realized P&L for account",
            context_provider={
                "account_number": account_number,
                "days_period": days,
                "start_date": (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat()
            }
        )
            
        start_date = datetime.datetime.now() - datetime.timedelta(days=days)
        
        result = self.db_session.execute(
            text("""
                SELECT COALESCE(SUM(realized_pnl), 0) 
                FROM executed_orders 
                WHERE exit_time >= :start_date AND account_number = :account_number
            """),
            {'start_date': start_date, 'account_number': account_number}
        ).scalar()
        
        pnl = Decimal(str(result or '0'))
        
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            f"Realized P&L query completed",
            context_provider={
                "account_number": account_number,
                "realized_pnl": float(pnl),
                "days_period": days,
                "query_execution_time": datetime.datetime.now().isoformat()
            },
            decision_reason="PNL_QUERY_COMPLETED"
        )
            
        return pnl

    def record_realized_pnl(self, order_id: int, symbol: str, pnl: Decimal, 
                          exit_date: datetime, account_number: str):
        """Record realized P&L for a closed trade with account context."""
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            f"Recording realized P&L for trade",
            symbol=symbol,
            context_provider={
                "order_id": order_id,
                "symbol": symbol,
                "pnl_amount": float(pnl),
                "exit_date": exit_date.isoformat(),
                "account_number": account_number
            }
        )
            
        try:
            self.db_session.execute(
                text("""
                    INSERT INTO executed_orders (order_id, symbol, realized_pnl, exit_time, account_number)
                    VALUES (:order_id, :symbol, :pnl, :exit_time, :account_number)
                """),
                {
                    'order_id': order_id,
                    'symbol': symbol,
                    'pnl': float(pnl),
                    'exit_time': exit_date,
                    'account_number': account_number
                }
            )
            self.db_session.commit()
            context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"P&L recorded successfully",
                symbol=symbol,
                context_provider={
                    "order_id": order_id,
                    "pnl_recorded": float(pnl),
                    "account_number": account_number
                },
                decision_reason="PNL_RECORDED"
            )
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Failed to record P&L: {e}",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "order_id": order_id,
                    "account_number": account_number,
                    "operation": "record_realized_pnl"
                },
                decision_reason="PNL_RECORDING_FAILED"
            )
            raise
    # Account Tracking Implementation - End

    def _find_planned_order_id(self, planned_order) -> Optional[int]:
        """Find the database ID for a planned order based on its parameters."""
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Finding planned order ID in database",
            symbol=planned_order.symbol,
            context_provider={
                "symbol": planned_order.symbol,
                "entry_price": planned_order.entry_price,
                "stop_loss": planned_order.stop_loss,
                "action": planned_order.action.value,
                "order_type": planned_order.order_type.value
            }
        )
            
        try:
            db_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=planned_order.symbol,
                entry_price=planned_order.entry_price,
                stop_loss=planned_order.stop_loss,
                action=planned_order.action.value,
                order_type=planned_order.order_type.value
            ).first()
            
            order_id = db_order.id if db_order else None
            context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                f"Planned order ID lookup completed",
                symbol=planned_order.symbol,
                context_provider={
                    "found_order_id": order_id,
                    "order_exists": order_id is not None,
                    "query_execution_time": datetime.datetime.now().isoformat()
                },
                decision_reason="ORDER_ID_LOOKUP_COMPLETED"
            )
                    
            return order_id
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error finding planned order in database: {e}",
                symbol=planned_order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_find_planned_order_id"
                },
                decision_reason="ORDER_ID_LOOKUP_FAILED"
            )
            return None

    def convert_to_db_model(self, planned_order) -> PlannedOrderDB:
        """Convert a domain PlannedOrder object to a PlannedOrderDB entity."""
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            f"Converting PlannedOrder to DB model",
            symbol=planned_order.symbol,
            context_provider={
                "symbol": planned_order.symbol,
                "action": planned_order.action.value,
                "order_type": planned_order.order_type.value,
                "security_type": planned_order.security_type.value,
                "position_strategy": getattr(planned_order.position_strategy, 'value', str(planned_order.position_strategy))
            }
        )

        # Resolve DB row; create if missing
        strategy_name = getattr(planned_order.position_strategy, 'value', str(planned_order.position_strategy))
        position_strategy = self.db_session.query(PositionStrategy).filter_by(name=strategy_name).first()

        if not position_strategy:
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                f"Creating new position strategy in database",
                symbol=planned_order.symbol,
                context_provider={
                    "strategy_name": strategy_name,
                    "action_taken": "auto_creation"
                },
                decision_reason="POSITION_STRATEGY_CREATED"
            )
            position_strategy = PositionStrategy(name=strategy_name)
            self.db_session.add(position_strategy)
            self.db_session.commit()

        db_model = PlannedOrderDB(
            symbol=planned_order.symbol,
            security_type=planned_order.security_type.value,
            action=planned_order.action.value,
            order_type=planned_order.order_type.value,
            entry_price=planned_order.entry_price,
            stop_loss=planned_order.stop_loss,
            risk_per_trade=planned_order.risk_per_trade,
            risk_reward_ratio=planned_order.risk_reward_ratio,
            priority=planned_order.priority,
            position_strategy_id=position_strategy.id,
            status='PENDING',
            overall_trend=planned_order.overall_trend,       # store human-entered value
            brief_analysis=planned_order.brief_analysis
        )
        
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            f"PlannedOrder converted to DB model successfully",
            symbol=planned_order.symbol,
            context_provider={
                "db_model_created": True,
                "position_strategy_id": position_strategy.id,
                "assigned_status": "PENDING"
            },
            decision_reason="DB_MODEL_CONVERSION_COMPLETED"
        )
        return db_model

    def handle_order_rejection(self, planned_order_id: int, rejection_reason: str) -> bool:
        """Mark a planned order as CANCELLED with a rejection reason in the database."""
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            f"Handling order rejection",
            context_provider={
                "planned_order_id": planned_order_id,
                "rejection_reason": rejection_reason
            }
        )
            
        try:
            order = self.db_session.query(PlannedOrderDB).filter_by(id=planned_order_id).first()
            if order:
                order.status = 'CANCELLED'
                order.rejection_reason = rejection_reason
                order.updated_at = datetime.datetime.now()
                self.db_session.commit()
                context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Order cancelled due to rejection",
                    symbol=order.symbol,
                    context_provider={
                        "order_id": planned_order_id,
                        "symbol": order.symbol,
                        "rejection_reason": rejection_reason,
                        "new_status": "CANCELLED"
                    },
                    decision_reason="ORDER_REJECTED"
                )
                return True
            else:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Order not found for rejection handling",
                    context_provider={
                        "planned_order_id": planned_order_id,
                        "error": "ORDER_NOT_FOUND"
                    },
                    decision_reason="REJECTION_HANDLING_FAILED"
                )
                return False
        except Exception as e:
            self.db_session.rollback()
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Failed to cancel rejected order: {e}",
                context_provider={
                    "planned_order_id": planned_order_id,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "handle_order_rejection"
                },
                decision_reason="REJECTION_HANDLING_ERROR"
            )
            return False

    def validate_sufficient_margin(self, symbol: str, quantity: float, entry_price: float,
                                currency: str = 'USD') -> Tuple[bool, str]:
        """Validate if the account has sufficient margin for a proposed trade. Returns (is_valid, message)."""
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            f"Validating margin for trade",
            symbol=symbol,
            context_provider={
                "symbol": symbol,
                "quantity": quantity,
                "entry_price": entry_price,
                "currency": currency,
                "trade_value": quantity * entry_price
            }
        )
            
        try:
            account_value = self.get_account_value()
            trade_value = quantity * entry_price

            if symbol in ['EUR', 'AUD', 'GBP', 'JPY', 'CAD']:
                margin_requirement = trade_value * 0.02
            else:
                margin_requirement = trade_value * 0.5

            max_allowed_margin = account_value * 0.8

            if margin_requirement > max_allowed_margin:
                message = (f"Insufficient margin. Required: ${margin_requirement:,.2f}, "
                        f"Available: ${max_allowed_margin:,.2f}, "
                        f"Account Value: ${account_value:,.2f}")
                context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    f"Margin validation failed",
                    symbol=symbol,
                    context_provider={
                        "margin_required": margin_requirement,
                        "margin_available": max_allowed_margin,
                        "account_value": account_value,
                        "validation_result": "FAILED"
                    },
                    decision_reason="INSUFFICIENT_MARGIN"
                )
                return False, message

            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                f"Margin validation passed",
                symbol=symbol,
                context_provider={
                    "margin_required": margin_requirement,
                    "margin_available": max_allowed_margin,
                    "account_value": account_value,
                    "validation_result": "PASSED"
                },
                decision_reason="SUFFICIENT_MARGIN"
            )
            return True, "Sufficient margin available"

        except Exception as e:
            error_msg = f"Margin validation error: {e}"
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Margin validation error occurred",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "validate_sufficient_margin"
                },
                decision_reason="MARGIN_VALIDATION_ERROR"
            )
            return False, error_msg

    def get_account_value(self, account_id: str = None) -> float:
        """Get the current account value. Currently a mock implementation."""
        try:
            mock_account_value = 100000.0
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                f"Retrieved account value",
                context_provider={
                    "account_value": mock_account_value,
                    "account_id": account_id,
                    "is_mock_data": True,
                    "data_source": "mock_implementation"
                }
            )
            return mock_account_value
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Failed to get account value: {e}",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "get_account_value"
                },
                decision_reason="ACCOUNT_VALUE_RETRIEVAL_FAILED"
            )
            return 50000.0

    def update_order_status(self, order, status: str, reason: str = "", order_ids=None) -> bool:
        """
        Update the status of a PlannedOrder in the database and synchronize all relevant fields.
        Always overwrites with current PlannedOrder values (blind update).
        
        Args:
            order: PlannedOrder domain object
            status: New status string ('PENDING', 'LIVE', etc.)
            reason: Optional reason for status update
            order_ids: Optional list of IBKR order IDs

        Returns:
            True if update/create succeeded, False otherwise
        """
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            f"Updating order status",
            symbol=order.symbol,
            context_provider={
                "symbol": order.symbol,
                "new_status": status,
                "reason": reason,
                "has_order_ids": order_ids is not None,
                "order_ids_count": len(order_ids) if order_ids else 0
            }
        )
            
        try:
            valid_statuses = ['PENDING', 'LIVE', 'LIVE_WORKING', 'FILLED', 'CANCELLED',
                            'EXPIRED', 'LIQUIDATED', 'REPLACED']

            if status not in valid_statuses:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Invalid order status provided",
                    symbol=order.symbol,
                    context_provider={
                        "provided_status": status,
                        "valid_statuses": valid_statuses,
                        "validation_result": "FAILED"
                    },
                    decision_reason="INVALID_ORDER_STATUS"
                )
                return False

            db_order = self._find_planned_order_db_record(order)

            if db_order:
                # Blindly update all relevant fields
                db_order.status = status
                db_order.overall_trend = getattr(order, 'overall_trend', db_order.overall_trend)
                db_order.brief_analysis = getattr(order, 'brief_analysis', db_order.brief_analysis)
                db_order.risk_per_trade = getattr(order, 'risk_per_trade', db_order.risk_per_trade)
                db_order.risk_reward_ratio = getattr(order, 'risk_reward_ratio', db_order.risk_reward_ratio)
                db_order.priority = getattr(order, 'priority', db_order.priority)
                db_order.entry_price = getattr(order, 'entry_price', db_order.entry_price)
                db_order.stop_loss = getattr(order, 'stop_loss', db_order.stop_loss)
                db_order.action = getattr(order.action, 'value', db_order.action)
                db_order.order_type = getattr(order.order_type, 'value', db_order.order_type)
                db_order.security_type = getattr(order.security_type, 'value', db_order.security_type)
                db_order.position_strategy_id = self.db_session.query(PositionStrategy).filter_by(
                    name=order.position_strategy.value).first().id
                if order_ids:
                    db_order.ibkr_order_ids = str(order_ids)
                if reason:
                    db_order.status_reason = reason[:255]
                db_order.updated_at = datetime.datetime.now()

                self.db_session.commit()
                context_logger.log_event(
                    TradingEventType.STATE_TRANSITION,
                    f"Order status updated successfully",
                    symbol=order.symbol,
                    context_provider={
                        "order_id": db_order.id,
                        "old_status": db_order.status,
                        "new_status": status,
                        "update_type": "existing_record"
                    },
                    decision_reason="ORDER_STATUS_UPDATED"
                )
                return True

            else:
                # Record does not exist, create new
                try:
                    db_model = self.convert_to_db_model(order)
                    db_model.status = status
                    db_model.overall_trend = getattr(order, 'overall_trend', None)
                    db_model.brief_analysis = getattr(order, 'brief_analysis', None)
                    if reason:
                        db_model.status_reason = reason[:255]
                    if order_ids:
                        db_model.ibkr_order_ids = str(order_ids)

                    self.db_session.add(db_model)
                    self.db_session.commit()
                    context_logger.log_event(
                        TradingEventType.STATE_TRANSITION,
                        f"New order record created with status",
                        symbol=order.symbol,
                        context_provider={
                            "new_order_id": db_model.id,
                            "status": status,
                            "creation_type": "new_record"
                        },
                        decision_reason="ORDER_RECORD_CREATED"
                    )
                    return True
                except Exception as create_error:
                    context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Failed to create order record: {create_error}",
                        symbol=order.symbol,
                        context_provider={
                            "error_type": type(create_error).__name__,
                            "error_details": str(create_error),
                            "operation": "update_order_status_create"
                        },
                        decision_reason="ORDER_CREATION_FAILED"
                    )
                    return False

        except Exception as e:
            self.db_session.rollback()
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Failed to update order status: {e}",
                symbol=order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "update_order_status"
                },
                decision_reason="ORDER_STATUS_UPDATE_FAILED"
            )
            return False

    def _find_planned_order_db_record(self, order) -> Optional[PlannedOrderDB]:
        """Find a PlannedOrderDB record in the database based on its parameters."""
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Finding planned order DB record",
            symbol=order.symbol,
            context_provider={
                "symbol": order.symbol,
                "entry_price": order.entry_price,
                "stop_loss": order.stop_loss,
                "action": order.action.value,
                "order_type": order.order_type.value
            }
        )
            
        try:
            db_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value,
                order_type=order.order_type.value
            ).first()
            
            context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                f"Planned order DB record lookup completed",
                symbol=order.symbol,
                context_provider={
                    "record_found": db_order is not None,
                    "order_id": db_order.id if db_order else None,
                    "query_execution_time": datetime.datetime.now().isoformat()
                },
                decision_reason="DB_RECORD_LOOKUP_COMPLETED"
            )
                    
            return db_order
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error finding planned order in database: {e}",
                symbol=order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_find_planned_order_db_record"
                },
                decision_reason="DB_RECORD_LOOKUP_FAILED"
            )
            return None

    # <Advanced Feature Integration - Begin>
    # Updated methods for account-specific performance analysis
    def get_trades_by_setup(self, setup_name: str, account_number: str, days_back: int = 90) -> List[Dict]:
        """
        Get all completed trades for a specific trading setup and account within time period.
        
        Args:
            setup_name: Name of the trading setup (e.g., 'Breakout', 'Reversal')
            account_number: Specific account to filter by
            days_back: Number of days to look back (default: 90 days)
            
        Returns:
            List of trade dictionaries with performance data
        """
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Querying trades by setup",
            context_provider={
                "setup_name": setup_name,
                "account_number": account_number,
                "days_back": days_back,
                "cutoff_date": (datetime.datetime.now() - datetime.timedelta(days=days_back)).isoformat()
            }
        )
            
        try:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_back)
            
            # Join PlannedOrderDB with ExecutedOrderDB to get setup information
            query = self.db_session.query(
                ExecutedOrderDB,
                PlannedOrderDB.trading_setup,
                PlannedOrderDB.timeframe,
                PlannedOrderDB.risk_reward_ratio
            ).join(
                PlannedOrderDB, ExecutedOrderDB.planned_order_id == PlannedOrderDB.id
            ).filter(
                PlannedOrderDB.trading_setup == setup_name,
                ExecutedOrderDB.executed_at >= cutoff_date,
                ExecutedOrderDB.status == 'FILLED',
                ExecutedOrderDB.filled_price.isnot(None),
                ExecutedOrderDB.filled_quantity.isnot(None),
                ExecutedOrderDB.account_number == account_number  # Account-specific filter
            )
            
            results = query.all()
            
            trades = []
            for executed_order, trading_setup, timeframe, risk_reward_ratio in results:
                # Calculate PnL if not already stored
                pnl = executed_order.pnl
                if pnl is None and executed_order.filled_price and executed_order.filled_quantity:
                    # Simple PnL calculation (can be enhanced based on your actual logic)
                    pnl = executed_order.filled_quantity * (executed_order.filled_price - 
                                                          executed_order.planned_order.entry_price)
                
                trade_data = {
                    'order_id': executed_order.id,
                    'symbol': executed_order.planned_order.symbol,
                    'entry_price': executed_order.planned_order.entry_price,
                    'exit_price': executed_order.filled_price,
                    'quantity': executed_order.filled_quantity,
                    'pnl': pnl,
                    'commission': executed_order.commission or 0.0,
                    'entry_time': executed_order.planned_order.created_at,
                    'exit_time': executed_order.executed_at,
                    'trading_setup': trading_setup,
                    'timeframe': timeframe,
                    'risk_reward_ratio': risk_reward_ratio,
                    'account_number': executed_order.account_number  # Include account info
                }
                
                # Calculate PnL percentage if possible
                if (executed_order.planned_order.entry_price and 
                    executed_order.filled_price and 
                    executed_order.planned_order.entry_price > 0):
                    if executed_order.planned_order.action.value == 'BUY':
                        pnl_percentage = ((executed_order.filled_price - executed_order.planned_order.entry_price) / 
                                        executed_order.planned_order.entry_price) * 100
                    else:  # SELL
                        pnl_percentage = ((executed_order.planned_order.entry_price - executed_order.filled_price) / 
                                        executed_order.planned_order.entry_price) * 100
                    trade_data['pnl_percentage'] = pnl_percentage
                
                trades.append(trade_data)
                
            context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                f"Trades by setup query completed",
                context_provider={
                    "setup_name": setup_name,
                    "account_number": account_number,
                    "trades_found": len(trades),
                    "query_execution_time": datetime.datetime.now().isoformat()
                },
                decision_reason="TRADES_BY_SETUP_QUERY_COMPLETED"
            )
            return trades
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error getting trades for setup: {e}",
                context_provider={
                    "setup_name": setup_name,
                    "account_number": account_number,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "get_trades_by_setup"
                },
                decision_reason="TRADES_BY_SETUP_QUERY_FAILED"
            )
            return []

    def get_all_trading_setups(self, account_number: str, days_back: int = 90) -> List[str]:
        """
        Get all unique trading setups with recent trading activity for specific account.
        
        Args:
            account_number: Specific account to filter by
            days_back: Number of days to look back (default: 90 days)
            
        Returns:
            List of unique setup names
        """
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Querying all trading setups for account",
            context_provider={
                "account_number": account_number,
                "days_back": days_back,
                "cutoff_date": (datetime.datetime.now() - datetime.timedelta(days=days_back)).isoformat()
            }
        )
            
        try:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_back)
            
            query = self.db_session.query(
                PlannedOrderDB.trading_setup
            ).join(
                ExecutedOrderDB, PlannedOrderDB.id == ExecutedOrderDB.planned_order_id
            ).filter(
                ExecutedOrderDB.executed_at >= cutoff_date,
                ExecutedOrderDB.status == 'FILLED',
                ExecutedOrderDB.account_number == account_number,  # Account-specific filter
                PlannedOrderDB.trading_setup.isnot(None),
                PlannedOrderDB.trading_setup != ''
            ).distinct()
            
            results = query.all()
            setups = [result[0] for result in results if result[0]]
            
            context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                f"Trading setups query completed",
                context_provider={
                    "account_number": account_number,
                    "setups_found": len(setups),
                    "setup_names": setups,
                    "query_execution_time": datetime.datetime.now().isoformat()
                },
                decision_reason="TRADING_SETUPS_QUERY_COMPLETED"
            )
            return setups
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error getting trading setups: {e}",
                context_provider={
                    "account_number": account_number,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "get_all_trading_setups"
                },
                decision_reason="TRADING_SETUPS_QUERY_FAILED"
            )
            return []

    def get_setup_performance_summary(self, account_number: str, days_back: int = 90) -> Dict[str, Dict]:
        """
        Get performance summary for all trading setups for specific account.
        
        Args:
            account_number: Specific account to filter by
            days_back: Number of days to look back
            
        Returns:
            Dictionary with setup names as keys and performance metrics as values
        """
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            f"Generating setup performance summary",
            context_provider={
                "account_number": account_number,
                "days_back": days_back
            }
        )
            
        try:
            setups = self.get_all_trading_setups(account_number, days_back)
            performance_summary = {}
            
            for setup in setups:
                trades = self.get_trades_by_setup(setup, account_number, days_back)
                if trades:
                    performance = self._calculate_setup_performance(trades)
                    performance_summary[setup] = performance
                    
            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                f"Setup performance summary generated",
                context_provider={
                    "account_number": account_number,
                    "setups_analyzed": len(performance_summary),
                    "setup_names": list(performance_summary.keys()),
                    "summary_generation_time": datetime.datetime.now().isoformat()
                },
                decision_reason="SETUP_PERFORMANCE_SUMMARY_GENERATED"
            )
            return performance_summary
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error generating setup performance summary: {e}",
                context_provider={
                    "account_number": account_number,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "get_setup_performance_summary"
                },
                decision_reason="SETUP_PERFORMANCE_SUMMARY_FAILED"
            )
            return {}
    # <Advanced Feature Integration - End>

    # <Database to Domain Conversion - Begin>
    def convert_to_planned_order(self, db_order: PlannedOrderDB) -> PlannedOrder:
        """
        Convert a PlannedOrderDB entity back to a PlannedOrder domain object.
        
        Args:
            db_order: Database order entity
            
        Returns:
            PlannedOrder domain object
        """
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            f"Converting DB order to PlannedOrder",
            symbol=db_order.symbol,
            context_provider={
                "db_order_id": db_order.id,
                "symbol": db_order.symbol,
                "current_status": db_order.status,
                "position_strategy_id": db_order.position_strategy_id
            }
        )
            
        try:
            # Convert string values back to enums using proper enum lookup
            action = self._string_to_enum(Action, db_order.action)
            order_type = self._string_to_enum(OrderType, db_order.order_type)
            security_type = self._string_to_enum(SecurityType, db_order.security_type)
            
            # Get position strategy from database - use SQLAlchemy model
            position_strategy_entity = self.db_session.query(PositionStrategy).filter_by(id=db_order.position_strategy_id).first()
            position_strategy = self._string_to_enum(PositionStrategyEnum, position_strategy_entity.name) if position_strategy_entity else PositionStrategyEnum.CORE
            
            # Create PlannedOrder with all required fields
            planned_order = PlannedOrder(
                symbol=db_order.symbol,
                security_type=security_type,
                action=action,
                order_type=order_type,
                entry_price=db_order.entry_price,
                stop_loss=db_order.stop_loss,
                risk_per_trade=db_order.risk_per_trade,
                risk_reward_ratio=db_order.risk_reward_ratio,
                priority=db_order.priority,
                position_strategy=position_strategy,
                overall_trend=db_order.overall_trend,
                brief_analysis=db_order.brief_analysis,
                exchange=getattr(db_order, 'exchange', 'SMART'),
                currency=getattr(db_order, 'currency', 'USD')
            )
            
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                f"DB order converted to PlannedOrder successfully",
                symbol=db_order.symbol,
                context_provider={
                    "conversion_successful": True,
                    "action_converted": action.value,
                    "order_type_converted": order_type.value,
                    "security_type_converted": security_type.value
                },
                decision_reason="DB_TO_DOMAIN_CONVERSION_COMPLETED"
            )
            return planned_order
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Failed to convert DB order to PlannedOrder: {e}",
                symbol=db_order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "db_order_id": db_order.id,
                    "operation": "convert_to_planned_order"
                },
                decision_reason="DB_TO_DOMAIN_CONVERSION_FAILED"
            )
            raise

    def _string_to_enum(self, enum_class, value: str):
        """Convert string value to enum member, handling case variations and spaces."""
        if value is None:
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                f"Using default enum member for None value",
                context_provider={
                    "enum_class": enum_class.__name__,
                    "action_taken": "used_first_member_as_default"
                }
            )
            return enum_class(list(enum_class)[0])  # Return first enum member as default
            
        # Clean up the string value
        clean_value = str(value).strip().upper().replace(' ', '_')
        
        # Try direct match first
        try:
            return enum_class[clean_value]
        except KeyError:
            pass
            
        # Try value-based lookup
        for member in enum_class:
            if member.value.upper() == clean_value or member.name.upper() == clean_value:
                return member
                
        # Fallback to first member
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            f"Could not map value to enum, using default",
            context_provider={
                "enum_class": enum_class.__name__,
                "input_value": value,
                "clean_value": clean_value,
                "action_taken": "used_first_member_as_fallback"
            },
            decision_reason="ENUM_MAPPING_FALLBACK"
        )
        return enum_class(list(enum_class)[0])
    # <Database to Domain Conversion - End>