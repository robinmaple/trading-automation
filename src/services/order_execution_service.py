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
# Phase B Additions - Begin
from src.core.models import OrderAttemptDB
# Phase B Additions - End

# <Context-Aware Logger Integration - Begin>
from src.core.context_aware_logger import get_context_logger, TradingEventType
# <Context-Aware Logger Integration - End>


class OrderExecutionService:
    """Encapsulates all logic for executing orders and interacting with the broker."""

    def __init__(self, trading_manager, ibkr_client):
        """Initialize the service with references to the trading manager and IBKR client."""
        # <Context-Aware Logger Initialization - Begin>
        self.context_logger = get_context_logger()
        # <Context-Aware Logger Initialization - End>
        
        # <Context-Aware Logging - Service Initialization Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "OrderExecutionService initialization starting",
            context_provider={
                "trading_manager_provided": trading_manager is not None,
                "ibkr_client_provided": ibkr_client is not None,
                "ibkr_client_connected": ibkr_client.connected if ibkr_client else False
            }
        )
        # <Context-Aware Logging - Service Initialization Start - End>
        
        self._trading_manager = trading_manager
        self._ibkr_client = ibkr_client
        self.order_persistence = None
        self.active_orders = None
        
        # <Context-Aware Logging - Service Initialization Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "OrderExecutionService initialization completed",
            context_provider={
                "dependencies_set": False,
                "ready_for_operations": True
            }
        )
        # <Context-Aware Logging - Service Initialization Complete - End>

    def set_dependencies(self, order_persistence, active_orders) -> None:
        """Inject dependencies for order execution and tracking."""
        # <Context-Aware Logging - Dependencies Set - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Setting OrderExecutionService dependencies",
            context_provider={
                "order_persistence_provided": order_persistence is not None,
                "active_orders_provided": active_orders is not None
            }
        )
        # <Context-Aware Logging - Dependencies Set - End>
        
        self.order_persistence = order_persistence
        self.active_orders = active_orders
        
        # <Context-Aware Logging - Dependencies Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "OrderExecutionService dependencies set successfully",
            context_provider={
                "dependencies_set": True,
                "ready_for_operations": True
            }
        )
        # <Context-Aware Logging - Dependencies Complete - End>

 # === NEW VALIDATION METHODS ===
    def _validate_order_basic(self, order) -> tuple[bool, str]:
        """Layer 3a: Basic field validation as safety net before execution.

        Accepts Action enums (Action.BUY / Action.SELL), plain strings ('BUY'), or objects
        whose string representation yields 'BUY'/'SELL'. Returns (bool, message).
        """
        # <Context-Aware Logging - Basic Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting basic order validation",
            symbol=getattr(order, 'symbol', 'UNKNOWN'),
            context_provider={
                "validation_layer": "basic_fields",
                "symbol_provided": hasattr(order, 'symbol'),
                "entry_price_provided": hasattr(order, 'entry_price'),
                "action_provided": hasattr(order, 'action')
            }
        )
        # <Context-Aware Logging - Basic Validation Start - End>
        
        try:
            # Symbol validation
            symbol_str = ""
            try:
                symbol_str = str(order.symbol).strip()
            except Exception:
                symbol_str = ""
            if not symbol_str or symbol_str in ['', '0', 'nan', 'None', 'null']:
                validation_error = f"Invalid symbol: '{order.symbol}'"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Basic validation failed - invalid symbol",
                    symbol=getattr(order, 'symbol', 'UNKNOWN'),
                    context_provider={
                        "symbol_value": str(order.symbol),
                        "symbol_cleaned": symbol_str
                    },
                    decision_reason=validation_error
                )
                return False, validation_error

            # Price validation
            if not hasattr(order, "entry_price") or order.entry_price is None or order.entry_price <= 0:
                validation_error = f"Invalid entry price: {getattr(order, 'entry_price', None)}"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Basic validation failed - invalid entry price",
                    symbol=order.symbol,
                    context_provider={
                        "entry_price_value": getattr(order, 'entry_price', None)
                    },
                    decision_reason=validation_error
                )
                return False, validation_error

            # Stop loss validation (basic syntax)
            if getattr(order, "stop_loss", None) is not None and order.stop_loss <= 0:
                validation_error = f"Invalid stop loss price: {order.stop_loss}"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Basic validation failed - invalid stop loss",
                    symbol=order.symbol,
                    context_provider={
                        "stop_loss_value": order.stop_loss
                    },
                    decision_reason=validation_error
                )
                return False, validation_error

            # Action validation - accept enums or strings
            action_val = None
            try:
                # Try common enum attributes first
                action_val = getattr(order.action, "value", None) or getattr(order.action, "name", None)
            except Exception:
                action_val = None

            if action_val is None:
                # Fallback to string representation
                try:
                    action_val = str(order.action)
                except Exception:
                    action_val = ""

            action_str = str(action_val).upper().strip()
            if action_str not in ("BUY", "SELL"):
                validation_error = f"Invalid action: {order.action}"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Basic validation failed - invalid action",
                    symbol=order.symbol,
                    context_provider={
                        "action_value": str(order.action),
                        "action_cleaned": action_str
                    },
                    decision_reason=validation_error
                )
                return False, validation_error

            # <Context-Aware Logging - Basic Validation Success - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Basic order validation passed",
                symbol=order.symbol,
                context_provider={
                    "symbol_valid": True,
                    "entry_price_valid": True,
                    "action_valid": True
                },
                decision_reason="All basic validation checks passed"
            )
            # <Context-Aware Logging - Basic Validation Success - End>
            
            return True, "Basic validation passed"

        except Exception as e:
            validation_error = f"Basic validation error: {e}"
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Basic validation failed with exception",
                symbol=getattr(order, 'symbol', 'UNKNOWN'),
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason=validation_error
            )
            return False, validation_error

    def _validate_market_data_available(self, order) -> tuple[bool, str]:
        """Layer 3b: Validate market data availability for the symbol."""
        # <Context-Aware Logging - Market Data Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting market data validation",
            symbol=order.symbol,
            context_provider={
                "validation_layer": "market_data"
            }
        )
        # <Context-Aware Logging - Market Data Validation Start - End>
        
        try:
            if not hasattr(self._trading_manager, 'data_feed'):
                validation_error = "Data feed not available"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Market data validation failed - no data feed",
                    symbol=order.symbol,
                    context_provider={
                        "data_feed_available": False
                    },
                    decision_reason=validation_error
                )
                return False, validation_error
                
            if not self._trading_manager.data_feed.is_connected():
                validation_error = "Data feed not connected"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Market data validation failed - data feed disconnected",
                    symbol=order.symbol,
                    context_provider={
                        "data_feed_connected": False
                    },
                    decision_reason=validation_error
                )
                return False, validation_error
                
            current_price = self._trading_manager.data_feed.get_current_price(order.symbol)
            if current_price is None or current_price <= 0:
                validation_error = f"No market data available for {order.symbol}"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Market data validation failed - no price data",
                    symbol=order.symbol,
                    context_provider={
                        "current_price": current_price,
                        "price_available": current_price is not None
                    },
                    decision_reason=validation_error
                )
                return False, validation_error
            
            # <Context-Aware Logging - Market Data Validation Success - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Market data validation passed",
                symbol=order.symbol,
                context_provider={
                    "data_feed_available": True,
                    "data_feed_connected": True,
                    "current_price": current_price
                },
                decision_reason="Market data available and valid"
            )
            # <Context-Aware Logging - Market Data Validation Success - End>
                
            return True, "Market data available"
            
        except Exception as e:
            validation_error = f"Market data validation error: {e}"
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Market data validation failed with exception",
                symbol=order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason=validation_error
            )
            return False, validation_error

    def _validate_broker_connection(self) -> tuple[bool, str]:
        """Layer 3c: Validate broker connection status."""
        # <Context-Aware Logging - Broker Connection Check - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Checking broker connection status",
            context_provider={
                "ibkr_client_available": self._ibkr_client is not None,
                "ibkr_client_connected": self._ibkr_client.connected if self._ibkr_client else False
            }
        )
        # <Context-Aware Logging - Broker Connection Check - End>
        
        if self._ibkr_client and self._ibkr_client.connected:
            return True, "Broker connected"
        
        validation_error = "Broker not connected"
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Broker connection validation failed",
            context_provider={
                "ibkr_client_available": self._ibkr_client is not None,
                "ibkr_client_connected": False
            },
            decision_reason=validation_error
        )
        return False, validation_error

    def _validate_execution_conditions(self, order, quantity, total_capital) -> tuple[bool, str]:
        """Layer 3: Comprehensive pre-execution validation."""
        # <Context-Aware Logging - Execution Conditions Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting comprehensive execution conditions validation",
            symbol=order.symbol,
            context_provider={
                "validation_layer": "execution_conditions",
                "quantity": quantity,
                "total_capital": total_capital
            }
        )
        # <Context-Aware Logging - Execution Conditions Validation Start - End>
        
        try:
            # Basic field validation (safety net)
            basic_valid, basic_message = self._validate_order_basic(order)
            if not basic_valid:
                validation_error = f"Basic validation failed: {basic_message}"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Execution conditions validation failed - basic validation",
                    symbol=order.symbol,
                    context_provider={
                        "basic_validation_passed": False,
                        "basic_validation_message": basic_message
                    },
                    decision_reason=validation_error
                )
                return False, validation_error
                
            # Market data availability
            market_valid, market_message = self._validate_market_data_available(order)
            if not market_valid:
                validation_error = f"Market data issue: {market_message}"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Execution conditions validation failed - market data",
                    symbol=order.symbol,
                    context_provider={
                        "market_data_validation_passed": False,
                        "market_data_message": market_message
                    },
                    decision_reason=validation_error
                )
                return False, validation_error
                
            # Broker connection
            broker_valid, broker_message = self._validate_broker_connection()
            if not broker_valid:
                validation_error = f"Broker issue: {broker_message}"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Execution conditions validation failed - broker connection",
                    symbol=order.symbol,
                    context_provider={
                        "broker_validation_passed": False,
                        "broker_message": broker_message
                    },
                    decision_reason=validation_error
                )
                return False, validation_error
                
            # Margin validation (existing)
            margin_valid, margin_message = self._validate_order_margin(order, quantity, total_capital)
            if not margin_valid:
                validation_error = f"Margin validation failed: {margin_message}"
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Execution conditions validation failed - margin validation",
                    symbol=order.symbol,
                    context_provider={
                        "margin_validation_passed": False,
                        "margin_message": margin_message
                    },
                    decision_reason=validation_error
                )
                return False, validation_error
            
            # <Context-Aware Logging - Execution Conditions Success - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Execution conditions validation passed",
                symbol=order.symbol,
                context_provider={
                    "basic_validation_passed": True,
                    "market_data_validation_passed": True,
                    "broker_validation_passed": True,
                    "margin_validation_passed": True
                },
                decision_reason="All execution conditions met"
            )
            # <Context-Aware Logging - Execution Conditions Success - End>
                
            return True, "All execution conditions met"
            
        except Exception as e:
            validation_error = f"Execution validation error: {e}"
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Execution conditions validation failed with exception",
                symbol=order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason=validation_error
            )
            return False, validation_error
    # === END NEW VALIDATION METHODS ===

    # Phase B Additions - Begin
    def _record_order_attempt(self, planned_order, attempt_type, fill_probability=None,
                            effective_priority=None, quantity=None, capital_commitment=None,
                            status=None, ib_order_ids=None, details=None,
                            account_number: Optional[str] = None):
        """
        Record an order attempt to the database for Phase B tracking.
        
        Args:
            planned_order: The planned order being attempted
            attempt_type: Type of attempt ('PLACEMENT', 'CANCELLATION', 'REPLACEMENT')
            fill_probability: Fill probability at time of attempt
            effective_priority: Effective priority score
            quantity: Quantity attempted
            capital_commitment: Capital commitment for the order
            status: Status of the attempt
            ib_order_ids: IBKR order IDs if available
            details: Additional details or error messages
            account_number: Account number for the attempt
        """
        # <Context-Aware Logging - Order Attempt Recording Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Recording order attempt",
            symbol=planned_order.symbol,
            context_provider={
                "attempt_type": attempt_type,
                "status": status,
                "fill_probability": fill_probability,
                "account_number": account_number,
                "ib_order_ids_provided": ib_order_ids is not None
            }
        )
        # <Context-Aware Logging - Order Attempt Recording Start - End>
        
        if not self.order_persistence or not hasattr(self.order_persistence, 'db_session'):
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Order attempt recording failed - persistence not available",
                symbol=planned_order.symbol,
                context_provider={
                    "order_persistence_available": self.order_persistence is not None,
                    "db_session_available": hasattr(self.order_persistence, 'db_session') if self.order_persistence else False
                },
                decision_reason="Cannot record order attempt - persistence service not available"
            )
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
                account_number=account_number  # Store account number
            )
            
            self.order_persistence.db_session.add(attempt)
            self.order_persistence.db_session.commit()
            
            # <Context-Aware Logging - Order Attempt Recording Success - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order attempt recorded successfully",
                symbol=planned_order.symbol,
                context_provider={
                    "attempt_id": attempt.id,
                    "attempt_type": attempt_type,
                    "status": status
                },
                decision_reason=f"Order attempt {attempt.id} recorded in database"
            )
            # <Context-Aware Logging - Order Attempt Recording Success - End>
            
            return attempt.id
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Order attempt recording failed",
                symbol=planned_order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "attempt_type": attempt_type
                },
                decision_reason=f"Failed to record order attempt: {e}"
            )
            print(f"❌ Failed to record order attempt: {e}")
            return None
    # Phase B Additions - End

    # Account Context Integration - Begin
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
        # <Context-Aware Logging - Place Order Wrapper - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Place order method called",
            symbol=planned_order.symbol,
            context_provider={
                "fill_probability": fill_probability,
                "effective_priority": effective_priority,
                "is_live_trading": is_live_trading,
                "account_number": account_number
            }
        )
        # <Context-Aware Logging - Place Order Wrapper - End>
        
        return self.execute_single_order(
            planned_order,
            fill_probability,
            effective_priority,
            total_capital,
            quantity,
            capital_commitment,
            is_live_trading,
            account_number  # Pass account number
        )
    # Account Context Integration - End

    def _validate_order_margin(self, order, quantity, total_capital) -> tuple[bool, str]:
        """Validate if the order has sufficient margin before execution."""
        # <Context-Aware Logging - Margin Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting margin validation",
            symbol=order.symbol,
            context_provider={
                "validation_layer": "margin",
                "quantity": quantity,
                "total_capital": total_capital
            }
        )
        # <Context-Aware Logging - Margin Validation Start - End>
        
        try:
            is_valid, message = self.order_persistence.validate_sufficient_margin(
                order.symbol, quantity, order.entry_price
            )
            if not is_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Margin validation failed",
                    symbol=order.symbol,
                    context_provider={
                        "margin_validation_passed": False,
                        "margin_message": message
                    },
                    decision_reason=f"Order rejected due to margin: {message}"
                )
                print(f"❌ Order rejected due to margin: {message}")
                return False, message
            
            # <Context-Aware Logging - Margin Validation Success - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Margin validation passed",
                symbol=order.symbol,
                context_provider={
                    "margin_validation_passed": True
                },
                decision_reason="Margin validation passed"
            )
            # <Context-Aware Logging - Margin Validation Success - End>
            
            return True, "Margin validation passed"
        except Exception as e:
            validation_error = f"Margin validation error: {e}"
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Margin validation failed with exception",
                symbol=order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason=validation_error
            )
            return False, validation_error

    # Account Context Integration - Begin
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
        """
        Execute a single order while incorporating fill probability into ActiveOrder tracking.
        Phase B: Supports unified execution record for entry/SL/PT.
        """
        # <Context-Aware Logging - Single Order Execution Start - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Starting single order execution",
            symbol=order.symbol,
            context_provider={
                "fill_probability": fill_probability,
                "effective_priority": effective_priority,
                "is_live_trading": is_live_trading,
                "account_number": account_number,
                "quantity": quantity,
                "capital_commitment": capital_commitment
            }
        )
        # <Context-Aware Logging - Single Order Execution Start - End>

    # === NEW VALIDATION - ADD THIS BLOCK ===
        exec_valid, exec_message = self._validate_execution_conditions(order, quantity, total_capital)
        if not exec_valid:
            # <Context-Aware Logging - Execution Validation Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Order execution rejected - validation failed",
                symbol=order.symbol,
                context_provider={
                    "validation_message": exec_message,
                    "execution_path": "REJECTED"
                },
                decision_reason=f"Order execution rejected: {exec_message}"
            )
            # <Context-Aware Logging - Execution Validation Failed - End>
            
            print(f"❌ Order execution rejected: {exec_message}")
            
            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                self.order_persistence.handle_order_rejection(db_id, exec_message)
            else:
                print(f"❌ Cannot mark order as canceled: Database ID not found for {order.symbol}")
            
            # Record failed attempt due to validation
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'FAILED', None, exec_message,
                account_number
            )
            return False
    # === END NEW VALIDATION BLOCK ===

        margin_valid, margin_message = self._validate_order_margin(order, quantity, total_capital)
        if not margin_valid:
            # <Context-Aware Logging - Margin Validation Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Order execution rejected - margin validation failed",
                symbol=order.symbol,
                context_provider={
                    "margin_message": margin_message,
                    "execution_path": "REJECTED"
                },
                decision_reason=f"Order execution rejected: {margin_message}"
            )
            # <Context-Aware Logging - Margin Validation Failed - End>
            
            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                self.order_persistence.handle_order_rejection(db_id, margin_message)
            else:
                print(f"❌ Cannot mark order as canceled: Database ID not found for {order.symbol}")
            
            
            # Phase B Additions - Begin
            # Record failed attempt due to margin validation
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'FAILED', None, margin_message,
                account_number  # Pass account number
            )
            # Phase B Additions - End
            return False

        ibkr_connected = self._ibkr_client and self._ibkr_client.connected

        # === LIVE ORDER PATH ===
        if ibkr_connected:
            # <Context-Aware Logging - Live Execution Path Selected - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Live order execution path selected",
                symbol=order.symbol,
                context_provider={
                    "execution_path": "LIVE",
                    "fill_probability": fill_probability,
                    "ibkr_connected": True
                }
            )
            # <Context-Aware Logging - Live Execution Path Selected - End>
            
            print(f"   Taking LIVE order execution path... FillProb={fill_probability:.3f}")
            
            # Phase B Additions - Begin
            # Record placement attempt before execution
            attempt_id = self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'SUBMITTING', None, None,
                account_number  # Pass account number
            )
            # Phase B Additions - End
            
            contract = order.to_ib_contract()
            order_ids = self._ibkr_client.place_bracket_order(
                contract,
                order.action.value,
                order.order_type.value,
                order.security_type.value,
                order.entry_price,
                order.stop_loss,
                order.risk_per_trade,
                order.risk_reward_ratio,
                total_capital
            )

            if not order_ids:
                # <Context-Aware Logging - Live Execution Failed - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Live order execution failed - no order IDs returned",
                    symbol=order.symbol,
                    context_provider={
                        "execution_path": "LIVE_FAILED",
                        "order_ids_returned": False
                    },
                    decision_reason="IBKR order placement failed - no order IDs returned"
                )
                # <Context-Aware Logging - Live Execution Failed - End>
                
                print("❌ Failed to place real order through IBKR")
                rejection_reason = "IBKR order placement failed - no order IDs returned"
                db_id = self._trading_manager._find_planned_order_db_id(order)
                if db_id:
                    self.order_persistence.handle_order_rejection(db_id, rejection_reason)
                
                # Phase B Additions - Begin
                # Update attempt with failure
                self._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'FAILED', None, rejection_reason,
                    account_number  # Pass account number
                )
                # Phase B Additions - End
                return False

            # Account Context Integration - Begin
            # Pass account number to persistence service
            execution_id = self.order_persistence.record_order_execution(
                order,
                order.entry_price,
                quantity,
                account_number,  # Pass account number
                status='SUBMITTED',
                is_live_trading=is_live_trading
            )
            # Account Context Integration - End

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
                    fill_probability=fill_probability
                )
                self.active_orders[order_ids[0]] = active_order
            else:
                print("⚠️  Could not create ActiveOrder - database ID not found")
            
            # Phase B Additions - Begin
            # Update attempt with success and order IDs
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'SUBMITTED', order_ids, None,
                account_number  # Pass account number
            )
            # Phase B Additions - End
            
            # <Context-Aware Logging - Live Execution Success - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Live order execution successful",
                symbol=order.symbol,
                context_provider={
                    "execution_path": "LIVE_SUCCESS",
                    "order_ids": order_ids,
                    "account_number": account_number,
                    "active_order_created": db_id is not None
                },
                decision_reason=f"Real order placed with IDs {order_ids}"
            )
            # <Context-Aware Logging - Live Execution Success - End>
            
            print(f"✅ REAL ORDER PLACED: Order IDs {order_ids} sent to IBKR (Account: {account_number})")
            return True

        # === SIMULATION PATH ===
        else:
            # <Context-Aware Logging - Simulation Path Selected - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Simulation order execution path selected",
                symbol=order.symbol,
                context_provider={
                    "execution_path": "SIMULATION",
                    "fill_probability": fill_probability,
                    "ibkr_connected": False
                }
            )
            # <Context-Aware Logging - Simulation Path Selected - End>
            
            print(f"   Taking SIMULATION order execution path... FillProb={fill_probability:.3f}")
            
            # Phase B Additions - Begin
            # Record simulation attempt
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'SIMULATION', None, None,
                account_number  # Pass account number
            )
            # Phase B Additions - End
            
            update_success = self.order_persistence.update_order_status(order, 'FILLED')
            
            # Account Context Integration - Begin
            # Pass account number to persistence service
            execution_id = self.order_persistence.record_order_execution(
                order,
                order.entry_price,
                quantity,
                account_number,  # Pass account number
                status='FILLED',
                is_live_trading=is_live_trading
            )
            # Account Context Integration - End

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
                    fill_probability=fill_probability
                )
                self.active_orders[active_order.order_ids[0]] = active_order
            
            # <Context-Aware Logging - Simulation Execution Success - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Simulation order execution successful",
                symbol=order.symbol,
                context_provider={
                    "execution_path": "SIMULATION_SUCCESS",
                    "simulation_order_id": f"SIM-{db_id}",
                    "account_number": account_number,
                    "active_order_created": db_id is not None
                },
                decision_reason="Simulation order executed successfully"
            )
            # <Context-Aware Logging - Simulation Execution Success - End>
            
            return True
    # Account Context Integration - End

    def cancel_order(self, order_id) -> bool:
        """Cancel a working order by delegating to the trading manager's logic."""
        # <Context-Aware Logging - Order Cancellation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting order cancellation",
            context_provider={
                "order_id": order_id
            }
        )
        # <Context-Aware Logging - Order Cancellation Start - End>
        
        # Phase B Additions - Begin
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
        # Phase B Additions - End
        
        success = self._trading_manager._cancel_single_order(order_id)
        
        # Phase B Additions - Begin
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
        # Phase B Additions - End
        
        # <Context-Aware Logging - Order Cancellation Result - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Order cancellation completed",
            context_provider={
                "order_id": order_id,
                "cancellation_success": success,
                "active_order_found": active_order is not None,
                "symbol": active_order.symbol if active_order else "UNKNOWN"
            },
            decision_reason=f"Order cancellation {'succeeded' if success else 'failed'}"
        )
        # <Context-Aware Logging - Order Cancellation Result - End>
        
        return success

    def close_position(self, position_data: Dict, account_number: Optional[str] = None) -> Optional[int]:
        """Close an open position by placing a market order through IBKR."""
        # <Context-Aware Logging - Position Close Start - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Starting position close",
            symbol=position_data['symbol'],
            context_provider={
                "action": position_data['action'],
                "quantity": position_data['quantity'],
                "security_type": position_data['security_type'],
                "account_number": account_number,
                "ibkr_connected": self._ibkr_client.connected if self._ibkr_client else False
            }
        )
        # <Context-Aware Logging - Position Close Start - End>
        
        if not self._ibkr_client or not self._ibkr_client.connected:
            # <Context-Aware Logging - Simulation Position Close - Begin>
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                "Simulation position close",
                symbol=position_data['symbol'],
                context_provider={
                    "execution_path": "SIMULATION",
                    "account_number": account_number
                },
                decision_reason="IBKR not connected - simulation mode"
            )
            # <Context-Aware Logging - Simulation Position Close - End>
            
            print(f"✅ Simulation: Would close {position_data['symbol']} position (Account: {account_number})")
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

            # <Context-Aware Logging - Live Position Close Success - Begin>
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                "Live position close successful",
                symbol=position_data['symbol'],
                context_provider={
                    "execution_path": "LIVE",
                    "order_id": order_id,
                    "account_number": account_number
                },
                decision_reason=f"Closing market order placed for {position_data['symbol']}"
            )
            # <Context-Aware Logging - Live Position Close Success - End>
            
            print(f"✅ Closing market order placed for {position_data['symbol']} (ID: {order_id}, Account: {account_number})")
            return order_id
        except Exception as e:
            # <Context-Aware Logging - Position Close Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Position close failed",
                symbol=position_data['symbol'],
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "account_number": account_number
                },
                decision_reason=f"Failed to close position {position_data['symbol']}: {e}"
            )
            # <Context-Aware Logging - Position Close Error - End>
            
            print(f"❌ Failed to close position {position_data['symbol']}: {e}")
            return None

    def cancel_orders_for_symbol(self, symbol: str) -> bool:
        """Cancel all active open orders for a specific symbol."""
        # <Context-Aware Logging - Symbol Orders Cancellation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting cancellation of all orders for symbol",
            symbol=symbol,
            context_provider={
                "ibkr_connected": self._ibkr_client.connected if self._ibkr_client else False
            }
        )
        # <Context-Aware Logging - Symbol Orders Cancellation Start - End>
        
        if not self._ibkr_client or not self._ibkr_client.connected:
            # <Context-Aware Logging - Simulation Symbol Cancellation - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Simulation cancellation of symbol orders",
                symbol=symbol,
                context_provider={
                    "execution_path": "SIMULATION"
                },
                decision_reason="IBKR not connected - simulation mode"
            )
            # <Context-Aware Logging - Simulation Symbol Cancellation - End>
            
            print(f"✅ Simulation: Would cancel orders for {symbol}")
            return True
        try:
            orders = self._ibkr_client.get_open_orders()
            symbol_orders = [
                o for o in orders
                if o.symbol == symbol and o.status in ['Submitted', 'PreSubmitted', 'PendingSubmit']
            ]
            if not symbol_orders:
                # <Context-Aware Logging - No Active Orders Found - Begin>
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "No active orders found to cancel for symbol",
                    symbol=symbol,
                    context_provider={
                        "orders_found": 0
                    },
                    decision_reason=f"No active orders found for {symbol}"
                )
                # <Context-Aware Logging - No Active Orders Found - End>
                
                print(f"ℹ️  No active orders found to cancel for {symbol}")
                return True

            success = True
            for order in symbol_orders:
                print(f"❌ Cancelling order {order.order_id} for {symbol}")
                if not self._ibkr_client.cancel_order(order.order_id):
                    success = False
                    print(f"⚠️  Failed to cancel order {order.order_id}")
            
            # <Context-Aware Logging - Symbol Orders Cancellation Result - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Symbol orders cancellation completed",
                symbol=symbol,
                context_provider={
                    "orders_cancelled": len(symbol_orders),
                    "cancellation_success": success,
                    "execution_path": "LIVE"
                },
                decision_reason=f"Cancelled {len(symbol_orders)} orders for {symbol}"
            )
            # <Context-Aware Logging - Symbol Orders Cancellation Result - End>
            
            return success
        except Exception as e:
            # <Context-Aware Logging - Symbol Orders Cancellation Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Symbol orders cancellation failed",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason=f"Error cancelling orders for {symbol}: {e}"
            )
            # <Context-Aware Logging - Symbol Orders Cancellation Error - End>
            
            print(f"❌ Error cancelling orders for {symbol}: {e}")
            return False

    def find_orders_by_symbol(self, symbol: str) -> List[Any]:
        """Find all open orders for a specific symbol from IBKR."""
        # <Context-Aware Logging - Find Orders by Symbol Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Finding orders by symbol",
            symbol=symbol,
            context_provider={
                "ibkr_connected": self._ibkr_client.connected if self._ibkr_client else False
            }
        )
        # <Context-Aware Logging - Find Orders by Symbol Start - End>
        
        if not self._ibkr_client or not self._ibkr_client.connected:
            # <Context-Aware Logging - Simulation Find Orders - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Simulation find orders by symbol",
                symbol=symbol,
                context_provider={
                    "execution_path": "SIMULATION"
                },
                decision_reason="IBKR not connected - simulation mode"
            )
            # <Context-Aware Logging - Simulation Find Orders - End>
            
            print(f"✅ Simulation: Would find orders for {symbol}")
            return []
        try:
            orders = self._ibkr_client.get_open_orders()
            found_orders = [o for o in orders if o.symbol == symbol]
            
            # <Context-Aware Logging - Find Orders Success - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Find orders by symbol completed",
                symbol=symbol,
                context_provider={
                    "orders_found": len(found_orders),
                    "execution_path": "LIVE"
                },
                decision_reason=f"Found {len(found_orders)} orders for {symbol}"
            )
            # <Context-Aware Logging - Find Orders Success - End>
            
            return found_orders
        except Exception as e:
            # <Context-Aware Logging - Find Orders Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Find orders by symbol failed",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason=f"Error finding orders for {symbol}: {e}"
            )
            # <Context-Aware Logging - Find Orders Error - End>
            
            print(f"❌ Error finding orders for {symbol}: {e}")
            return []