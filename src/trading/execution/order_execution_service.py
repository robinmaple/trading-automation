"""
Service responsible for all order execution interactions with the brokerage client.
Handles placement, cancellation, monitoring of orders, validation, live/simulation mode,
and persistence of execution results, including Phase B: fill probability and unified execution tracking.

REFACTORED: Now uses extracted services for better separation of concerns.
"""

import datetime
from typing import Any, Dict, Optional, List
from ibapi.contract import Contract
from ibapi.order import Order

from src.trading.orders.planned_order import ActiveOrder
from src.trading.execution.services.execution_validator import ExecutionValidator
from src.trading.execution.services.bracket_order_executor import BracketOrderExecutor
from src.trading.execution.services.duplicate_detector import DuplicateDetector
from src.trading.execution.services.price_adjustment_service import PriceAdjustmentService
from src.trading.execution.services.execution_attempt_tracker import ExecutionAttemptTracker
from src.trading.execution.services.rollback_manager import RollbackManager
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
        
        # Initialize extracted services
        self._validator = ExecutionValidator(trading_manager, ibkr_client, None)  # order_persistence will be set later
        self._bracket_executor = BracketOrderExecutor(ibkr_client)
        self._duplicate_detector = DuplicateDetector(ibkr_client)
        self._price_service = PriceAdjustmentService(trading_manager)
        self._attempt_tracker = ExecutionAttemptTracker(None, trading_manager)  # order_persistence will be set later
        self._rollback_manager = RollbackManager(ibkr_client)
        
        # Minimal initialization logging
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Order execution service ready",
            context_provider={
                "ibkr_connected": ibkr_client.connected if ibkr_client else False,
                "refactored": True,
                "extracted_services_initialized": True
            }
        )

    def set_dependencies(self, order_persistence, active_orders) -> None:
        """Inject dependencies for order execution and tracking."""
        self.order_persistence = order_persistence
        self.active_orders = active_orders
        
        # Update services with dependencies
        self._validator._order_persistence = order_persistence
        self._attempt_tracker.order_persistence = order_persistence
        self._attempt_tracker.active_orders = active_orders

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
            self._attempt_tracker._record_order_attempt(
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
            self._attempt_tracker._record_order_attempt(
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
        
        REFACTORED: Now uses extracted services for cleaner implementation.
        """
        
        ibkr_order_ids = None
        attempt_id = None
        
        try:
            # Enhanced diagnostic logging for bracket orders with price adjustment support
            is_bracket_order = hasattr(order, 'order_type') and getattr(order, 'order_type') is not None
            current_market_price = self._price_service._get_current_market_price_for_order(order)
            
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
                    "transmission_verification_enabled": True,
                    "refactored": True
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
                    "Executing order with transmission verification (REFACTORED)",
                    symbol=order.symbol,
                    context_provider=adjustment_context
                )

            # Enhanced execution conditions validation with price adjustment support
            exec_valid, exec_message = self._validator._validate_execution_conditions(order, quantity, total_capital)
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
                        "transmission_verification_skipped": True,
                        "refactored": True
                    },
                    decision_reason=f"Execution validation failed: {exec_message}"
                )
                
                db_id = self._trading_manager._find_planned_order_db_id(order)
                if db_id:
                    self.order_persistence.handle_order_rejection(db_id, exec_message)
                
                self._attempt_tracker._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'FAILED', None, exec_message,
                    account_number
                )
                return False

            margin_valid, margin_message = self._validator._validate_order_margin(order, quantity, total_capital)
            if not margin_valid:
                db_id = self._trading_manager._find_planned_order_db_id(order)
                if db_id:
                    self.order_persistence.handle_order_rejection(db_id, margin_message)
                
                self._attempt_tracker._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'FAILED', None, margin_message,
                    account_number
                )
                return False

            ibkr_connected = self._ibkr_client and self._ibkr_client.connected

            # === DUPLICATION PREVENTION CHECK ===
            if ibkr_connected:
                # Check for duplicate active orders
                if self._duplicate_detector._is_duplicate_order_active(order, account_number):
                    rejection_reason = "Duplicate order prevention - similar order already active in IBKR"
                    db_id = self._trading_manager._find_planned_order_db_id(order)
                    if db_id:
                        self.order_persistence.handle_order_rejection(db_id, rejection_reason)
                    
                    self._attempt_tracker._record_order_attempt(
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
                            "transmission_verification_skipped": True,
                            "refactored": True
                        },
                        decision_reason="DUPLICATE_ORDER_BLOCKED"
                    )
                    return False

                # Check for rapid retries
                if self._attempt_tracker._has_recent_execution_attempt(order):
                    rejection_reason = "Rapid retry prevention - recent execution attempt detected"
                    db_id = self._trading_manager._find_planned_order_db_id(order)
                    if db_id:
                        self.order_persistence.handle_order_rejection(db_id, rejection_reason)
                    
                    self._attempt_tracker._record_order_attempt(
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
                            "transmission_verification_skipped": True,
                            "refactored": True
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
                    "transmission_verification_enabled": True,
                    "refactored": True
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
                    "BRACKET ORDER PRE-PLACEMENT DIAGNOSTICS with Transmission Verification (REFACTORED)",
                    symbol=order.symbol,
                    context_provider=adjustment_diagnostics,
                    decision_reason="Bracket order parameters validated with transmission verification"
                )

                # Record placement attempt before execution
                attempt_id = self._attempt_tracker._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'SUBMITTING', None, None,
                    account_number
                )
                
                contract = order.to_ib_contract()
                
                # Enhanced bracket order call with transmission verification
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Calling bracket order with transmission verification (REFACTORED)",
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
                        "current_market_price_provided": current_market_price is not None,
                        "refactored": True
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

                # STEP 1a: Validate bracket order result using BracketOrderExecutor
                bracket_valid, bracket_message, valid_order_ids = self._bracket_executor._validate_bracket_order_result(
                    ibkr_order_ids, order.symbol, account_number
                )
                
                if not bracket_valid:
                    # Handle bracket validation failure
                    self._bracket_executor._handle_bracket_order_failure(ibkr_order_ids, order.symbol, bracket_message, account_number)
                    raise Exception(bracket_message)
                
                # Use validated order IDs
                ibkr_order_ids = valid_order_ids
                parent_order_id = ibkr_order_ids[0] if ibkr_order_ids else None
                
                # STEP 1b: Wait for bracket transmission verification using BracketOrderExecutor
                if parent_order_id:
                    transmission_verified = self._bracket_executor._wait_for_bracket_transmission(parent_order_id, order.symbol, account_number)
                    if not transmission_verified:
                        error_msg = "Bracket order transmission verification failed - not all components transmitted"
                        self._bracket_executor._handle_bracket_order_failure(ibkr_order_ids, order.symbol, error_msg, account_number)
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
                self._attempt_tracker._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'SUBMITTED', ibkr_order_ids, None,
                    account_number
                )
                
                # Enhanced success logging with transmission verification context
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Bracket order placed successfully - Transmission Verified (REFACTORED)",
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
                        "market_price_available_at_execution": current_market_price is not None,
                        "refactored": True
                    },
                    decision_reason="Bracket order successfully submitted with transmission verification"
                )
                
                return True

            # === SIMULATION PATH ===
            else:
                # Simulation doesn't need atomicity since no real orders
                self._attempt_tracker._record_order_attempt(
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
                    "Simulation order executed with transmission verification awareness (REFACTORED)",
                    symbol=order.symbol,
                    context_provider={
                        "account": account_number,
                        "ibkr_connected": False,
                        "bracket_order_fix": "simulation_skip_v3",
                        "price_adjustment_simulated": current_market_price is not None,
                        "transmission_verification_skipped": True,
                        "refactored": True
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
                "current_market_price_available": current_market_price is not None,
                "refactored": True
            }
            
            # Add specific context for partial bracket orders
            if ibkr_order_ids and len(ibkr_order_ids) != 3:
                error_context["partial_bracket_detected"] = True
                error_context["expected_components"] = 3
                error_context["actual_components"] = len(ibkr_order_ids)
                error_context["missing_profit_target"] = len(ibkr_order_ids) == 2
                error_context["transmission_verification_failed"] = True
            
            # ATOMIC ROLLBACK: Use RollbackManager to cancel IBKR orders if anything failed
            if ibkr_order_ids:
                self._rollback_manager.execute_rollback(ibkr_order_ids, order, str(e), account_number)
            
            # Update attempt with failure
            self._attempt_tracker._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'FAILED', ibkr_order_ids, str(e),
                account_number
            )
            
            # Log atomic execution failure with enhanced transmission verification context
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Bracket order execution failed with transmission verification diagnostics (REFACTORED)",
                symbol=order.symbol,
                context_provider=error_context,
                decision_reason=f"Bracket order execution failed: {e}"
            )
            
            return False