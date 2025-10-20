# src/core/order_execution_orchestrator.py
"""
Handles the complete order execution workflow including:
- Order viability checking and prioritization
- Capital commitment calculations  
- Order placement through execution service
- Order replacement and cancellation logic
- Status tracking and persistence integration
"""

import datetime
from typing import Dict, Any, Optional, Tuple
from src.core.planned_order import PlannedOrder, ActiveOrder
from src.core.probability_engine import FillProbabilityEngine
from src.services.order_execution_service import OrderExecutionService
from src.services.position_sizing_service import PositionSizingService
from src.services.order_persistence_service import OrderPersistenceService
from src.services.state_service import StateService
from src.core.ibkr_client import IbkrClient

# <AON Integration - Begin>
from src.core.order_lifecycle_manager import OrderLifecycleManager
from src.core.shared_enums import OrderState as SharedOrderState
# <AON Integration - End>

# <Context-Aware Logger Integration - Begin>
from src.core.context_aware_logger import get_context_logger, TradingEventType
# <Context-Aware Logger Integration - End>


class OrderExecutionOrchestrator:
    """Orchestrates order execution with viability checks and prioritization."""
    
    # OrderExecutionOrchestrator.__init__ - Begin (UPDATED - reduced logging)
    def __init__(self, execution_service: OrderExecutionService,
                 sizing_service: PositionSizingService,
                 persistence_service: OrderPersistenceService,
                 state_service: StateService,
                 probability_engine: FillProbabilityEngine,
                 ibkr_client: Optional[IbkrClient] = None,
                 config: Optional[Dict[str, Any]] = None,
                 lifecycle_manager: Optional[OrderLifecycleManager] = None):
        """Initialize the order execution orchestrator with required services."""
        self.context_logger = get_context_logger()
        
        # Reduced initialization logging - only critical insights
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Orchestrator initialized",
            context_provider={
                "services_ready": all([
                    execution_service is not None,
                    sizing_service is not None, 
                    persistence_service is not None,
                    state_service is not None,
                    probability_engine is not None
                ]),
                "ibkr_available": ibkr_client is not None,
                "aon_enabled": lifecycle_manager is not None
            }
        )
        
        self.execution_service = execution_service
        self.sizing_service = sizing_service
        self.persistence_service = persistence_service
        self.state_service = state_service
        self.probability_engine = probability_engine
        self.ibkr_client = ibkr_client
        self.lifecycle_manager = lifecycle_manager
        
        # Load configuration
        self._load_configuration(config or {})
    # OrderExecutionOrchestrator.__init__ - End
    
    # <AON Validation Methods - Begin>
    def _check_aon_viability(self, order: PlannedOrder, total_capital: float) -> Tuple[bool, str]:
        """
        Check if order meets AON execution criteria.
        
        Args:
            order: PlannedOrder to validate
            total_capital: Total account capital for notional calculation
            
        Returns:
            Tuple of (is_valid, reason_message)
        """
        # Check if AON is enabled in configuration
        if not self.aon_config.get('enabled', True):
            return True, "AON validation disabled"
            
        # Use lifecycle manager for AON validation if available
        if self.lifecycle_manager:
            return self.lifecycle_manager.validate_order_for_aon(order, total_capital)
        else:
            # Fallback basic AON validation
            return self._basic_aon_validation(order, total_capital)
            
    def _basic_aon_validation(self, order: PlannedOrder, total_capital: float) -> Tuple[bool, str]:
        """
        Basic AON validation fallback when lifecycle manager is not available.
        
        Args:
            order: PlannedOrder to validate
            total_capital: Total account capital
            
        Returns:
            Tuple of (is_valid, reason_message)
        """
        try:
            # Calculate order notional value
            quantity = order.calculate_quantity(total_capital)
            notional_value = order.entry_price * quantity
            
            # Get fallback threshold from config
            fallback_threshold = self.aon_config.get('fallback_fixed_notional', 50000)
            
            # Check against fallback threshold
            if notional_value > fallback_threshold:
                return False, f"Order notional ${notional_value:,.2f} exceeds AON fallback threshold ${fallback_threshold:,.2f}"
                
            return True, f"AON valid: ${notional_value:,.2f} <= ${fallback_threshold:,.2f} (fallback)"
            
        except Exception as e:
            return False, f"AON validation error: {e}"
    # <AON Validation Methods - End>
            
# OrderExecutionOrchestrator._get_total_capital - Begin (UPDATED - reduced logging)
    def _get_total_capital(self) -> float:
        """Get total capital from IBKR. Fail safely if capital cannot be retrieved."""
        if self.ibkr_client and self.ibkr_client.connected:
            try:
                capital = self.ibkr_client.get_account_value()
                # Only log capital retrieval on first successful call or errors
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Capital retrieved",
                    context_provider={
                        "capital": capital,
                        "paper_account": self.ibkr_client.is_paper_account
                    }
                )
                return capital
            except Exception as e:
                # Critical error - must log
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Capital retrieval failed",
                    context_provider={
                        "error": str(e)
                    },
                    decision_reason="IBKR capital unavailable"
                )
                raise RuntimeError(f"Failed to retrieve account capital from IBKR: {e}") from e
        
        # No connection - critical insight
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "No IBKR connection",
            context_provider={
                "client_available": self.ibkr_client is not None,
                "connected": self.ibkr_client.connected if self.ibkr_client else False
            },
            decision_reason="Cannot determine capital without connection"
        )
        raise RuntimeError("No IBKR connection available - cannot determine account capital")
# OrderExecutionOrchestrator._get_total_capital - End

# OrderExecutionOrchestrator._get_trading_mode - Begin (UPDATED - reduced logging)
    def _get_trading_mode(self) -> bool:
        """Determine if trading mode is live or paper based on IBKR connection."""
        is_live = (self.ibkr_client and self.ibkr_client.connected and 
                not self.ibkr_client.is_paper_account)
        
        # Only log mode changes, not every check
        if not hasattr(self, '_last_trading_mode') or self._last_trading_mode != is_live:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Trading mode detected",
                context_provider={
                    "live_trading": is_live,
                    "paper_account": not is_live
                }
            )
            self._last_trading_mode = is_live
                
        return is_live
# OrderExecutionOrchestrator._get_trading_mode - End

# OrderExecutionOrchestrator._calculate_position_details - Begin (UPDATED - reduced logging)
    def _calculate_position_details(self, order: PlannedOrder, total_capital: float) -> Tuple[float, float]:
        """Calculate position quantity and capital commitment for an order."""
        if order.entry_price is None:
            # Critical error - must log
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Position calc failed - no entry price",
                symbol=order.symbol,
                context_provider={
                    "symbol": order.symbol,
                    "action": order.action.value
                },
                decision_reason="Missing entry price"
            )
            raise Exception("Failed to calculate position details: entry price is None")

        quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
        capital_commitment = order.entry_price * quantity
        
        # Only log meaningful position calculations (significant commitments)
        if capital_commitment > total_capital * 0.01:  # Only log positions >1% of capital
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                "Position calculated",
                symbol=order.symbol,
                context_provider={
                    "quantity": quantity,
                    "capital": capital_commitment,
                    "risk_amount": total_capital * order.risk_per_trade
                },
                decision_reason=f"Position: {quantity} units"
            )
        
        return quantity, capital_commitment
# OrderExecutionOrchestrator._calculate_position_details - End
             
    def _handle_execution_failure(self, order: PlannedOrder, error_message: str) -> None:
        """Handle execution failures with proper error logging and status updates."""
        # <Context-Aware Logging - Execution Failure Handling - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Handling execution failure",
            symbol=order.symbol,
            context_provider={
                "error_message": error_message,
                "new_status": 'FAILED'
            },
            decision_reason=f"Execution failed: {error_message}"
        )
        # <Context-Aware Logging - Execution Failure Handling - End>
        self.persistence_service.update_order_status(
            order, 'FAILED', f"Execution failed: {error_message}"
        )
        
# OrderExecutionOrchestrator.validate_order_execution - Begin (UPDATED - reduced logging)
    def validate_order_execution(self, order: PlannedOrder, active_orders: Dict[int, ActiveOrder], 
                               max_open_orders: int = 5) -> bool:
        """Validate if an order can be executed based on system constraints."""
        # Check maximum open orders limit
        working_orders = sum(1 for ao in active_orders.values() if ao.is_working())
        if working_orders >= max_open_orders:
            # Critical rejection - must log
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order rejected - max orders limit",
                symbol=order.symbol,
                context_provider={
                    "working_orders": working_orders,
                    "max_orders": max_open_orders
                },
                decision_reason="Max orders limit"
            )
            return False
            
        # Check for duplicate active orders
        if self._has_duplicate_active_order(order, active_orders):
            # Critical rejection - must log
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order rejected - duplicate exists",
                symbol=order.symbol,
                context_provider={
                    "symbol": order.symbol,
                    "action": order.action.value
                },
                decision_reason="Duplicate order"
            )
            return False
            
        # Basic order validation
        if order.entry_price is None:
            # Critical rejection - must log
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order rejected - no entry price",
                symbol=order.symbol,
                context_provider={
                    "symbol": order.symbol
                },
                decision_reason="Missing entry price"
            )
            return False
            
        # Validation passed - minimal logging for success
        return True
# OrderExecutionOrchestrator.validate_order_execution - End
        
# OrderExecutionOrchestrator.get_execution_summary - Begin (UPDATED - reduced logging)
    def get_execution_summary(self, order: PlannedOrder, fill_probability: float, 
                            total_capital: float) -> Dict[str, any]:
        """Generate an execution summary for logging and monitoring purposes."""
        try:
            if total_capital is None:
                total_capital = self._get_total_capital()
                
            quantity, capital_commitment = self._calculate_position_details(order, total_capital)
            is_live_trading = self._get_trading_mode()
            effective_priority = self.calculate_effective_priority(order, fill_probability)
            
            # AON validation
            aon_valid, aon_reason = self._check_aon_viability(order, total_capital)
            
            summary = {
                'symbol': order.symbol,
                'action': order.action.value,
                'quantity': quantity,
                'capital_commitment': capital_commitment,
                'fill_probability': fill_probability,
                'effective_priority': effective_priority,
                'is_live_trading': is_live_trading,
                'total_capital': total_capital,
                'aon_valid': aon_valid,
                'aon_reason': aon_reason
            }
            
            # Only log summary for significant orders
            if capital_commitment > total_capital * 0.02:  # Only log >2% of capital
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Execution summary",
                    symbol=order.symbol,
                    context_provider={
                        "quantity": quantity,
                        "capital": capital_commitment,
                        "probability": fill_probability
                    }
                )
            
            return summary
        except Exception as e:
            # Critical error - must log
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Summary generation failed",
                symbol=order.symbol,
                context_provider={
                    "error": str(e)
                },
                decision_reason="Summary error"
            )
            return {
                'symbol': order.symbol,
                'error': str(e),
                'execution_viable': False
            }
# OrderExecutionOrchestrator.get_execution_summary - End

# OrderExecutionOrchestrator.execute_single_order - Begin (UPDATED - Use provided parameters)
    def execute_single_order(self, planned_order, fill_probability, effective_priority=None, 
                            total_capital=None, quantity=None, capital_commitment=None, 
                            is_live_trading=None, account_number=None):
        """Execute a single order with validation and viability checks.
        
        FIXED: Now uses provided parameters from trading manager instead of recalculating them
        to maintain parameter consistency throughout the execution chain.
        """
        # Single start log with key insights
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Order execution starting with provided parameters",
            symbol=planned_order.symbol,
            context_provider={
                "probability": fill_probability,
                "action": planned_order.action.value,
                "order_type": planned_order.order_type.value,
                "effective_priority": effective_priority,
                "total_capital_provided": total_capital is not None,
                "quantity_provided": quantity is not None,
                "capital_commitment_provided": capital_commitment is not None,
                "is_live_trading_provided": is_live_trading is not None,
                "account_number_provided": account_number is not None
            }
        )
        
        try:
            # ✅ FIXED: Use provided parameters or calculate fallbacks only if needed
            # Use provided total_capital or calculate if not provided
            if total_capital is None:
                total_capital = self._get_total_capital()
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using calculated total_capital (fallback)",
                    symbol=planned_order.symbol,
                    context_provider={
                        "calculated_capital": total_capital,
                        "parameter_source": "fallback_calculation"
                    }
                )
            else:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using provided total_capital",
                    symbol=planned_order.symbol,
                    context_provider={
                        "provided_capital": total_capital,
                        "parameter_source": "trading_manager"
                    }
                )
                
            # Use provided quantity and capital_commitment or calculate if not provided
            if quantity is None or capital_commitment is None:
                calculated_quantity, calculated_capital_commitment = self._calculate_position_details(
                    planned_order, total_capital
                )
                quantity = quantity or calculated_quantity
                capital_commitment = capital_commitment or calculated_capital_commitment
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using calculated position details (fallback)",
                    symbol=planned_order.symbol,
                    context_provider={
                        "calculated_quantity": calculated_quantity,
                        "calculated_capital_commitment": calculated_capital_commitment,
                        "parameter_source": "fallback_calculation"
                    }
                )
            else:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using provided position details",
                    symbol=planned_order.symbol,
                    context_provider={
                        "provided_quantity": quantity,
                        "provided_capital_commitment": capital_commitment,
                        "parameter_source": "trading_manager"
                    }
                )
                
            # Use provided is_live_trading or calculate if not provided
            if is_live_trading is None:
                is_live_trading = self._get_trading_mode()
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using calculated trading mode (fallback)",
                    symbol=planned_order.symbol,
                    context_provider={
                        "calculated_live_trading": is_live_trading,
                        "parameter_source": "fallback_calculation"
                    }
                )
            else:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using provided trading mode",
                    symbol=planned_order.symbol,
                    context_provider={
                        "provided_live_trading": is_live_trading,
                        "parameter_source": "trading_manager"
                    }
                )
                
            # Calculate effective priority if not provided
            if effective_priority is None:
                effective_priority = planned_order.priority * fill_probability
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using calculated effective priority (fallback)",
                    symbol=planned_order.symbol,
                    context_provider={
                        "calculated_effective_priority": effective_priority,
                        "parameter_source": "fallback_calculation"
                    }
                )
            else:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using provided effective priority",
                    symbol=planned_order.symbol,
                    context_provider={
                        "provided_effective_priority": effective_priority,
                        "parameter_source": "trading_manager"
                    }
                )
            
            # Check order viability
            if not self._check_order_viability(planned_order, fill_probability):
                return False
                
            # AON validation - only log failures
            if self.lifecycle_manager:
                aon_valid, aon_message = self.lifecycle_manager.validate_order_for_aon(
                    planned_order, total_capital
                )
                
                if not aon_valid:
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "AON validation failed",
                        symbol=planned_order.symbol,
                        context_provider={
                            "reason": aon_message,
                            "capital": total_capital
                        },
                        decision_reason="AON rejection"
                    )
                    self.persistence_service.update_order_status(
                        planned_order, 'FAILED', f"AON rejection: {aon_message}"
                    )
                    return False
            
            # ✅ FIXED: Pass ALL parameters (both provided and calculated) to execution service
            result = self._execute_via_service(
                planned_order, fill_probability, effective_priority, total_capital,
                quantity, capital_commitment, is_live_trading, account_number
            )
            
            # Only log execution result if meaningful
            if result:
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Order execution completed with consistent parameters",
                    symbol=planned_order.symbol,
                    context_provider={
                        "quantity": quantity,
                        "capital_commitment": capital_commitment,
                        "total_capital": total_capital,
                        "is_live_trading": is_live_trading,
                        "account_number_used": account_number,
                        "parameter_consistency": "maintained"
                    },
                    decision_reason="Execution successful with consistent parameter flow"
                )
            else:
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    "Order execution failed despite consistent parameters",
                    symbol=planned_order.symbol,
                    context_provider={
                        "service_result": result,
                        "parameter_consistency": "maintained"
                    },
                    decision_reason="Execution service failure with consistent parameters"
                )
            
            return result
                
        except Exception as e:
            # Critical error - must log
            error_msg = f"Execution failed: {str(e)}"
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Order execution error with parameter flow",
                symbol=planned_order.symbol,
                context_provider={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "total_capital_provided": total_capital is not None,
                    "quantity_provided": quantity is not None,
                    "capital_commitment_provided": capital_commitment is not None,
                    "parameter_flow_issue": True
                },
                decision_reason="Execution exception in parameter flow"
            )
            self.persistence_service.update_order_status(
                planned_order, 'FAILED', error_msg
            )
            return False
# OrderExecutionOrchestrator.execute_single_order - End

# OrderExecutionOrchestrator._execute_via_service - Begin (UPDATED - Enhanced parameter validation)
    def _execute_via_service(self, order: PlannedOrder, fill_probability: float, 
                        effective_priority: Optional[float], total_capital: float,
                        quantity: float, capital_commitment: float, is_live_trading: bool,
                        account_number: Optional[str] = None) -> bool:
        """Execute order through the execution service with proper status tracking.
        
        FIXED: Enhanced parameter validation to ensure all bracket order parameters are valid.
        """
        # ✅ FIXED: Validate all parameters before calling execution service
        parameter_issues = []
        
        if total_capital is None or total_capital <= 0:
            parameter_issues.append(f"Invalid total_capital: {total_capital}")
        
        if quantity is None or quantity <= 0:
            parameter_issues.append(f"Invalid quantity: {quantity}")
        
        if capital_commitment is None or capital_commitment <= 0:
            parameter_issues.append(f"Invalid capital_commitment: {capital_commitment}")
        
        if parameter_issues:
            error_msg = f"Parameter validation failed: {', '.join(parameter_issues)}"
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Execution service call blocked - invalid parameters",
                symbol=order.symbol,
                context_provider={
                    "parameter_issues": parameter_issues,
                    "total_capital": total_capital,
                    "quantity": quantity,
                    "capital_commitment": capital_commitment,
                    "is_live_trading": is_live_trading
                },
                decision_reason=f"Parameter validation failed: {error_msg}"
            )
            self.persistence_service.update_order_status(
                order, 'FAILED', error_msg
            )
            return False
        
        # Calculate effective priority if not provided
        if effective_priority is None:
            effective_priority = order.priority * fill_probability
            
        # Single execution log with key insights
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Calling execution service with validated parameters",
            symbol=order.symbol,
            context_provider={
                "quantity": quantity,
                "capital_commitment": capital_commitment,
                "total_capital": total_capital,
                "live_trading": is_live_trading,
                "priority": effective_priority,
                "account_number_provided": account_number is not None,
                "parameter_validation": "passed"
            }
        )
        
        # Execute through execution service - pass all validated parameters
        success = self.execution_service.place_order(
            order, fill_probability, effective_priority,
            total_capital, quantity, capital_commitment, is_live_trading,
            account_number
        )
        
        # Update order status based on execution result
        if success:
            self.persistence_service.update_order_status(
                order, SharedOrderState.LIVE_WORKING.value, 
                f"Order executing with fill_prob={fill_probability:.2%}"
            )
        else:
            self.persistence_service.update_order_status(
                order, 'FAILED', 
                "Execution service returned failure despite valid parameters"
            )
            
        return success
# OrderExecutionOrchestrator._execute_via_service - End

# OrderExecutionOrchestrator._check_order_viability - Begin (UPDATED - reduced logging)
    def _check_order_viability(self, order: PlannedOrder, fill_probability: float) -> bool:
        """Check if an order meets minimum viability criteria for execution."""
        if self.state_service.has_open_position(order.symbol):
            # Critical rejection - must log
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Order rejected - open position exists",
                symbol=order.symbol,
                context_provider={
                    "symbol": order.symbol,
                    "action": order.action.value
                },
                decision_reason="Open position conflict"
            )
            self.persistence_service.update_order_status(
                order, 'REJECTED', 
                f"Open position exists for {order.symbol}"
            )
            return False
            
        # Viability passed - minimal logging for success cases
        return True
# OrderExecutionOrchestrator._check_order_viability - End

# OrderExecutionOrchestrator._has_duplicate_active_order - Begin (UPDATED - reduced logging)
    def _has_duplicate_active_order(self, order: PlannedOrder, active_orders: Dict[int, ActiveOrder]) -> bool:
        """Check if an identical order is already active."""
        order_key = f"{order.symbol}_{order.action.value}_{order.entry_price}_{order.stop_loss}"
        
        for active_order in active_orders.values():
            if not active_order.is_working():
                continue
                
            active_order_obj = active_order.planned_order
            active_key = f"{active_order_obj.symbol}_{active_order_obj.action.value}_{active_order_obj.entry_price}_{active_order_obj.stop_loss}"
            
            if order_key == active_key:
                # Duplicate found - minimal logging
                return True
                
        return False
# OrderExecutionOrchestrator._has_duplicate_active_order - End

# OrderExecutionOrchestrator.calculate_effective_priority - Begin (UPDATED - reduced logging)
    def calculate_effective_priority(self, order: PlannedOrder, fill_probability: float) -> float:
        """Calculate the effective priority score for an order."""
        effective_priority = order.priority * fill_probability
        # No logging for routine calculations - only log if used in critical decisions
        return effective_priority
# OrderExecutionOrchestrator.calculate_effective_priority - End

# OrderExecutionOrchestrator._load_configuration - Begin (UPDATED - reduced logging)
    def _load_configuration(self, config: Dict[str, Any]) -> None:
        """Load configuration parameters."""
        execution_config = config.get('execution', {})
        simulation_config = config.get('simulation', {})
        aon_config = config.get('aon_execution', {})
        
        self.aon_config = aon_config
        
        # Minimal configuration logging - only key insights
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Configuration loaded",
            context_provider={
                "aon_enabled": self.aon_config.get('enabled', True),
                "config_sections": list(config.keys())
            }
        )
# OrderExecutionOrchestrator._load_configuration - End