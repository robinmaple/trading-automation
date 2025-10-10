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
    
    def __init__(self, execution_service: OrderExecutionService,
                 sizing_service: PositionSizingService,
                 persistence_service: OrderPersistenceService,
                 state_service: StateService,
                 probability_engine: FillProbabilityEngine,
                 ibkr_client: Optional[IbkrClient] = None,
                 config: Optional[Dict[str, Any]] = None,  # <-- ADD CONFIG PARAMETER
                 # <AON Integration - Begin>
                 lifecycle_manager: Optional[OrderLifecycleManager] = None
                 # <AON Integration - End>
                 ):
        """Initialize the order execution orchestrator with required services."""
        # <Context-Aware Logger Initialization - Begin>
        self.context_logger = get_context_logger()
        # <Context-Aware Logger Initialization - End>
        
        # <Context-Aware Logging - Orchestrator Initialization Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "OrderExecutionOrchestrator initialization starting",
            context_provider={
                "services_provided": [
                    "OrderExecutionService", "PositionSizingService", 
                    "OrderPersistenceService", "StateService", "FillProbabilityEngine"
                ],
                "ibkr_client_available": ibkr_client is not None,
                "lifecycle_manager_available": lifecycle_manager is not None,
                "config_provided": config is not None
            }
        )
        # <Context-Aware Logging - Orchestrator Initialization Start - End>
        
        self.execution_service = execution_service
        self.sizing_service = sizing_service
        self.persistence_service = persistence_service
        self.state_service = state_service
        self.probability_engine = probability_engine
        self.ibkr_client = ibkr_client
        # <AON Integration - Begin>
        self.lifecycle_manager = lifecycle_manager
        # <AON Integration - End>
        
        # Load configuration instead of hardcoded values
        self._load_configuration(config or {})
        
        # <Context-Aware Logging - Orchestrator Initialization Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "OrderExecutionOrchestrator initialization completed",
            context_provider={
                "min_fill_probability": self.min_fill_probability,
                "default_capital": self.default_capital,
                "aon_enabled": self.aon_config.get('enabled', True) if hasattr(self, 'aon_config') else False
            }
        )
        # <Context-Aware Logging - Orchestrator Initialization Complete - End>
    
    def _load_configuration(self, config: Dict[str, Any]) -> None:
        """Load configuration parameters."""
        # <Context-Aware Logging - Configuration Loading Start - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Loading orchestrator configuration",
            context_provider={
                "config_sections": list(config.keys()),
                "execution_config_available": 'execution' in config,
                "simulation_config_available": 'simulation' in config,
                "aon_config_available": 'aon_execution' in config
            }
        )
        # <Context-Aware Logging - Configuration Loading Start - End>
        
        execution_config = config.get('execution', {})
        simulation_config = config.get('simulation', {})
        # <AON Configuration Integration - Begin>
        aon_config = config.get('aon_execution', {})
        # <AON Configuration Integration - End>
        
        # Use configurable defaults with fallback to original hardcoded values
        self.min_fill_probability = execution_config.get('min_fill_probability', 0.4)
        self.default_capital = simulation_config.get('default_equity', 100000)
        # <AON Configuration Integration - Begin>
        self.aon_config = aon_config
        # <AON Configuration Integration - End>
        
        # <Context-Aware Logging - Configuration Loading Complete - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Orchestrator configuration loaded",
            context_provider={
                "min_fill_probability": self.min_fill_probability,
                "default_capital": self.default_capital,
                "aon_enabled": self.aon_config.get('enabled', True),
                "aon_fallback_threshold": self.aon_config.get('fallback_fixed_notional', 50000)
            }
        )
        # <Context-Aware Logging - Configuration Loading Complete - End>
        
        # Optional: Add logging for debugging
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"OrderExecutionOrchestrator configured: min_fill_probability={self.min_fill_probability}, default_capital={self.default_capital}")        
    
    # Account Context Integration - Begin
    def execute_single_order(self, planned_order, fill_probability):
        """Execute a single order with validation and viability checks."""
        # <Context-Aware Logging - Single Order Execution Start - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Starting single order execution",
            symbol=planned_order.symbol,
            context_provider={
                "fill_probability": fill_probability,
                "action": planned_order.action.value,
                "order_type": planned_order.order_type.value,
                "entry_price": planned_order.entry_price,
                "stop_loss": planned_order.stop_loss
            }
        )
        # <Context-Aware Logging - Single Order Execution Start - End>
        
        try:
            # Get trading capital and mode
            total_capital = self._get_total_capital()
            is_live_trading = self._get_trading_mode()
            
            # Calculate position details
            quantity, capital_commitment = self._calculate_position_details(
                planned_order, total_capital
            )
            
            # Check order viability (probability, existing positions, etc.)
            if not self._check_order_viability(planned_order, fill_probability):
                return False
                
            # AON validation via lifecycle manager
            if self.lifecycle_manager:
                aon_valid, aon_message = self.lifecycle_manager.validate_order_for_aon(
                    planned_order, total_capital
                )
                if not aon_valid:
                    # <Context-Aware Logging - AON Validation Failed - Begin>
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "AON validation failed",
                        symbol=planned_order.symbol,
                        context_provider={
                            "aon_message": aon_message,
                            "total_capital": total_capital,
                            "lifecycle_manager_used": True
                        },
                        decision_reason=f"AON rejection: {aon_message}"
                    )
                    # <Context-Aware Logging - AON Validation Failed - End>
                    # Use 'FAILED' status to match test expectations
                    self.persistence_service.update_order_status(
                        planned_order, 'FAILED', f"AON rejection: {aon_message}"
                    )
                    return False
            
            # Execute the order
            return self._execute_via_service(
                planned_order, fill_probability, None, total_capital,
                quantity, capital_commitment, is_live_trading
            )
            
        except Exception as e:
            error_msg = f"Execution failed: {str(e)}"
            # <Context-Aware Logging - Execution Exception - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Order execution failed with exception",
                symbol=planned_order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "fill_probability": fill_probability
                },
                decision_reason=f"Execution exception: {e}"
            )
            # <Context-Aware Logging - Execution Exception - End>
            self.persistence_service.update_order_status(
                planned_order, 'FAILED', error_msg
            )
            return False
        
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
            
    def _get_total_capital(self) -> float:
        """Get total capital from IBKR or use default simulation capital."""
        if self.ibkr_client and self.ibkr_client.connected:
            try:
                capital = self.ibkr_client.get_account_value()
                # <Context-Aware Logging - Live Capital Retrieved - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Live trading capital retrieved from IBKR",
                    context_provider={
                        "capital_amount": capital,
                        "ibkr_connected": True,
                        "is_paper_account": self.ibkr_client.is_paper_account
                    }
                )
                # <Context-Aware Logging - Live Capital Retrieved - End>
                return capital
            except Exception as e:
                # <Context-Aware Logging - Capital Fallback - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Using fallback capital due to IBKR error",
                    context_provider={
                        "error": str(e),
                        "fallback_capital": self.default_capital
                    },
                    decision_reason=f"IBKR capital retrieval failed: {e}"
                )
                # <Context-Aware Logging - Capital Fallback - End>
                return self.default_capital
        # <Context-Aware Logging - Simulation Capital - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Using simulation capital",
            context_provider={
                "simulation_capital": self.default_capital,
                "ibkr_connected": False
            }
        )
        # <Context-Aware Logging - Simulation Capital - End>
        return self.default_capital
        
    def _get_trading_mode(self) -> bool:
        """Determine if trading mode is live or paper based on IBKR connection."""
        is_live = (self.ibkr_client and self.ibkr_client.connected and 
                not self.ibkr_client.is_paper_account)
        
        # <Context-Aware Logging - Trading Mode Detection - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Trading mode detected",
            context_provider={
                "is_live_trading": is_live,
                "ibkr_connected": self.ibkr_client.connected if self.ibkr_client else False,
                "is_paper_account": self.ibkr_client.is_paper_account if self.ibkr_client else True
            }
        )
        # <Context-Aware Logging - Trading Mode Detection - End>
                
        return is_live
                
    def _calculate_position_details(self, order: PlannedOrder, total_capital: float) -> Tuple[float, float]:
        """Calculate position quantity and capital commitment for an order."""
        # <Context-Aware Logging - Position Calculation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Calculating position details",
            symbol=order.symbol,
            context_provider={
                "total_capital": total_capital,
                "entry_price": order.entry_price,
                "stop_loss": order.stop_loss,
                "risk_per_trade": float(order.risk_per_trade),
                "security_type": order.security_type.value
            }
        )
        # <Context-Aware Logging - Position Calculation Start - End>
        
        if order.entry_price is None:
            calculation_error = "Failed to calculate position details: entry price is None"
            # <Context-Aware Logging - Position Calculation Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Position calculation failed - missing entry price",
                symbol=order.symbol,
                context_provider={
                    "entry_price_available": False
                },
                decision_reason=calculation_error
            )
            # <Context-Aware Logging - Position Calculation Error - End>
            raise Exception(calculation_error)

        quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
        capital_commitment = order.entry_price * quantity
        
        # <Context-Aware Logging - Position Calculation Success - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Position details calculated successfully",
            symbol=order.symbol,
            context_provider={
                "quantity": quantity,
                "capital_commitment": capital_commitment,
                "risk_per_share": abs(order.entry_price - order.stop_loss),
                "risk_amount": total_capital * order.risk_per_trade
            },
            decision_reason=f"Position size: {quantity} units, Capital: ${capital_commitment:,.2f}"
        )
        # <Context-Aware Logging - Position Calculation Success - End>
        
        return quantity, capital_commitment
            
    def _check_order_viability(self, order: PlannedOrder, fill_probability: float) -> bool:
        """Check if an order meets minimum viability criteria for execution."""
        # <Context-Aware Logging - Viability Check Start - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Starting order viability check",
            symbol=order.symbol,
            context_provider={
                "fill_probability": fill_probability,
                "min_fill_probability": self.min_fill_probability,
                "threshold_check": fill_probability >= self.min_fill_probability
            }
        )
        # <Context-Aware Logging - Viability Check Start - End>
        
        if fill_probability < self.min_fill_probability:
            # <Context-Aware Logging - Probability Threshold Failed - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Order viability failed - fill probability below threshold",
                symbol=order.symbol,
                context_provider={
                    "fill_probability": fill_probability,
                    "min_fill_probability": self.min_fill_probability,
                    "difference": self.min_fill_probability - fill_probability
                },
                decision_reason=f"Fill probability {fill_probability:.2%} < threshold {self.min_fill_probability:.2%}"
            )
            # <Context-Aware Logging - Probability Threshold Failed - End>
            self.persistence_service.update_order_status(
                order, 'REJECTED', 
                f"Fill probability below threshold ({fill_probability:.2%} < {self.min_fill_probability:.2%})"
            )
            return False
            
        if self.state_service.has_open_position(order.symbol):
            # <Context-Aware Logging - Open Position Conflict - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Order viability failed - open position exists",
                symbol=order.symbol,
                context_provider={
                    "has_open_position": True
                },
                decision_reason=f"Open position exists for {order.symbol}"
            )
            # <Context-Aware Logging - Open Position Conflict - End>
            self.persistence_service.update_order_status(
                order, 'REJECTED', 
                f"Open position exists for {order.symbol}"
            )
            return False
            
        # <Context-Aware Logging - Viability Check Passed - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Order viability check passed",
            symbol=order.symbol,
            context_provider={
                "fill_probability_adequate": True,
                "no_open_position": True
            },
            decision_reason="Order meets all viability criteria"
        )
        # <Context-Aware Logging - Viability Check Passed - End>
        
        return True
        
    # Account Context Integration - Begin
    def _execute_via_service(self, order: PlannedOrder, fill_probability: float, 
                           effective_priority: Optional[float], total_capital: float,
                           quantity: float, capital_commitment: float, is_live_trading: bool,
                           account_number: Optional[str] = None) -> bool:
        """Execute order through the execution service with proper status tracking."""
        # <Context-Aware Logging - Service Execution Start - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Starting order execution via service",
            symbol=order.symbol,
            context_provider={
                "quantity": quantity,
                "capital_commitment": capital_commitment,
                "is_live_trading": is_live_trading,
                "account_number_provided": account_number is not None,
                "effective_priority": effective_priority,
                "total_capital": total_capital
            }
        )
        # <Context-Aware Logging - Service Execution Start - End>
        
        # Calculate effective priority if not provided
        if effective_priority is None:
            effective_priority = order.priority * fill_probability
            
        # Execute through execution service with account context
        success = self.execution_service.place_order(
            order, fill_probability, effective_priority,
            total_capital, quantity, capital_commitment, is_live_trading,
            account_number  # Pass account number to execution service
        )
        
        # Update order status based on execution result
        if success:
            # <Context-Aware Logging - Service Execution Success - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Order execution via service successful",
                symbol=order.symbol,
                context_provider={
                    "order_status": SharedOrderState.LIVE_WORKING.value,
                    "fill_probability": fill_probability
                },
                decision_reason="Execution service returned success"
            )
            # <Context-Aware Logging - Service Execution Success - End>
            # <AON Status Integration - Begin>
            self.persistence_service.update_order_status(
                order, SharedOrderState.LIVE_WORKING.value, 
                f"AON order executing with fill_prob={fill_probability:.2%}"
            )
            # <AON Status Integration - End>
        else:
            # <Context-Aware Logging - Service Execution Failure - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Order execution via service failed",
                symbol=order.symbol,
                context_provider={
                    "order_status": 'FAILED'
                },
                decision_reason="Execution service returned failure"
            )
            # <Context-Aware Logging - Service Execution Failure - End>
            self.persistence_service.update_order_status(
                order, 'FAILED', 
                "Execution service returned failure"
            )
            
        return success
    # Account Context Integration - End
        
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
        
    def validate_order_execution(self, order: PlannedOrder, active_orders: Dict[int, ActiveOrder], 
                               max_open_orders: int = 5) -> bool:
        """Validate if an order can be executed based on system constraints."""
        # <Context-Aware Logging - Order Validation Start - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting order execution validation",
            symbol=order.symbol,
            context_provider={
                "active_orders_count": len(active_orders),
                "max_open_orders": max_open_orders,
                "entry_price_available": order.entry_price is not None
            }
        )
        # <Context-Aware Logging - Order Validation Start - End>
        
        # Check maximum open orders limit
        working_orders = sum(1 for ao in active_orders.values() if ao.is_working())
        if working_orders >= max_open_orders:
            # <Context-Aware Logging - Max Orders Limit - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order validation failed - max open orders limit reached",
                symbol=order.symbol,
                context_provider={
                    "working_orders": working_orders,
                    "max_open_orders": max_open_orders
                },
                decision_reason=f"Max open orders limit reached: {working_orders}/{max_open_orders}"
            )
            # <Context-Aware Logging - Max Orders Limit - End>
            return False
            
        # Check for duplicate active orders
        if self._has_duplicate_active_order(order, active_orders):
            # <Context-Aware Logging - Duplicate Order - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order validation failed - duplicate active order exists",
                symbol=order.symbol,
                context_provider={
                    "duplicate_found": True
                },
                decision_reason="Duplicate active order exists"
            )
            # <Context-Aware Logging - Duplicate Order - End>
            return False
            
        # Basic order validation
        if order.entry_price is None:
            # <Context-Aware Logging - Missing Entry Price - Begin>
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order validation failed - missing entry price",
                symbol=order.symbol,
                context_provider={
                    "entry_price_available": False
                },
                decision_reason="Entry price is required for validation"
            )
            # <Context-Aware Logging - Missing Entry Price - End>
            return False
            
        # <Context-Aware Logging - Order Validation Passed - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Order validation passed",
            symbol=order.symbol,
            context_provider={
                "working_orders_within_limit": True,
                "no_duplicate_orders": True,
                "entry_price_available": True
            },
            decision_reason="Order passes all execution validation checks"
        )
        # <Context-Aware Logging - Order Validation Passed - End>
        
        return True
        
    def _has_duplicate_active_order(self, order: PlannedOrder, active_orders: Dict[int, ActiveOrder]) -> bool:
        """Check if an identical order is already active."""
        order_key = f"{order.symbol}_{order.action.value}_{order.entry_price}_{order.stop_loss}"
        
        for active_order in active_orders.values():
            if not active_order.is_working():
                continue
                
            active_order_obj = active_order.planned_order
            active_key = f"{active_order_obj.symbol}_{active_order_obj.action.value}_{active_order_obj.entry_price}_{active_order_obj.stop_loss}"
            
            if order_key == active_key:
                # <Context-Aware Logging - Duplicate Detected - Begin>
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Duplicate active order detected",
                    symbol=order.symbol,
                    context_provider={
                        "duplicate_order_key": order_key,
                        "active_order_status": active_order.status
                    },
                    decision_reason=f"Duplicate order found with key: {order_key}"
                )
                # <Context-Aware Logging - Duplicate Detected - End>
                return True
                
        return False
        
    def calculate_effective_priority(self, order: PlannedOrder, fill_probability: float) -> float:
        """Calculate the effective priority score for an order."""
        effective_priority = order.priority * fill_probability
        
        # <Context-Aware Logging - Effective Priority Calculation - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Effective priority calculated",
            symbol=order.symbol,
            context_provider={
                "base_priority": order.priority,
                "fill_probability": fill_probability,
                "effective_priority": effective_priority
            }
        )
        # <Context-Aware Logging - Effective Priority Calculation - End>
        
        return effective_priority
        
    def get_execution_summary(self, order: PlannedOrder, fill_probability: float, 
                            total_capital: float) -> Dict[str, any]:
        """Generate an execution summary for logging and monitoring purposes."""
        # <Context-Aware Logging - Execution Summary Generation - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Generating execution summary",
            symbol=order.symbol,
            context_provider={
                "fill_probability": fill_probability,
                "total_capital": total_capital
            }
        )
        # <Context-Aware Logging - Execution Summary Generation - End>
        
        try:
            quantity, capital_commitment = self._calculate_position_details(order, total_capital)
            is_live_trading = self._get_trading_mode()
            effective_priority = self.calculate_effective_priority(order, fill_probability)
            is_viable = fill_probability >= self.min_fill_probability
            
            # <AON Summary Integration - Begin>
            aon_valid, aon_reason = self._check_aon_viability(order, total_capital)
            # <AON Summary Integration - End>
            
            summary = {
                'symbol': order.symbol,
                'action': order.action.value,
                'quantity': quantity,
                'capital_commitment': capital_commitment,
                'fill_probability': fill_probability,
                'effective_priority': effective_priority,
                'is_viable': is_viable,
                'is_live_trading': is_live_trading,
                'total_capital': total_capital,
                # <AON Summary Integration - Begin>
                'aon_valid': aon_valid,
                'aon_reason': aon_reason
                # <AON Summary Integration - End>
            }
            
            # <Context-Aware Logging - Execution Summary Complete - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Execution summary generated successfully",
                symbol=order.symbol,
                context_provider=summary,
                decision_reason="Execution summary ready for monitoring"
            )
            # <Context-Aware Logging - Execution Summary Complete - End>
            
            return summary
        except Exception as e:
            # <Context-Aware Logging - Execution Summary Error - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Execution summary generation failed",
                symbol=order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason=f"Execution summary error: {e}"
            )
            # <Context-Aware Logging - Execution Summary Error - End>
            return {}