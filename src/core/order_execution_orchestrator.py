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
from typing import Dict, List, Optional, Tuple
from src.core.planned_order import PlannedOrder, ActiveOrder
from src.core.probability_engine import FillProbabilityEngine
from src.services.order_execution_service import OrderExecutionService
from src.services.position_sizing_service import PositionSizingService
from src.services.order_persistence_service import OrderPersistenceService
from src.services.state_service import StateService
from src.core.ibkr_client import IbkrClient


class OrderExecutionOrchestrator:
    """Orchestrates order execution with viability checks and prioritization."""
    
    def __init__(self, execution_service: OrderExecutionService,
                 sizing_service: PositionSizingService,
                 persistence_service: OrderPersistenceService,
                 state_service: StateService,
                 probability_engine: FillProbabilityEngine,
                 ibkr_client: Optional[IbkrClient] = None):
        """Initialize the order execution orchestrator with required services."""
        self.execution_service = execution_service
        self.sizing_service = sizing_service
        self.persistence_service = persistence_service
        self.state_service = state_service
        self.probability_engine = probability_engine
        self.ibkr_client = ibkr_client
        self.default_capital = 100000
        self.min_fill_probability = 0.4
        
    def execute_single_order(self, order: PlannedOrder, fill_probability: float, 
                           effective_priority: Optional[float] = None) -> bool:
        """Execute a single order with viability checks and proper status tracking."""
        try:
            total_capital = self._get_total_capital()
            is_live_trading = self._get_trading_mode()
            
            # Calculate position size and capital commitment
            quantity, capital_commitment = self._calculate_position_details(order, total_capital)
            
            # Check order viability
            is_viable = self._check_order_viability(order, fill_probability)
            if not is_viable:
                return False
            
            # Execute the order through execution service
            execution_success = self._execute_via_service(
                order, fill_probability, effective_priority, 
                total_capital, quantity, capital_commitment, is_live_trading
            )
            
            return execution_success
            
        except Exception as e:
            self._handle_execution_failure(order, str(e))
            return False
            
    def _get_total_capital(self) -> float:
        """Get total capital from IBKR or use default simulation capital."""
        if self.ibkr_client and self.ibkr_client.connected:
            try:
                return self.ibkr_client.get_account_value()
            except Exception:
                return self.default_capital
        return self.default_capital
        
    def _get_trading_mode(self) -> bool:
        """Determine if trading mode is live or paper based on IBKR connection."""
        return (self.ibkr_client and self.ibkr_client.connected and 
                not self.ibkr_client.is_paper_account)
                
    def _calculate_position_details(self, order: PlannedOrder, total_capital: float) -> Tuple[float, float]:
        """Calculate position quantity and capital commitment for an order."""
        if order.entry_price is None:
            raise Exception("Failed to calculate position details: entry price is None")

        quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
        capital_commitment = order.entry_price * quantity
        return quantity, capital_commitment
            
    def _check_order_viability(self, order: PlannedOrder, fill_probability: float) -> bool:
        """Check if an order meets minimum viability criteria for execution."""
        if fill_probability < self.min_fill_probability:
            self.persistence_service.update_order_status(
                order, 'REJECTED', 
                f"Fill probability below threshold ({fill_probability:.2%} < {self.min_fill_probability:.2%})"
            )
            return False
            
        if self.state_service.has_open_position(order.symbol):
            self.persistence_service.update_order_status(
                order, 'REJECTED', 
                f"Open position exists for {order.symbol}"
            )
            return False
            
        return True
        
    def _execute_via_service(self, order: PlannedOrder, fill_probability: float, 
                           effective_priority: Optional[float], total_capital: float,
                           quantity: float, capital_commitment: float, is_live_trading: bool) -> bool:
        """Execute order through the execution service with proper status tracking."""
        # Calculate effective priority if not provided
        if effective_priority is None:
            effective_priority = order.priority * fill_probability
            
        # Execute through execution service
        success = self.execution_service.place_order(
            order, fill_probability, effective_priority,
            total_capital, quantity, capital_commitment, is_live_trading
        )
        
        # Update order status based on execution result
        if success:
            self.persistence_service.update_order_status(
                order, 'EXECUTING', 
                f"Order executing with fill_prob={fill_probability:.2%}"
            )
        else:
            self.persistence_service.update_order_status(
                order, 'FAILED', 
                "Execution service returned failure"
            )
            
        return success
        
    def _handle_execution_failure(self, order: PlannedOrder, error_message: str) -> None:
        """Handle execution failures with proper error logging and status updates."""
        self.persistence_service.update_order_status(
            order, 'FAILED', f"Execution failed: {error_message}"
        )
        
    def validate_order_execution(self, order: PlannedOrder, active_orders: Dict[int, ActiveOrder], 
                               max_open_orders: int = 5) -> bool:
        """Validate if an order can be executed based on system constraints."""
        # Check maximum open orders limit
        working_orders = sum(1 for ao in active_orders.values() if ao.is_working())
        if working_orders >= max_open_orders:
            return False
            
        # Check for duplicate active orders
        if self._has_duplicate_active_order(order, active_orders):
            return False
            
        # Basic order validation
        if order.entry_price is None:
            return False
            
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
                return True
                
        return False
        
    def calculate_effective_priority(self, order: PlannedOrder, fill_probability: float) -> float:
        """Calculate the effective priority score for an order."""
        return order.priority * fill_probability
        
    def get_execution_summary(self, order: PlannedOrder, fill_probability: float, 
                            total_capital: float) -> Dict[str, any]:
        """Generate an execution summary for logging and monitoring purposes."""
        try:
            quantity, capital_commitment = self._calculate_position_details(order, total_capital)
            is_live_trading = self._get_trading_mode()
            effective_priority = self.calculate_effective_priority(order, fill_probability)
            is_viable = fill_probability >= self.min_fill_probability
            
            return {
                'symbol': order.symbol,
                'action': order.action.value,
                'quantity': quantity,
                'capital_commitment': capital_commitment,
                'fill_probability': fill_probability,
                'effective_priority': effective_priority,
                'is_viable': is_viable,
                'is_live_trading': is_live_trading,
                'total_capital': total_capital
            }
        except Exception:
            return {}