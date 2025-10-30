"""
Prioritization Service for Phase B - Implements deterministic scoring and capital allocation.
Combines fill probability, manual priority, capital efficiency, and other factors to rank orders.

REFACTORED: Now uses extracted services for better separation of concerns.
"""

from typing import List, Dict, Optional, Tuple
import datetime
from src.trading.orders.planned_order import PlannedOrder
from src.trading.risk.position_sizing_service import PositionSizingService

# Import extracted services
from src.services.prioritization.component_calculator import ComponentCalculator
from src.services.prioritization.priority_scoring_service import PriorityScoringService
from src.services.prioritization.resource_allocator import ResourceAllocator
from src.services.prioritization.viability_checker import ViabilityChecker
from src.services.prioritization.configuration_manager import ConfigurationManager
from src.services.prioritization.advanced_feature_integrator import AdvancedFeatureIntegrator

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)


class PrioritizationService:
    """
    Service responsible for ranking and allocating capital to executable orders.
    Implements Phase B deterministic scoring algorithm with configurable weights.
    
    REFACTORED: Now uses extracted services for cleaner implementation.
    """

    def __init__(self, sizing_service: PositionSizingService, config: Optional[Dict] = None,
                market_context_service: Optional[object] = None,
                historical_performance_service: Optional[object] = None):
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing PrioritizationService (REFACTORED)",
            context_provider={
                "sizing_service_type": type(sizing_service).__name__,
                "config_provided": config is not None,
                "market_context_service_provided": market_context_service is not None,
                "historical_performance_service_provided": historical_performance_service is not None,
                "refactored": True
            }
        )
        # <Context-Aware Logging Integration - End>
            
        self.sizing_service = sizing_service
        
        # Initialize extracted services
        self._config_manager = ConfigurationManager()
        self.config = config or self._config_manager._get_default_config()
        self._config_manager._validate_config(self.config)
        
        self._feature_integrator = AdvancedFeatureIntegrator(
            market_context_service, 
            historical_performance_service
        )
        
        self._component_calculator = ComponentCalculator(
            sizing_service,
            self.config,
            market_context_service,
            historical_performance_service
        )
        
        self._scoring_service = PriorityScoringService(
            sizing_service,
            self.config,
            self._component_calculator
        )
        
        self._resource_allocator = ResourceAllocator(
            sizing_service,
            self.config,
            self._scoring_service,
            self._component_calculator
        )
        
        self._viability_checker = ViabilityChecker()
        
        # Log configuration type for debugging
        two_layer_enabled = self.config.get('two_layer_prioritization', {}).get('enabled', False)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "PrioritizationService configuration loaded (REFACTORED)",
            context_provider={
                "two_layer_prioritization_enabled": two_layer_enabled,
                "max_open_orders": self.config.get('max_open_orders'),
                "max_capital_utilization": self.config.get('max_capital_utilization'),
                "enable_advanced_features": self.config.get('enable_advanced_features', False),
                "refactored": True,
                "extracted_services_initialized": True
            }
        )
        # <Context-Aware Logging Integration - End>

    def prioritize_orders(self, executable_orders: List[Dict], total_capital: float, 
                        current_working_orders: Optional[List] = None) -> List[Dict]:
        """Prioritize orders with comprehensive safety checks (REFACTORED)."""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            f"Starting prioritization of {len(executable_orders)} orders (REFACTORED)",
            context_provider={
                "total_capital": total_capital,
                "executable_orders_count": len(executable_orders),
                "current_working_orders_count": len(current_working_orders) if current_working_orders else 0,
                "refactored": True
            }
        )
        # <Context-Aware Logging Integration - End>
            
        # CRITICAL FIX: Add comprehensive input validation
        if not executable_orders or not isinstance(executable_orders, list):
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No executable orders to prioritize or invalid input",
                context_provider={
                    "executable_orders_type": type(executable_orders).__name__,
                    "executable_orders_length": len(executable_orders) if executable_orders else 0,
                    "refactored": True
                },
                decision_reason="Invalid input for prioritization"
            )
            return []
        
        # Filter out any invalid orders before processing
        valid_executable_orders = []
        for i, order_data in enumerate(executable_orders):
            if not isinstance(order_data, dict):
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Skipping invalid order data at index {i}",
                    context_provider={
                        "order_data_type": type(order_data).__name__,
                        "refactored": True
                    },
                    decision_reason="Invalid order data format"
                )
                continue
                
            order = order_data.get('order')
            if order is None:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Skipping order with None order object at index {i}",
                    context_provider={
                        "refactored": True
                    },
                    decision_reason="Missing order object"
                )
                continue
                
            if not hasattr(order, 'symbol'):
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Skipping order without symbol at index {i}",
                    context_provider={
                        "order_type": type(order).__name__,
                        "refactored": True
                    },
                    decision_reason="Invalid order object"
                )
                continue
                
            valid_executable_orders.append(order_data)
        
        if len(valid_executable_orders) != len(executable_orders):
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Filtered {len(executable_orders) - len(valid_executable_orders)} invalid orders",
                context_provider={
                    "original_count": len(executable_orders),
                    "valid_count": len(valid_executable_orders),
                    "refactored": True
                },
                decision_reason="Order validation completed"
            )
        
        if not valid_executable_orders:
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                "No valid executable orders to prioritize",
                context_provider={
                    "refactored": True
                },
                decision_reason="No valid orders to process"
            )
            return []
            
        # Check if two-layer prioritization is enabled
        two_layer_config = self.config.get('two_layer_prioritization', {})
        two_layer_enabled = two_layer_config.get('enabled', False)
        
        if not two_layer_enabled:
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                "Using legacy single-layer prioritization",
                context_provider={
                    "refactored": True
                },
                decision_reason="Two-layer prioritization disabled"
            )
            # Fall back to legacy single-layer prioritization
            return self._resource_allocator._prioritize_orders_legacy(
                valid_executable_orders, total_capital, current_working_orders
            )

        # CRITICAL FIX: Add timeout protection for the entire prioritization process
        try:
            return self._resource_allocator._prioritize_orders_with_timeout(
                valid_executable_orders, total_capital, current_working_orders
            )
        except TimeoutError as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Prioritization timed out - using fallback",
                context_provider={
                    "error_message": str(e),
                    "timeout_seconds": 30,
                    "refactored": True
                },
                decision_reason="Prioritization timeout - falling back to legacy mode"
            )
            # Fall back to legacy mode on timeout
            return self._resource_allocator._prioritize_orders_legacy(
                valid_executable_orders, total_capital, current_working_orders
            )
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Prioritization failed with error - using fallback",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "refactored": True
                },
                decision_reason="Prioritization error - falling back to legacy mode"
            )
            # Fall back to legacy mode on any error
            return self._resource_allocator._prioritize_orders_legacy(
                valid_executable_orders, total_capital, current_working_orders
            )

    def is_order_viable(self, order_data: Dict) -> Tuple[bool, str]:
        """Check if order meets minimum viability criteria (REFACTORED)."""
        return self._viability_checker.is_order_viable(order_data)

    # Generate summary of prioritization results - Begin
    def get_prioritization_summary(self, prioritized_orders: List[Dict]) -> Dict:
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Generating prioritization summary (REFACTORED)",
            context_provider={
                "prioritized_orders_count": len(prioritized_orders),
                "refactored": True
            }
        )
        # <Context-Aware Logging Integration - End>
            
        allocated = [o for o in prioritized_orders if o.get('allocated', False)]
        not_allocated = [o for o in prioritized_orders if not o.get('allocated', False)]
        
        # Handle both two-layer and legacy modes
        if prioritized_orders and 'viable' in prioritized_orders[0]:
            viable = [o for o in prioritized_orders if o.get('viable', False)]
            non_viable = [o for o in prioritized_orders if not o.get('viable', False)]
            avg_score_key = 'quality_score'
        else:
            # Legacy mode - all orders are considered viable
            viable = prioritized_orders
            non_viable = []
            avg_score_key = 'deterministic_score'
        
        total_commitment = sum(o.get('capital_commitment', 0) for o in allocated)
        
        # Calculate average score
        viable_scores = [o.get(avg_score_key, 0) for o in viable]
        avg_score = sum(viable_scores) / len(viable_scores) if viable_scores else 0
        
        allocation_reasons = {
            reason: sum(1 for o in not_allocated if o.get('allocation_reason') == reason)
            for reason in set(o.get('allocation_reason') for o in not_allocated)
        }
        
        summary = {
            'total_allocated': len(allocated),
            'total_rejected': len(not_allocated),
            'total_viable': len(viable),
            'total_non_viable': len(non_viable),
            'total_capital_commitment': total_commitment,
            'average_score': avg_score,
            'allocation_reasons': allocation_reasons
        }
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Prioritization summary generated (REFACTORED)",
            context_provider=summary,
            decision_reason="Summary calculation completed"
        )
        # <Context-Aware Logging Integration - End>
        return summary
    # Generate summary of prioritization results - End