"""
Viability checking service for order prioritization.
Handles order viability assessment and validation.
"""

from typing import Dict, Tuple
from src.core.context_aware_logger import get_context_logger, TradingEventType


class ViabilityChecker:
    """Handles order viability checking and validation."""
    
    def __init__(self):
        self.context_logger = get_context_logger()

    def is_order_viable(self, order_data: Dict) -> Tuple[bool, str]:
        """Check if order meets minimum viability criteria.
        
        UPDATED: Probability scores are used for prioritization only, not for blocking execution.
        All orders that pass basic business rules are considered viable.
        """
        order = order_data.get('order')
        safe_symbol = getattr(order, 'symbol', 'Unknown') if order else 'Unknown'
        fill_prob = order_data.get('fill_probability', 0)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            f"Checking viability for {safe_symbol}",
            symbol=safe_symbol if safe_symbol != 'Unknown' else None,
            context_provider={
                "fill_probability": fill_prob,
                "order_provided": order is not None
            }
        )
        # <Context-Aware Logging Integration - End>
            
        # UPDATED: Remove probability threshold check - probability is for prioritization only
        # All orders that reach this point are considered viable for execution
        # Probability scores will be used to determine execution sequence, not block execution
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            f"Order {safe_symbol} is viable - probability used for sequencing only",
            symbol=safe_symbol,
            context_provider={
                "fill_probability": fill_prob,
                "decision": "All orders viable - probability affects sequence only"
            },
            decision_reason="Order meets basic business rules - probability used for prioritization"
        )
        # <Context-Aware Logging Integration - End>
        return True, "Viable - probability used for sequencing"