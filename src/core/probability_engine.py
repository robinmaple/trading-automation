# Imports - Begin (unchanged)
import datetime
from src.market_data.feeds.abstract_data_feed import AbstractDataFeed
from typing import Dict, Any, Optional

# Context-aware logging import - replacing simple_logger
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()


class FillProbabilityEngine:
    def __init__(self, data_feed: AbstractDataFeed, config: Optional[Dict[str, Any]] = None):
        # Minimal initialization logging
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Probability engine initialized",
            context_provider={
                "data_feed_ready": data_feed is not None,
                "purpose": "prioritization_scoring"
            }
        )
            
        self.data_feed = data_feed

    # score_fill - Begin (UPDATED - reduced logging)
    def score_fill(self, order, return_features=False) -> float:
        current_data = self.data_feed.get_current_price(order.symbol)
        if not current_data:
            # Only log missing data for significant orders
            if hasattr(order, 'priority') and order.priority > 5:  # High priority orders
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "No market data for high priority order",
                    symbol=order.symbol,
                    context_provider={
                        "order_priority": order.priority
                    },
                    decision_reason="Missing market data"
                )
            return 0.5 if not return_features else (0.5, {})

        current_price = current_data['price']
        
        # Simplified binary logic - no volatility calculations
        if order.order_type.value == 'LMT':
            if order.action.value == 'BUY':
                if current_price <= order.entry_price:
                    final_score = 0.9
                    score_reason = "price_below_entry"
                else:
                    final_score = 0.3
                    score_reason = "price_above_entry"
            elif order.action.value == 'SELL':
                if current_price >= order.entry_price:
                    final_score = 0.9
                    score_reason = "price_above_entry"
                else:
                    final_score = 0.3
                    score_reason = "price_below_entry"
        else:
            # Market orders have high fill probability
            final_score = 0.8
            score_reason = "market_order"
        
        # Only log probability calculations for high priority orders or significant scores
        if (hasattr(order, 'priority') and order.priority > 7) or final_score > 0.8:
            context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Probability calculated",
                symbol=order.symbol,
                context_provider={
                    "probability": final_score,
                    "reason": score_reason,
                    "price_diff": current_price - order.entry_price
                }
            )
        
        if return_features:
            # Return empty features dict for API compatibility
            return final_score, {}
        else:
            return final_score
    # score_fill - End

    # calculate_fill_probability - Begin (UPDATED - reduced logging)
    def calculate_fill_probability(self, order, current_price, volatility) -> float:
        # Simplified version - ignore volatility parameter
        if order.order_type.value == 'LMT':
            if order.action.value == 'BUY':
                if current_price <= order.entry_price:
                    probability = 0.9
                else:
                    probability = 0.3
            elif order.action.value == 'SELL':
                if current_price >= order.entry_price:
                    probability = 0.9
                else:
                    probability = 0.3
        else:
            probability = 0.8

        # Minimal logging - only for debugging or high priority
        if hasattr(order, 'priority') and order.priority > 8:
            context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Detailed probability calculated",
                symbol=order.symbol,
                context_provider={
                    "probability": probability,
                    "current_vs_entry": current_price - order.entry_price
                }
            )
            
        return probability
    # calculate_fill_probability - End

    # score_outcome_stub - Begin (UPDATED - reduced logging)
    def score_outcome_stub(self, order):
        # Stub implementation - minimal logging
        return None
    # score_outcome_stub - End