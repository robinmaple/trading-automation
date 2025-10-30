"""
Priority scoring service for order prioritization.
Handles deterministic scoring, quality scoring, and score calculation algorithms.
"""

from typing import List, Dict, Optional
from src.core.context_aware_logger import get_context_logger, TradingEventType


class PriorityScoringService:
    """Handles priority scoring algorithms for order prioritization."""
    
    def __init__(self, sizing_service, config, component_calculator):
        self.context_logger = get_context_logger()
        self.sizing_service = sizing_service
        self.config = config
        self.component_calculator = component_calculator

    def calculate_deterministic_score(self, order, fill_prob: float, 
                                   total_capital: float, current_scores: Optional[List[float]] = None) -> Dict:
        """Compute final score using Phase B formula."""
        safe_symbol = getattr(order, 'symbol', 'Unknown')
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Calculating deterministic score for {safe_symbol}",
            symbol=safe_symbol,
            context_provider={
                "fill_probability": fill_prob,
                "total_capital": total_capital,
                "current_scores_provided": current_scores is not None,
                "current_scores_count": len(current_scores) if current_scores else 0
            }
        )
        # <Context-Aware Logging Integration - End>
            
        weights = self.config['weights']
        
        priority_norm = (6 - order.priority) / 5.0
        
        efficiency = self.component_calculator.calculate_efficiency(order, total_capital)
        efficiency_norm = efficiency
        if current_scores:
            max_eff = max([s.get('efficiency', 0) for s in current_scores] + [efficiency])
            min_eff = min([s.get('efficiency', 0) for s in current_scores] + [efficiency])
            if max_eff > min_eff:
                efficiency_norm = (efficiency - min_eff) / (max_eff - min_eff)
        
        try:
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity
            size_pref = 1.0 - min(capital_commitment / total_capital, 1.0)
        except (ValueError, ZeroDivisionError):
            size_pref = 0.5
            
        timeframe_match = self.component_calculator.calculate_timeframe_match_score(order)
        setup_bias = self.component_calculator.calculate_setup_bias_score(order)
        
        score = (
            weights['fill_prob'] * fill_prob +
            weights['manual_priority'] * priority_norm +
            weights['efficiency'] * efficiency_norm +
            weights['size_pref'] * size_pref +
            weights['timeframe_match'] * timeframe_match +
            weights['setup_bias'] * setup_bias
        )
        
        result = {
            'final_score': score,
            'components': {
                'fill_prob': fill_prob,
                'priority_norm': priority_norm,
                'efficiency': efficiency,
                'efficiency_norm': efficiency_norm,
                'size_pref': size_pref,
                'timeframe_match': timeframe_match,
                'setup_bias': setup_bias
            },
            'weights': weights,
            'capital_commitment': order.entry_price * self.sizing_service.calculate_order_quantity(order, total_capital) 
                                  if order.entry_price else 0
        }
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Deterministic score calculation completed for {safe_symbol}",
            symbol=safe_symbol,
            context_provider={
                "final_score": score,
                "fill_prob": fill_prob,
                "priority_norm": priority_norm,
                "efficiency": efficiency,
                "efficiency_norm": efficiency_norm,
                "size_pref": size_pref,
                "timeframe_match": timeframe_match,
                "setup_bias": setup_bias,
                "weights": weights
            },
            decision_reason="Deterministic score calculation successful"
        )
        # <Context-Aware Logging Integration - End>
        return result

    def calculate_quality_score(self, order, total_capital: float) -> Dict:
        """Calculate quality score for viable orders only."""
        safe_symbol = getattr(order, 'symbol', 'Unknown')
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Calculating quality score for {safe_symbol}",
            symbol=safe_symbol,
            context_provider={
                "total_capital": total_capital,
                "two_layer_enabled": self.config.get('two_layer_prioritization', {}).get('enabled', False)
            }
        )
        # <Context-Aware Logging Integration - End>
            
        two_layer_config = self.config.get('two_layer_prioritization', {})
        quality_weights = two_layer_config.get('quality_weights', {})
        
        # Manual priority normalization
        priority_norm = (6 - order.priority) / 5.0
        
        # Capital efficiency
        efficiency = self.component_calculator.calculate_efficiency(order, total_capital)
        
        # Risk/reward score
        risk_reward_score = self.component_calculator.calculate_risk_reward_score(order)
        
        # Advanced features (if enabled)
        timeframe_match = self.component_calculator.calculate_timeframe_match_score(order)
        setup_bias = self.component_calculator.calculate_setup_bias_score(order)
        
        # Calculate quality score
        quality_score = (
            quality_weights.get('manual_priority', 0.3) * priority_norm +
            quality_weights.get('efficiency', 0.25) * efficiency +
            quality_weights.get('risk_reward', 0.25) * risk_reward_score +
            quality_weights.get('timeframe_match', 0.1) * timeframe_match +
            quality_weights.get('setup_bias', 0.1) * setup_bias
        )
        
        result = {
            'quality_score': quality_score,
            'components': {
                'priority_norm': priority_norm,
                'efficiency': efficiency,
                'risk_reward_score': risk_reward_score,
                'timeframe_match': timeframe_match,
                'setup_bias': setup_bias
            },
            'weights': quality_weights
        }
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Quality score calculation completed for {safe_symbol}",
            symbol=safe_symbol,
            context_provider={
                "quality_score": quality_score,
                "priority_norm": priority_norm,
                "efficiency": efficiency,
                "risk_reward_score": risk_reward_score,
                "timeframe_match": timeframe_match,
                "setup_bias": setup_bias,
                "weights": quality_weights
            },
            decision_reason="Quality score calculation successful"
        )
        # <Context-Aware Logging Integration - End>
        return result