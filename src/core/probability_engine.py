"""
Engine responsible for estimating the probability of order execution.
Uses current market data and simple logic to decide if an order should be placed.
This is a placeholder implementation for a future, more sophisticated model.
"""

from src.core.abstract_data_feed import AbstractDataFeed
from typing import Dict, Any, Optional
import datetime

# Context-aware logging import - replacing simple_logger
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()


class FillProbabilityEngine:
    def __init__(self, data_feed: AbstractDataFeed, config: Optional[Dict[str, Any]] = None):
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing FillProbabilityEngine",
            context_provider={
                "data_feed_provided": data_feed is not None,
                "data_feed_type": type(data_feed).__name__ if data_feed else "None",
                "config_provided": config is not None,
                "config_keys": list(config.keys()) if config else []
            }
        )
            
        self.data_feed = data_feed
        self._load_configuration(config or {})
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "FillProbabilityEngine initialized successfully",
            context_provider={
                "session_id": context_logger.session_id,
                "execution_threshold": self.execution_threshold
            }
        )
    
    def _load_configuration(self, config: Dict[str, Any]) -> None:
        """Load configuration parameters."""
        execution_config = config.get('execution', {})
        self.execution_threshold = execution_config.get('fill_probability_threshold', 0.7)
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "ProbabilityEngine configuration loaded",
            context_provider={
                "execution_threshold": self.execution_threshold,
                "config_source": "execution.fill_probability_threshold",
                "default_used": execution_config.get('fill_probability_threshold') is None
            }
        )

    # Compute fill probability score with optional feature extraction
    def score_fill(self, order, return_features=False) -> float:
        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Calculating fill probability for order",
            symbol=order.symbol,
            context_provider={
                "order_type": order.order_type.value,
                "action": order.action.value,
                "entry_price": order.entry_price,
                "return_features_requested": return_features
            }
        )
            
        current_data = self.data_feed.get_current_price(order.symbol)
        if not current_data:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No market data available - using default probability",
                symbol=order.symbol,
                context_provider={
                    "data_available": False,
                    "fallback_probability": 0.5,
                    "operation": "score_fill"
                },
                decision_reason="MARKET_DATA_UNAVAILABLE"
            )
            return 0.5 if not return_features else (0.5, {})

        current_price = current_data['price']
        price_history = current_data.get('history', [])
        volatility = self.estimate_volatility(order.symbol, price_history, order)

        score = 0.5  # baseline
        if order.order_type.value == 'LMT':
            if order.action.value == 'BUY':
                if current_price <= order.entry_price:
                    score = 0.9
                    score_reason = "current_price_below_entry"
                else:
                    price_diff = current_price - order.entry_price
                    adjustment = price_diff / (order.entry_price * (1 + volatility))
                    score = max(0.1, 1.0 - adjustment)
                    score_reason = "price_above_entry_adjusted"
            elif order.action.value == 'SELL':
                if current_price >= order.entry_price:
                    score = 0.9
                    score_reason = "current_price_above_entry"
                else:
                    price_diff = order.entry_price - current_price
                    adjustment = price_diff / (order.entry_price * (1 + volatility))
                    score = max(0.1, 1.0 - adjustment)
                    score_reason = "price_below_entry_adjusted"

        final_score = max(0.0, min(1.0, score))
        
        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Fill probability calculation completed",
            symbol=order.symbol,
            context_provider={
                "final_probability": final_score,
                "current_price": current_price,
                "target_entry_price": order.entry_price,
                "volatility_estimate": volatility,
                "score_reason": score_reason if 'score_reason' in locals() else "baseline",
                "price_difference": current_price - order.entry_price,
                "price_difference_percent": ((current_price - order.entry_price) / order.entry_price * 100) if order.entry_price else None,
                "calculation_method": "limit_order_adjusted" if order.order_type.value == 'LMT' else "baseline"
            },
            decision_reason="FILL_PROBABILITY_CALCULATED"
        )
        
        if return_features:
            features = self.extract_features(order, current_data)
            return final_score, features
        else:
            return final_score

    # Compatibility wrapper for execution decisions
    def should_execute_order(self, order) -> tuple[bool, float]:
        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Checking execution viability for order",
            symbol=order.symbol,
            context_provider={
                "order_type": order.order_type.value,
                "action": order.action.value,
                "entry_price": order.entry_price,
                "execution_threshold": self.execution_threshold
            }
        )
            
        fill_prob = self.score_fill(order)
        execute = fill_prob >= self.execution_threshold

        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Execution decision made",
            symbol=order.symbol,
            context_provider={
                "fill_probability": fill_prob,
                "execution_threshold": self.execution_threshold,
                "execute_decision": execute,
                "threshold_met": fill_prob >= self.execution_threshold,
                "probability_margin": fill_prob - self.execution_threshold
            },
            decision_reason="EXECUTION_DECISION_COMPLETED"
        )

        return execute, fill_prob

    # Calculate fill probability for limit orders
    def calculate_fill_probability(self, order, current_price, volatility) -> float:
        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Calculating detailed fill probability",
            symbol=order.symbol,
            context_provider={
                "current_price": current_price,
                "volatility_estimate": volatility,
                "order_type": order.order_type.value,
                "action": order.action.value,
                "entry_price": order.entry_price
            }
        )
            
        if order.order_type.value == 'LMT':
            if order.action.value == 'BUY':
                if current_price <= order.entry_price:
                    probability = 0.95
                    reason = "favorable_buy_condition"
                else:
                    probability = 0.1
                    reason = "unfavorable_buy_condition"
            elif order.action.value == 'SELL':
                if current_price >= order.entry_price:
                    probability = 0.95
                    reason = "favorable_sell_condition"
                else:
                    probability = 0.1
                    reason = "unfavorable_sell_condition"
        else:
            probability = 0.5
            reason = "non_limit_order_baseline"

        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Detailed fill probability calculated",
            symbol=order.symbol,
            context_provider={
                "calculated_probability": probability,
                "calculation_reason": reason,
                "order_condition_met": (
                    (order.action.value == 'BUY' and current_price <= order.entry_price) or
                    (order.action.value == 'SELL' and current_price >= order.entry_price)
                )
            }
        )
        return probability

    # Extract comprehensive features for ML foundation
    def extract_features(self, order, current_data) -> dict:
        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Extracting features for ML analysis",
            symbol=order.symbol,
            context_provider={
                "order_type": order.order_type.value,
                "action": order.action.value,
                "data_available": current_data is not None
            }
        )
            
        if not current_data:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No current data for feature extraction",
                symbol=order.symbol,
                context_provider={
                    "operation": "extract_features",
                    "fallback_action": "return_empty_dict"
                },
                decision_reason="FEATURE_EXTRACTION_FAILED_NO_DATA"
            )
            return {}
        
        current_price = current_data['price']
        current_time = datetime.datetime.now()
        
        features = {
            'timestamp': current_time.isoformat(),
            'time_of_day_seconds': current_time.hour * 3600 + current_time.minute * 60 + current_time.second,
            'day_of_week': current_time.weekday(),
            'seconds_since_midnight': current_time.hour * 3600 + current_time.minute * 60 + current_time.second,
            'current_price': current_price,
            'bid': current_data.get('bid'),
            'ask': current_data.get('ask'),
            'bid_size': current_data.get('bid_size'),
            'ask_size': current_data.get('ask_size'),
            'last_price': current_data.get('last'),
            'volume': current_data.get('volume'),
            'spread_absolute': current_data.get('ask', 0) - current_data.get('bid', 0) if current_data.get('ask') and current_data.get('bid') else None,
            'spread_relative': (current_data.get('ask', 0) - current_data.get('bid', 0)) / current_price if current_data.get('ask') and current_data.get('bid') and current_price else None,
            'symbol': order.symbol,
            'order_side': order.action.value,
            'order_type': order.order_type.value,
            'entry_price': order.entry_price,
            'stop_loss': order.stop_loss,
            'priority_manual': getattr(order, 'priority', 3),
            'trading_setup': getattr(order, 'trading_setup', None),
            'core_timeframe': getattr(order, 'core_timeframe', None),
            'price_diff_absolute': current_price - order.entry_price if order.entry_price else None,
            'price_diff_relative': (current_price - order.entry_price) / order.entry_price if order.entry_price else None,
            'volatility_estimate': self.estimate_volatility(order.symbol, current_data.get('history', []), order),
            'overall_trend_human': getattr(order, 'overall_trend', None),
            'system_trend_score': getattr(order, 'system_trend_score', None),
            'brief_analysis': getattr(order, 'brief_analysis', None)
        }
        
        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Feature extraction completed",
            symbol=order.symbol,
            context_provider={
                "features_extracted": len(features),
                "feature_categories": {
                    "temporal": 4,
                    "market_data": 9,
                    "order_attributes": 8,
                    "calculated_metrics": 4
                },
                "key_features_available": {
                    "current_price": current_price is not None,
                    "bid_ask_data": current_data.get('bid') is not None and current_data.get('ask') is not None,
                    "volatility_estimate": features['volatility_estimate'] is not None,
                    "price_difference": features['price_diff_absolute'] is not None
                }
            },
            decision_reason="FEATURE_EXTRACTION_COMPLETED"
        )
        
        return features

    # Placeholder for volatility estimation
    def estimate_volatility(self, symbol, price_history, order) -> float:
        """Calculate real volatility from IBKR market data."""
        context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Estimating volatility for symbol",
            symbol=symbol,
            context_provider={
                "price_history_available": price_history is not None,
                "price_history_length": len(price_history) if price_history else 0,
                "minimum_required_history": 2
            }
        )
            
        if not price_history or len(price_history) < 2:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Insufficient price history - using default volatility",
                symbol=symbol,
                context_provider={
                    "available_history": len(price_history) if price_history else 0,
                    "default_volatility_used": 0.01,
                    "fallback_reason": "insufficient_data"
                },
                decision_reason="VOLATILITY_ESTIMATION_DEFAULT_USED"
            )
            return 0.01  # Default 1% if no history
        
        # Calculate from recent price movements
        recent_prices = [p['price'] for p in price_history[-20:]]  # Last 20 ticks
        if len(recent_prices) < 2:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Insufficient recent prices for volatility calculation",
                symbol=symbol,
                context_provider={
                    "recent_prices_available": len(recent_prices),
                    "default_volatility_used": 0.01
                },
                decision_reason="VOLATILITY_ESTIMATION_INSUFFICIENT_RECENT_DATA"
            )
            return 0.01
        
        returns = []
        for i in range(1, len(recent_prices)):
            if recent_prices[i-1] != 0:
                returns.append((recent_prices[i] - recent_prices[i-1]) / recent_prices[i-1])
        
        if not returns:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "No valid returns calculated for volatility",
                symbol=symbol,
                context_provider={
                    "recent_prices_count": len(recent_prices),
                    "valid_returns_count": 0,
                    "default_volatility_used": 0.01
                },
                decision_reason="VOLATILITY_ESTIMATION_NO_VALID_RETURNS"
            )
            return 0.01
        
        # Annualized volatility (assuming 252 trading days)
        import numpy as np
        daily_volatility = np.std(returns)
        annualized_volatility = daily_volatility * np.sqrt(252)
        
        context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Volatility estimation completed",
            symbol=symbol,
            context_provider={
                "daily_volatility": daily_volatility,
                "annualized_volatility": annualized_volatility,
                "returns_used": len(returns),
                "price_points_analyzed": len(recent_prices),
                "calculation_method": "standard_deviation_annualized",
                "trading_days_assumption": 252
            },
            decision_reason="VOLATILITY_ESTIMATION_COMPLETED"
        )
        
        return annualized_volatility

    # Placeholder for outcome probability scoring
    def score_outcome_stub(self, order):
        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Calculating outcome score stub",
            symbol=order.symbol,
            context_provider={
                "order_type": order.order_type.value,
                "action": order.action.value,
                "method_status": "stub_implementation"
            }
        )
        return None