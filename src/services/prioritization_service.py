"""
Prioritization Service for Phase B - Implements deterministic scoring and capital allocation.
Combines fill probability, manual priority, capital efficiency, and other factors to rank orders.
"""

from typing import List, Dict, Optional, Tuple
import datetime
from src.core.planned_order import PlannedOrder
from src.services.position_sizing_service import PositionSizingService
import signal
import time
from functools import wraps

# <Advanced Feature Integration - Begin>
# New imports for advanced features
from src.services.market_context_service import MarketContextService
from src.services.historical_performance_service import HistoricalPerformanceService
# <Advanced Feature Integration - End>

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)

# Prioritization Service - Main class definition - Begin
class PrioritizationService:
    """
    Service responsible for ranking and allocating capital to executable orders.
    Implements Phase B deterministic scoring algorithm with configurable weights.
    """

    def __init__(self, sizing_service: PositionSizingService, config: Optional[Dict] = None,
                market_context_service: Optional[MarketContextService] = None,
                historical_performance_service: Optional[HistoricalPerformanceService] = None):
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing PrioritizationService",
            context_provider={
                "sizing_service_type": type(sizing_service).__name__,
                "config_provided": config is not None,
                "market_context_service_provided": market_context_service is not None,
                "historical_performance_service_provided": historical_performance_service is not None
            }
        )
        # <Context-Aware Logging Integration - End>
            
        self.sizing_service = sizing_service
        self.config = config or self._get_default_config()
        
        # Validate and normalize the configuration
        self._validate_config(self.config)
        
        self.market_context_service = market_context_service
        self.historical_performance_service = historical_performance_service
        
        # Log configuration type for debugging
        two_layer_enabled = self.config.get('two_layer_prioritization', {}).get('enabled', False)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "PrioritizationService configuration loaded",
            context_provider={
                "two_layer_prioritization_enabled": two_layer_enabled,
                "max_open_orders": self.config.get('max_open_orders'),
                "max_capital_utilization": self.config.get('max_capital_utilization'),
                "enable_advanced_features": self.config.get('enable_advanced_features', False)
            }
        )
        # <Context-Aware Logging Integration - End>

    def _get_default_config(self) -> Dict:
        """Get default configuration that matches the new prioritization_config.py structure."""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Loading default prioritization configuration",
            context_provider={}
        )
        # <Context-Aware Logging Integration - End>
            
        return {
            'weights': {
                'fill_prob': 0.35,      # Reduced from 0.45 to match new config
                'manual_priority': 0.20,
                'efficiency': 0.15,
                'timeframe_match': 0.15, # Increased from 0.08
                'setup_bias': 0.10,      # Increased from 0.02
                'size_pref': 0.03,       # Reduced from 0.10
                'timeframe_match_legacy': 0.01,
                'setup_bias_legacy': 0.01
            },
            'max_open_orders': 5,
            'max_capital_utilization': 0.8,
            'enable_advanced_features': True,
            'timeframe_compatibility_map': {
                '1min': ['1min', '5min'],
                '5min': ['1min', '5min', '15min'],
                '15min': ['5min', '15min', '30min', '1H'],
                '30min': ['15min', '30min', '1H'],
                '1H': ['30min', '1H', '4H', '15min'],
                '4H': ['1H', '4H', '1D'],
                '1D': ['4H', '1D', '1W'],
                '1W': ['1D', '1W']
            },
            'setup_performance_thresholds': {
                'min_trades_for_bias': 10,
                'min_win_rate': 0.4,
                'min_profit_factor': 1.2,
                'recent_period_days': 90,
                'confidence_threshold': 0.7
            },
            'two_layer_prioritization': {
                'enabled': True,
                'min_fill_probability': 0.4,
                'quality_weights': {
                    'manual_priority': 0.30,
                    'efficiency': 0.25,
                    'risk_reward': 0.25,
                    'timeframe_match': 0.10,
                    'setup_bias': 0.10
                }
            }
        }

    def _validate_config(self, config: Dict) -> bool:
        """Validate that the configuration has the expected structure."""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Validating prioritization configuration",
            context_provider={
                "config_keys": list(config.keys()) if config else [],
                "config_has_two_layer": 'two_layer_prioritization' in config
            }
        )
        # <Context-Aware Logging Integration - End>
            
        if not config:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Empty configuration provided",
                context_provider={},
                decision_reason="Configuration validation failed"
            )
            # <Context-Aware Logging Integration - End>
            return False
        
        # Check if this is the new two-layer config format
        if 'two_layer_prioritization' in config:
            two_layer = config['two_layer_prioritization']
            if not isinstance(two_layer, dict):
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Invalid two_layer_prioritization configuration",
                    context_provider={
                        "two_layer_type": type(two_layer).__name__
                    },
                    decision_reason="Configuration validation failed"
                )
                # <Context-Aware Logging Integration - End>
                return False
            if 'enabled' not in two_layer:
                two_layer['enabled'] = True  # Default to enabled
            if 'min_fill_probability' not in two_layer:
                two_layer['min_fill_probability'] = 0.4  # Default value
            if 'quality_weights' not in two_layer:
                two_layer['quality_weights'] = {
                    'manual_priority': 0.30,
                    'efficiency': 0.25,
                    'risk_reward': 0.25,
                    'timeframe_match': 0.10,
                    'setup_bias': 0.10
                }
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Configuration validation successful",
            context_provider={
                "two_layer_enabled": config.get('two_layer_prioritization', {}).get('enabled', False)
            }
        )
        # <Context-Aware Logging Integration - End>
        return True

    def calculate_efficiency(self, order: PlannedOrder, total_capital: float) -> float:
        """
        Calculate capital efficiency (reward per committed dollar)
        Returns 0.0 for invalid orders, None inputs, or calculation errors
        """
        # Safe logging for potentially None order
        safe_symbol = order.symbol if order and hasattr(order, 'symbol') else 'None'
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Calculating efficiency for {safe_symbol}",
            symbol=safe_symbol if safe_symbol != 'None' else None,
            context_provider={
                "total_capital": total_capital,
                "order_provided": order is not None,
                "order_has_attributes": hasattr(order, 'entry_price') and hasattr(order, 'stop_loss') if order else False
            }
        )
        # <Context-Aware Logging Integration - End>
            
        # NULL CHECK MUST BE FIRST - BEFORE ANY ATTRIBUTE ACCESS
        if order is None:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Order is None, returning efficiency 0.0",
                context_provider={},
                decision_reason="Invalid order input"
            )
            # <Context-Aware Logging Integration - End>
            return 0.0
            
        # Check if object has required attributes
        if not hasattr(order, 'entry_price') or not hasattr(order, 'stop_loss'):
            safe_symbol = getattr(order, 'symbol', 'Unknown')
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Order {safe_symbol} missing required attributes, returning efficiency 0.0",
                symbol=safe_symbol,
                context_provider={
                    "has_entry_price": hasattr(order, 'entry_price'),
                    "has_stop_loss": hasattr(order, 'stop_loss')
                },
                decision_reason="Missing required order attributes"
            )
            # <Context-Aware Logging Integration - End>
            return 0.0
            
        # Check if required price data is available
        if order.entry_price is None or order.stop_loss is None:
            safe_symbol = getattr(order, 'symbol', 'Unknown')
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Order {safe_symbol} has None price data, returning efficiency 0.0",
                symbol=safe_symbol,
                context_provider={
                    "entry_price": order.entry_price,
                    "stop_loss": order.stop_loss
                },
                decision_reason="Missing price data"
            )
            # <Context-Aware Logging Integration - End>
            return 0.0
            
        try:
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity
            
            if capital_commitment <= 0:
                safe_symbol = getattr(order, 'symbol', 'Unknown')
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Order {safe_symbol} has zero/negative capital commitment, returning efficiency 0.0",
                    symbol=safe_symbol,
                    context_provider={
                        "capital_commitment": capital_commitment,
                        "quantity": quantity,
                        "entry_price": order.entry_price
                    },
                    decision_reason="Invalid capital commitment"
                )
                # <Context-Aware Logging Integration - End>
                return 0.0
                
            # ==================== SAFE ATTRIBUTE ACCESS - BEGIN ====================
            # Get action value safely with multiple fallbacks
            action_value = None
            try:
                if hasattr(order, 'action'):
                    if hasattr(order.action, 'value'):
                        action_value = order.action.value
                    elif isinstance(order.action, str):
                        action_value = order.action
            except:
                action_value = None
                
            if action_value is None:
                safe_symbol = getattr(order, 'symbol', 'Unknown')
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Order {safe_symbol} has invalid action, returning efficiency 0.0",
                    symbol=safe_symbol,
                    context_provider={
                        "action_attribute": hasattr(order, 'action')
                    },
                    decision_reason="Invalid order action"
                )
                # <Context-Aware Logging Integration - End>
                return 0.0
            # ==================== SAFE ATTRIBUTE ACCESS - END ====================
                
            if action_value == 'BUY':
                profit_target = order.entry_price + (order.entry_price - order.stop_loss) * order.risk_reward_ratio
                expected_profit_per_share = profit_target - order.entry_price
            else:
                profit_target = order.entry_price - (order.stop_loss - order.entry_price) * order.risk_reward_ratio
                expected_profit_per_share = order.entry_price - profit_target
                
            expected_profit_total = expected_profit_per_share * quantity
            efficiency = expected_profit_total / capital_commitment
            
            result = max(0.0, efficiency)
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Efficiency calculation completed for {safe_symbol}",
                symbol=safe_symbol,
                context_provider={
                    "efficiency_result": result,
                    "quantity": quantity,
                    "capital_commitment": capital_commitment,
                    "expected_profit_total": expected_profit_total,
                    "action": action_value,
                    "profit_target": profit_target,
                    "expected_profit_per_share": expected_profit_per_share
                },
                decision_reason="Efficiency calculation successful"
            )
            # <Context-Aware Logging Integration - End>
            return result
            
        except (ValueError, ZeroDivisionError, AttributeError, TypeError) as e:
            safe_symbol = getattr(order, 'symbol', 'Unknown')
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Error calculating efficiency for {safe_symbol}",
                symbol=safe_symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason="Efficiency calculation failed"
            )
            # <Context-Aware Logging Integration - End>
            return 0.0
    
    # Calculate timeframe compatibility with market conditions - Begin
    def calculate_timeframe_match_score(self, order: PlannedOrder) -> float:
        safe_symbol = getattr(order, 'symbol', 'Unknown')
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Calculating timeframe match for {safe_symbol}",
            symbol=safe_symbol,
            context_provider={
                "order_timeframe": getattr(order, 'core_timeframe', 'Unknown'),
                "enable_advanced_features": self.config.get('enable_advanced_features', False),
                "market_context_service_available": self.market_context_service is not None
            }
        )
        # <Context-Aware Logging Integration - End>
            
        if not self.config.get('enable_advanced_features', False) or not self.market_context_service:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Advanced features disabled, returning default timeframe match 0.5 for {safe_symbol}",
                symbol=safe_symbol,
                context_provider={
                    "enable_advanced_features": self.config.get('enable_advanced_features', False),
                    "market_context_service_available": self.market_context_service is not None
                },
                decision_reason="Advanced features disabled"
            )
            # <Context-Aware Logging Integration - End>
            return 0.5
            
        try:
            dominant_timeframe = self.market_context_service.get_dominant_timeframe(order.symbol)
            order_timeframe = order.core_timeframe
            
            if order_timeframe == dominant_timeframe:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Perfect timeframe match for {safe_symbol}",
                    symbol=safe_symbol,
                    context_provider={
                        "order_timeframe": order_timeframe,
                        "dominant_timeframe": dominant_timeframe,
                        "score": 1.0
                    },
                    decision_reason="Perfect timeframe match"
                )
                # <Context-Aware Logging Integration - End>
                return 1.0
            
            compatible_timeframes = self.config.get('timeframe_compatibility_map', {}).get(
                dominant_timeframe, []
            )
            if order_timeframe in compatible_timeframes:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Compatible timeframe for {safe_symbol}",
                    symbol=safe_symbol,
                    context_provider={
                        "order_timeframe": order_timeframe,
                        "dominant_timeframe": dominant_timeframe,
                        "compatible_timeframes": compatible_timeframes,
                        "score": 0.7
                    },
                    decision_reason="Compatible timeframe"
                )
                # <Context-Aware Logging Integration - End>
                return 0.7
                
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Incompatible timeframe for {safe_symbol}",
                symbol=safe_symbol,
                context_provider={
                    "order_timeframe": order_timeframe,
                    "dominant_timeframe": dominant_timeframe,
                    "compatible_timeframes": compatible_timeframes,
                    "score": 0.3
                },
                decision_reason="Incompatible timeframe"
            )
            # <Context-Aware Logging Integration - End>
            return 0.3
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Error calculating timeframe match for {safe_symbol}",
                symbol=safe_symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason="Timeframe match calculation failed"
            )
            # <Context-Aware Logging Integration - End>
            return 0.5
    # Calculate timeframe compatibility with market conditions - End

    # Calculate bias based on historical setup performance - Begin
    def calculate_setup_bias_score(self, order: PlannedOrder) -> float:
        safe_symbol = getattr(order, 'symbol', 'Unknown')
        setup_name = getattr(order, 'trading_setup', 'Unknown')
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Calculating setup bias for {safe_symbol}",
            symbol=safe_symbol,
            context_provider={
                "setup_name": setup_name,
                "enable_advanced_features": self.config.get('enable_advanced_features', False),
                "historical_performance_service_available": self.historical_performance_service is not None
            }
        )
        # <Context-Aware Logging Integration - End>
            
        if not self.config.get('enable_advanced_features', False) or not self.historical_performance_service:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Advanced features disabled, returning default setup bias 0.5 for {safe_symbol}",
                symbol=safe_symbol,
                context_provider={
                    "enable_advanced_features": self.config.get('enable_advanced_features', False),
                    "historical_performance_service_available": self.historical_performance_service is not None
                },
                decision_reason="Advanced features disabled"
            )
            # <Context-Aware Logging Integration - End>
            return 0.5
            
        try:
            setup_name = order.trading_setup
            if not setup_name:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"No setup name for {safe_symbol}, returning default 0.5",
                    symbol=safe_symbol,
                    context_provider={},
                    decision_reason="No setup name provided"
                )
                # <Context-Aware Logging Integration - End>
                return 0.5
                
            performance = self.historical_performance_service.get_setup_performance(setup_name)
            
            if not performance:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"No performance data for setup {setup_name}, returning default 0.5",
                    symbol=safe_symbol,
                    context_provider={
                        "setup_name": setup_name
                    },
                    decision_reason="No performance data available"
                )
                # <Context-Aware Logging Integration - End>
                return 0.5
                
            thresholds = self.config.get('setup_performance_thresholds', {})
            min_trades = thresholds.get('min_trades_for_bias', 10)
            min_win_rate = thresholds.get('min_win_rate', 0.4)
            min_profit_factor = thresholds.get('min_profit_factor', 1.2)
            
            if (performance.get('total_trades', 0) < min_trades or
                performance.get('win_rate', 0) < min_win_rate or
                performance.get('profit_factor', 0) < min_profit_factor):
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.EXECUTION_DECISION,
                    f"Setup {setup_name} below thresholds, returning 0.3",
                    symbol=safe_symbol,
                    context_provider={
                        "setup_name": setup_name,
                        "total_trades": performance.get('total_trades', 0),
                        "win_rate": performance.get('win_rate', 0),
                        "profit_factor": performance.get('profit_factor', 0),
                        "min_trades": min_trades,
                        "min_win_rate": min_win_rate,
                        "min_profit_factor": min_profit_factor
                    },
                    decision_reason="Setup below performance thresholds"
                )
                # <Context-Aware Logging Integration - End>
                return 0.3
                
            win_rate = performance.get('win_rate', 0.5)
            profit_factor = min(performance.get('profit_factor', 1.0), 5.0)
            
            score = (win_rate * 0.6) + (profit_factor * 0.4) / 5.0
            result = max(0.1, min(score, 1.0))
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Setup bias calculation completed for {setup_name}",
                symbol=safe_symbol,
                context_provider={
                    "setup_name": setup_name,
                    "score": result,
                    "win_rate": win_rate,
                    "profit_factor": profit_factor,
                    "total_trades": performance.get('total_trades', 0)
                },
                decision_reason="Setup bias calculation successful"
            )
            # <Context-Aware Logging Integration - End>
            return result
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Error calculating setup bias for {setup_name}",
                symbol=safe_symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "setup_name": setup_name
                },
                decision_reason="Setup bias calculation failed"
            )
            # <Context-Aware Logging Integration - End>
            return 0.5
    # Calculate bias based on historical setup performance - End

    # <Two-Layer Prioritization - Begin>
    def calculate_risk_reward_score(self, order: PlannedOrder) -> float:
        """Calculate score based on risk/reward ratio quality."""
        safe_symbol = getattr(order, 'symbol', 'Unknown')
        rr_ratio = getattr(order, 'risk_reward_ratio', 0)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Calculating risk/reward score for {safe_symbol}",
            symbol=safe_symbol,
            context_provider={
                "risk_reward_ratio": rr_ratio
            }
        )
        # <Context-Aware Logging Integration - End>
            
        rr_ratio = order.risk_reward_ratio
        
        # Base scoring: 1:1 → 0.5, 3:1 → 1.0, 5:1 → 1.2 (capped)
        rr_score = min(0.5 + (rr_ratio - 1) * 0.25, 1.2)
        
        # Adjust for probability of achieving reward (higher R/R often has lower probability)
        probability_adjustment = 1.0 - (rr_ratio - 1) * 0.1
        rr_score *= max(probability_adjustment, 0.6)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            f"Risk/reward score calculation completed for {safe_symbol}",
            symbol=safe_symbol,
            context_provider={
                "risk_reward_ratio": rr_ratio,
                "base_score": min(0.5 + (rr_ratio - 1) * 0.25, 1.2),
                "probability_adjustment": probability_adjustment,
                "final_score": rr_score
            },
            decision_reason="Risk/reward score calculation successful"
        )
        # <Context-Aware Logging Integration - End>
        return rr_score

    def calculate_quality_score(self, order: PlannedOrder, total_capital: float) -> Dict:
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
        efficiency = self.calculate_efficiency(order, total_capital)
        
        # Risk/reward score
        risk_reward_score = self.calculate_risk_reward_score(order)
        
        # Advanced features (if enabled)
        timeframe_match = self.calculate_timeframe_match_score(order)
        setup_bias = self.calculate_setup_bias_score(order)
        
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

    # <Two-Layer Prioritization - End>

    # Compute final score using Phase B formula - Begin
    def calculate_deterministic_score(self, order: PlannedOrder, fill_prob: float, 
                                   total_capital: float, current_scores: Optional[List[float]] = None) -> Dict:
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
        
        efficiency = self.calculate_efficiency(order, total_capital)
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
            
        timeframe_match = self.calculate_timeframe_match_score(order)
        setup_bias = self.calculate_setup_bias_score(order)
        
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
    # Compute final score using Phase B formula - End

    def _prioritize_orders_legacy(self, executable_orders: List[Dict], total_capital: float,
                                current_working_orders: Optional[List] = None) -> List[Dict]:
        """Legacy single-layer prioritization for backward compatibility."""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Using legacy single-layer prioritization",
            context_provider={
                "executable_orders_count": len(executable_orders),
                "total_capital": total_capital
            }
        )
        # <Context-Aware Logging Integration - End>
            
        if not executable_orders:
            return []
        
        committed_capital = 0.0
        working_order_count = 0
        if current_working_orders:
            committed_capital = sum(order.get('capital_commitment', 0) 
                                for order in current_working_orders)
            working_order_count = len(current_working_orders)
        
        available_capital = total_capital * self.config['max_capital_utilization'] - committed_capital
        available_capital = max(0, available_capital)
        
        available_slots = self.config['max_open_orders'] - working_order_count
        available_slots = max(0, available_slots)
        
        # Calculate scores using legacy method
        scored_orders = []
        for order_data in executable_orders:
            order = order_data['order']
            safe_symbol = getattr(order, 'symbol', 'Unknown')
            fill_prob = order_data.get('fill_probability', 0)
            
            score_result = self.calculate_deterministic_score(order, fill_prob, total_capital)
            
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity if order.entry_price else 0
            
            scored_order = {
                **order_data,
                'deterministic_score': score_result['final_score'],
                'score_components': score_result['components'],
                'quantity': quantity,
                'capital_commitment': capital_commitment,
                'allocated': False,
                'allocation_reason': 'Pending allocation',
                'viable': True  # All orders are viable in legacy mode
            }
            scored_orders.append(scored_order)
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Legacy scoring completed for {safe_symbol}",
                symbol=safe_symbol,
                context_provider={
                    "deterministic_score": score_result['final_score'],
                    "capital_commitment": capital_commitment,
                    "fill_probability": fill_prob
                }
            )
            # <Context-Aware Logging Integration - End>
        
        # Sort by score (highest first)
        scored_orders.sort(key=lambda x: x['deterministic_score'], reverse=True)
        
        # Allocate to top orders
        allocated_orders = []
        total_allocated_capital = 0
        allocated_count = 0
        
        for order in scored_orders:
            safe_symbol = getattr(order['order'], 'symbol', 'Unknown')
            
            if allocated_count >= available_slots:
                order['allocation_reason'] = 'Max open orders reached'
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Legacy: Order {safe_symbol} not allocated - max open orders",
                    symbol=safe_symbol,
                    context_provider={
                        "allocated_count": allocated_count,
                        "available_slots": available_slots
                    }
                )
                # <Context-Aware Logging Integration - End>
                continue
                
            if total_allocated_capital + order['capital_commitment'] > available_capital:
                order['allocation_reason'] = 'Insufficient capital'
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Legacy: Order {safe_symbol} not allocated - insufficient capital",
                    symbol=safe_symbol,
                    context_provider={
                        "total_allocated_capital": total_allocated_capital,
                        "order_capital_commitment": order['capital_commitment'],
                        "available_capital": available_capital
                    }
                )
                # <Context-Aware Logging Integration - End>
                continue
                
            order['allocated'] = True
            order['allocation_reason'] = 'Allocated'
            total_allocated_capital += order['capital_commitment']
            allocated_count += 1
            allocated_orders.append(order)
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Legacy: Order {safe_symbol} allocated",
                symbol=safe_symbol,
                context_provider={
                    "deterministic_score": order['deterministic_score'],
                    "capital_commitment": order['capital_commitment']
                }
            )
            # <Context-Aware Logging Integration - End>
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Legacy prioritization completed",
            context_provider={
                "allocated_orders_count": len(allocated_orders),
                "total_allocated_capital": total_allocated_capital
            }
        )
        # <Context-Aware Logging Integration - End>
        return scored_orders

    # Generate summary of prioritization results - Begin
    def get_prioritization_summary(self, prioritized_orders: List[Dict]) -> Dict:
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Generating prioritization summary",
            context_provider={
                "prioritized_orders_count": len(prioritized_orders)
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
            "Prioritization summary generated",
            context_provider=summary,
            decision_reason="Summary calculation completed"
        )
        # <Context-Aware Logging Integration - End>
        return summary
    # Generate summary of prioritization results - End
# Prioritization Service - Main class definition - End

    # Fix timeout decorator implementation - Begin
    @staticmethod
    def timeout(seconds=10, error_message="Function call timed out"):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                def timeout_handler(signum, frame):
                    raise TimeoutError(error_message)
                
                # Set up signal handler (Unix/Linux/Mac only)
                try:
                    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(seconds)
                    
                    try:
                        result = func(*args, **kwargs)
                    finally:
                        # Restore original signal handler
                        signal.alarm(0)
                        signal.signal(signal.SIGALRM, old_handler)
                    
                    return result
                except (AttributeError, ValueError):
                    # Windows compatibility - signal.SIGALRM not available
                    # Fall back to no timeout on Windows
                    return func(*args, **kwargs)
            return wrapper
        return decorator
    # Fix timeout decorator implementation - End

    # Fix prioritize_orders with comprehensive safety checks - Begin
    def prioritize_orders(self, executable_orders: List[Dict], total_capital: float, 
                        current_working_orders: Optional[List] = None) -> List[Dict]:
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            f"Starting prioritization of {len(executable_orders)} orders",
            context_provider={
                "total_capital": total_capital,
                "executable_orders_count": len(executable_orders),
                "current_working_orders_count": len(current_working_orders) if current_working_orders else 0
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
                    "executable_orders_length": len(executable_orders) if executable_orders else 0
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
                        "order_data_type": type(order_data).__name__
                    },
                    decision_reason="Invalid order data format"
                )
                continue
                
            order = order_data.get('order')
            if order is None:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Skipping order with None order object at index {i}",
                    context_provider={},
                    decision_reason="Missing order object"
                )
                continue
                
            if not hasattr(order, 'symbol'):
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Skipping order without symbol at index {i}",
                    context_provider={
                        "order_type": type(order).__name__
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
                    "valid_count": len(valid_executable_orders)
                },
                decision_reason="Order validation completed"
            )
        
        if not valid_executable_orders:
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                "No valid executable orders to prioritize",
                context_provider={},
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
                context_provider={},
                decision_reason="Two-layer prioritization disabled"
            )
            # Fall back to legacy single-layer prioritization
            return self._prioritize_orders_legacy(valid_executable_orders, total_capital, current_working_orders)

        # CRITICAL FIX: Add timeout protection for the entire prioritization process
        try:
            return self._prioritize_orders_with_timeout(valid_executable_orders, total_capital, current_working_orders)
        except TimeoutError as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Prioritization timed out - using fallback",
                context_provider={
                    "error_message": str(e),
                    "timeout_seconds": 30
                },
                decision_reason="Prioritization timeout - falling back to legacy mode"
            )
            # Fall back to legacy mode on timeout
            return self._prioritize_orders_legacy(valid_executable_orders, total_capital, current_working_orders)
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Prioritization failed with error - using fallback",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason="Prioritization error - falling back to legacy mode"
            )
            # Fall back to legacy mode on any error
            return self._prioritize_orders_legacy(valid_executable_orders, total_capital, current_working_orders)
    # Fix prioritize_orders with comprehensive safety checks - End

    # is_order_viable - Begin (UPDATED - remove probability threshold)
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
    # is_order_viable - End

    # _prioritize_orders_with_timeout - Begin (UPDATED - remove viability filtering)
    @timeout(seconds=30, error_message="Two-layer prioritization timed out after 30 seconds")
    def _prioritize_orders_with_timeout(self, executable_orders: List[Dict], total_capital: float, 
                                    current_working_orders: Optional[List] = None) -> List[Dict]:
        """Two-layer prioritization with timeout protection.
        
        UPDATED: All orders are considered viable - probability affects sequence only.
        """
        # ... (rest of method setup code remains unchanged)
        
        committed_capital = 0.0
        working_order_count = 0
        if current_working_orders:
            committed_capital = sum(order.get('capital_commitment', 0) 
                                for order in current_working_orders)
            working_order_count = len(current_working_orders)
        
        available_capital = total_capital * self.config['max_capital_utilization'] - committed_capital
        available_capital = max(0, available_capital)
        
        available_slots = self.config['max_open_orders'] - working_order_count
        available_slots = max(0, available_slots)
        
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Capital and slot availability calculated",
            context_provider={
                "available_capital": available_capital,
                "available_slots": available_slots,
                "committed_capital": committed_capital,
                "working_order_count": working_order_count,
                "max_capital_utilization": self.config['max_capital_utilization'],
                "max_open_orders": self.config['max_open_orders']
            }
        )

        # UPDATED: First pass - ALL orders are considered viable, calculate quality scores for all
        viable_orders = []
        
        for order_data in executable_orders:
            order = order_data['order']
            safe_symbol = getattr(order, 'symbol', 'Unknown')
            
            # UPDATED: All orders are viable - probability affects sequence only
            # Calculate quality score for ALL orders with error handling
            try:
                quality_result = self.calculate_quality_score(order, total_capital)
                
                # Safe quantity and capital commitment calculation
                quantity = 0
                capital_commitment = 0
                try:
                    quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
                    if hasattr(order, 'entry_price') and order.entry_price is not None:
                        capital_commitment = order.entry_price * quantity
                except Exception as e:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Capital commitment calculation failed for {safe_symbol}",
                        symbol=safe_symbol,
                        context_provider={
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        decision_reason="Capital calculation error"
                    )
                    # Skip orders with capital calculation errors
                    continue
                
                viable_order = {
                    **order_data,
                    'quality_score': quality_result['quality_score'],
                    'quality_components': quality_result['components'],
                    'quantity': quantity,
                    'capital_commitment': capital_commitment,
                    'viable': True,  # UPDATED: All orders are viable
                    'allocation_reason': 'Viable - awaiting allocation',
                    'allocated': False
                }
                viable_orders.append(viable_order)
                
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Order {safe_symbol} processed for prioritization",
                    symbol=safe_symbol,
                    context_provider={
                        "quality_score": quality_result['quality_score'],
                        "capital_commitment": capital_commitment,
                        "quantity": quantity,
                        "fill_probability": order_data.get('fill_probability', 0)
                    },
                    decision_reason="Order processed for prioritization - probability affects sequence"
                )
                
            except Exception as e:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Quality score calculation failed for {safe_symbol}",
                    symbol=safe_symbol,
                    context_provider={
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    decision_reason="Quality score calculation error - order skipped"
                )
                # Skip orders with quality calculation errors
        
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Viability processing completed - all orders considered viable",
            context_provider={
                "viable_orders_count": len(viable_orders),
                "original_orders_count": len(executable_orders)
            }
        )

        # Sort viable orders by quality score (highest first) - probability affects sequence here
        viable_orders.sort(key=lambda x: x.get('quality_score', 0), reverse=True)
        
        # Second pass: Allocate capital to top orders based on quality score
        allocated_orders = []
        total_allocated_capital = 0
        allocated_count = 0
        
        for order in viable_orders:
            safe_symbol = getattr(order['order'], 'symbol', 'Unknown')
            
            if allocated_count >= available_slots:
                order['allocation_reason'] = 'Max open orders reached'
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Order {safe_symbol} not allocated - max open orders reached",
                    symbol=safe_symbol,
                    context_provider={
                        "allocated_count": allocated_count,
                        "available_slots": available_slots
                    },
                    decision_reason="Order slot limit reached"
                )
                continue
                
            capital_commitment = order.get('capital_commitment', 0)
            if total_allocated_capital + capital_commitment > available_capital:
                order['allocation_reason'] = 'Insufficient capital'
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Order {safe_symbol} not allocated - insufficient capital",
                    symbol=safe_symbol,
                    context_provider={
                        "total_allocated_capital": total_allocated_capital,
                        "order_capital_commitment": capital_commitment,
                        "available_capital": available_capital
                    },
                    decision_reason="Capital limit reached"
                )
                continue
                
            order['allocated'] = True
            order['allocation_reason'] = 'Allocated'
            total_allocated_capital += capital_commitment
            allocated_count += 1
            allocated_orders.append(order)
            
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Order {safe_symbol} allocated successfully",
                symbol=safe_symbol,
                context_provider={
                    "quality_score": order.get('quality_score', 0),
                    "capital_commitment": capital_commitment,
                    "total_allocated_capital": total_allocated_capital,
                    "allocated_count": allocated_count
                },
                decision_reason="Order allocated based on quality score"
            )
        
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Prioritization allocation completed",
            context_provider={
                "allocated_orders_count": len(allocated_orders),
                "total_allocated_capital": total_allocated_capital,
                "available_capital_remaining": available_capital - total_allocated_capital,
                "available_slots_remaining": available_slots - allocated_count
            },
            decision_reason="Prioritization process completed - probability used for sequence only"
        )

        # Return all processed orders (both allocated and not allocated)
        result = viable_orders
        
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Prioritization process finished",
            context_provider={
                "total_orders_processed": len(result),
                "allocated_orders": len(allocated_orders),
                "viable_orders_not_allocated": len(viable_orders) - len(allocated_orders)
            }
        )
        return result
    # _prioritize_orders_with_timeout - End