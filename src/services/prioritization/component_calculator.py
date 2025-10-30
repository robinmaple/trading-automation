"""
Component calculation service for prioritization scoring.
Handles individual score components like efficiency, timeframe matching, setup bias, and risk/reward.
"""

from typing import Optional
from src.core.context_aware_logger import get_context_logger, TradingEventType


class ComponentCalculator:
    """Calculates individual score components for order prioritization."""
    
    def __init__(self, sizing_service, config, market_context_service=None, historical_performance_service=None):
        self.context_logger = get_context_logger()
        self.sizing_service = sizing_service
        self.config = config
        self.market_context_service = market_context_service
        self.historical_performance_service = historical_performance_service

    def calculate_efficiency(self, order, total_capital: float) -> float:
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

    def calculate_timeframe_match_score(self, order) -> float:
        """Calculate timeframe compatibility with market conditions."""
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

    def calculate_setup_bias_score(self, order) -> float:
        """Calculate bias based on historical setup performance."""
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

    def calculate_risk_reward_score(self, order) -> float:
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