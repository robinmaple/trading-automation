"""
Historical Performance Service for Phase B - Tracks and analyzes trading setup performance.
Provides data-driven insights for setup bias scoring in prioritization decisions.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from decimal import Decimal

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)

# Historical Performance Service - Main class definition - Begin
class HistoricalPerformanceService:
    """Service for tracking and analyzing historical performance of trading setups."""
    
    # Initialize service with order persistence dependency - Begin
    def __init__(self, order_persistence):
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing Historical Performance Service",
            context_provider={
                "order_persistence_provided": order_persistence is not None,
                "order_persistence_type": type(order_persistence).__name__,
                "cache_expiry_minutes": 30
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self.order_persistence = order_persistence
        self._cache = {}
        self._cache_expiry = timedelta(minutes=30)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Historical Performance Service initialized successfully",
            context_provider={},
            decision_reason="Service initialization completed"
        )
        # <Context-Aware Logging Integration - End>
    # Initialize service with order persistence dependency - End

    # Get performance metrics for specific trading setup - Begin
    def get_setup_performance(self, setup_name: str, days_back: int = 90) -> Optional[Dict]:
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Getting performance for setup: {setup_name}",
            context_provider={
                "setup_name": setup_name,
                "days_back": days_back
            }
        )
        # <Context-Aware Logging Integration - End>
        
        cache_key = f"{setup_name}_{days_back}"
        if self._is_cache_valid(cache_key):
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Using cached performance data for {setup_name}",
                context_provider={
                    "setup_name": setup_name,
                    "cache_key": cache_key
                }
            )
            # <Context-Aware Logging Integration - End>
            return self._cache[cache_key]['value']
            
        try:
            trades = self.order_persistence.get_trades_by_setup(setup_name, days_back)
            
            if not trades:
                self._cache[cache_key] = {'value': None, 'expiry': self._get_cache_expiry()}
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"No trades found for setup: {setup_name}",
                    context_provider={
                        "setup_name": setup_name,
                        "days_back": days_back,
                        "trades_found": 0
                    },
                    decision_reason="No performance data available"
                )
                # <Context-Aware Logging Integration - End>
                return None
            
            performance = self._calculate_performance_metrics(trades)
            
            self._cache[cache_key] = {
                'value': performance,
                'expiry': self._get_cache_expiry()
            }
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Performance calculated for setup: {setup_name}",
                context_provider={
                    "setup_name": setup_name,
                    "total_trades": performance['total_trades'],
                    "win_rate": performance['win_rate'],
                    "profit_factor": performance['profit_factor'],
                    "winning_trades": performance['winning_trades'],
                    "losing_trades": performance['losing_trades'],
                    "cache_key": cache_key
                },
                decision_reason="Setup performance calculation completed"
            )
            # <Context-Aware Logging Integration - End>
            
            return performance
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error getting performance for setup '{setup_name}'",
                context_provider={
                    "setup_name": setup_name,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason="Setup performance retrieval failed"
            )
            # <Context-Aware Logging Integration - End>
            return None
    # Get performance metrics for specific trading setup - End

    # Get performance metrics for all trading setups - Begin
    def get_all_setups_performance(self, days_back: int = 90) -> Dict[str, Dict]:
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Getting performance for all trading setups",
            context_provider={
                "days_back": days_back
            }
        )
        # <Context-Aware Logging Integration - End>
        
        cache_key = f"all_setups_{days_back}"
        if self._is_cache_valid(cache_key):
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Using cached performance data for all setups",
                context_provider={
                    "cache_key": cache_key,
                    "cached_setups_count": len(self._cache[cache_key]['value'])
                }
            )
            # <Context-Aware Logging Integration - End>
            return self._cache[cache_key]['value']
            
        try:
            setups = self.order_persistence.get_all_trading_setups(days_back)
            performance_data = {}
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Found {len(setups)} trading setups to analyze",
                context_provider={
                    "total_setups_found": len(setups),
                    "setups_list": setups
                }
            )
            # <Context-Aware Logging Integration - End>
            
            for setup in setups:
                perf = self.get_setup_performance(setup, days_back)
                if perf:
                    performance_data[setup] = perf
            
            self._cache[cache_key] = {
                'value': performance_data,
                'expiry': self._get_cache_expiry()
            }
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "All setups performance analysis completed",
                context_provider={
                    "total_setups_analyzed": len(performance_data),
                    "days_back": days_back,
                    "cache_key": cache_key
                },
                decision_reason="Bulk performance analysis completed"
            )
            # <Context-Aware Logging Integration - End>
            
            return performance_data
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error getting all setups performance",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "days_back": days_back
                },
                decision_reason="Bulk performance analysis failed"
            )
            # <Context-Aware Logging Integration - End>
            return {}
    # Get performance metrics for all trading setups - End

    # Calculate bias score based on historical performance - Begin
    def get_setup_bias_score(self, setup_name: str, days_back: int = 90, 
                           min_trades: int = 10, min_win_rate: float = 0.4,
                           min_profit_factor: float = 1.2) -> float:
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            f"Calculating bias score for setup: {setup_name}",
            context_provider={
                "setup_name": setup_name,
                "days_back": days_back,
                "min_trades": min_trades,
                "min_win_rate": min_win_rate,
                "min_profit_factor": min_profit_factor
            }
        )
        # <Context-Aware Logging Integration - End>
        
        try:
            performance = self.get_setup_performance(setup_name, days_back)
            
            if not performance:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    f"No performance data for setup: {setup_name}, returning default score",
                    context_provider={
                        "setup_name": setup_name
                    },
                    decision_reason="Bias score calculation - no performance data"
                )
                # <Context-Aware Logging Integration - End>
                return 0.5
            
            if (performance['total_trades'] < min_trades or
                performance['win_rate'] < min_win_rate or
                performance['profit_factor'] < min_profit_factor):
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    f"Setup {setup_name} below performance thresholds, returning low score",
                    context_provider={
                        "setup_name": setup_name,
                        "total_trades": performance['total_trades'],
                        "win_rate": performance['win_rate'],
                        "profit_factor": performance['profit_factor'],
                        "min_trades": min_trades,
                        "min_win_rate": min_win_rate,
                        "min_profit_factor": min_profit_factor
                    },
                    decision_reason="Bias score calculation - below thresholds"
                )
                # <Context-Aware Logging Integration - End>
                return 0.3
            
            win_rate = performance['win_rate']
            profit_factor = min(performance['profit_factor'], 5.0)
            
            score = (win_rate * 0.6) + (profit_factor * 0.4) / 5.0
            final_score = max(0.1, min(score, 1.0))
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                f"Bias score calculated for setup: {setup_name}",
                context_provider={
                    "setup_name": setup_name,
                    "final_bias_score": final_score,
                    "win_rate": win_rate,
                    "profit_factor": profit_factor,
                    "score_components": {
                        "win_rate_contribution": win_rate * 0.6,
                        "profit_factor_contribution": (profit_factor * 0.4) / 5.0
                    }
                },
                decision_reason="Bias score calculation completed"
            )
            # <Context-Aware Logging Integration - End>
            
            return final_score
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                f"Error calculating bias score for '{setup_name}'",
                context_provider={
                    "setup_name": setup_name,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason="Bias score calculation failed"
            )
            # <Context-Aware Logging Integration - End>
            return 0.5
    # Calculate bias score based on historical performance - End

    # Get top performing trading setups - Begin
    def get_top_performing_setups(self, days_back: int = 90, limit: int = 5) -> List[Dict]:
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Identifying top performing trading setups",
            context_provider={
                "days_back": days_back,
                "limit": limit
            }
        )
        # <Context-Aware Logging Integration - End>
        
        try:
            all_performance = self.get_all_setups_performance(days_back)
            
            qualified_setups = {}
            for setup_name, performance in all_performance.items():
                if (performance['total_trades'] >= 10 and
                    performance['win_rate'] >= 0.4 and
                    performance['profit_factor'] >= 1.2):
                    qualified_setups[setup_name] = performance
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Qualified setups for top performance analysis",
                context_provider={
                    "total_setups_analyzed": len(all_performance),
                    "qualified_setups_count": len(qualified_setups),
                    "qualification_rate": len(qualified_setups) / len(all_performance) if all_performance else 0
                }
            )
            # <Context-Aware Logging Integration - End>
            
            scored_setups = []
            for setup_name, performance in qualified_setups.items():
                score = self.get_setup_bias_score(setup_name, days_back)
                scored_setups.append({
                    'setup_name': setup_name,
                    'performance_score': score,
                    **performance
                })
            
            scored_setups.sort(key=lambda x: x['performance_score'], reverse=True)
            top_setups = scored_setups[:limit]
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Top performing setups identified",
                context_provider={
                    "top_setups_count": len(top_setups),
                    "top_setup_names": [s['setup_name'] for s in top_setups],
                    "top_scores": [s['performance_score'] for s in top_setups],
                    "requested_limit": limit
                },
                decision_reason="Top setups identification completed"
            )
            # <Context-Aware Logging Integration - End>
            
            return top_setups
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error getting top performing setups",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason="Top setups identification failed"
            )
            # <Context-Aware Logging Integration - End>
            return []
    # Get top performing trading setups - End

    # Get overall performance summary across all setups - Begin
    def get_performance_summary(self) -> Dict:
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Generating overall performance summary",
            context_provider={}
        )
        # <Context-Aware Logging Integration - End>
        
        try:
            all_performance = self.get_all_setups_performance()
            
            if not all_performance:
                summary = {
                    'total_setups': 0,
                    'total_trades': 0,
                    'overall_win_rate': 0,
                    'overall_profit_factor': 0
                }
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "No performance data available for summary",
                    context_provider=summary,
                    decision_reason="Performance summary - no data"
                )
                # <Context-Aware Logging Integration - End>
                return summary
            
            total_trades = sum(perf['total_trades'] for perf in all_performance.values())
            total_profit = sum(perf['total_profit'] for perf in all_performance.values())
            total_loss = sum(perf['total_loss'] for perf in all_performance.values())
            
            winning_trades = sum(perf['winning_trades'] for perf in all_performance.values())
            overall_win_rate = winning_trades / total_trades if total_trades > 0 else 0
            overall_profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
            
            summary = {
                'total_setups': len(all_performance),
                'total_trades': total_trades,
                'overall_win_rate': round(overall_win_rate, 4),
                'overall_profit_factor': round(overall_profit_factor, 2),
                'total_profit': round(total_profit, 2),
                'total_loss': round(total_loss, 2),
                'timestamp': datetime.now().isoformat()
            }
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Overall performance summary generated",
                context_provider=summary,
                decision_reason="Performance summary generation completed"
            )
            # <Context-Aware Logging Integration - End>
            
            return summary
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error getting performance summary",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                decision_reason="Performance summary generation failed"
            )
            # <Context-Aware Logging Integration - End>
            return {
                'total_setups': 0,
                'total_trades': 0,
                'overall_win_rate': 0,
                'overall_profit_factor': 0
            }
    # Get overall performance summary across all setups - End

    # Calculate comprehensive performance metrics - Begin
    def _calculate_performance_metrics(self, trades: List[Dict]) -> Dict:
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Calculating comprehensive performance metrics",
            context_provider={
                "trades_count": len(trades)
            }
        )
        # <Context-Aware Logging Integration - End>
        
        if not trades:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No trades provided for performance calculation",
                context_provider={},
                decision_reason="Performance calculation skipped - no trades"
            )
            # <Context-Aware Logging Integration - End>
            return None
        
        winning_trades = [t for t in trades if t.get('pnl', 0) > 0]
        losing_trades = [t for t in trades if t.get('pnl', 0) <= 0]
        
        total_trades = len(trades)
        winning_trades_count = len(winning_trades)
        losing_trades_count = len(losing_trades)
        
        win_rate = winning_trades_count / total_trades if total_trades > 0 else 0
        
        total_profit = sum(t.get('pnl', 0) for t in winning_trades)
        total_loss = abs(sum(t.get('pnl', 0) for t in losing_trades))
        
        profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
        avg_pnl = sum(t.get('pnl', 0) for t in trades) / total_trades if total_trades > 0 else 0
        
        avg_win = total_profit / winning_trades_count if winning_trades_count > 0 else 0
        avg_loss = total_loss / losing_trades_count if losing_trades_count > 0 else 0
        risk_reward_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')
        
        holding_periods = []
        for trade in trades:
            if trade.get('entry_time') and trade.get('exit_time'):
                holding_period = (trade['exit_time'] - trade['entry_time']).total_seconds() / 60
                holding_periods.append(holding_period)
        
        avg_holding_period = sum(holding_periods) / len(holding_periods) if holding_periods else 0
        
        performance_metrics = {
            'total_trades': total_trades,
            'winning_trades': winning_trades_count,
            'losing_trades': losing_trades_count,
            'win_rate': round(win_rate, 4),
            'profit_factor': round(min(profit_factor, 10.0), 2),
            'total_profit': round(total_profit, 2),
            'total_loss': round(total_loss, 2),
            'avg_pnl': round(avg_pnl, 2),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'risk_reward_ratio': round(min(risk_reward_ratio, 10.0), 2),
            'avg_holding_period': round(avg_holding_period, 1),
            'analysis_period_days': 90
        }
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Performance metrics calculation completed",
            context_provider=performance_metrics,
            decision_reason="Performance metrics calculation finished"
        )
        # <Context-Aware Logging Integration - End>
        
        return performance_metrics
    # Calculate comprehensive performance metrics - End

    # Check if cached value is still valid - Begin
    def _is_cache_valid(self, cache_key: str) -> bool:
        if cache_key in self._cache:
            cached_data = self._cache[cache_key]
            is_valid = datetime.now() < cached_data['expiry']
            # <Context-Aware Logging Integration - Begin>
            if is_valid:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Cache hit for key: {cache_key}",
                    context_provider={
                        "cache_key": cache_key,
                        "cache_size": len(self._cache),
                        "cache_valid": True
                    }
                )
            # <Context-Aware Logging Integration - End>
            return is_valid
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Cache miss for key: {cache_key}",
            context_provider={
                "cache_key": cache_key,
                "cache_size": len(self._cache),
                "cache_valid": False
            }
        )
        # <Context-Aware Logging Integration - End>
        return False
    # Check if cached value is still valid - End

    # Get cache expiry timestamp - Begin
    def _get_cache_expiry(self) -> datetime:
        expiry = datetime.now() + self._cache_expiry
        return expiry
    # Get cache expiry timestamp - End

    # Clear all cached performance data - Begin
    def clear_cache(self):
        cache_size_before = len(self._cache)
        self._cache = {}
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Performance cache cleared",
            context_provider={
                "cache_entries_cleared": cache_size_before,
                "cache_size_after": 0
            },
            decision_reason="Cache clearing completed"
        )
        # <Context-Aware Logging Integration - End>
    # Clear all cached performance data - End
# Historical Performance Service - Main class definition - End