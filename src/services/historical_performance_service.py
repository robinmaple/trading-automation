"""
Historical Performance Service for Phase B - Tracks and analyzes trading setup performance.
Provides data-driven insights for setup bias scoring in prioritization decisions.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

# Historical Performance Service - Main class definition - Begin
class HistoricalPerformanceService:
    """Service for tracking and analyzing historical performance of trading setups."""
    
    # Initialize service with order persistence dependency - Begin
    def __init__(self, order_persistence):
        self.order_persistence = order_persistence
        self._cache = {}
        self._cache_expiry = timedelta(minutes=30)
        logger.info("Historical Performance Service initialized")
    # Initialize service with order persistence dependency - End

    # Get performance metrics for specific trading setup - Begin
    def get_setup_performance(self, setup_name: str, days_back: int = 90) -> Optional[Dict]:
        cache_key = f"{setup_name}_{days_back}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['value']
            
        try:
            trades = self.order_persistence.get_trades_by_setup(setup_name, days_back)
            
            if not trades:
                self._cache[cache_key] = {'value': None, 'expiry': self._get_cache_expiry()}
                return None
            
            performance = self._calculate_performance_metrics(trades)
            
            self._cache[cache_key] = {
                'value': performance,
                'expiry': self._get_cache_expiry()
            }
            
            logger.info(f"Performance for setup '{setup_name}': {performance}")
            return performance
            
        except Exception as e:
            logger.error(f"Error getting performance for setup '{setup_name}': {e}")
            return None
    # Get performance metrics for specific trading setup - End

    # Get performance metrics for all trading setups - Begin
    def get_all_setups_performance(self, days_back: int = 90) -> Dict[str, Dict]:
        cache_key = f"all_setups_{days_back}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['value']
            
        try:
            setups = self.order_persistence.get_all_trading_setups(days_back)
            performance_data = {}
            
            for setup in setups:
                perf = self.get_setup_performance(setup, days_back)
                if perf:
                    performance_data[setup] = perf
            
            self._cache[cache_key] = {
                'value': performance_data,
                'expiry': self._get_cache_expiry()
            }
            
            logger.info(f"Performance data for {len(performance_data)} setups loaded")
            return performance_data
            
        except Exception as e:
            logger.error(f"Error getting all setups performance: {e}")
            return {}
    # Get performance metrics for all trading setups - End

    # Calculate bias score based on historical performance - Begin
    def get_setup_bias_score(self, setup_name: str, days_back: int = 90, 
                           min_trades: int = 10, min_win_rate: float = 0.4,
                           min_profit_factor: float = 1.2) -> float:
        try:
            performance = self.get_setup_performance(setup_name, days_back)
            
            if not performance:
                return 0.5
            
            if (performance['total_trades'] < min_trades or
                performance['win_rate'] < min_win_rate or
                performance['profit_factor'] < min_profit_factor):
                return 0.3
            
            win_rate = performance['win_rate']
            profit_factor = min(performance['profit_factor'], 5.0)
            
            score = (win_rate * 0.6) + (profit_factor * 0.4) / 5.0
            
            return max(0.1, min(score, 1.0))
            
        except Exception as e:
            logger.error(f"Error calculating bias score for '{setup_name}': {e}")
            return 0.5
    # Calculate bias score based on historical performance - End

    # Get top performing trading setups - Begin
    def get_top_performing_setups(self, days_back: int = 90, limit: int = 5) -> List[Dict]:
        try:
            all_performance = self.get_all_setups_performance(days_back)
            
            qualified_setups = {}
            for setup_name, performance in all_performance.items():
                if (performance['total_trades'] >= 10 and
                    performance['win_rate'] >= 0.4 and
                    performance['profit_factor'] >= 1.2):
                    qualified_setups[setup_name] = performance
            
            scored_setups = []
            for setup_name, performance in qualified_setups.items():
                score = self.get_setup_bias_score(setup_name, days_back)
                scored_setups.append({
                    'setup_name': setup_name,
                    'performance_score': score,
                    **performance
                })
            
            scored_setups.sort(key=lambda x: x['performance_score'], reverse=True)
            
            return scored_setups[:limit]
            
        except Exception as e:
            logger.error(f"Error getting top performing setups: {e}")
            return []
    # Get top performing trading setups - End

    # Get overall performance summary across all setups - Begin
    def get_performance_summary(self) -> Dict:
        try:
            all_performance = self.get_all_setups_performance()
            
            if not all_performance:
                return {
                    'total_setups': 0,
                    'total_trades': 0,
                    'overall_win_rate': 0,
                    'overall_profit_factor': 0
                }
            
            total_trades = sum(perf['total_trades'] for perf in all_performance.values())
            total_profit = sum(perf['total_profit'] for perf in all_performance.values())
            total_loss = sum(perf['total_loss'] for perf in all_performance.values())
            
            winning_trades = sum(perf['winning_trades'] for perf in all_performance.values())
            overall_win_rate = winning_trades / total_trades if total_trades > 0 else 0
            overall_profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
            
            return {
                'total_setups': len(all_performance),
                'total_trades': total_trades,
                'overall_win_rate': round(overall_win_rate, 4),
                'overall_profit_factor': round(overall_profit_factor, 2),
                'total_profit': round(total_profit, 2),
                'total_loss': round(total_loss, 2),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting performance summary: {e}")
            return {
                'total_setups': 0,
                'total_trades': 0,
                'overall_win_rate': 0,
                'overall_profit_factor': 0
            }
    # Get overall performance summary across all setups - End

    # Calculate comprehensive performance metrics - Begin
    def _calculate_performance_metrics(self, trades: List[Dict]) -> Dict:
        if not trades:
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
        
        return {
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
    # Calculate comprehensive performance metrics - End

    # Check if cached value is still valid - Begin
    def _is_cache_valid(self, cache_key: str) -> bool:
        if cache_key in self._cache:
            cached_data = self._cache[cache_key]
            return datetime.now() < cached_data['expiry']
        return False
    # Check if cached value is still valid - End

    # Get cache expiry timestamp - Begin
    def _get_cache_expiry(self) -> datetime:
        return datetime.now() + self._cache_expiry
    # Get cache expiry timestamp - End

    # Clear all cached performance data - Begin
    def clear_cache(self):
        self._cache = {}
        logger.info("Performance cache cleared")
    # Clear all cached performance data - End
# Historical Performance Service - Main class definition - End