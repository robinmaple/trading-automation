"""
Historical Performance Service for Phase B - Tracks and analyzes trading setup performance.
Provides data-driven insights for setup bias scoring in prioritization decisions.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

class HistoricalPerformanceService:
    """
    Service for tracking and analyzing historical performance of trading setups.
    Provides data for setup bias scoring in the prioritization process.
    """
    
    def __init__(self, order_persistence):
        """
        Initialize the historical performance service.
        
        Args:
            order_persistence: OrderPersistenceService instance for data access
        """
        self.order_persistence = order_persistence
        self._cache = {}
        self._cache_expiry = timedelta(minutes=30)
        logger.info("Historical Performance Service initialized")
    
    def get_setup_performance(self, setup_name: str, days_back: int = 90) -> Optional[Dict]:
        """
        Get performance metrics for a specific trading setup.
        
        Args:
            setup_name: Name of the trading setup (e.g., 'Breakout', 'Reversal')
            days_back: Number of days to look back for performance data
            
        Returns:
            Dictionary with performance metrics or None if no data available
        """
        cache_key = f"{setup_name}_{days_back}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['value']
            
        try:
            # Get trades for this setup
            trades = self.order_persistence.get_trades_by_setup(setup_name, days_back)
            
            if not trades:
                self._cache[cache_key] = {'value': None, 'expiry': self._get_cache_expiry()}
                return None
            
            # Calculate performance metrics
            performance = self._calculate_performance_metrics(trades)
            
            # Cache the result
            self._cache[cache_key] = {
                'value': performance,
                'expiry': self._get_cache_expiry()
            }
            
            logger.info(f"Performance for setup '{setup_name}': {performance}")
            return performance
            
        except Exception as e:
            logger.error(f"Error getting performance for setup '{setup_name}': {e}")
            return None
    
    def get_all_setups_performance(self, days_back: int = 90) -> Dict[str, Dict]:
        """
        Get performance metrics for all trading setups with recent activity.
        
        Args:
            days_back: Number of days to look back for performance data
            
        Returns:
            Dictionary with setup names as keys and performance metrics as values
        """
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
            
            # Cache the result
            self._cache[cache_key] = {
                'value': performance_data,
                'expiry': self._get_cache_expiry()
            }
            
            logger.info(f"Performance data for {len(performance_data)} setups loaded")
            return performance_data
            
        except Exception as e:
            logger.error(f"Error getting all setups performance: {e}")
            return {}
    
    def get_setup_bias_score(self, setup_name: str, days_back: int = 90, 
                           min_trades: int = 10, min_win_rate: float = 0.4,
                           min_profit_factor: float = 1.2) -> float:
        """
        Calculate a bias score for a trading setup based on historical performance.
        
        Args:
            setup_name: Name of the trading setup
            days_back: Lookback period for performance data
            min_trades: Minimum trades required for reliable scoring
            min_win_rate: Minimum win rate for positive bias
            min_profit_factor: Minimum profit factor for positive bias
            
        Returns:
            Bias score between 0.1 and 1.0 (higher is better)
        """
        try:
            performance = self.get_setup_performance(setup_name, days_back)
            
            if not performance:
                return 0.5  # Neutral score for unknown setups
            
            # Check minimum thresholds
            if (performance['total_trades'] < min_trades or
                performance['win_rate'] < min_win_rate or
                performance['profit_factor'] < min_profit_factor):
                return 0.3  # Below minimum thresholds
            
            # Calculate composite score
            win_rate = performance['win_rate']
            profit_factor = min(performance['profit_factor'], 5.0)  # Cap at 5.0
            
            # Weighted score: 60% win rate, 40% profit factor (normalized)
            score = (win_rate * 0.6) + (profit_factor * 0.4) / 5.0
            
            return max(0.1, min(score, 1.0))
            
        except Exception as e:
            logger.error(f"Error calculating bias score for '{setup_name}': {e}")
            return 0.5  # Fallback to neutral score
    
    def get_top_performing_setups(self, days_back: int = 90, limit: int = 5) -> List[Dict]:
        """
        Get the top performing trading setups based on historical performance.
        
        Args:
            days_back: Lookback period for performance data
            limit: Maximum number of setups to return
            
        Returns:
            List of top performing setups with their performance metrics
        """
        try:
            all_performance = self.get_all_setups_performance(days_back)
            
            # Filter setups with sufficient data
            qualified_setups = {}
            for setup_name, performance in all_performance.items():
                if (performance['total_trades'] >= 10 and
                    performance['win_rate'] >= 0.4 and
                    performance['profit_factor'] >= 1.2):
                    qualified_setups[setup_name] = performance
            
            # Calculate composite performance score
            scored_setups = []
            for setup_name, performance in qualified_setups.items():
                score = self.get_setup_bias_score(setup_name, days_back)
                scored_setups.append({
                    'setup_name': setup_name,
                    'performance_score': score,
                    **performance
                })
            
            # Sort by performance score descending
            scored_setups.sort(key=lambda x: x['performance_score'], reverse=True)
            
            return scored_setups[:limit]
            
        except Exception as e:
            logger.error(f"Error getting top performing setups: {e}")
            return []
    
    def _calculate_performance_metrics(self, trades: List[Dict]) -> Dict:
        """
        Calculate comprehensive performance metrics from trade data.
        
        Args:
            trades: List of trade dictionaries
            
        Returns:
            Dictionary with performance metrics
        """
        if not trades:
            return None
        
        # Separate winning and losing trades
        winning_trades = [t for t in trades if t.get('pnl', 0) > 0]
        losing_trades = [t for t in trades if t.get('pnl', 0) <= 0]
        
        total_trades = len(trades)
        winning_trades_count = len(winning_trades)
        losing_trades_count = len(losing_trades)
        
        # Calculate basic metrics
        win_rate = winning_trades_count / total_trades if total_trades > 0 else 0
        
        total_profit = sum(t.get('pnl', 0) for t in winning_trades)
        total_loss = abs(sum(t.get('pnl', 0) for t in losing_trades))
        
        profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
        avg_pnl = sum(t.get('pnl', 0) for t in trades) / total_trades if total_trades > 0 else 0
        
        # Calculate risk-adjusted metrics
        avg_win = total_profit / winning_trades_count if winning_trades_count > 0 else 0
        avg_loss = total_loss / losing_trades_count if losing_trades_count > 0 else 0
        risk_reward_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')
        
        # Calculate holding period statistics
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
            'profit_factor': round(min(profit_factor, 10.0), 2),  # Cap at 10.0
            'total_profit': round(total_profit, 2),
            'total_loss': round(total_loss, 2),
            'avg_pnl': round(avg_pnl, 2),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'risk_reward_ratio': round(min(risk_reward_ratio, 10.0), 2),  # Cap at 10.0
            'avg_holding_period': round(avg_holding_period, 1),
            'analysis_period_days': 90  # Default analysis period
        }
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached value is still valid."""
        if cache_key in self._cache:
            cached_data = self._cache[cache_key]
            return datetime.now() < cached_data['expiry']
        return False
    
    def _get_cache_expiry(self) -> datetime:
        """Get cache expiry timestamp."""
        return datetime.now() + self._cache_expiry
    
    def clear_cache(self):
        """Clear all cached performance data."""
        self._cache = {}
        logger.info("Performance cache cleared")
    
    def get_performance_summary(self) -> Dict:
        """
        Get overall performance summary across all setups.
        
        Returns:
            Dictionary with aggregate performance metrics
        """
        try:
            all_performance = self.get_all_setups_performance()
            
            if not all_performance:
                return {
                    'total_setups': 0,
                    'total_trades': 0,
                    'overall_win_rate': 0,
                    'overall_profit_factor': 0
                }
            
            # Aggregate metrics across all setups
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