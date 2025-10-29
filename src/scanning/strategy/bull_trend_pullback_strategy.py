# src/scanner/strategy/bull_trend_pullback_strategy.py
from .strategy_core import Strategy, StrategyConfig, StrategyType
from typing import Dict, Any, Optional

class BullTrendPullbackStrategy(Strategy):
    """
    Bull Trend Pullback Strategy
    Criteria:
    1. EMA(ST) > EMA(MD) > EMA(LT) - Strong uptrend structure
    2. Price pulling back to EMA(ST) - Entry opportunity
    3. All base criteria (volume, market cap, price)
    """
    
    def evaluate_strategy_specific(self, scan_result, stock_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Get EMA values from scan result
        ema_values = scan_result.ema_values
        
        # Strategy-specific parameters
        short_term_ema = self.config.parameters.get('short_term_ema', 10)
        medium_term_ema = self.config.parameters.get('medium_term_ema', 20) 
        long_term_ema = self.config.parameters.get('long_term_ema', 50)
        max_pullback_pct = self.config.parameters.get('max_pullback_pct', 3.0)
        
        # Check EMA alignment
        if not self._check_ema_alignment(ema_values, short_term_ema, medium_term_ema, long_term_ema):
            return None
        
        # Check price pullback
        pullback_analysis = self._analyze_pullback(scan_result, short_term_ema, max_pullback_pct)
        if not pullback_analysis['valid_pullback']:
            return None
        
        return {
            'setup_type': 'BULL_TREND_PULLBACK',
            'ema_alignment': 'perfect',
            'pullback_depth_pct': pullback_analysis['pullback_depth_pct'],
            'entry_signal': 'PULLBACK_TO_EMA_ST',
            'risk_level': self._assess_risk_level(pullback_analysis),
            'targets': self._calculate_targets(scan_result, ema_values),
            'pullback_metadata': pullback_analysis
        }
    
    def calculate_strategy_confidence(self, scan_result, stock_data: Dict[str, Any]) -> float:
        """
        Calculate confidence score for bull trend pullback setup
        """
        ema_values = scan_result.ema_values
        current_price = scan_result.current_price
        
        # Strategy parameters
        short_term_ema = self.config.parameters.get('short_term_ema', 10)
        medium_term_ema = self.config.parameters.get('medium_term_ema', 20)
        long_term_ema = self.config.parameters.get('long_term_ema', 50)
        max_pullback_pct = self.config.parameters.get('max_pullback_pct', 3.0)
        
        confidence = 0
        
        # 1. EMA Alignment Score (40 points max)
        alignment_score = self._calculate_ema_alignment_score(ema_values, short_term_ema, medium_term_ema, long_term_ema)
        confidence += alignment_score * 0.4
        
        # 2. Pullback Quality Score (40 points max)
        pullback_score = self._calculate_pullback_quality_score(scan_result, short_term_ema, max_pullback_pct)
        confidence += pullback_score * 0.4
        
        # 3. Overall Trend Score (20 points max)
        trend_score = min(scan_result.bull_trend_score, 100) * 0.2
        
        confidence += trend_score
        
        return min(confidence, 100)
    
    def _check_ema_alignment(self, ema_values: Dict[int, float], st_period: int, md_period: int, lt_period: int) -> bool:
        """Check if EMAs are in perfect bull alignment"""
        if not all(period in ema_values for period in [st_period, md_period, lt_period]):
            return False
        
        return (ema_values[st_period] > ema_values[md_period] > ema_values[lt_period])
    
    def _analyze_pullback(self, scan_result, st_period: int, max_pullback_pct: float) -> Dict[str, Any]:
        """Analyze price pullback to short-term EMA"""
        current_price = scan_result.current_price
        ema_st = scan_result.ema_values.get(st_period, 0)
        
        if ema_st == 0:
            return {'valid_pullback': False, 'pullback_depth_pct': 0}
        
        pullback_depth_pct = abs(current_price - ema_st) / ema_st * 100
        valid_pullback = pullback_depth_pct <= max_pullback_pct
        
        return {
            'valid_pullback': valid_pullback,
            'pullback_depth_pct': pullback_depth_pct,
            'ema_st_value': ema_st,
            'max_allowed_pullback_pct': max_pullback_pct,
            'is_at_or_near_ema': valid_pullback
        }
    
    def _calculate_ema_alignment_score(self, ema_values: Dict[int, float], st_period: int, md_period: int, lt_period: int) -> float:
        """Calculate score for EMA alignment quality"""
        if not self._check_ema_alignment(ema_values, st_period, md_period, lt_period):
            return 0
        
        # Perfect alignment gets 100, but we can add nuances later
        return 100
    
    def _calculate_pullback_quality_score(self, scan_result, st_period: int, max_pullback_pct: float) -> float:
        """Calculate score for pullback quality"""
        pullback_analysis = self._analyze_pullback(scan_result, st_period, max_pullback_pct)
        
        if not pullback_analysis['valid_pullback']:
            return 0
        
        # Score based on proximity to EMA (closer = better)
        pullback_depth = pullback_analysis['pullback_depth_pct']
        proximity_score = (1 - (pullback_depth / max_pullback_pct)) * 100
        
        return proximity_score
    
    def _assess_risk_level(self, pullback_analysis: Dict[str, Any]) -> str:
        """Assess risk level based on pullback characteristics"""
        pullback_depth = pullback_analysis['pullback_depth_pct']
        
        if pullback_depth <= 1.0:
            return 'LOW'
        elif pullback_depth <= 2.0:
            return 'MEDIUM_LOW'
        elif pullback_depth <= 3.0:
            return 'MEDIUM'
        else:
            return 'HIGH'
    
    def _calculate_targets(self, scan_result, ema_values: Dict[int, float]) -> Dict[str, float]:
        """Calculate profit targets and stop loss"""
        current_price = scan_result.current_price
        ema_20 = ema_values.get(20, current_price * 1.05)  # Use EMA20 as resistance
        ema_50 = ema_values.get(50, current_price * 0.98)  # Use EMA50 as support
        
        return {
            'profit_target_1': ema_20,  # First target at EMA20 resistance
            'profit_target_2': current_price * 1.08,  # Second target 8% up
            'stop_loss': ema_50 * 0.98,  # Stop below EMA50 support
            'risk_reward_ratio': (ema_20 - current_price) / (current_price - ema_50 * 0.98)
        }