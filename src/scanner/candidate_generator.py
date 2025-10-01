# src/scanner/strategy/strategy_definitions.py
from src.scanner.strategy.strategy_core import Strategy, StrategyConfig, StrategyType
from typing import Dict, Any, Optional

class BullTrendStrategy(Strategy):
    """Strong uptrend strategy - Price > EMA10 > EMA50 > EMA100"""
    
    def evaluate(self, scan_result) -> Optional[Dict[str, Any]]:
        # Check basic filters
        if not self._passes_basic_filters(scan_result):
            return None
            
        # Check trend alignment
        if not self._has_perfect_trend_alignment(scan_result.ema_values):
            return None
            
        confidence = self.calculate_confidence(scan_result)
        
        if confidence >= 50:  # Minimum confidence threshold
            signal = self.generate_signal(scan_result)
            signal.update({
                'entry_signal': 'TREND_CONTINUATION',
                'risk_level': self._assess_risk(scan_result),
                'targets': self._calculate_targets(scan_result)
            })
            return signal
        return None
    
    def calculate_confidence(self, scan_result) -> float:
        emas = scan_result.ema_values
        
        # Base confidence from trend score
        confidence = scan_result.bull_trend_score * 0.7
        
        # Boost for perfect EMA alignment
        if self._has_perfect_trend_alignment(emas):
            confidence += 20
            
        # Boost for strong pullback score (shows healthy trend)
        if scan_result.bull_pullback_score > 60:
            confidence += 10
            
        return min(confidence, 100)
    
    def _passes_basic_filters(self, scan_result) -> bool:
        return (scan_result.total_score >= self.config.min_total_score and
                scan_result.bull_trend_score >= self.config.min_trend_score)
    
    def _has_perfect_trend_alignment(self, emas: Dict[int, float]) -> bool:
        return (emas.get(10, 0) > emas.get(50, 0) > emas.get(100, 0))
    
    def _assess_risk(self, scan_result) -> str:
        if scan_result.bull_pullback_score > 70:
            return 'LOW'
        elif scan_result.total_score > 80:
            return 'MEDIUM'
        else:
            return 'HIGH'
    
    def _calculate_targets(self, scan_result) -> Dict[str, float]:
        current = scan_result.current_price
        ema20 = scan_result.ema_values.get(20, current)
        return {
            'profit_target_1': current * 1.05,  # 5% target
            'profit_target_2': current * 1.08,  # 8% target  
            'stop_loss': ema20 * 0.98,  # 2% below EMA20
        }

class BullPullbackStrategy(Strategy):
    """Uptrend stocks pulling back to dynamic support"""
    
    def evaluate(self, scan_result) -> Optional[Dict[str, Any]]:
        if not self._passes_basic_filters(scan_result):
            return None
            
        if not self._is_valid_pullback(scan_result):
            return None
            
        confidence = self.calculate_confidence(scan_result)
        
        if confidence >= 50:
            signal = self.generate_signal(scan_result)
            signal.update({
                'entry_signal': 'PULLBACK_TO_SUPPORT',
                'risk_level': 'LOW',
                'support_level': scan_result.ema_values.get(20),
                'pullback_depth': self._calculate_pullback_depth(scan_result),
                'targets': self._calculate_targets(scan_result)
            })
            return signal
        return None
    
    def calculate_confidence(self, scan_result) -> float:
        # Base confidence from pullback score
        confidence = scan_result.bull_pullback_score * 0.8
        
        # Boost if trend is still strong
        if scan_result.bull_trend_score > 70:
            confidence += 15
            
        # Penalize if too far from EMA20
        pullback_depth = self._calculate_pullback_depth(scan_result)
        if pullback_depth > 3.0:  # More than 3% pullback
            confidence -= 20
            
        return max(0, min(confidence, 100))
    
    def _passes_basic_filters(self, scan_result) -> bool:
        return (scan_result.total_score >= self.config.min_total_score and
                scan_result.bull_pullback_score >= self.config.min_pullback_score and
                scan_result.bull_trend_score >= 50)  # Must be in uptrend
    
    def _is_valid_pullback(self, scan_result) -> bool:
        """Check if this is a valid pullback in an uptrend"""
        emas = scan_result.ema_values
        current = scan_result.current_price
        
        # Must be above EMA50 (main trend support)
        if current <= emas.get(50, 0):
            return False
            
        # Must be near EMA20 (pullback support)
        pullback_depth = self._calculate_pullback_depth(scan_result)
        return pullback_depth <= 2.5  # Within 2.5% of EMA20
    
    def _calculate_pullback_depth(self, scan_result) -> float:
        current = scan_result.current_price
        ema20 = scan_result.ema_values.get(20, current)
        return abs(current - ema20) / ema20 * 100
    
    def _calculate_targets(self, scan_result) -> Dict[str, float]:
        current = scan_result.current_price
        ema10 = scan_result.ema_values.get(10, current * 1.03)
        return {
            'profit_target_1': ema10,  # Target EMA10 resistance
            'profit_target_2': current * 1.06,  # 6% target
            'stop_loss': scan_result.ema_values.get(20, current) * 0.97,
        }

class MomentumBreakoutStrategy(Strategy):
    """Stocks showing strong momentum characteristics"""
    
    def evaluate(self, scan_result) -> Optional[Dict[str, Any]]:
        if not self._passes_basic_filters(scan_result):
            return None
            
        momentum_strength = self._calculate_momentum_strength(scan_result)
        
        if momentum_strength >= 60:
            confidence = self.calculate_confidence(scan_result)
            
            if confidence >= 55:
                signal = self.generate_signal(scan_result)
                signal.update({
                    'entry_signal': 'MOMENTUM_ACCELERATION',
                    'risk_level': 'MEDIUM',
                    'momentum_strength': momentum_strength,
                    'targets': self._calculate_targets(scan_result)
                })
                return signal
        return None
    
    def calculate_confidence(self, scan_result) -> float:
        momentum = self._calculate_momentum_strength(scan_result)
        trend_strength = scan_result.bull_trend_score * 0.3
        total_score = scan_result.total_score * 0.4
        
        return momentum * 0.3 + trend_strength + total_score
    
    def _passes_basic_filters(self, scan_result) -> bool:
        return scan_result.total_score >= self.config.min_total_score
    
    def _calculate_momentum_strength(self, scan_result) -> float:
        """Calculate momentum based on price position relative to EMAs"""
        emas = scan_result.ema_values
        current = scan_result.current_price
        
        # How far above key EMAs
        above_ema10 = max(0, (current - emas.get(10, current)) / current * 100)
        above_ema20 = max(0, (current - emas.get(20, current)) / current * 100)
        
        # EMA slope approximation (would need historical EMA values for real slope)
        momentum = (above_ema10 * 0.6 + above_ema20 * 0.4) * 10
        return min(momentum, 100)
    
    def _calculate_targets(self, scan_result) -> Dict[str, float]:
        current = scan_result.current_price
        return {
            'profit_target_1': current * 1.08,
            'profit_target_2': current * 1.12, 
            'stop_loss': current * 0.94,
        }