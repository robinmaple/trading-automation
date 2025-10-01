# src/scanner/criteria/technical_criteria.py
from .criteria_core import BaseCriteria, CriteriaConfig, CriteriaType
from typing import Dict, Any, List
import numpy as np
import pandas as pd

class TechnicalCriteria(BaseCriteria):
    """Technical analysis criteria for chart patterns, indicators, and price action"""
    
    def evaluate(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        if self.config.name == "ema_trend_alignment":
            return self._evaluate_ema_trend_alignment(stock_data)
        elif self.config.name == "rsi_momentum":
            return self._evaluate_rsi_momentum(stock_data)
        elif self.config.name == "macd_signal":
            return self._evaluate_macd_signal(stock_data)
        elif self.config.name == "support_resistance":
            return self._evaluate_support_resistance(stock_data)
        elif self.config.name == "volatility_range":
            return self._evaluate_volatility_range(stock_data)
        elif self.config.name == "price_position":
            return self._evaluate_price_position(stock_data)
        elif self.config.name == "volume_confirmation":
            return self._evaluate_volume_confirmation(stock_data)
        elif self.config.name == "trend_strength":
            return self._evaluate_trend_strength(stock_data)
        else:
            return {'passed': False, 'score': 0, 'message': 'Unknown technical criteria'}
    
    def get_required_fields(self) -> List[str]:
        field_map = {
            "ema_trend_alignment": ['price_history', 'ema_values'],
            "rsi_momentum": ['price_history'],
            "macd_signal": ['price_history'],
            "support_resistance": ['price_history', 'high_history', 'low_history'],
            "volatility_range": ['price_history', 'high_history', 'low_history'],
            "price_position": ['price_history', 'high_history', 'low_history'],
            "volume_confirmation": ['price_history', 'volume_history'],
            "trend_strength": ['price_history']
        }
        return field_map.get(self.config.name, [])
    
    def _evaluate_ema_trend_alignment(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate EMA trend alignment - bullish when shorter EMAs > longer EMAs
        """
        ema_values = stock_data.get('ema_values', {})
        price_history = stock_data.get('price_history', [])
        
        if not ema_values or len(price_history) < 50:
            return {'passed': False, 'score': 0, 'message': 'Insufficient EMA data'}
        
        current_price = price_history[-1] if price_history else 0
        required_emas = [9, 20, 50]  # Common EMA periods
        
        # Check if we have all required EMAs
        if not all(period in ema_values for period in required_emas):
            return {'passed': False, 'score': 0, 'message': 'Missing EMA values'}
        
        # Bullish alignment: Price > EMA9 > EMA20 > EMA50
        ema_9 = ema_values[9]
        ema_20 = ema_values[20]
        ema_50 = ema_values[50]
        
        conditions = [
            current_price > ema_9,
            ema_9 > ema_20,
            ema_20 > ema_50
        ]
        
        passed_conditions = sum(conditions)
        score = (passed_conditions / len(conditions)) * 100
        passed = passed_conditions >= 2  # At least 2 out of 3 conditions
        
        return {
            'passed': passed,
            'score': score,
            'message': f"EMA Alignment: {passed_conditions}/{len(conditions)} conditions met",
            'metadata': {
                'price_vs_ema9': current_price > ema_9,
                'ema9_vs_ema20': ema_9 > ema_20,
                'ema20_vs_ema50': ema_20 > ema_50,
                'current_price': current_price,
                'ema_values': ema_values
            }
        }
    
    def _evaluate_rsi_momentum(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate RSI momentum - avoid overbought/oversold extremes
        """
        price_history = stock_data.get('price_history', [])
        period = self.config.parameters.get('rsi_period', 14)
        
        if len(price_history) < period + 1:
            return {'passed': False, 'score': 0, 'message': 'Insufficient price history for RSI'}
        
        # Calculate RSI
        rsi = self._calculate_rsi(price_history, period)
        
        # Parameters
        overbought = self.config.parameters.get('overbought', 70)
        oversold = self.config.parameters.get('oversold', 30)
        ideal_min = self.config.parameters.get('ideal_min', 40)
        ideal_max = self.config.parameters.get('ideal_max', 65)
        
        # Score based on position in ideal range
        if ideal_min <= rsi <= ideal_max:
            score = 100
            passed = True
            message = f"RSI in ideal range: {rsi:.1f}"
        elif oversold < rsi < overbought:
            # In neutral zone but not ideal
            distance_to_ideal = min(abs(rsi - ideal_min), abs(rsi - ideal_max))
            score = max(0, 100 - (distance_to_ideal * 5))
            passed = score >= 50
            message = f"RSI in neutral zone: {rsi:.1f}"
        else:
            # Overbought or oversold
            score = 0
            passed = False
            message = f"RSI in extreme: {rsi:.1f}"
        
        return {
            'passed': passed,
            'score': score,
            'message': message,
            'metadata': {
                'rsi_value': rsi,
                'rsi_period': period,
                'overbought_level': overbought,
                'oversold_level': oversold
            }
        }
    
    def _evaluate_macd_signal(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate MACD signal - bullish when MACD > signal line and positive
        """
        price_history = stock_data.get('price_history', [])
        
        if len(price_history) < 26:  # Need enough data for MACD calculation
            return {'passed': False, 'score': 0, 'message': 'Insufficient data for MACD'}
        
        # Calculate MACD
        macd, signal, histogram = self._calculate_macd(price_history)
        
        # Bullish conditions
        conditions = [
            macd > signal,  # MACD above signal line
            macd > 0,       # MACD positive (bullish momentum)
            histogram > 0   # Histogram positive (momentum increasing)
        ]
        
        passed_conditions = sum(conditions)
        score = (passed_conditions / len(conditions)) * 100
        passed = passed_conditions >= 2
        
        return {
            'passed': passed,
            'score': score,
            'message': f"MACD Bullish: {passed_conditions}/{len(conditions)} conditions",
            'metadata': {
                'macd': macd,
                'signal': signal,
                'histogram': histogram,
                'macd_above_signal': macd > signal,
                'macd_positive': macd > 0,
                'histogram_positive': histogram > 0
            }
        }
    
    def _evaluate_support_resistance(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate proximity to key support/resistance levels
        """
        price_history = stock_data.get('price_history', [])
        high_history = stock_data.get('high_history', [])
        low_history = stock_data.get('low_history', [])
        
        if len(price_history) < 20:
            return {'passed': False, 'score': 0, 'message': 'Insufficient price history'}
        
        current_price = price_history[-1]
        
        # Calculate recent support and resistance
        lookback = min(50, len(high_history))
        recent_high = max(high_history[-lookback:])
        recent_low = min(low_history[-lookback:])
        
        # Calculate key levels (Fibonacci-like)
        resistance_1 = recent_low + (recent_high - recent_low) * 0.618  # 61.8% retracement
        support_1 = recent_high - (recent_high - recent_low) * 0.618   # 61.8% retracement
        
        # Score based on position relative to key levels
        distance_to_resistance = abs(current_price - resistance_1) / resistance_1
        distance_to_support = abs(current_price - support_1) / support_1
        
        # Prefer stocks near support (for buys) or near resistance (for sells)
        # For bullish strategies, prefer near support with room to resistance
        min_distance = self.config.parameters.get('min_distance_from_support', 0.02)  # 2%
        max_distance = self.config.parameters.get('max_distance_from_resistance', 0.10)  # 10%
        
        near_support = distance_to_support <= min_distance
        room_to_resistance = distance_to_resistance >= max_distance
        
        conditions = [near_support, room_to_resistance]
        passed_conditions = sum(conditions)
        score = (passed_conditions / len(conditions)) * 100
        passed = passed_conditions >= 1  # At least one condition
        
        return {
            'passed': passed,
            'score': score,
            'message': f"Support/Resistance: Near Support: {near_support}, Room to Resistance: {room_to_resistance}",
            'metadata': {
                'current_price': current_price,
                'recent_high': recent_high,
                'recent_low': recent_low,
                'resistance_level': resistance_1,
                'support_level': support_1,
                'distance_to_resistance_pct': distance_to_resistance * 100,
                'distance_to_support_pct': distance_to_support * 100
            }
        }
    
    def _evaluate_volatility_range(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate if stock is in a reasonable volatility range
        Avoid extremely volatile or completely stagnant stocks
        """
        price_history = stock_data.get('price_history', [])
        
        if len(price_history) < 20:
            return {'passed': False, 'score': 0, 'message': 'Insufficient price history'}
        
        # Calculate volatility (standard deviation of returns)
        returns = np.diff(price_history) / price_history[:-1]
        volatility = np.std(returns) * 100  # As percentage
        
        # Parameters for ideal volatility range
        min_volatility = self.config.parameters.get('min_volatility', 1.0)   # 1% minimum
        max_volatility = self.config.parameters.get('max_volatility', 5.0)   # 5% maximum
        
        # Score based on position in ideal range
        if min_volatility <= volatility <= max_volatility:
            score = 100
            passed = True
            message = f"Volatility in ideal range: {volatility:.2f}%"
        elif volatility < min_volatility:
            # Too stagnant
            score = (volatility / min_volatility) * 50  # Max 50 points for low volatility
            passed = score >= 30
            message = f"Low volatility: {volatility:.2f}%"
        else:
            # Too volatile
            excess_volatility = volatility - max_volatility
            score = max(0, 100 - (excess_volatility * 20))  # Penalize high volatility
            passed = score >= 50
            message = f"High volatility: {volatility:.2f}%"
        
        return {
            'passed': passed,
            'score': score,
            'message': message,
            'metadata': {
                'volatility_pct': volatility,
                'min_volatility': min_volatility,
                'max_volatility': max_volatility,
                'returns_std': np.std(returns)
            }
        }
    
    def _evaluate_price_position(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate where price is within recent trading range
        """
        high_history = stock_data.get('high_history', [])
        low_history = stock_data.get('low_history', [])
        current_price = stock_data.get('price_history', [0])[-1]
        
        if len(high_history) < 20 or len(low_history) < 20:
            return {'passed': False, 'score': 0, 'message': 'Insufficient price history'}
        
        lookback = min(20, len(high_history))
        recent_high = max(high_history[-lookback:])
        recent_low = min(low_history[-lookback:])
        
        if recent_high == recent_low:
            return {'passed': False, 'score': 0, 'message': 'No price range'}
        
        # Calculate position within range (0 = at bottom, 100 = at top)
        range_position = ((current_price - recent_low) / (recent_high - recent_low)) * 100
        
        # For bullish strategies, prefer not being at the very top
        max_position = self.config.parameters.get('max_range_position', 80)  # Don't buy at top 20%
        ideal_min = self.config.parameters.get('ideal_min_position', 30)     # Prefer above bottom 30%
        ideal_max = self.config.parameters.get('ideal_max_position', 70)     # Prefer below top 30%
        
        if ideal_min <= range_position <= ideal_max:
            score = 100
            passed = True
            message = f"Price in ideal range position: {range_position:.1f}%"
        elif range_position <= max_position:
            # Below maximum but not in ideal range
            distance_to_ideal = min(abs(range_position - ideal_min), abs(range_position - ideal_max))
            score = max(0, 80 - (distance_to_ideal * 2))
            passed = score >= 50
            message = f"Price in acceptable range: {range_position:.1f}%"
        else:
            # Too high in range
            score = 0
            passed = False
            message = f"Price too high in range: {range_position:.1f}%"
        
        return {
            'passed': passed,
            'score': score,
            'message': message,
            'metadata': {
                'range_position_pct': range_position,
                'recent_high': recent_high,
                'recent_low': recent_low,
                'current_price': current_price
            }
        }
    
    def _evaluate_volume_confirmation(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate volume confirmation for price moves
        """
        price_history = stock_data.get('price_history', [])
        volume_history = stock_data.get('volume_history', [])
        
        if len(price_history) < 10 or len(volume_history) < 10:
            return {'passed': False, 'score': 0, 'message': 'Insufficient volume data'}
        
        # Calculate price change and volume ratio
        recent_prices = price_history[-10:]
        recent_volumes = volume_history[-10:]
        
        price_change = (recent_prices[-1] - recent_prices[0]) / recent_prices[0] * 100
        avg_volume = np.mean(recent_volumes)
        current_volume_ratio = recent_volumes[-1] / avg_volume if avg_volume > 0 else 1
        
        # Bullish: Price up with increasing volume, or price down with decreasing volume
        if price_change > 0 and current_volume_ratio > 1.2:
            # Up move with high volume - very bullish
            score = 100
            passed = True
            message = "Bullish volume confirmation: Up move with high volume"
        elif price_change < 0 and current_volume_ratio < 0.8:
            # Down move with low volume - could be healthy pullback
            score = 80
            passed = True
            message = "Potential pullback: Down move with low volume"
        elif abs(price_change) < 2:  # Small move
            score = 60
            passed = True
            message = "Neutral volume: Small price move with average volume"
        else:
            # Divergence: Up move with low volume or down move with high volume
            score = 30
            passed = self.config.parameters.get('allow_volume_divergence', False)
            message = "Volume divergence detected"
        
        return {
            'passed': passed,
            'score': score,
            'message': message,
            'metadata': {
                'price_change_pct': price_change,
                'volume_ratio': current_volume_ratio,
                'recent_avg_volume': avg_volume
            }
        }
    
    def _evaluate_trend_strength(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate overall trend strength using linear regression
        """
        price_history = stock_data.get('price_history', [])
        
        if len(price_history) < 20:
            return {'passed': False, 'score': 0, 'message': 'Insufficient price history'}
        
        # Use last 20 periods for trend analysis
        lookback = min(20, len(price_history))
        recent_prices = price_history[-lookback:]
        
        # Calculate linear regression slope
        x = np.arange(len(recent_prices))
        y = np.array(recent_prices)
        
        if len(y) < 2:
            return {'passed': False, 'score': 0, 'message': 'Not enough data points'}
        
        slope, intercept = np.polyfit(x, y, 1)
        
        # Normalize slope by average price to get percentage slope
        avg_price = np.mean(y)
        slope_pct = (slope / avg_price) * 100
        
        # Calculate R-squared for trend strength
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        
        # Score based on slope and R-squared
        min_slope = self.config.parameters.get('min_slope_pct', 0.05)  # 0.05% per period
        min_r_squared = self.config.parameters.get('min_r_squared', 0.3)
        
        slope_score = min(100, (abs(slope_pct) / min_slope) * 50)  # Up to 50 points for slope
        r_squared_score = min(100, (r_squared / min_r_squared) * 50)  # Up to 50 points for strength
        
        total_score = slope_score + r_squared_score
        passed = total_score >= 60 and slope_pct > 0  # Positive slope required for bullish
        
        return {
            'passed': passed,
            'score': total_score,
            'message': f"Trend Strength: Slope {slope_pct:.3f}%/period, R² {r_squared:.3f}",
            'metadata': {
                'slope_pct_per_period': slope_pct,
                'r_squared': r_squared,
                'trend_direction': 'up' if slope_pct > 0 else 'down',
                'trend_strength': 'strong' if r_squared > 0.6 else 'moderate' if r_squared > 0.3 else 'weak'
            }
        }
    
    # Technical Indicator Calculations
    
    def _calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """Calculate RSI indicator"""
        if len(prices) < period + 1:
            return 50  # Neutral default
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gains = np.mean(gains[-period:])
        avg_losses = np.mean(losses[-period:])
        
        if avg_losses == 0:
            return 100 if avg_gains > 0 else 50
        
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _calculate_macd(self, prices: List[float]) -> tuple:
        """Calculate MACD indicator"""
        if len(prices) < 26:
            return 0, 0, 0
        
        # Convert to pandas Series for easier EMA calculation
        series = pd.Series(prices)
        
        ema_12 = series.ewm(span=12).mean().iloc[-1]
        ema_26 = series.ewm(span=26).mean().iloc[-1]
        
        macd = ema_12 - ema_26
        signal = series.ewm(span=9).mean().iloc[-1]  # Signal line is EMA of MACD
        histogram = macd - signal
        
        return macd, signal, histogram