"""
Market Context Service for Phase B - Analyzes market conditions for intelligent timeframe matching.
Provides real-time market regime detection and dominant timeframe analysis for prioritization.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

class MarketContextService:
    """Service for analyzing market context and providing timeframe matching intelligence."""
    
    def __init__(self, data_feed, analytics_service=None):
        """
        Initialize the market context service.
        
        Args:
            data_feed: Data feed service for market data access
            analytics_service: Analytics service for technical indicators (optional)
        """
        self.data_feed = data_feed
        self.analytics_service = analytics_service
        self._cache = {}
        self._cache_expiry = timedelta(minutes=15)
        self.logger = logging.getLogger(__name__)
        
    def get_dominant_timeframe(self, symbol: str) -> str:
        """
        Determine the dominant timeframe for a symbol based on recent market activity.
        
        Args:
            symbol: Trading symbol to analyze
            
        Returns:
            Dominant timeframe string (e.g., '15min', '1H', '4H')
        """
        cache_key = f"{symbol}_timeframe"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['value']
            
        try:
            # Analyze multiple timeframes to find the most active one
            timeframes = ['15min', '1H', '4H', '1D']
            scores = {}
            
            for timeframe in timeframes:
                scores[timeframe] = self._analyze_timeframe_strength(symbol, timeframe)
            
            # Find timeframe with highest score
            dominant_tf = max(scores.items(), key=lambda x: x[1])[0]
            
            # Cache the result
            self._cache[cache_key] = {
                'value': dominant_tf,
                'expiry': datetime.now() + self._cache_expiry
            }
            
            self.logger.info(f"Dominant timeframe for {symbol}: {dominant_tf} (scores: {scores})")
            return dominant_tf
            
        except Exception as e:
            self.logger.error(f"Error determining dominant timeframe for {symbol}: {e}")
            return '1H'  # Fallback to 1H timeframe
    
    def get_market_regime(self, symbol: str) -> str:
        """
        Identify current market regime based on price action and volatility.
        
        Args:
            symbol: Trading symbol to analyze
            
        Returns:
            Market regime string ('trending', 'ranging', 'volatile', 'calm')
        """
        cache_key = f"{symbol}_regime"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['value']
            
        try:
            # Get recent price data for analysis
            prices = self._get_historical_prices(symbol, '1H', 50)
            
            if len(prices) < 20:
                self.logger.warning(f"Insufficient data for {symbol} regime analysis")
                return 'ranging'  # Default with insufficient data
                
            # Calculate regime indicators
            volatility = self._calculate_volatility(prices)
            trend_strength = self._calculate_trend_strength(prices)
            adx = self._calculate_adx(prices) if self.analytics_service else 0.5
            
            # Classify regime based on indicators
            if trend_strength > 0.7 and adx > 25:
                regime = 'trending'
            elif volatility > 0.8:
                regime = 'volatile'
            elif volatility < 0.3:
                regime = 'calm'
            else:
                regime = 'ranging'
                
            # Cache the result
            self._cache[cache_key] = {
                'value': regime,
                'expiry': datetime.now() + self._cache_expiry
            }
            
            self.logger.info(f"Market regime for {symbol}: {regime} "
                           f"(volatility: {volatility:.2f}, trend: {trend_strength:.2f}, ADX: {adx:.1f})")
            return regime
            
        except Exception as e:
            self.logger.error(f"Error determining market regime for {symbol}: {e}")
            return 'ranging'  # Fallback to ranging
    
    def get_timeframe_compatibility(self, symbol: str, order_timeframe: str) -> float:
        """
        Calculate compatibility score between order timeframe and current market context.
        
        Args:
            symbol: Trading symbol
            order_timeframe: Order's timeframe (e.g., '15min', '1H')
            
        Returns:
            Compatibility score (0-1, higher is better)
        """
        try:
            dominant_timeframe = self.get_dominant_timeframe(symbol)
            
            if order_timeframe == dominant_timeframe:
                return 1.0  # Perfect match
            
            # Check if timeframes are compatible
            compatible_timeframes = self._get_compatible_timeframes(dominant_timeframe)
            if order_timeframe in compatible_timeframes:
                return 0.7  # Compatible timeframes
                
            return 0.3  # Mismatched timeframes
            
        except Exception as e:
            self.logger.error(f"Error calculating timeframe compatibility for {symbol}: {e}")
            return 0.5  # Neutral fallback
    
    def _analyze_timeframe_strength(self, symbol: str, timeframe: str) -> float:
        """Analyze strength of a particular timeframe for a symbol."""
        try:
            # Get OHLC data for the timeframe
            ohlc_data = self._get_historical_prices(symbol, timeframe, 20)
            
            if len(ohlc_data) < 10:
                return 0.5  # Neutral score with insufficient data
                
            # Calculate strength metrics
            volatility = self._calculate_volatility(ohlc_data)
            volume_ratio = self._calculate_volume_ratio(ohlc_data)
            trend_consistency = self._calculate_trend_consistency(ohlc_data)
            
            # Composite score (0-1)
            score = (volatility * 0.4) + (volume_ratio * 0.3) + (trend_consistency * 0.3)
            return max(0.1, min(score, 1.0))
            
        except Exception as e:
            self.logger.error(f"Error analyzing timeframe {timeframe} for {symbol}: {e}")
            return 0.5
    
    def _get_historical_prices(self, symbol: str, timeframe: str, bars: int) -> pd.DataFrame:
        """Get historical price data from data feed."""
        try:
            # This will depend on your data feed's interface
            # Adjust based on your actual data feed methods
            if hasattr(self.data_feed, 'get_historical_data'):
                return self.data_feed.get_historical_data(symbol, timeframe, bars)
            else:
                # Fallback implementation - adjust based on your data feed
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error getting historical data for {symbol}: {e}")
            return pd.DataFrame()
    
    def _calculate_volatility(self, prices: pd.DataFrame) -> float:
        """Calculate normalized volatility (0-1)."""
        if len(prices) < 2:
            return 0.5
            
        try:
            returns = prices['close'].pct_change().dropna()
            if len(returns) == 0:
                return 0.5
                
            volatility = returns.std()
            # Normalize to 0-1 range (assuming typical volatility ranges)
            return min(volatility * 10, 1.0)
            
        except Exception as e:
            self.logger.error(f"Error calculating volatility: {e}")
            return 0.5
    
    def _calculate_volume_ratio(self, prices: pd.DataFrame) -> float:
        """Calculate volume ratio compared to recent average."""
        if len(prices) < 10 or 'volume' not in prices.columns:
            return 0.5
            
        try:
            current_volume = prices['volume'].iloc[-1]
            avg_volume = prices['volume'].rolling(10).mean().iloc[-1]
            
            if avg_volume <= 0:
                return 0.5
                
            ratio = current_volume / avg_volume
            return min(ratio, 2.0) / 2.0  # Normalize to 0-1
            
        except Exception as e:
            self.logger.error(f"Error calculating volume ratio: {e}")
            return 0.5
    
    def _calculate_trend_consistency(self, prices: pd.DataFrame) -> float:
        """Calculate how consistent the trend is."""
        if len(prices) < 10:
            return 0.5
            
        try:
            returns = prices['close'].pct_change().dropna()
            positive_days = (returns > 0).sum() / len(returns)
            # Convert to 0-1 range where 1 is very consistent trend
            return abs(positive_days - 0.5) * 2
            
        except Exception as e:
            self.logger.error(f"Error calculating trend consistency: {e}")
            return 0.5
    
    def _calculate_trend_strength(self, prices: pd.DataFrame) -> float:
        """Calculate trend strength using simple moving average slope."""
        if len(prices) < 20:
            return 0.5
            
        try:
            # Simple trend strength calculation
            short_ma = prices['close'].rolling(10).mean()
            long_ma = prices['close'].rolling(20).mean()
            
            if pd.isna(short_ma.iloc[-1]) or pd.isna(long_ma.iloc[-1]):
                return 0.5
                
            # Normalize trend strength to 0-1
            trend_strength = abs(short_ma.iloc[-1] - long_ma.iloc[-1]) / long_ma.iloc[-1]
            return min(trend_strength * 10, 1.0)
            
        except Exception as e:
            self.logger.error(f"Error calculating trend strength: {e}")
            return 0.5
    
    def _calculate_adx(self, prices: pd.DataFrame) -> float:
        """Calculate ADX using analytics service if available."""
        if not self.analytics_service or not hasattr(self.analytics_service, 'calculate_adx'):
            return 25.0  # Default neutral ADX
            
        try:
            return self.analytics_service.calculate_adx(prices, period=14)
        except Exception as e:
            self.logger.error(f"Error calculating ADX: {e}")
            return 25.0
    
    def _get_compatible_timeframes(self, timeframe: str) -> List[str]:
        """Get compatible timeframes for a given dominant timeframe."""
        # Basic compatibility mapping - can be enhanced with config
        compatibility_map = {
            '15min': ['5min', '15min', '30min', '1H'],
            '1H': ['30min', '1H', '4H', '15min'],
            '4H': ['1H', '4H', '1D'],
            '1D': ['4H', '1D', '1W'],
            '1W': ['1D', '1W']
        }
        return compatibility_map.get(timeframe, [timeframe])
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached value is still valid."""
        if cache_key in self._cache:
            cached_data = self._cache[cache_key]
            return datetime.now() < cached_data['expiry']
        return False
    
    def clear_cache(self):
        """Clear all cached data."""
        self._cache = {}
        self.logger.info("Market context cache cleared")