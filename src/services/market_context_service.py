"""
Market Context Service for Phase B - Analyzes market conditions for intelligent timeframe matching.
Provides real-time market regime detection and dominant timeframe analysis for prioritization.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

# Market Context Service - Main class definition - Begin
class MarketContextService:
    """Service for analyzing market context and providing timeframe matching intelligence."""
    
    # Initialize service with data feed and analytics dependencies - Begin
    def __init__(self, data_feed, analytics_service=None):
        self.data_feed = data_feed
        self.analytics_service = analytics_service
        self._cache = {}
        self._cache_expiry = timedelta(minutes=15)
        self.logger = logging.getLogger(__name__)
    # Initialize service with data feed and analytics dependencies - End

    # Determine dominant timeframe based on market activity - Begin
    def get_dominant_timeframe(self, symbol: str) -> str:
        cache_key = f"{symbol}_timeframe"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['value']
            
        try:
            timeframes = ['15min', '1H', '4H', '1D']
            scores = {}
            
            for timeframe in timeframes:
                scores[timeframe] = self._analyze_timeframe_strength(symbol, timeframe)
            
            dominant_tf = max(scores.items(), key=lambda x: x[1])[0]
            
            self._cache[cache_key] = {
                'value': dominant_tf,
                'expiry': datetime.now() + self._cache_expiry
            }
            
            self.logger.info(f"Dominant timeframe for {symbol}: {dominant_tf} (scores: {scores})")
            return dominant_tf
            
        except Exception as e:
            self.logger.error(f"Error determining dominant timeframe for {symbol}: {e}")
            return '1H'
    # Determine dominant timeframe based on market activity - End

    # Identify current market regime based on price action - Begin
    def get_market_regime(self, symbol: str) -> str:
        cache_key = f"{symbol}_regime"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['value']
            
        try:
            prices = self._get_historical_prices(symbol, '1H', 50)
            
            if len(prices) < 20:
                self.logger.warning(f"Insufficient data for {symbol} regime analysis")
                return 'ranging'
                
            volatility = self._calculate_volatility(prices)
            trend_strength = self._calculate_trend_strength(prices)
            adx = self._calculate_adx(prices) if self.analytics_service else 0.5
            
            if trend_strength > 0.7 and adx > 25:
                regime = 'trending'
            elif volatility > 0.8:
                regime = 'volatile'
            elif volatility < 0.3:
                regime = 'calm'
            else:
                regime = 'ranging'
                
            self._cache[cache_key] = {
                'value': regime,
                'expiry': datetime.now() + self._cache_expiry
            }
            
            self.logger.info(f"Market regime for {symbol}: {regime} "
                           f"(volatility: {volatility:.2f}, trend: {trend_strength:.2f}, ADX: {adx:.1f})")
            return regime
            
        except Exception as e:
            self.logger.error(f"Error determining market regime for {symbol}: {e}")
            return 'ranging'
    # Identify current market regime based on price action - End

    # Calculate compatibility between order and market timeframes - Begin
    def get_timeframe_compatibility(self, symbol: str, order_timeframe: str) -> float:
        try:
            dominant_timeframe = self.get_dominant_timeframe(symbol)
            
            if order_timeframe == dominant_timeframe:
                return 1.0
            
            compatible_timeframes = self._get_compatible_timeframes(dominant_timeframe)
            if order_timeframe in compatible_timeframes:
                return 0.7
                
            return 0.3
            
        except Exception as e:
            self.logger.error(f"Error calculating timeframe compatibility for {symbol}: {e}")
            return 0.5
    # Calculate compatibility between order and market timeframes - End

    # Analyze strength of specific timeframe - Begin
    def _analyze_timeframe_strength(self, symbol: str, timeframe: str) -> float:
        try:
            ohlc_data = self._get_historical_prices(symbol, timeframe, 20)
            
            if len(ohlc_data) < 10:
                return 0.5
                
            volatility = self._calculate_volatility(ohlc_data)
            volume_ratio = self._calculate_volume_ratio(ohlc_data)
            trend_consistency = self._calculate_trend_consistency(ohlc_data)
            
            score = (volatility * 0.4) + (volume_ratio * 0.3) + (trend_consistency * 0.3)
            return max(0.1, min(score, 1.0))
            
        except Exception as e:
            self.logger.error(f"Error analyzing timeframe {timeframe} for {symbol}: {e}")
            return 0.5
    # Analyze strength of specific timeframe - End

    # Retrieve historical price data from data feed - Begin
    def _get_historical_prices(self, symbol: str, timeframe: str, bars: int) -> pd.DataFrame:
        try:
            if hasattr(self.data_feed, 'get_historical_data'):
                return self.data_feed.get_historical_data(symbol, timeframe, bars)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error getting historical data for {symbol}: {e}")
            return pd.DataFrame()
    # Retrieve historical price data from data feed - End

    # Calculate normalized volatility metric - Begin
    def _calculate_volatility(self, prices: pd.DataFrame) -> float:
        if len(prices) < 2:
            return 0.5
            
        try:
            returns = prices['close'].pct_change().dropna()
            if len(returns) == 0:
                return 0.5
                
            volatility = returns.std()
            return min(volatility * 10, 1.0)
            
        except Exception as e:
            self.logger.error(f"Error calculating volatility: {e}")
            return 0.5
    # Calculate normalized volatility metric - End

    # Calculate volume ratio compared to recent average - Begin
    def _calculate_volume_ratio(self, prices: pd.DataFrame) -> float:
        if len(prices) < 10 or 'volume' not in prices.columns:
            return 0.5
            
        try:
            current_volume = prices['volume'].iloc[-1]
            avg_volume = prices['volume'].rolling(10).mean().iloc[-1]
            
            if avg_volume <= 0:
                return 0.5
                
            ratio = current_volume / avg_volume
            return min(ratio, 2.0) / 2.0
            
        except Exception as e:
            self.logger.error(f"Error calculating volume ratio: {e}")
            return 0.5
    # Calculate volume ratio compared to recent average - End

    # Measure trend consistency - Begin
    def _calculate_trend_consistency(self, prices: pd.DataFrame) -> float:
        if len(prices) < 10:
            return 0.5
            
        try:
            returns = prices['close'].pct_change().dropna()
            positive_days = (returns > 0).sum() / len(returns)
            return abs(positive_days - 0.5) * 2
            
        except Exception as e:
            self.logger.error(f"Error calculating trend consistency: {e}")
            return 0.5
    # Measure trend consistency - End

    # Calculate trend strength using moving averages - Begin
    def _calculate_trend_strength(self, prices: pd.DataFrame) -> float:
        if len(prices) < 20:
            return 0.5
            
        try:
            short_ma = prices['close'].rolling(10).mean()
            long_ma = prices['close'].rolling(20).mean()
            
            if pd.isna(short_ma.iloc[-1]) or pd.isna(long_ma.iloc[-1]):
                return 0.5
                
            trend_strength = abs(short_ma.iloc[-1] - long_ma.iloc[-1]) / long_ma.iloc[-1]
            return min(trend_strength * 10, 1.0)
            
        except Exception as e:
            self.logger.error(f"Error calculating trend strength: {e}")
            return 0.5
    # Calculate trend strength using moving averages - End

    # Calculate ADX using analytics service - Begin
    def _calculate_adx(self, prices: pd.DataFrame) -> float:
        if not self.analytics_service or not hasattr(self.analytics_service, 'calculate_adx'):
            return 25.0
            
        try:
            return self.analytics_service.calculate_adx(prices, period=14)
        except Exception as e:
            self.logger.error(f"Error calculating ADX: {e}")
            return 25.0
    # Calculate ADX using analytics service - End

    # Get compatible timeframes mapping - Begin
    def _get_compatible_timeframes(self, timeframe: str) -> List[str]:
        compatibility_map = {
            '15min': ['5min', '15min', '30min', '1H'],
            '1H': ['30min', '1H', '4H', '15min'],
            '4H': ['1H', '4H', '1D'],
            '1D': ['4H', '1D', '1W'],
            '1W': ['1D', '1W']
        }
        return compatibility_map.get(timeframe, [timeframe])
    # Get compatible timeframes mapping - End

    # Check if cached value is still valid - Begin
    def _is_cache_valid(self, cache_key: str) -> bool:
        if cache_key in self._cache:
            cached_data = self._cache[cache_key]
            return datetime.now() < cached_data['expiry']
        return False
    # Check if cached value is still valid - End

    # Clear all cached data - Begin
    def clear_cache(self):
        self._cache = {}
        self.logger.info("Market context cache cleared")
    # Clear all cached data - End
# Market Context Service - Main class definition - End