"""
Market Context Service for Phase B - Analyzes market conditions for intelligent timeframe matching.
Provides real-time market regime detection and dominant timeframe analysis for prioritization.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Context-aware logging import - replacing standard logging
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()

# Market Context Service - Main class definition - Begin
class MarketContextService:
    """Service for analyzing market context and providing timeframe matching intelligence."""
    
    # Initialize service with data feed and analytics dependencies - Begin
    def __init__(self, data_feed, analytics_service=None):
        self.data_feed = data_feed
        self.analytics_service = analytics_service
        self._cache = {}
        self._cache_expiry = timedelta(minutes=15)
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing MarketContextService",
            context_provider={
                "data_feed_provided": data_feed is not None,
                "analytics_service_provided": analytics_service is not None,
                "cache_expiry_minutes": 15
            }
        )
    # Initialize service with data feed and analytics dependencies - End

    # Determine dominant timeframe based on market activity - Begin
    def get_dominant_timeframe(self, symbol: str) -> str:
        cache_key = f"{symbol}_timeframe"
        if self._is_cache_valid(cache_key):
            cached_value = self._cache[cache_key]['value']
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Using cached dominant timeframe",
                symbol=symbol,
                context_provider={
                    "cache_key": cache_key,
                    "cached_value": cached_value,
                    "cache_hit": True
                }
            )
            return cached_value
            
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
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Dominant timeframe analysis completed",
                symbol=symbol,
                context_provider={
                    "dominant_timeframe": dominant_tf,
                    "all_scores": scores,
                    "timeframes_analyzed": timeframes,
                    "highest_score": scores[dominant_tf],
                    "cache_updated": True
                },
                decision_reason="TIME_FRAME_ANALYSIS_COMPLETED"
            )
            return dominant_tf
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error determining dominant timeframe",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "get_dominant_timeframe"
                },
                decision_reason="TIME_FRAME_ANALYSIS_FAILED"
            )
            return '1H'
    # Determine dominant timeframe based on market activity - End

    # Identify current market regime based on price action - Begin
    def get_market_regime(self, symbol: str) -> str:
        cache_key = f"{symbol}_regime"
        if self._is_cache_valid(cache_key):
            cached_value = self._cache[cache_key]['value']
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Using cached market regime",
                symbol=symbol,
                context_provider={
                    "cache_key": cache_key,
                    "cached_value": cached_value,
                    "cache_hit": True
                }
            )
            return cached_value
            
        try:
            prices = self._get_historical_prices(symbol, '1H', 50)
            
            if len(prices) < 20:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Insufficient data for regime analysis",
                    symbol=symbol,
                    context_provider={
                        "data_points_available": len(prices),
                        "minimum_required": 20,
                        "analysis_result": "insufficient_data"
                    },
                    decision_reason="INSUFFICIENT_DATA_FOR_REGIME_ANALYSIS"
                )
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
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Market regime analysis completed",
                symbol=symbol,
                context_provider={
                    "detected_regime": regime,
                    "volatility_score": volatility,
                    "trend_strength_score": trend_strength,
                    "adx_value": adx,
                    "data_points_used": len(prices),
                    "thresholds_applied": {
                        "trending": "trend_strength > 0.7 and ADX > 25",
                        "volatile": "volatility > 0.8",
                        "calm": "volatility < 0.3"
                    },
                    "cache_updated": True
                },
                decision_reason="MARKET_REGIME_ANALYSIS_COMPLETED"
            )
            return regime
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error determining market regime",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "get_market_regime"
                },
                decision_reason="MARKET_REGIME_ANALYSIS_FAILED"
            )
            return 'ranging'
    # Identify current market regime based on price action - End

    # Calculate compatibility between order and market timeframes - Begin
    def get_timeframe_compatibility(self, symbol: str, order_timeframe: str) -> float:
        try:
            dominant_timeframe = self.get_dominant_timeframe(symbol)
            
            if order_timeframe == dominant_timeframe:
                compatibility_score = 1.0
                compatibility_reason = "exact_match"
            else:
                compatible_timeframes = self._get_compatible_timeframes(dominant_timeframe)
                if order_timeframe in compatible_timeframes:
                    compatibility_score = 0.7
                    compatibility_reason = "compatible_timeframe"
                else:
                    compatibility_score = 0.3
                    compatibility_reason = "incompatible_timeframe"
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Timeframe compatibility calculated",
                symbol=symbol,
                context_provider={
                    "order_timeframe": order_timeframe,
                    "dominant_timeframe": dominant_timeframe,
                    "compatibility_score": compatibility_score,
                    "compatibility_reason": compatibility_reason,
                    "compatible_timeframes_list": compatible_timeframes if 'compatible_timeframes' in locals() else []
                },
                decision_reason="TIME_FRAME_COMPATIBILITY_CALCULATED"
            )
            return compatibility_score
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error calculating timeframe compatibility",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "order_timeframe": order_timeframe,
                    "operation": "get_timeframe_compatibility"
                },
                decision_reason="TIME_FRAME_COMPATIBILITY_CALCULATION_FAILED"
            )
            return 0.5
    # Calculate compatibility between order and market timeframes - End

    # Analyze strength of specific timeframe - Begin
    def _analyze_timeframe_strength(self, symbol: str, timeframe: str) -> float:
        try:
            ohlc_data = self._get_historical_prices(symbol, timeframe, 20)
            
            if len(ohlc_data) < 10:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Insufficient data for timeframe strength analysis",
                    symbol=symbol,
                    context_provider={
                        "timeframe": timeframe,
                        "data_points_available": len(ohlc_data),
                        "minimum_required": 10,
                        "analysis_result": "insufficient_data"
                    }
                )
                return 0.5
                
            volatility = self._calculate_volatility(ohlc_data)
            volume_ratio = self._calculate_volume_ratio(ohlc_data)
            trend_consistency = self._calculate_trend_consistency(ohlc_data)
            
            score = (volatility * 0.4) + (volume_ratio * 0.3) + (trend_consistency * 0.3)
            final_score = max(0.1, min(score, 1.0))
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Timeframe strength analysis completed",
                symbol=symbol,
                context_provider={
                    "timeframe": timeframe,
                    "volatility_score": volatility,
                    "volume_ratio_score": volume_ratio,
                    "trend_consistency_score": trend_consistency,
                    "raw_score": score,
                    "final_score": final_score,
                    "weighting_used": {"volatility": 0.4, "volume_ratio": 0.3, "trend_consistency": 0.3},
                    "data_points_used": len(ohlc_data)
                }
            )
            return final_score
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error analyzing timeframe strength",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "timeframe": timeframe,
                    "operation": "_analyze_timeframe_strength"
                }
            )
            return 0.5
    # Analyze strength of specific timeframe - End

    # Retrieve historical price data from data feed - Begin
    def _get_historical_prices(self, symbol: str, timeframe: str, bars: int) -> pd.DataFrame:
        try:
            if hasattr(self.data_feed, 'get_historical_data'):
                data = self.data_feed.get_historical_data(symbol, timeframe, bars)
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Historical data retrieved",
                    symbol=symbol,
                    context_provider={
                        "timeframe": timeframe,
                        "bars_requested": bars,
                        "data_points_returned": len(data),
                        "columns_available": list(data.columns) if not data.empty else [],
                        "data_feed_method": "get_historical_data"
                    }
                )
                return data
            else:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Data feed missing required method",
                    symbol=symbol,
                    context_provider={
                        "timeframe": timeframe,
                        "bars_requested": bars,
                        "data_feed_type": type(self.data_feed).__name__,
                        "missing_method": "get_historical_data"
                    }
                )
                return pd.DataFrame()
                
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error getting historical data",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "timeframe": timeframe,
                    "bars_requested": bars,
                    "operation": "_get_historical_prices"
                }
            )
            return pd.DataFrame()
    # Retrieve historical price data from data feed - End

    # Calculate normalized volatility metric - Begin
    def _calculate_volatility(self, prices: pd.DataFrame) -> float:
        if len(prices) < 2:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Insufficient data for volatility calculation",
                context_provider={
                    "data_points_available": len(prices),
                    "minimum_required": 2,
                    "calculation_result": "insufficient_data"
                }
            )
            return 0.5
            
        try:
            returns = prices['close'].pct_change().dropna()
            if len(returns) == 0:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "No valid returns for volatility calculation",
                    context_provider={
                        "data_points_available": len(prices),
                        "valid_returns": 0,
                        "calculation_result": "no_valid_returns"
                    }
                )
                return 0.5
                
            volatility = returns.std()
            normalized_volatility = min(volatility * 10, 1.0)
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Volatility calculation completed",
                context_provider={
                    "raw_volatility": volatility,
                    "normalized_volatility": normalized_volatility,
                    "returns_used": len(returns),
                    "scaling_factor": 10,
                    "calculation_method": "returns_standard_deviation"
                }
            )
            return normalized_volatility
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error calculating volatility",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "data_points": len(prices),
                    "operation": "_calculate_volatility"
                }
            )
            return 0.5
    # Calculate normalized volatility metric - End

    # Calculate volume ratio compared to recent average - Begin
    def _calculate_volume_ratio(self, prices: pd.DataFrame) -> float:
        if len(prices) < 10 or 'volume' not in prices.columns:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Insufficient data for volume ratio calculation",
                context_provider={
                    "data_points_available": len(prices),
                    "volume_column_present": 'volume' in prices.columns,
                    "minimum_required": 10,
                    "calculation_result": "insufficient_data"
                }
            )
            return 0.5
            
        try:
            current_volume = prices['volume'].iloc[-1]
            avg_volume = prices['volume'].rolling(10).mean().iloc[-1]
            
            if avg_volume <= 0:
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Invalid average volume for ratio calculation",
                    context_provider={
                        "current_volume": current_volume,
                        "average_volume": avg_volume,
                        "calculation_result": "invalid_average_volume"
                    }
                )
                return 0.5
                
            ratio = current_volume / avg_volume
            normalized_ratio = min(ratio, 2.0) / 2.0
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Volume ratio calculation completed",
                context_provider={
                    "current_volume": current_volume,
                    "average_volume": avg_volume,
                    "raw_ratio": ratio,
                    "normalized_ratio": normalized_ratio,
                    "window_size": 10,
                    "max_ratio_cap": 2.0
                }
            )
            return normalized_ratio
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error calculating volume ratio",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "data_points": len(prices),
                    "operation": "_calculate_volume_ratio"
                }
            )
            return 0.5
    # Calculate volume ratio compared to recent average - End

    # Measure trend consistency - Begin
    def _calculate_trend_consistency(self, prices: pd.DataFrame) -> float:
        if len(prices) < 10:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Insufficient data for trend consistency calculation",
                context_provider={
                    "data_points_available": len(prices),
                    "minimum_required": 10,
                    "calculation_result": "insufficient_data"
                }
            )
            return 0.5
            
        try:
            returns = prices['close'].pct_change().dropna()
            positive_days = (returns > 0).sum() / len(returns)
            consistency_score = abs(positive_days - 0.5) * 2
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Trend consistency calculation completed",
                context_provider={
                    "positive_returns_ratio": positive_days,
                    "consistency_score": consistency_score,
                    "returns_analyzed": len(returns),
                    "calculation_method": "absolute_deviation_from_0.5"
                }
            )
            return consistency_score
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error calculating trend consistency",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "data_points": len(prices),
                    "operation": "_calculate_trend_consistency"
                }
            )
            return 0.5
    # Measure trend consistency - End

    # Calculate trend strength using moving averages - Begin
    def _calculate_trend_strength(self, prices: pd.DataFrame) -> float:
        if len(prices) < 20:
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Insufficient data for trend strength calculation",
                context_provider={
                    "data_points_available": len(prices),
                    "minimum_required": 20,
                    "calculation_result": "insufficient_data"
                }
            )
            return 0.5
            
        try:
            short_ma = prices['close'].rolling(10).mean()
            long_ma = prices['close'].rolling(20).mean()
            
            if pd.isna(short_ma.iloc[-1]) or pd.isna(long_ma.iloc[-1]):
                context_logger.log_event(
                    TradingEventType.MARKET_CONDITION,
                    "Incomplete moving averages for trend strength",
                    context_provider={
                        "short_ma_value": short_ma.iloc[-1],
                        "long_ma_value": long_ma.iloc[-1],
                        "calculation_result": "incomplete_moving_averages"
                    }
                )
                return 0.5
                
            price_diff = abs(short_ma.iloc[-1] - long_ma.iloc[-1])
            trend_strength = price_diff / long_ma.iloc[-1]
            normalized_strength = min(trend_strength * 10, 1.0)
            
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Trend strength calculation completed",
                context_provider={
                    "short_ma_period": 10,
                    "long_ma_period": 20,
                    "price_difference": price_diff,
                    "raw_trend_strength": trend_strength,
                    "normalized_strength": normalized_strength,
                    "scaling_factor": 10,
                    "calculation_method": "moving_average_divergence"
                }
            )
            return normalized_strength
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error calculating trend strength",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "data_points": len(prices),
                    "operation": "_calculate_trend_strength"
                }
            )
            return 0.5
    # Calculate trend strength using moving averages - End

    # Calculate ADX using analytics service - Begin
    def _calculate_adx(self, prices: pd.DataFrame) -> float:
        if not self.analytics_service or not hasattr(self.analytics_service, 'calculate_adx'):
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "Using default ADX value - analytics service unavailable",
                context_provider={
                    "analytics_service_available": self.analytics_service is not None,
                    "calculate_adx_method_available": hasattr(self.analytics_service, 'calculate_adx') if self.analytics_service else False,
                    "default_adx_value": 25.0
                }
            )
            return 25.0
            
        try:
            adx_value = self.analytics_service.calculate_adx(prices, period=14)
            context_logger.log_event(
                TradingEventType.MARKET_CONDITION,
                "ADX calculation completed via analytics service",
                context_provider={
                    "adx_period": 14,
                    "calculated_adx": adx_value,
                    "data_points_used": len(prices)
                }
            )
            return adx_value
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error calculating ADX via analytics service",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "adx_period": 14,
                    "operation": "_calculate_adx"
                }
            )
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
        compatible_timeframes = compatibility_map.get(timeframe, [timeframe])
        
        context_logger.log_event(
            TradingEventType.MARKET_CONDITION,
            "Compatible timeframes retrieved",
            context_provider={
                "base_timeframe": timeframe,
                "compatible_timeframes": compatible_timeframes,
                "mapping_source": "predefined_compatibility_map"
            }
        )
        return compatible_timeframes
    # Get compatible timeframes mapping - End

    # Check if cached value is still valid - Begin
    def _is_cache_valid(self, cache_key: str) -> bool:
        if cache_key in self._cache:
            cached_data = self._cache[cache_key]
            is_valid = datetime.now() < cached_data['expiry']
            
            if is_valid:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Cache hit - using cached value",
                    context_provider={
                        "cache_key": cache_key,
                        "cached_value": cached_data['value'],
                        "expiry_time": cached_data['expiry'].isoformat(),
                        "time_remaining_minutes": (cached_data['expiry'] - datetime.now()).total_seconds() / 60
                    }
                )
            else:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Cache expired - will refresh",
                    context_provider={
                        "cache_key": cache_key,
                        "cached_value": cached_data['value'],
                        "expiry_time": cached_data['expiry'].isoformat(),
                        "current_time": datetime.now().isoformat()
                    }
                )
            return is_valid
        return False
    # Check if cached value is still valid - End

    # Clear all cached data - Begin
    def clear_cache(self):
        cache_size_before = len(self._cache)
        self._cache = {}
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Market context cache cleared",
            context_provider={
                "cache_entries_cleared": cache_size_before,
                "current_cache_size": 0
            },
            decision_reason="CACHE_CLEARED_MANUALLY"
        )
    # Clear all cached data - End
# Market Context Service - Main class definition - End