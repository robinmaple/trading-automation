# src/scanner/technical_scorer.py
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime
import logging

class TechnicalScorer:
    """Handles technical analysis and scoring based on EMA criteria"""
    
    def __init__(self, ema_periods: List[int] = None, pullback_threshold: float = 0.02):
        self.ema_periods = ema_periods or [10, 20, 50, 100]
        self.pullback_threshold = pullback_threshold
        self.logger = logging.getLogger(__name__)
    
    def calculate_emas(self, prices: pd.Series) -> Dict[int, float]:
        """Calculate EMA values for configured periods"""
        emas = {}
        for period in self.ema_periods:
            if len(prices) >= period:
                emas[period] = prices.ewm(span=period).mean().iloc[-1]
            else:
                emas[period] = None
        return emas
    
    def calculate_bull_trend_score(self, current_price: float, emas: Dict[int, float]) -> int:
        """
        Bull Trend Score: Price > EMA10 > EMA50 > EMA100
        Returns score 0-100
        """
        required_emas = [10, 50, 100]
        
        # Check if we have all required EMAs
        if any(emas.get(period) is None for period in required_emas):
            return 0
        
        # Check trend alignment
        conditions = [
            current_price > emas[10],
            emas[10] > emas[50],
            emas[50] > emas[100]
        ]
        
        # Score based on how many conditions are met
        met_conditions = sum(conditions)
        return int((met_conditions / len(conditions)) * 100)
    
    def calculate_bull_pullback_score(self, current_price: float, emas: Dict[int, float]) -> int:
        """
        Bull Pullback Score: Price within 2% of EMA20 (but above EMA50)
        Returns score 0-100
        """
        if emas.get(20) is None or emas.get(50) is None:
            return 0
        
        # Check if above EMA50
        if current_price <= emas[50]:
            return 0
        
        # Calculate distance from EMA20
        price_distance = abs(current_price - emas[20]) / emas[20]
        
        # Score based on proximity to EMA20 (closer = higher score)
        if price_distance <= self.pullback_threshold:
            # Normalize to 0-100 scale (closer = higher score)
            proximity_score = (1 - (price_distance / self.pullback_threshold)) * 100
            return int(proximity_score)
        
        return 0
    
    def calculate_total_score(self, trend_score: int, pullback_score: int) -> int:
        """Calculate weighted total score"""
        return int(0.6 * trend_score + 0.4 * pullback_score)