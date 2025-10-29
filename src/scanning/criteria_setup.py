# src/scanner/criteria_setup.py
from typing import List
from .criteria import CriteriaRegistry, FundamentalCriteria, TechnicalCriteria, LiquidityCriteria
from .criteria.criteria_core import CriteriaConfig, CriteriaType

def create_configurable_criteria_registry(scanner_config) -> CriteriaRegistry:
    """Create criteria registry with fully configurable parameters"""
    registry = CriteriaRegistry()
    
    # 1. Fundamental Criteria (Configurable)
    registry.register(FundamentalCriteria(CriteriaConfig(
        name="min_volume",
        criteria_type=CriteriaType.FUNDAMENTAL,
        parameters={'min_volume': scanner_config.min_volume}
    )))
    
    registry.register(FundamentalCriteria(CriteriaConfig(
        name="min_market_cap",
        criteria_type=CriteriaType.FUNDAMENTAL, 
        parameters={'min_market_cap': scanner_config.min_market_cap}
    )))
    
    registry.register(FundamentalCriteria(CriteriaConfig(
        name="min_price",
        criteria_type=CriteriaType.FUNDAMENTAL,
        parameters={'min_price': scanner_config.min_price}
    )))
    
    # Add max price if configured
    if scanner_config.max_price:
        registry.register(FundamentalCriteria(CriteriaConfig(
            name="max_price",
            criteria_type=CriteriaType.FUNDAMENTAL,
            parameters={'max_price': scanner_config.max_price}
        )))
    
    # 2. Technical Criteria (Fully Configurable EMA Periods)
    registry.register(TechnicalCriteria(CriteriaConfig(
        name="bull_trend_ema_alignment",
        criteria_type=CriteriaType.TECHNICAL,
        parameters={
            'short_term_ema': scanner_config.ema_short_term,
            'medium_term_ema': scanner_config.ema_medium_term,
            'long_term_ema': scanner_config.ema_long_term
        }
    )))
    
    registry.register(TechnicalCriteria(CriteriaConfig(
        name="price_pullback_to_ema",
        criteria_type=CriteriaType.TECHNICAL,
        parameters={
            'short_term_ema': scanner_config.ema_short_term,
            'max_pullback_distance_pct': scanner_config.max_pullback_distance_pct,
            'ideal_pullback_range_pct': scanner_config.ideal_pullback_range_pct
        }
    )))
    
    # 3. Additional Configurable Technical Criteria
    registry.register(TechnicalCriteria(CriteriaConfig(
        name="volume_confirmation",
        criteria_type=CriteriaType.TECHNICAL,
        parameters={
            'min_volume_ratio': 1.2,  # Configurable volume spike threshold
            'lookback_period': 10     # Configurable lookback
        }
    )))
    
    return registry

def get_bull_trend_pullback_criteria_list(scanner_config) -> List[str]:
    """Get the list of criteria names for bull trend pullback strategy"""
    base_criteria = [
        'min_volume',
        'min_market_cap', 
        'min_price',
        'bull_trend_ema_alignment',
        'price_pullback_to_ema'
    ]
    
    # Add max_price if configured
    if scanner_config.max_price:
        base_criteria.append('max_price')
    
    return base_criteria