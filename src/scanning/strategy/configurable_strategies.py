# src/scanner/strategy/configurable_strategies.py
from src.scanning.strategy.strategy_core import StrategyConfig, StrategyType
from config.scanner_config import ScannerConfig
from typing import List

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

def create_configurable_bull_trend_pullback_config(scanner_config) -> StrategyConfig:
    """Create fully configurable bull trend pullback strategy"""
    return StrategyConfig(
        name="bull_trend_pullback",
        strategy_type=StrategyType.BULL_PULLBACK,
        required_criteria=get_bull_trend_pullback_criteria_list(scanner_config),
        parameters={
            # Configurable EMA Periods
            'short_term_ema': scanner_config.ema_short_term,
            'medium_term_ema': scanner_config.ema_medium_term,
            'long_term_ema': scanner_config.ema_long_term,
            
            # Configurable Pullback
            'max_pullback_pct': scanner_config.max_pullback_distance_pct,
            'ideal_pullback_range': scanner_config.ideal_pullback_range_pct,
            
            # Configurable Behavior
            'min_confidence': scanner_config.min_confidence_score,
            'max_candidates': scanner_config.max_candidates
        },
        weights={
            'base_criteria': 0.3,
            'strategy': 0.7
        }
    )

def create_aggressive_bull_trend_config() -> StrategyConfig:
    """Example: Aggressive configuration"""
    
    aggressive_config = ScannerConfig(
        min_volume=500_000,           # Lower volume requirement
        min_market_cap=5_000_000_000, # Lower market cap  
        min_price=2.0,                # Lower price
        ema_short_term=8,             # More sensitive short-term EMA
        ema_medium_term=15,           # Tighter medium-term
        ema_long_term=34,             # Different long-term
        max_pullback_distance_pct=5.0, # Wider pullback allowance
        min_confidence_score=50       # Lower confidence threshold
    )
    
    return create_configurable_bull_trend_pullback_config(aggressive_config)

def create_conservative_bull_trend_config() -> StrategyConfig:
    """Example: Conservative configuration"""
   
    conservative_config = ScannerConfig(
        min_volume=2_000_000,          # Higher volume requirement
        min_market_cap=20_000_000_000, # Higher market cap
        min_price=10.0,                # Higher price
        ema_short_term=13,             # Less sensitive short-term
        ema_medium_term=21,            # Standard medium-term  
        ema_long_term=55,              # Longer-term view
        max_pullback_distance_pct=2.0, # Tighter pullback
        min_confidence_score=70        # Higher confidence threshold
    )
    
    return create_configurable_bull_trend_pullback_config(conservative_config)