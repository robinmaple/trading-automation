"""
Configuration for enhanced prioritization service with advanced features.
Provides timeframe compatibility mapping and setup performance thresholds.
"""

DEFAULT_PRIORITIZATION_CONFIG = {
    'weights': {
        'fill_prob': 0.35,      # Fill probability importance (reduced from 0.45)
        'manual_priority': 0.20, # Manual priority importance  
        'efficiency': 0.15,      # Capital efficiency importance
        'timeframe_match': 0.15, # Timeframe matching importance (increased from 0.08)
        'setup_bias': 0.10,      # Setup bias importance (increased from 0.02)
        'size_pref': 0.03,       # Size preference (reduced from 0.10)
        'timeframe_match_legacy': 0.01,  # Legacy compatibility
        'setup_bias_legacy': 0.01         # Legacy compatibility
    },
    
    'max_open_orders': 5,
    'max_capital_utilization': 0.8,
    'enable_advanced_features': True,  # Master toggle for advanced features
    
    'timeframe_compatibility_map': {
        '1min': ['1min', '5min'],
        '5min': ['1min', '5min', '15min'],
        '15min': ['5min', '15min', '30min', '1H'],
        '30min': ['15min', '30min', '1H'],
        '1H': ['30min', '1H', '4H', '15min'],
        '4H': ['1H', '4H', '1D'],
        '1D': ['4H', '1D', '1W'],
        '1W': ['1D', '1W']
    },
    
    'setup_performance_thresholds': {
        'min_trades_for_bias': 10,      # Minimum trades to consider setup performance
        'min_win_rate': 0.4,            # Minimum win rate for positive bias
        'min_profit_factor': 1.2,       # Minimum profit factor for positive bias
        'recent_period_days': 90,       # Lookback period for performance analysis
        'confidence_threshold': 0.7     # Minimum confidence for strong bias
    },
    
    'market_regime_weights': {
        'trending': {
            'timeframe_match': 0.20,    # Higher weight for timeframe matching in trends
            'fill_prob': 0.30           # Lower weight for fill probability in trends
        },
        'ranging': {
            'fill_prob': 0.40,          # Higher weight for fill probability in ranges
            'efficiency': 0.20          # Higher weight for efficiency in ranges
        },
        'volatile': {
            'size_pref': 0.08,          # Higher preference for smaller sizes in volatility
            'manual_priority': 0.25     # Higher weight for manual override in volatility
        },
        'calm': {
            'efficiency': 0.25,         # Higher weight for efficiency in calm markets
            'setup_bias': 0.12          # Higher weight for setup bias in calm markets
        }
    }
}

CONSERVATIVE_CONFIG = {
    **DEFAULT_PRIORITIZATION_CONFIG,
    'weights': {
        'fill_prob': 0.40,
        'manual_priority': 0.25,
        'efficiency': 0.15,
        'timeframe_match': 0.10,
        'setup_bias': 0.05,
        'size_pref': 0.03,
        'timeframe_match_legacy': 0.01,
        'setup_bias_legacy': 0.01
    },
    'setup_performance_thresholds': {
        **DEFAULT_PRIORITIZATION_CONFIG['setup_performance_thresholds'],
        'min_trades_for_bias': 15,
        'min_win_rate': 0.45,
        'min_profit_factor': 1.5
    }
}

AGGRESSIVE_CONFIG = {
    **DEFAULT_PRIORITIZATION_CONFIG,
    'weights': {
        'fill_prob': 0.30,
        'manual_priority': 0.15,
        'efficiency': 0.10,
        'timeframe_match': 0.20,
        'setup_bias': 0.15,
        'size_pref': 0.05,
        'timeframe_match_legacy': 0.02,
        'setup_bias_legacy': 0.03
    },
    'setup_performance_thresholds': {
        **DEFAULT_PRIORITIZATION_CONFIG['setup_performance_thresholds'],
        'min_trades_for_bias': 5,
        'min_win_rate': 0.35,
        'min_profit_factor': 1.1
    }
}

def get_config(environment: str = 'default') -> dict:
    """
    Get prioritization configuration for specific environment.
    
    Args:
        environment: Configuration environment ('default', 'conservative', 'aggressive')
        
    Returns:
        Configuration dictionary for the specified environment
    """
    configs = {
        'default': DEFAULT_PRIORITIZATION_CONFIG,
        'conservative': CONSERVATIVE_CONFIG,
        'aggressive': AGGRESSIVE_CONFIG
    }
    
    return configs.get(environment, DEFAULT_PRIORITIZATION_CONFIG).copy()

def validate_config(config: dict) -> tuple[bool, str]:
    """
    Validate prioritization configuration.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check weights sum to approximately 1.0
    weights_sum = sum(config['weights'].values())
    if abs(weights_sum - 1.0) > 0.001:
        return False, f"Weights sum to {weights_sum:.3f}, should be 1.0"
    
    # Check required sections exist
    required_sections = ['weights', 'max_open_orders', 'max_capital_utilization']
    for section in required_sections:
        if section not in config:
            return False, f"Missing required section: {section}"
    
    # Check timeframe compatibility map
    if 'timeframe_compatibility_map' in config:
        for timeframe, compatibilities in config['timeframe_compatibility_map'].items():
            if timeframe not in compatibilities:
                return False, f"Timeframe {timeframe} must include itself in compatibility list"
    
    return True, "Configuration is valid"