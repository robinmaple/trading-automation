"""
Configuration for Phase B Prioritization Service weights and parameters.
This file contains the default conservative weights as specified in Phase B requirements.
"""

# Default conservative configuration as specified in Phase B requirements
DEFAULT_CONFIG = {
    'weights': {
        'fill_prob': 0.45,      # Fill probability importance (45%)
        'manual_priority': 0.20, # Manual priority importance (20%)  
        'efficiency': 0.15,      # Capital efficiency importance (15%)
        'size_pref': 0.10,       # Size preference - prefer smaller positions (10%)
        'timeframe_match': 0.08, # Timeframe matching score (8%) - placeholder for future
        'setup_bias': 0.02       # Setup bias score (2%) - placeholder for future
    },
    'max_open_orders': 5,        # Maximum number of open working orders
    'max_capital_utilization': 0.8,  # Maximum fraction of total capital to commit (80%)
    'min_fill_probability': 0.3,  # Minimum fill probability to consider for execution
    'min_deterministic_score': 0.4  # Minimum overall score to consider for execution
}

# Aggressive configuration for high-conviction trading
AGGRESSIVE_CONFIG = {
    'weights': {
        'fill_prob': 0.35,      # Lower emphasis on fill probability
        'manual_priority': 0.25, # Higher emphasis on manual priority
        'efficiency': 0.20,      # Higher emphasis on capital efficiency
        'size_pref': 0.10,       # Same size preference
        'timeframe_match': 0.07, # Slightly lower timeframe matching
        'setup_bias': 0.03       # Slightly higher setup bias
    },
    'max_open_orders': 8,        # Allow more open orders
    'max_capital_utilization': 0.9,  # Use more capital
    'min_fill_probability': 0.2,  # Lower minimum fill probability
    'min_deterministic_score': 0.3  # Lower minimum score
}

# Conservative configuration for risk-averse trading
CONSERVATIVE_CONFIG = {
    'weights': {
        'fill_prob': 0.55,      # Higher emphasis on fill probability
        'manual_priority': 0.15, # Lower emphasis on manual priority
        'efficiency': 0.12,      # Lower emphasis on capital efficiency
        'size_pref': 0.12,       # Higher size preference (more cautious)
        'timeframe_match': 0.05, # Lower timeframe matching
        'setup_bias': 0.01       # Lower setup bias
    },
    'max_open_orders': 3,        # Fewer open orders
    'max_capital_utilization': 0.6,  # Use less capital
    'min_fill_probability': 0.5,  # Higher minimum fill probability
    'min_deterministic_score': 0.5  # Higher minimum score
}

# Configuration focused on capital efficiency
EFFICIENCY_CONFIG = {
    'weights': {
        'fill_prob': 0.30,      # Lower emphasis on fill probability
        'manual_priority': 0.15, # Lower emphasis on manual priority
        'efficiency': 0.40,      # Much higher emphasis on capital efficiency
        'size_pref': 0.10,       # Standard size preference
        'timeframe_match': 0.04, # Lower timeframe matching
        'setup_bias': 0.01       # Lower setup bias
    },
    'max_open_orders': 4,
    'max_capital_utilization': 0.7,
    'min_fill_probability': 0.4,
    'min_deterministic_score': 0.45
}

# Configuration mappings for easy reference
CONFIGURATIONS = {
    'default': DEFAULT_CONFIG,
    'aggressive': AGGRESSIVE_CONFIG,
    'conservative': CONSERVATIVE_CONFIG,
    'efficiency': EFFICIENCY_CONFIG
}

def get_configuration(config_name='default'):
    """
    Get a prioritization configuration by name.
    
    Args:
        config_name: Name of the configuration ('default', 'aggressive', 'conservative', 'efficiency')
    
    Returns:
        Configuration dictionary
        
    Raises:
        ValueError: If config_name is not found
    """
    if config_name not in CONFIGURATIONS:
        raise ValueError(f"Unknown configuration: {config_name}. Available: {list(CONFIGURATIONS.keys())}")
    
    return CONFIGURATIONS[config_name].copy()  # Return a copy to avoid modification

def validate_config(config):
    """
    Validate a prioritization configuration.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required keys
    required_keys = ['weights', 'max_open_orders', 'max_capital_utilization']
    for key in required_keys:
        if key not in config:
            return False, f"Missing required key: {key}"
    
    # Check weights sum to approximately 1.0
    weights_sum = sum(config['weights'].values())
    if abs(weights_sum - 1.0) > 0.001:
        return False, f"Weights must sum to 1.0 (current sum: {weights_sum})"
    
    # Check weight keys
    required_weight_keys = ['fill_prob', 'manual_priority', 'efficiency', 'size_pref', 'timeframe_match', 'setup_bias']
    for key in required_weight_keys:
        if key not in config['weights']:
            return False, f"Missing required weight key: {key}"
    
    # Check parameter ranges
    if config['max_open_orders'] < 1:
        return False, "max_open_orders must be at least 1"
    
    if not (0 < config['max_capital_utilization'] <= 1.0):
        return False, "max_capital_utilization must be between 0 and 1.0"
    
    return True, "Configuration is valid"