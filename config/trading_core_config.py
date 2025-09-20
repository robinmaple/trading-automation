"""
Configuration for core trading system parameters.
Contains risk limits, execution thresholds, order defaults, and simulation settings.
"""

from decimal import Decimal
from typing import Any, Dict

DEFAULT_TRADING_CORE_CONFIG: Dict[str, Any] = {
    'risk_limits': {
        'daily_loss_pct': Decimal('0.02'),    # 2%
        'weekly_loss_pct': Decimal('0.05'),   # 5%
        'monthly_loss_pct': Decimal('0.08'),  # 8%
        'max_open_orders': 5,                 # Maximum simultaneous open orders
        'max_risk_per_trade': Decimal('0.02') # 2% maximum risk per trade (validation)
    },
    'execution': {
        'fill_probability_threshold': Decimal('0.7'), # Threshold for execution (e.g., 70%)
        'min_fill_probability': Decimal('0.4')        # Minimum probability to be considered viable
    },
    'order_defaults': {
        'risk_per_trade': Decimal('0.005'), # 0.5% default risk per trade
        'risk_reward_ratio': Decimal('2.0'),# Default risk/reward ratio
        'priority': 3                       # Default priority (1-5 scale)
    },
    'simulation': {
        'default_equity': Decimal('100000') # Default equity for simulation/paper trading
    },
    'monitoring': {
        'interval_seconds': 5,           # Main monitoring loop interval
        'max_errors': 10,                # Maximum consecutive errors before backing off
        'error_backoff_base': 60,        # Base backoff time in seconds
        'max_backoff': 300,              # Maximum backoff time in seconds
    },
    'market_close': {
        'buffer_minutes': 10            # Minutes before market close to start closing positions
    },
    'labeling': {
        'hours_back': 24,                # Hours back for labeling completed orders
        'state_change_hours_back': 1,    # Hours back for state change triggered labeling
    }
}

def get_config(environment: str = 'default') -> Dict[str, Any]:
    """
    Get trading core configuration for specific environment.
    
    Args:
        environment: Configuration environment. Currently only 'default' is supported.
        
    Returns:
        Configuration dictionary for the specified environment.
        
    Raises:
        ValueError: If the requested environment is not found.
    """
    configs = {
        'default': DEFAULT_TRADING_CORE_CONFIG,
    }
    
    if environment not in configs:
        raise ValueError(f"Unknown trading core environment: {environment}. "
                         f"Available: {list(configs.keys())}")
    
    # Return a deep copy to prevent accidental mutation of the default config
    import copy
    return copy.deepcopy(configs[environment])

def validate_config(config: Dict[str, Any]) -> tuple[bool, str]:
    """
    Validate trading core configuration.

    Args:
        config: Configuration dictionary to validate.

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required top-level sections exist
    required_sections = ['risk_limits', 'execution', 'order_defaults', 'simulation']
    for section in required_sections:
        if section not in config:
            return False, f"Missing required section: {section}"

    # Validate risk_limits
    risk_limits = config['risk_limits']
    required_risk_keys = ['daily_loss_pct', 'weekly_loss_pct', 'monthly_loss_pct', 
                         'max_open_orders', 'max_risk_per_trade']
    for key in required_risk_keys:
        if key not in risk_limits:
            return False, f"Missing required risk_limits key: {key}"

    # Validate percentages are positive and reasonable
    for period in ['daily', 'weekly', 'monthly']:
        loss_pct = risk_limits[f'{period}_loss_pct']
        if loss_pct <= Decimal('0') or loss_pct > Decimal('0.5'):  # 50% upper bound
            return False, f"{period}_loss_pct must be between 0 and 0.5, got {loss_pct}"

    if risk_limits['max_open_orders'] <= 0:
        return False, "max_open_orders must be positive"

    # Validate execution thresholds
    execution = config['execution']
    if not Decimal('0') <= execution['fill_probability_threshold'] <= Decimal('1'):
        return False, "fill_probability_threshold must be between 0 and 1"
    if not Decimal('0') <= execution['min_fill_probability'] <= Decimal('1'):
        return False, "min_fill_probability must be between 0 and 1"

    # Validate order defaults
    order_defaults = config['order_defaults']
    if not Decimal('0') < order_defaults['risk_per_trade'] <= Decimal('0.1'):  # 10% upper bound
        return False, "risk_per_trade must be between 0 and 0.1"
    if order_defaults['risk_reward_ratio'] < Decimal('1.0'):
        return False, "risk_reward_ratio must be at least 1.0"
    if not 1 <= order_defaults['priority'] <= 5:
        return False, "priority must be between 1 and 5"

    # Validate simulation settings
    simulation = config['simulation']
    if simulation['default_equity'] <= Decimal('0'):
        return False, "default_equity must be positive"

    return True, "Configuration is valid"


# Example usage and self-test when run directly
if __name__ == "__main__":
    # Test loading default config
    try:
        config = get_config('default')
        print("✓ Successfully loaded default configuration")
        
        # Validate the configuration
        is_valid, message = validate_config(config)
        if is_valid:
            print("✓ Configuration validation passed")
        else:
            print(f"✗ Configuration validation failed: {message}")
            
    except Exception as e:
        print(f"✗ Failed to load configuration: {e}")