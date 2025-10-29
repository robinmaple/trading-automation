"""
Risk management configuration with sensible defaults.
All values can be overridden by passing a custom config to RiskManagementService.
"""
from decimal import Decimal
from typing import Dict, Any
from src.trading.orders.planned_order import PositionStrategy


# Default risk configuration - can be overridden
DEFAULT_RISK_CONFIG = {
    'position_limits': {
        PositionStrategy.CORE: {
            'single_trade': Decimal('0.3'),    # 30% per trade
            'total_exposure': Decimal('0.6')   # 60% total exposure
        },
        PositionStrategy.HYBRID: {
            'single_trade': Decimal('0.3'),    # 30% per trade  
            'total_exposure': Decimal('0.6')   # 60% total exposure
        }
    },
    
    'loss_limits': {
        'daily': Decimal('0.02'),    # 2% daily loss limit
        'weekly': Decimal('0.05'),   # 5% weekly loss limit
        'monthly': Decimal('0.08')   # 8% monthly loss limit
    },
    
    'check_intervals': {
        'trading_halt_check': 300,    # 5 minutes (seconds)
        'exposure_recalculation': 60,  # 1 minute (seconds)
        'risk_status_refresh': 30      # 30 seconds (seconds)
    },
    
    'defaults': {
        'simulation_equity': Decimal('100000'),  # Default paper trading equity
        'max_open_orders': 5,                    # Maximum simultaneous orders
        'min_fill_probability': Decimal('0.4')   # Minimum fill probability for execution
    },
    
    'trading_halts': {
        'enabled': True,              # Whether trading halts are enabled
        'hard_stop': True,            # True = hard block, False = soft warning
        'notify_on_halt': True,       # Send notifications on trading halts
        'auto_resume': False          # Automatically resume when limits are no longer exceeded
    }
}


def validate_risk_config(config: Dict[str, Any]) -> tuple[bool, str]:
    """
    Validate risk configuration parameters.
    
    Args:
        config: Risk configuration dictionary to validate
        
    Returns:
        Tuple of (is_valid, message) where message describes any validation errors
    """
    try:
        # Validate position limits
        position_limits = config.get('position_limits', {})
        for strategy, limits in position_limits.items():
            if not (Decimal('0') < limits['single_trade'] <= Decimal('1')):
                return False, f"Position limit for {strategy} must be between 0 and 1"
            if not (Decimal('0') < limits['total_exposure'] <= Decimal('1')):
                return False, f"Total exposure for {strategy} must be between 0 and 1"
        
        # Validate loss limits
        loss_limits = config.get('loss_limits', {})
        for period, limit in loss_limits.items():
            if not (Decimal('0') <= limit <= Decimal('0.5')):
                return False, f"{period} loss limit must be between 0 and 0.5"
        
        # Validate check intervals
        intervals = config.get('check_intervals', {})
        for interval_name, seconds in intervals.items():
            if not (0 < seconds <= 3600):
                return False, f"{interval_name} must be between 1 and 3600 seconds"
        
        # Validate defaults
        defaults = config.get('defaults', {})
        if defaults['simulation_equity'] <= Decimal('0'):
            return False, "Simulation equity must be positive"
        if defaults['max_open_orders'] <= 0:
            return False, "Max open orders must be positive"
        if not (Decimal('0') <= defaults['min_fill_probability'] <= Decimal('1')):
            return False, "Min fill probability must be between 0 and 1"
            
        return True, "Configuration is valid"
        
    except (KeyError, TypeError, ValueError) as e:
        return False, f"Invalid configuration structure: {str(e)}"


def get_risk_config(environment: str = 'default') -> Dict[str, Any]:
    """
    Get risk configuration for a specific environment.
    
    Args:
        environment: Environment name ('default', 'paper', 'live', etc.)
        
    Returns:
        Risk configuration dictionary for the specified environment
        
    Raises:
        ValueError: If environment is not found
    """
    # For now, we only have default configuration
    # In future, could support different environments
    if environment != 'default':
        raise ValueError(f"Unknown risk environment: {environment}")
    
    return DEFAULT_RISK_CONFIG.copy()  # Return a copy to avoid modification


def merge_risk_config(custom_config: Dict[str, Any], base_config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Merge custom configuration with base configuration.
    
    Args:
        custom_config: Custom configuration to apply
        base_config: Base configuration to merge into (defaults to DEFAULT_RISK_CONFIG)
        
    Returns:
        Merged configuration dictionary
    """
    if base_config is None:
        base_config = DEFAULT_RISK_CONFIG.copy()
    
    merged_config = base_config.copy()
    
    # Deep merge for nested dictionaries
    for key, value in custom_config.items():
        if (key in merged_config and 
            isinstance(merged_config[key], dict) and 
            isinstance(value, dict)):
            # Recursively merge nested dictionaries
            merged_config[key] = merge_risk_config(value, merged_config[key])
        else:
            # Overwrite or add new key
            merged_config[key] = value
            
    return merged_config