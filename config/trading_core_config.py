"""
Configuration for core trading system parameters.
Contains risk limits, execution thresholds, order defaults, and simulation settings.
"""

from decimal import Decimal
from typing import Any, Dict
import copy

# Base configuration structure
BASE_TRADING_CORE_CONFIG: Dict[str, Any] = {
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
    },
    # <AON Execution Configuration - Begin>
    'aon_execution': {
        'enabled': True,                                  # AON enabled by default
        'default_volume_percentage': Decimal('0.001'),    # 0.1% of daily volume
        'symbol_specific': {                              # Symbol-specific percentages
            'SPY': Decimal('0.002'),      # 0.2% - High liquidity ETF
            'QQQ': Decimal('0.002'),      # 0.2% - High liquidity ETF  
            'IWM': Decimal('0.0005'),     # 0.05% - Lower liquidity ETF
            'AAPL': Decimal('0.0015'),    # 0.15% - High liquidity stock
            'TSLA': Decimal('0.0008'),    # 0.08% - Medium liquidity stock
        },
        'fallback_fixed_notional': 50000, # Fixed fallback if volume data unavailable
        'track_metrics': True             # Enable AON performance tracking
    },
    # <AON Execution Configuration - End>
    # <Event System Configuration - Begin>
    'event_system': {
        'event_bus': {
            'enabled': True,              # Enable Event Bus system
            'enable_logging': True,       # Log event publishing/subscriptions
            'max_subscribers': 50,        # Maximum subscribers per event type
            'log_level': 'INFO'           # DEBUG, INFO, WARNING
        },
        'events': {
            'price_update': {
                'enabled': True,          # Enable price update events
                'min_price_change': 0.01, # Minimum price change to trigger event ($0.01)
                'log_level': 'INFO'
            },
            'order_executed': {
                'enabled': True,          # Enable order executed events
                'log_level': 'INFO'
            },
            'system_health': {
                'enabled': True,          # Enable system health events
                'log_level': 'WARNING'
            }
        }
    },
    # <Event System Configuration - End>
    # <End of Day Configuration - Begin>
    'end_of_day': {
        'enabled': True,                  # Enable EOD process by default
        'close_buffer_minutes': 15,       # 15 minutes before market close to start closing
        'pre_market_start_minutes': 30,   # 30 minutes before market open to start program
        'post_market_end_minutes': 30,    # 30 minutes after market close to stop program
        'max_close_attempts': 3,          # Maximum attempts to close a position
        'close_day_positions': True,      # Close all DAY strategy positions
        'close_expired_hybrid': True,     # Close expired HYBRID positions
        'expire_planned_orders': True,    # Expire corresponding PlannedOrders
        'leave_core_positions': True      # Leave CORE positions with bracket orders
    }
    # <End of Day Configuration - End>
}

# Paper trading configuration - same as base but with explicit name
PAPER_TRADING_CONFIG = copy.deepcopy(BASE_TRADING_CORE_CONFIG)

# Live trading configuration - more conservative settings
LIVE_TRADING_CONFIG = copy.deepcopy(BASE_TRADING_CORE_CONFIG)
LIVE_TRADING_CONFIG['risk_limits'].update({
    'daily_loss_pct': Decimal('0.015'),   # 1.5% (more conservative)
    'weekly_loss_pct': Decimal('0.04'),   # 4% (more conservative)
    'monthly_loss_pct': Decimal('0.06'),  # 6% (more conservative)
    'max_risk_per_trade': Decimal('0.015') # 1.5% (more conservative)
})
LIVE_TRADING_CONFIG['order_defaults'].update({
    'risk_per_trade': Decimal('0.003'),   # 0.3% (more conservative)
    'risk_reward_ratio': Decimal('2.5')   # 2.5 (higher reward target)
})
# <AON Live Trading Configuration - Begin>
LIVE_TRADING_CONFIG['aon_execution'].update({
    'default_volume_percentage': Decimal('0.0008'),  # More conservative 0.08%
    'fallback_fixed_notional': 25000,                # Lower fallback for live
    'symbol_specific': {
        'SPY': Decimal('0.0015'),    # More conservative for live
        'QQQ': Decimal('0.0015'),    # More conservative for live
        'IWM': Decimal('0.0003'),    # More conservative for live
        'AAPL': Decimal('0.0010'),   # More conservative for live
        'TSLA': Decimal('0.0005'),   # More conservative for live
    }
})
# <AON Live Trading Configuration - End>

# <End of Day Live Trading Configuration - Begin>
LIVE_TRADING_CONFIG['end_of_day'].update({
    'close_buffer_minutes': 20,      # More conservative: 20 minutes before close
    'max_close_attempts': 5,         # More attempts for live trading reliability
    'pre_market_start_minutes': 45,  # Start earlier for live trading prep
    'post_market_end_minutes': 60    # Run longer for post-market analysis
})
# <End of Day Live Trading Configuration - End>

# Environment configurations
CONFIGS = {
    'paper': PAPER_TRADING_CONFIG,
    'live': LIVE_TRADING_CONFIG,
    'default': BASE_TRADING_CORE_CONFIG
}

def get_config(environment: str = 'default') -> Dict[str, Any]:
    """
    Get trading core configuration for specific environment.
    
    Args:
        environment: Configuration environment ('paper', 'live', 'default')
        
    Returns:
        Configuration dictionary for the specified environment.
        
    Raises:
        ValueError: If the requested environment is not found.
    """
    if environment not in CONFIGS:
        raise ValueError(f"Unknown trading core environment: {environment}. "
                         f"Available: {list(CONFIGS.keys())}")
    
    # Return a deep copy to prevent accidental mutation
    return copy.deepcopy(CONFIGS[environment])

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
    if not Decimal('0') < order_defaults['risk_per_trade'] <= Decimal('0.02'):  # 2% upper bound
        return False, "risk_per_trade must be between 0 and 0.1"
    if order_defaults['risk_reward_ratio'] < Decimal('1.0'):
        return False, "risk_reward_ratio must be at least 1.0"
    if not 1 <= order_defaults['priority'] <= 5:
        return False, "priority must be between 1 and 5"

    # Validate simulation settings
    simulation = config['simulation']
    if simulation['default_equity'] <= Decimal('0'):
        return False, "default_equity must be positive"

    # <AON Configuration Validation - Begin>
    # Validate AON execution settings if present
    if 'aon_execution' in config:
        aon_config = config['aon_execution']
        
        # Validate volume percentages are reasonable
        if 'default_volume_percentage' in aon_config:
            vol_pct = aon_config['default_volume_percentage']
            if vol_pct <= Decimal('0') or vol_pct > Decimal('0.01'):  # 1% upper bound
                return False, f"default_volume_percentage must be between 0 and 0.01, got {vol_pct}"
                
        # Validate symbol-specific percentages
        if 'symbol_specific' in aon_config:
            for symbol, percentage in aon_config['symbol_specific'].items():
                if percentage <= Decimal('0') or percentage > Decimal('0.01'):
                    return False, f"symbol_specific percentage for {symbol} must be between 0 and 0.01, got {percentage}"
                    
        # Validate fallback notional
        if 'fallback_fixed_notional' in aon_config:
            if aon_config['fallback_fixed_notional'] <= 0:
                return False, "fallback_fixed_notional must be positive"
    # <AON Configuration Validation - End>

    # <Event System Configuration Validation - Begin>
    # Validate event system settings if present
    if 'event_system' in config:
        event_config = config['event_system']
        
        # Validate event bus settings
        if 'event_bus' in event_config:
            event_bus_config = event_config['event_bus']
            if 'max_subscribers' in event_bus_config and event_bus_config['max_subscribers'] <= 0:
                return False, "event_bus.max_subscribers must be positive"
        
        # Validate individual event settings
        if 'events' in event_config:
            events_config = event_config['events']
            for event_name, event_settings in events_config.items():
                if 'min_price_change' in event_settings and event_settings['min_price_change'] < 0:
                    return False, f"events.{event_name}.min_price_change must be non-negative"
    # <Event System Configuration Validation - End>

    # <End of Day Configuration Validation - Begin>
    # Validate EOD settings if present
    if 'end_of_day' in config:
        eod_config = config['end_of_day']
        
        # Validate timing parameters
        timing_params = [
            'close_buffer_minutes',
            'pre_market_start_minutes', 
            'post_market_end_minutes'
        ]
        
        for param in timing_params:
            if param in eod_config:
                value = eod_config[param]
                if value < 0 or value > 240:  # 4 hours maximum
                    return False, f"end_of_day.{param} must be between 0 and 240 minutes, got {value}"
        
        # Validate max close attempts
        if 'max_close_attempts' in eod_config:
            attempts = eod_config['max_close_attempts']
            if attempts < 1 or attempts > 10:
                return False, f"end_of_day.max_close_attempts must be between 1 and 10, got {attempts}"
                
        # Validate boolean flags
        boolean_flags = [
            'enabled', 'close_day_positions', 'close_expired_hybrid',
            'expire_planned_orders', 'leave_core_positions'
        ]
        
        for flag in boolean_flags:
            if flag in eod_config and not isinstance(eod_config[flag], bool):
                return False, f"end_of_day.{flag} must be a boolean value"
    # <End of Day Configuration Validation - End>

    return True, "Configuration is valid"


# Example usage and self-test when run directly
if __name__ == "__main__":
    # Test loading all environment configs
    environments = ['paper', 'live', 'default']
    
    for env in environments:
        try:
            config = get_config(env)
            print(f"✓ Successfully loaded {env} configuration")
            
            # Validate the configuration
            is_valid, message = validate_config(config)
            if is_valid:
                print(f"✓ {env} configuration validation passed")
                
                # Show some key differences
                if env == 'live':
                    print(f"  Live settings: {config['risk_limits']['daily_loss_pct']} daily loss, "
                          f"{config['order_defaults']['risk_per_trade']} risk/trade")
                
                # Show AON settings
                if 'aon_execution' in config:
                    aon = config['aon_execution']
                    print(f"  AON: enabled={aon['enabled']}, default_volume_pct={aon['default_volume_percentage']}")
                    if aon['symbol_specific']:
                        print(f"  AON symbol-specific: {list(aon['symbol_specific'].keys())[:3]}...")
                
                # Show Event System settings
                if 'event_system' in config:
                    event_sys = config['event_system']
                    print(f"  Event System: enabled={event_sys['event_bus']['enabled']}")
                    print(f"  Events enabled: {list(event_sys['events'].keys())}")
                
                # Show End of Day settings
                if 'end_of_day' in config:
                    eod = config['end_of_day']
                    print(f"  EOD: enabled={eod['enabled']}, close_buffer={eod['close_buffer_minutes']}min")
                    print(f"  EOD operational window: {eod['pre_market_start_minutes']}min pre, {eod['post_market_end_minutes']}min post")
                    print(f"  EOD actions: DAY={eod['close_day_positions']}, HYBRID={eod['close_expired_hybrid']}, CORE={eod['leave_core_positions']}")
            else:
                print(f"✗ {env} configuration validation failed: {message}")
                
        except Exception as e:
            print(f"✗ Failed to load {env} configuration: {e}")
        
        print()