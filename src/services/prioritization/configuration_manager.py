"""
Configuration management service for prioritization.
Handles configuration validation, defaults, and management.
"""

from typing import Dict
from src.core.context_aware_logger import get_context_logger, TradingEventType


class ConfigurationManager:
    """Handles configuration management for prioritization service."""
    
    def __init__(self):
        self.context_logger = get_context_logger()

    def _get_default_config(self) -> Dict:
        """Get default configuration that matches the new prioritization_config.py structure."""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Loading default prioritization configuration",
            context_provider={}
        )
        # <Context-Aware Logging Integration - End>
            
        return {
            'weights': {
                'fill_prob': 0.35,      # Reduced from 0.45 to match new config
                'manual_priority': 0.20,
                'efficiency': 0.15,
                'timeframe_match': 0.15, # Increased from 0.08
                'setup_bias': 0.10,      # Increased from 0.02
                'size_pref': 0.03,       # Reduced from 0.10
                'timeframe_match_legacy': 0.01,
                'setup_bias_legacy': 0.01
            },
            'max_open_orders': 5,
            'max_capital_utilization': 0.8,
            'enable_advanced_features': True,
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
                'min_trades_for_bias': 10,
                'min_win_rate': 0.4,
                'min_profit_factor': 1.2,
                'recent_period_days': 90,
                'confidence_threshold': 0.7
            },
            'two_layer_prioritization': {
                'enabled': True,
                'min_fill_probability': 0.4,
                'quality_weights': {
                    'manual_priority': 0.30,
                    'efficiency': 0.25,
                    'risk_reward': 0.25,
                    'timeframe_match': 0.10,
                    'setup_bias': 0.10
                }
            }
        }

    def _validate_config(self, config: Dict) -> bool:
        """Validate that the configuration has the expected structure."""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Validating prioritization configuration",
            context_provider={
                "config_keys": list(config.keys()) if config else [],
                "config_has_two_layer": 'two_layer_prioritization' in config
            }
        )
        # <Context-Aware Logging Integration - End>
            
        if not config:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Empty configuration provided",
                context_provider={},
                decision_reason="Configuration validation failed"
            )
            # <Context-Aware Logging Integration - End>
            return False
        
        # Check if this is the new two-layer config format
        if 'two_layer_prioritization' in config:
            two_layer = config['two_layer_prioritization']
            if not isinstance(two_layer, dict):
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Invalid two_layer_prioritization configuration",
                    context_provider={
                        "two_layer_type": type(two_layer).__name__
                    },
                    decision_reason="Configuration validation failed"
                )
                # <Context-Aware Logging Integration - End>
                return False
            if 'enabled' not in two_layer:
                two_layer['enabled'] = True  # Default to enabled
            if 'min_fill_probability' not in two_layer:
                two_layer['min_fill_probability'] = 0.4  # Default value
            if 'quality_weights' not in two_layer:
                two_layer['quality_weights'] = {
                    'manual_priority': 0.30,
                    'efficiency': 0.25,
                    'risk_reward': 0.25,
                    'timeframe_match': 0.10,
                    'setup_bias': 0.10
                }
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Configuration validation successful",
            context_provider={
                "two_layer_enabled": config.get('two_layer_prioritization', {}).get('enabled', False)
            }
        )
        # <Context-Aware Logging Integration - End>
        return True