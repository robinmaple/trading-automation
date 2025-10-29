"""
Tests for risk configuration module.
"""
import pytest
from decimal import Decimal
from config.risk_config import (
    DEFAULT_RISK_CONFIG, 
    validate_risk_config, 
    get_risk_config,
    merge_risk_config
)
from src.trading.orders.planned_order import PositionStrategy


class TestRiskConfig:
    """Test risk configuration functionality."""
    
    def test_default_config_valid(self):
        """Test that default configuration passes validation."""
        is_valid, message = validate_risk_config(DEFAULT_RISK_CONFIG)
        assert is_valid, f"Default config should be valid: {message}"
    
    def test_get_risk_config_default(self):
        """Test getting default risk configuration."""
        config = get_risk_config('default')
        assert config is not None
        assert config == DEFAULT_RISK_CONFIG
    
    def test_get_risk_config_invalid_environment(self):
        """Test that invalid environment raises error."""
        with pytest.raises(ValueError, match="Unknown risk environment"):
            get_risk_config('invalid_env')
    
    def test_merge_risk_config(self):
        """Test merging custom configuration with defaults."""
        custom_config = {
            'loss_limits': {
                'daily': Decimal('0.03')  # Override daily loss limit
            },
            'new_setting': 'custom_value'  # Add new setting
        }
        
        merged = merge_risk_config(custom_config)
        
        # Should have overridden values
        assert merged['loss_limits']['daily'] == Decimal('0.03')
        # Should have preserved other values
        assert merged['loss_limits']['weekly'] == Decimal('0.05')
        # Should have new setting
        assert merged['new_setting'] == 'custom_value'
    
    def test_validate_invalid_loss_limit(self):
        """Test validation fails for invalid loss limits."""
        invalid_config = DEFAULT_RISK_CONFIG.copy()
        invalid_config['loss_limits']['daily'] = Decimal('0.6')  # Too high
        
        is_valid, message = validate_risk_config(invalid_config)
        assert not is_valid
        assert "daily loss limit must be between 0 and 0.5" in message
    
    def test_validate_invalid_position_limit(self):
        """Test validation fails for invalid position limits."""
        invalid_config = DEFAULT_RISK_CONFIG.copy()
        invalid_config['position_limits'][PositionStrategy.CORE]['single_trade'] = Decimal('1.5')  # Too high
        
        is_valid, message = validate_risk_config(invalid_config)
        assert not is_valid
        assert "must be between 0 and 1" in message