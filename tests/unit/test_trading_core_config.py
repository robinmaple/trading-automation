# tests/unit/test_trading_core_config.py
"""
Tests for trading core configuration module.
Validates loading, structure, validation, and default values.
"""

import pytest
from decimal import Decimal
from config.trading_core_config import get_config, validate_config, BASE_TRADING_CORE_CONFIG

class TestTradingCoreConfig:
    """Test suite for trading core configuration."""

    def test_load_default_config(self):
        """Test that default configuration loads successfully."""
        # Act
        config = get_config('default')
        
        # Assert
        assert config is not None
        assert isinstance(config, dict)
        
        # Verify all main sections exist
        assert 'risk_limits' in config
        assert 'execution' in config
        assert 'order_defaults' in config
        assert 'simulation' in config

    def test_load_paper_config(self):
        """Test that paper trading configuration loads successfully."""
        # Act
        config = get_config('paper')
        
        # Assert
        assert config is not None
        assert isinstance(config, dict)
        assert config['risk_limits']['daily_loss_pct'] == Decimal('0.02')  # Same as base

    def test_load_live_config(self):
        """Test that live trading configuration loads successfully."""
        # Act
        config = get_config('live')
        
        # Assert
        assert config is not None
        assert isinstance(config, dict)
        # Live config should have more conservative values
        assert config['risk_limits']['daily_loss_pct'] == Decimal('0.015')
        assert config['order_defaults']['risk_per_trade'] == Decimal('0.003')

    def test_load_nonexistent_environment(self):
        """Test that loading non-existent environment raises ValueError."""
        # Act & Assert
        with pytest.raises(ValueError, match="Unknown trading core environment"):
            get_config('nonexistent')

    def test_config_immutability(self):
        """Test that returned config is a copy, not the original dict."""
        # Arrange
        config1 = get_config('default')
        config2 = get_config('default')
        
        # Act - Modify first config
        config1['risk_limits']['max_open_orders'] = 999
        
        # Assert - Second config should be unaffected
        assert config2['risk_limits']['max_open_orders'] == 5
        assert config1['risk_limits']['max_open_orders'] == 999

    def test_validate_default_config(self):
        """Test that default configuration passes validation."""
        # Arrange
        config = get_config('default')
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert is_valid, f"Validation failed: {message}"
        assert message == "Configuration is valid"

    def test_validate_paper_config(self):
        """Test that paper trading configuration passes validation."""
        # Arrange
        config = get_config('paper')
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert is_valid, f"Validation failed: {message}"
        assert message == "Configuration is valid"

    def test_validate_live_config(self):
        """Test that live trading configuration passes validation."""
        # Arrange
        config = get_config('live')
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert is_valid, f"Validation failed: {message}"
        assert message == "Configuration is valid"

    def test_validate_missing_section(self):
        """Test validation fails when required section is missing."""
        # Arrange
        config = get_config('default').copy()
        del config['risk_limits']
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert not is_valid
        assert "Missing required section: risk_limits" in message

    def test_validate_missing_risk_key(self):
        """Test validation fails when required risk_limits key is missing."""
        # Arrange
        config = get_config('default').copy()
        del config['risk_limits']['daily_loss_pct']
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert not is_valid
        assert "Missing required risk_limits key: daily_loss_pct" in message

    def test_validate_invalid_loss_percentage(self):
        """Test validation fails for invalid loss percentages."""
        # Arrange
        config = get_config('default').copy()
        config['risk_limits']['daily_loss_pct'] = Decimal('-0.01')  # Negative
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert not is_valid
        assert "daily_loss_pct must be between 0 and 0.5" in message

    def test_validate_loss_percentage_too_high(self):
        """Test validation fails for excessively high loss percentages."""
        # Arrange
        config = get_config('default').copy()
        config['risk_limits']['daily_loss_pct'] = Decimal('0.6')  # Too high
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert not is_valid
        assert "daily_loss_pct must be between 0 and 0.5" in message

    def test_validate_invalid_max_open_orders(self):
        """Test validation fails for non-positive max_open_orders."""
        # Arrange
        config = get_config('default').copy()
        config['risk_limits']['max_open_orders'] = 0
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert not is_valid
        assert "max_open_orders must be positive" in message

    def test_validate_invalid_probability_threshold(self):
        """Test validation fails for probability thresholds outside 0-1 range."""
        # Arrange
        config = get_config('default').copy()
        config['execution']['fill_probability_threshold'] = Decimal('1.1')  # >1
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert not is_valid
        assert "fill_probability_threshold must be between 0 and 1" in message

    def test_validate_invalid_risk_reward_ratio(self):
        """Test validation fails for risk_reward_ratio < 1.0."""
        # Arrange
        config = get_config('default').copy()
        config['order_defaults']['risk_reward_ratio'] = Decimal('0.5')  # <1
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert not is_valid
        assert "risk_reward_ratio must be at least 1.0" in message

    def test_validate_invalid_default_equity(self):
        """Test validation fails for non-positive default_equity."""
        # Arrange
        config = get_config('default').copy()
        config['simulation']['default_equity'] = Decimal('0')  # Zero
        
        # Act
        is_valid, message = validate_config(config)
        
        # Assert
        assert not is_valid
        assert "default_equity must be positive" in message

    def test_default_values_correct(self):
        """Test that all default values match expected values."""
        # Arrange
        config = get_config('default')
        
        # Assert - Risk Limits
        assert config['risk_limits']['daily_loss_pct'] == Decimal('0.02')
        assert config['risk_limits']['weekly_loss_pct'] == Decimal('0.05')
        assert config['risk_limits']['monthly_loss_pct'] == Decimal('0.08')
        assert config['risk_limits']['max_open_orders'] == 5
        assert config['risk_limits']['max_risk_per_trade'] == Decimal('0.02')
        
        # Assert - Execution
        assert config['execution']['fill_probability_threshold'] == Decimal('0.7')
        assert config['execution']['min_fill_probability'] == Decimal('0.4')
        
        # Assert - Order Defaults
        assert config['order_defaults']['risk_per_trade'] == Decimal('0.005')
        assert config['order_defaults']['risk_reward_ratio'] == Decimal('2.0')
        assert config['order_defaults']['priority'] == 3
        
        # Assert - Simulation
        assert config['simulation']['default_equity'] == Decimal('100000')

    def test_live_config_more_conservative(self):
        """Test that live trading config has more conservative values."""
        # Arrange
        paper_config = get_config('paper')
        live_config = get_config('live')
        
        # Assert - Live should be more conservative
        assert live_config['risk_limits']['daily_loss_pct'] < paper_config['risk_limits']['daily_loss_pct']
        assert live_config['risk_limits']['max_risk_per_trade'] < paper_config['risk_limits']['max_risk_per_trade']
        assert live_config['order_defaults']['risk_per_trade'] < paper_config['order_defaults']['risk_per_trade']
        assert live_config['order_defaults']['risk_reward_ratio'] > paper_config['order_defaults']['risk_reward_ratio']

    def test_config_structure_deep_copy(self):
        """Test that nested structures are also deep copied."""
        # Arrange
        config1 = get_config('default')
        config2 = get_config('default')
        
        # Act - Modify nested structure in first config
        config1['risk_limits']['daily_loss_pct'] = Decimal('0.99')
        
        # Assert - Second config should be unaffected
        assert config2['risk_limits']['daily_loss_pct'] == Decimal('0.02')
        assert config1['risk_limits']['daily_loss_pct'] == Decimal('0.99')

    def test_validate_risk_per_trade_zero(self):
        """Test validation fails for zero risk_per_trade."""
        config = get_config('default').copy()
        config['order_defaults']['risk_per_trade'] = Decimal('0')
        
        is_valid, message = validate_config(config)
        
        assert not is_valid
        assert "risk_per_trade must be between 0 and 0.1" in message

    def test_validate_risk_per_trade_negative(self):
        """Test validation fails for negative risk_per_trade."""
        config = get_config('default').copy()
        config['order_defaults']['risk_per_trade'] = Decimal('-0.01')
        
        is_valid, message = validate_config(config)
        
        assert not is_valid
        assert "risk_per_trade must be between 0 and 0.1" in message

    def test_validate_risk_per_trade_too_high(self):
        """Test validation fails for risk_per_trade > 0.1."""
        config = get_config('default').copy()
        config['order_defaults']['risk_per_trade'] = Decimal('0.11')
        
        is_valid, message = validate_config(config)
        
        assert not is_valid
        assert "risk_per_trade must be between 0 and 0.1" in message

    def test_validate_priority_zero(self):
        """Test validation fails for priority 0."""
        config = get_config('default').copy()
        config['order_defaults']['priority'] = 0
        
        is_valid, message = validate_config(config)
        
        assert not is_valid
        assert "priority must be between 1 and 5" in message

    def test_validate_priority_six(self):
        """Test validation fails for priority 6."""
        config = get_config('default').copy()
        config['order_defaults']['priority'] = 6
        
        is_valid, message = validate_config(config)
        
        assert not is_valid
        assert "priority must be between 1 and 5" in message

    def test_validate_priority_negative(self):
        """Test validation fails for negative priority."""
        config = get_config('default').copy()
        config['order_defaults']['priority'] = -1
        
        is_valid, message = validate_config(config)
        
        assert not is_valid
        assert "priority must be between 1 and 5" in message