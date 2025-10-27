"""
Unit tests for OrderExecutionOrchestrator class - updated to match current implementation.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from decimal import Decimal
import datetime

from src.core.order_execution_orchestrator import OrderExecutionOrchestrator
from src.core.planned_order import PlannedOrder, Action, OrderType, PositionStrategy, SecurityType

# -----------------------------
# Mock helpers
# -----------------------------

def create_mock_planned_order(symbol="AAPL", action=Action.BUY, order_type=OrderType.LMT,
                              entry_price=150.0, stop_loss=145.0, risk_per_trade=0.01,
                              risk_reward_ratio=2.0, priority=1, position_strategy=PositionStrategy.DAY,
                              security_type=SecurityType.STK):
    """Create a mock PlannedOrder for testing."""
    mock_order = MagicMock(spec=PlannedOrder)
    mock_order.symbol = symbol
    mock_order.action = action
    mock_order.order_type = order_type
    mock_order.entry_price = entry_price
    mock_order.stop_loss = stop_loss
    mock_order.risk_per_trade = risk_per_trade
    mock_order.risk_reward_ratio = risk_reward_ratio
    mock_order.priority = priority
    mock_order.position_strategy = position_strategy
    mock_order.security_type = security_type
    return mock_order

def create_mock_active_order(is_working=True, planned_order=None):
    """Create a mock ActiveOrder for testing."""
    mock_active = MagicMock()
    mock_active.is_working.return_value = is_working
    mock_active.planned_order = planned_order or create_mock_planned_order()
    return mock_active

# -----------------------------
# Test class
# -----------------------------

class TestOrderExecutionOrchestrator:
    """Test suite for OrderExecutionOrchestrator."""
    
    @pytest.fixture
    def mock_services(self):
        """Create mock services for execution orchestrator."""
        return {
            'execution_service': Mock(),
            'sizing_service': Mock(),
            'persistence_service': Mock(),
            'state_service': Mock(),
            'probability_engine': Mock(),
            'ibkr_client': Mock()
        }
    
    @pytest.fixture
    def orchestrator(self, mock_services):
        """Create OrderExecutionOrchestrator instance with mock services."""
        return OrderExecutionOrchestrator(**mock_services)
    
    @pytest.fixture
    def sample_order(self):
        """Create a sample order for testing."""
        return create_mock_planned_order()

    # -----------------------------
    # Initialization Tests - UPDATED
    # -----------------------------

    def test_initialization_with_config(self, mock_services):
        """Test initialization with configuration parameters."""
        config = {
            'aon_execution': {'enabled': True}
        }
        
        orchestrator = OrderExecutionOrchestrator(config=config, **mock_services)
        
        # Verify AON config is loaded
        assert orchestrator.aon_config == {'enabled': True}
        assert orchestrator.execution_service == mock_services['execution_service']

    def test_initialization_defaults(self, mock_services):
        """Test initialization with default values when no config provided."""
        orchestrator = OrderExecutionOrchestrator(**mock_services)
        
        assert orchestrator.execution_service == mock_services['execution_service']
        assert orchestrator.sizing_service == mock_services['sizing_service']
        assert orchestrator.persistence_service == mock_services['persistence_service']
        assert orchestrator.state_service == mock_services['state_service']
        assert hasattr(orchestrator, 'aon_config')  # Should have AON config

    def test_empty_config_uses_hardcoded_defaults(self, mock_services):
        """Test that empty config uses hardcoded defaults."""
        orchestrator = OrderExecutionOrchestrator(**mock_services, config={})
        
        # Should initialize without errors
        assert orchestrator.execution_service == mock_services['execution_service']
        assert hasattr(orchestrator, 'aon_config')

    def test_partial_config_uses_mixed_defaults(self, mock_services):
        """Test that partial config uses provided values and falls back for others."""
        test_config = {
            'aon_execution': {'enabled': False}
        }
        
        orchestrator = OrderExecutionOrchestrator(**mock_services, config=test_config)
        
        assert orchestrator.aon_config == {'enabled': False}

    # -----------------------------
    # Capital and Trading Mode Tests
    # -----------------------------

    def test_get_total_capital_live(self, orchestrator, mock_services):
        """Test getting total capital from live IBKR connection."""
        mock_services['ibkr_client'].connected = True
        mock_services['ibkr_client'].get_account_value.return_value = 50000.0
        
        capital = orchestrator._get_total_capital()
        
        assert capital == 50000.0
        mock_services['ibkr_client'].get_account_value.assert_called_once()

    def test_get_total_capital_simulation(self, orchestrator, mock_services):
        """Test getting capital when IBKR is not connected."""
        mock_services['ibkr_client'].connected = False
        
        with pytest.raises(RuntimeError, match="No IBKR connection available"):
            orchestrator._get_total_capital()

    def test_get_trading_mode_live(self, orchestrator, mock_services):
        """Test detecting live trading mode."""
        mock_services['ibkr_client'].connected = True
        mock_services['ibkr_client'].is_paper_account = False
        
        is_live = orchestrator._get_trading_mode()
        
        assert is_live is True

    def test_get_trading_mode_paper(self, orchestrator, mock_services):
        """Test detecting paper trading mode."""
        mock_services['ibkr_client'].connected = True
        mock_services['ibkr_client'].is_paper_account = True
        
        is_live = orchestrator._get_trading_mode()
        
        assert is_live is False

    def test_get_trading_mode_disconnected(self, orchestrator, mock_services):
        """Test detecting simulation mode when disconnected."""
        mock_services['ibkr_client'].connected = False
        
        is_live = orchestrator._get_trading_mode()
        
        assert is_live is False

    # -----------------------------
    # Position Calculation Tests
    # -----------------------------

    def test_calculate_position_details_success(self, orchestrator, mock_services, sample_order):
        """Test successful position details calculation."""
        mock_services['sizing_service'].calculate_order_quantity.return_value = 66.67
        
        quantity, capital_commitment = orchestrator._calculate_position_details(sample_order, 100000)
        
        assert quantity == 66.67
        assert capital_commitment == 150.0 * 66.67
        mock_services['sizing_service'].calculate_order_quantity.assert_called_once_with(sample_order, 100000)

    def test_calculate_position_details_no_entry_price(self, orchestrator, sample_order):
        """Test position calculation with None entry price raises exception."""
        sample_order.entry_price = None

        with pytest.raises(Exception) as excinfo:
            orchestrator._calculate_position_details(sample_order, 100000)

        assert "Failed to calculate position details" in str(excinfo.value)

    def test_calculate_effective_priority(self, orchestrator, sample_order):
        """Test effective priority calculation."""
        sample_order.priority = 3
        fill_probability = 0.8
        
        effective_priority = orchestrator.calculate_effective_priority(sample_order, fill_probability)
        
        assert effective_priority == 3 * 0.8

    # -----------------------------
    # Order Viability Tests - UPDATED
    # -----------------------------

    def test_check_order_viability_success(self, orchestrator, mock_services, sample_order):
        """Test successful order viability check."""
        mock_services['state_service'].has_open_position.return_value = False
        
        is_viable = orchestrator._check_order_viability(sample_order, 0.8)
        
        assert is_viable is True
        mock_services['state_service'].has_open_position.assert_called_once_with("AAPL")

    def test_check_order_viability_open_position(self, orchestrator, mock_services, sample_order):
        """Test order rejection due to existing open position."""
        mock_services['state_service'].has_open_position.return_value = True
        
        is_viable = orchestrator._check_order_viability(sample_order, 0.8)
        
        assert is_viable is False
        mock_services['persistence_service'].update_order_status.assert_called_once()
        args = mock_services['persistence_service'].update_order_status.call_args[0]
        assert "Open position exists" in args[2]
        assert "AAPL" in args[2]

    # -----------------------------
    # Order Validation Tests
    # -----------------------------

    def test_validate_order_execution_success(self, orchestrator, sample_order):
        """Test successful order validation."""
        active_orders = {}
        
        is_valid = orchestrator.validate_order_execution(sample_order, active_orders)
        
        assert is_valid is True

    def test_validate_order_execution_no_entry_price(self, orchestrator, sample_order):
        """Test order validation failure due to missing entry price."""
        sample_order.entry_price = None
        
        is_valid = orchestrator.validate_order_execution(sample_order, {})
        
        assert is_valid is False

    def test_validate_order_execution_max_orders(self, orchestrator, sample_order):
        """Test order validation failure due to maximum open orders limit."""
        active_orders = {
            1: create_mock_active_order(is_working=True),
            2: create_mock_active_order(is_working=True),
            3: create_mock_active_order(is_working=True),
            4: create_mock_active_order(is_working=True),
            5: create_mock_active_order(is_working=True),
            6: create_mock_active_order(is_working=False)  # Not counted
        }
        is_valid = orchestrator.validate_order_execution(sample_order, active_orders, max_open_orders=5)
        assert is_valid is False

    def test_validate_order_execution_duplicate_order(self, orchestrator, sample_order):
        """Test order validation failure due to duplicate active order."""
        active_order_mock = create_mock_active_order(
            is_working=True,
            planned_order=create_mock_planned_order(
                symbol="AAPL", 
                action=Action.BUY, 
                entry_price=150.0, 
                stop_loss=145.0
            )
        )
        active_orders = {1: active_order_mock}
        is_valid = orchestrator.validate_order_execution(sample_order, active_orders)
        assert is_valid is False

    # -----------------------------
    # Duplicate Detection Tests
    # -----------------------------

    def test_has_duplicate_active_order_true(self, orchestrator, sample_order):
        """Test duplicate detection with matching active order."""
        active_order_mock = create_mock_active_order(
            is_working=True,
            planned_order=create_mock_planned_order(
                symbol="AAPL", 
                action=Action.BUY, 
                entry_price=150.0, 
                stop_loss=145.0
            )
        )
        active_orders = {1: active_order_mock}
        assert orchestrator._has_duplicate_active_order(sample_order, active_orders) is True

    def test_has_duplicate_active_order_false(self, orchestrator, sample_order):
        """Test duplicate detection with different active order."""
        active_order_mock = create_mock_active_order(
            is_working=True,
            planned_order=create_mock_planned_order(
                symbol="MSFT", 
                action=Action.BUY, 
                entry_price=150.0, 
                stop_loss=145.0
            )
        )
        active_orders = {1: active_order_mock}
        assert orchestrator._has_duplicate_active_order(sample_order, active_orders) is False

    # -----------------------------
    # Order Execution Tests - UPDATED
    # -----------------------------

    def test_execute_single_order_success(self, orchestrator, mock_services, sample_order):
        """Test successful order execution."""
        # Mock internal methods
        with patch.object(orchestrator, '_get_total_capital', return_value=100000), \
             patch.object(orchestrator, '_get_trading_mode', return_value=False), \
             patch.object(orchestrator, '_calculate_position_details', return_value=(66.67, 10000.0)), \
             patch.object(orchestrator, '_check_order_viability', return_value=True), \
             patch.object(orchestrator, '_execute_via_service', return_value=True):
            
            success = orchestrator.execute_single_order(sample_order, 0.8)
            
            assert success is True

    def test_execute_single_order_viability_failure(self, orchestrator, mock_services, sample_order):
        """Test order execution failure due to viability check."""
        with patch.object(orchestrator, '_get_total_capital', return_value=100000), \
             patch.object(orchestrator, '_check_order_viability', return_value=False):
            
            success = orchestrator.execute_single_order(sample_order, 0.8)
            
            assert success is False

    def test_execute_single_order_exception(self, orchestrator, mock_services, sample_order):
        """Test order execution with exception handling."""
        with patch.object(orchestrator, '_get_total_capital', side_effect=Exception("Test error")):
            
            success = orchestrator.execute_single_order(sample_order, 0.8)
            
            assert success is False
            mock_services['persistence_service'].update_order_status.assert_called_once()
            args = mock_services['persistence_service'].update_order_status.call_args[0]
            assert "Test error" in args[2]

    # -----------------------------
    # Execution Summary Tests - UPDATED
    # -----------------------------

    def test_get_execution_summary_success(self, orchestrator, mock_services, sample_order):
        """Test successful execution summary generation."""
        with patch.object(orchestrator, '_calculate_position_details', return_value=(66.67, 10000.0)), \
             patch.object(orchestrator, '_get_trading_mode', return_value=True), \
             patch.object(orchestrator, 'calculate_effective_priority', return_value=2.4), \
             patch.object(orchestrator, '_check_aon_viability', return_value=(True, "AON valid")):
            
            summary = orchestrator.get_execution_summary(sample_order, 0.8, 100000)
            
            assert summary['symbol'] == "AAPL"
            assert summary['action'] == Action.BUY.value
            assert summary['quantity'] == 66.67
            assert summary['capital_commitment'] == 10000.0
            assert summary['fill_probability'] == 0.8
            assert summary['effective_priority'] == 2.4
            assert summary['is_live_trading'] is True
            assert summary['total_capital'] == 100000
            assert summary['aon_valid'] is True
            assert summary['aon_reason'] == "AON valid"

    def test_get_execution_summary_failure(self, orchestrator, sample_order):
        """Test execution summary with calculation failure."""
        with patch.object(orchestrator, '_calculate_position_details', side_effect=Exception("Test error")):
            
            summary = orchestrator.get_execution_summary(sample_order, 0.8, 100000)
            
            # Should return error dict, not empty dict
            assert 'error' in summary
            assert 'Test error' in summary['error']

    # -----------------------------
    # Configuration Data Type Tests - UPDATED
    # -----------------------------

    def test_config_with_different_data_types(self, mock_services):
        """Test that configuration handles different data types correctly."""
        test_config = {
            'aon_execution': {'enabled': True, 'fallback_fixed_notional': 50000}
        }
        
        orchestrator = OrderExecutionOrchestrator(**mock_services, config=test_config)
        
        assert orchestrator.aon_config == {'enabled': True, 'fallback_fixed_notional': 50000}