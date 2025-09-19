"""
Unit tests for OrderExecutionOrchestrator class.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from decimal import Decimal
import datetime

from src.core.order_execution_orchestrator import OrderExecutionOrchestrator
from src.core.planned_order import Action, OrderType, SecurityType, PositionStrategy

# -----------------------------
# Updated mock helpers
# -----------------------------

def create_mock_planned_order(symbol="AAPL", action=Action.BUY, order_type=OrderType.LMT,
                              entry_price=150.0, stop_loss=145.0, risk_per_trade=0.01,
                              risk_reward_ratio=2.0, priority=1, security_type=SecurityType.STK,
                              exchange="SMART", currency="USD"):
    mock_order = MagicMock(spec=["symbol", "action", "order_type", "entry_price",
                                  "stop_loss", "risk_per_trade", "risk_reward_ratio",
                                  "priority", "security_type", "exchange", "currency"])
    mock_order.symbol = symbol
    mock_order.action = action
    mock_order.order_type = order_type
    mock_order.entry_price = entry_price
    mock_order.stop_loss = stop_loss
    mock_order.risk_per_trade = risk_per_trade
    mock_order.risk_reward_ratio = risk_reward_ratio
    mock_order.priority = priority
    mock_order.security_type = security_type
    mock_order.exchange = exchange
    mock_order.currency = currency
    return mock_order

def create_mock_active_order(is_working=True, planned_order=None):
    mock_active = MagicMock()
    # Provide is_working() method
    mock_active.is_working.return_value = is_working
    mock_active.planned_order = planned_order
    return mock_active

# -----------------------------
# Test class
# -----------------------------

class TestOrderExecutionOrchestrator:
    """Test suite for OrderExecutionOrchestrator."""
    
    @pytest.fixture
    def mock_services(self):
        return {
            'execution': Mock(),
            'sizing': Mock(),
            'persistence': Mock(),
            'state': Mock(),
            'probability': Mock(),
            'ibkr': Mock()
        }
    
    @pytest.fixture
    def orchestrator(self, mock_services):
        return OrderExecutionOrchestrator(
            execution_service=mock_services['execution'],
            sizing_service=mock_services['sizing'],
            persistence_service=mock_services['persistence'],
            state_service=mock_services['state'],
            probability_engine=mock_services['probability'],
            ibkr_client=mock_services['ibkr']
        )
    
    @pytest.fixture
    def sample_order(self):
        return create_mock_planned_order()

    # -----------------------------
    # Tests
    # -----------------------------

    def test_initialization(self, mock_services, orchestrator):
        assert orchestrator.execution_service == mock_services['execution']
        assert orchestrator.sizing_service == mock_services['sizing']
        assert orchestrator.persistence_service == mock_services['persistence']
        assert orchestrator.state_service == mock_services['state']
        assert orchestrator.default_capital == 100000
        assert orchestrator.min_fill_probability == 0.4

    def test_calculate_position_details_success(self, orchestrator, mock_services, sample_order):
        mock_services['sizing'].calculate_order_quantity.return_value = 66.67
        quantity, capital_commitment = orchestrator._calculate_position_details(sample_order, 100000)
        assert quantity == 66.67
        assert capital_commitment == 150.0 * 66.67

    def test_calculate_position_details_no_entry_price(self, orchestrator, sample_order):
        """Test position calculation with None entry price raises exception."""
        sample_order.entry_price = None

        with pytest.raises(Exception) as excinfo:
            orchestrator._calculate_position_details(sample_order, 100000)

        assert "Failed to calculate position details" in str(excinfo.value)

    def test_validate_order_execution_max_orders(self, orchestrator, sample_order):
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
        active_order_mock = create_mock_active_order(
            is_working=True,
            planned_order=create_mock_planned_order(symbol="AAPL", action=Action.BUY, entry_price=150.0, stop_loss=145.0)
        )
        active_orders = {1: active_order_mock}
        is_valid = orchestrator.validate_order_execution(sample_order, active_orders)
        assert is_valid is False

    def test_has_duplicate_active_order_true(self, orchestrator, sample_order):
        active_order_mock = create_mock_active_order(
            is_working=True,
            planned_order=create_mock_planned_order(symbol="AAPL", action=Action.BUY, entry_price=150.0, stop_loss=145.0)
        )
        active_orders = {1: active_order_mock}
        assert orchestrator._has_duplicate_active_order(sample_order, active_orders) is True

    def test_has_duplicate_active_order_false(self, orchestrator, sample_order):
        active_order_mock = create_mock_active_order(
            is_working=True,
            planned_order=create_mock_planned_order(symbol="MSFT", action=Action.BUY, entry_price=150.0, stop_loss=145.0)
        )
        active_orders = {1: active_order_mock}
        assert orchestrator._has_duplicate_active_order(sample_order, active_orders) is False

    def test_has_duplicate_active_order_non_working(self, orchestrator, sample_order):
        active_order_mock = create_mock_active_order(
            is_working=False,
            planned_order=create_mock_planned_order(symbol="AAPL", action=Action.BUY, entry_price=150.0, stop_loss=145.0)
        )
        active_orders = {1: active_order_mock}
        assert orchestrator._has_duplicate_active_order(sample_order, active_orders) is False

    # ... keep your other tests as-is, just ensure all `active_order` mocks use the updated helper

    def test_initialization(self, mock_services, orchestrator):
        """Test that orchestrator initializes with correct services."""
        assert orchestrator.execution_service == mock_services['execution']
        assert orchestrator.sizing_service == mock_services['sizing']
        assert orchestrator.persistence_service == mock_services['persistence']
        assert orchestrator.state_service == mock_services['state']
        assert orchestrator.default_capital == 100000
        assert orchestrator.min_fill_probability == 0.4
    
    def test_get_total_capital_live(self, orchestrator, mock_services):
        """Test getting total capital from live IBKR connection."""
        mock_services['ibkr'].connected = True
        mock_services['ibkr'].get_account_value.return_value = 50000.0
        
        capital = orchestrator._get_total_capital()
        
        assert capital == 50000.0
        mock_services['ibkr'].get_account_value.assert_called_once()
    
    def test_get_total_capital_simulation(self, orchestrator, mock_services):
        """Test getting default capital for simulation mode."""
        mock_services['ibkr'].connected = False
        
        capital = orchestrator._get_total_capital()
        
        assert capital == 100000.0
        mock_services['ibkr'].get_account_value.assert_not_called()
    
    def test_get_trading_mode_live(self, orchestrator, mock_services):
        """Test detecting live trading mode."""
        mock_services['ibkr'].connected = True
        mock_services['ibkr'].is_paper_account = False
        
        is_live = orchestrator._get_trading_mode()
        
        assert is_live is True
    
    def test_get_trading_mode_paper(self, orchestrator, mock_services):
        """Test detecting paper trading mode."""
        mock_services['ibkr'].connected = True
        mock_services['ibkr'].is_paper_account = True
        
        is_live = orchestrator._get_trading_mode()
        
        assert is_live is False
    
    def test_get_trading_mode_disconnected(self, orchestrator, mock_services):
        """Test detecting simulation mode when disconnected."""
        mock_services['ibkr'].connected = False
        
        is_live = orchestrator._get_trading_mode()
        
        assert is_live is False
    
    def test_calculate_position_details_success(self, orchestrator, mock_services, sample_order):
        """Test successful position details calculation."""
        mock_services['sizing'].calculate_order_quantity.return_value = 66.67
        
        quantity, capital_commitment = orchestrator._calculate_position_details(sample_order, 100000)
        
        assert quantity == 66.67
        assert capital_commitment == 150.0 * 66.67
        mock_services['sizing'].calculate_order_quantity.assert_called_once_with(sample_order, 100000)
    
    def test_calculate_effective_priority(self, orchestrator, sample_order):
        """Test effective priority calculation."""
        sample_order.priority = 3
        fill_probability = 0.8
        
        effective_priority = orchestrator.calculate_effective_priority(sample_order, fill_probability)
        
        assert effective_priority == 3 * 0.8
    
    def test_check_order_viability_success(self, orchestrator, mock_services, sample_order):
        """Test successful order viability check."""
        mock_services['state'].has_open_position.return_value = False
        
        is_viable = orchestrator._check_order_viability(sample_order, 0.8)
        
        assert is_viable is True
        mock_services['state'].has_open_position.assert_called_once_with("AAPL")
    
    def test_check_order_viability_low_probability(self, orchestrator, mock_services, sample_order):
        """Test order rejection due to low fill probability."""
        is_viable = orchestrator._check_order_viability(sample_order, 0.3)
        
        assert is_viable is False
        mock_services['persistence'].update_order_status.assert_called_once()
        args = mock_services['persistence'].update_order_status.call_args[0]
        assert "below threshold" in args[2]
    
    def test_check_order_viability_open_position(self, orchestrator, mock_services, sample_order):
        """Test order rejection due to existing open position."""
        mock_services['state'].has_open_position.return_value = True
        
        is_viable = orchestrator._check_order_viability(sample_order, 0.8)
        
        assert is_viable is False
        mock_services['persistence'].update_order_status.assert_called_once()
        args = mock_services['persistence'].update_order_status.call_args[0]
        assert "Open position exists" in args[2]
    
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
            mock_services['persistence'].update_order_status.assert_called_once()
            args = mock_services['persistence'].update_order_status.call_args[0]
            assert "Test error" in args[2]
    
    def test_get_execution_summary_success(self, orchestrator, mock_services, sample_order):
        """Test successful execution summary generation."""
        with patch.object(orchestrator, '_calculate_position_details', return_value=(66.67, 10000.0)), \
             patch.object(orchestrator, '_get_trading_mode', return_value=True), \
             patch.object(orchestrator, 'calculate_effective_priority', return_value=2.4):
            
            summary = orchestrator.get_execution_summary(sample_order, 0.8, 100000)
            
            assert summary['symbol'] == "AAPL"
            assert summary['action'] == "BUY"
            assert summary['quantity'] == 66.67
            assert summary['capital_commitment'] == 10000.0
            assert summary['fill_probability'] == 0.8
            assert summary['effective_priority'] == 2.4
            assert summary['is_viable'] is True
            assert summary['is_live_trading'] is True
            assert summary['total_capital'] == 100000
    
    def test_get_execution_summary_failure(self, orchestrator, sample_order):
        """Test execution summary with calculation failure."""
        with patch.object(orchestrator, '_calculate_position_details', side_effect=Exception("Test error")):
            
            summary = orchestrator.get_execution_summary(sample_order, 0.8, 100000)
            
            assert summary == {}
    

        """Test duplicate detection ignores non-working orders."""
        active_order_mock = create_mock_active_order(
            is_working=False,  # Not working
            planned_order=create_mock_planned_order(
                symbol="AAPL",
                action=Action.BUY,
                entry_price=150.0,
                stop_loss=145.0
            )
        )
        
        active_orders = {1: active_order_mock}
        
        has_duplicate = orchestrator._has_duplicate_active_order(sample_order, active_orders)
        
        assert has_duplicate is False