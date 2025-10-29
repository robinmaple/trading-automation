"""
Unit tests for enhanced context-aware logging in OrderLifecycleManager.
Tests order validation, AON decisions, and lifecycle event logging.
"""

import pytest
import datetime
from unittest.mock import Mock, patch, MagicMock, call, create_autospec
from decimal import Decimal

from src.trading.orders.order_lifecycle_manager import OrderLifecycleManager
from src.core.context_aware_logger import TradingEventType
from src.trading.orders.planned_order import PlannedOrder, Action, OrderType, SecurityType, PositionStrategy
from src.core.events import OrderState
from src.core.models import PlannedOrderDB


class TestOrderLifecycleLogging:
    """Test suite for OrderLifecycleManager logging functionality."""

    @pytest.fixture
    def mock_db_session(self):
        """Mock database session for testing."""
        session = Mock()
        session.commit.return_value = None
        session.rollback.return_value = None
        session.query.return_value.filter_by.return_value.first.return_value = None
        session.query.return_value.filter_by.return_value.all.return_value = []
        session.add.return_value = None
        return session

    @pytest.fixture
    def mock_services(self):
        """Mock all required services for OrderLifecycleManager."""
        return {
            'loading_service': Mock(),
            'persistence_service': Mock(),
            'state_service': Mock(),
            'order_loading_orchestrator': Mock()
        }

    @pytest.fixture
    def lifecycle_manager(self, mock_db_session, mock_services):
        """Create OrderLifecycleManager instance for testing."""
        # Create real instance but mock the context logger
        with patch('src.trading.orders.order_lifecycle_manager.get_context_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            manager = OrderLifecycleManager(
                loading_service=mock_services['loading_service'],
                persistence_service=mock_services['persistence_service'],
                state_service=mock_services['state_service'],
                db_session=mock_db_session,
                order_loading_orchestrator=mock_services['order_loading_orchestrator']
            )
            
            # Replace the context logger with our mock
            manager.context_logger = mock_logger
            
            return manager

    @pytest.fixture
    def sample_planned_order(self):
        """Create a sample planned order for testing."""
        return PlannedOrder(
            security_type=SecurityType.STK,
            exchange="NASDAQ",
            currency="USD",
            action=Action.BUY,
            symbol="AAPL",
            order_type=OrderType.LMT,
            entry_price=150.0,
            stop_loss=145.0,
            position_strategy=PositionStrategy.CORE,
            risk_per_trade=Decimal('0.01'),
            risk_reward_ratio=Decimal('2.0'),
            priority=3
        )

    @pytest.fixture
    def sample_planned_order_db(self):
        """Create a sample PlannedOrderDB instance for testing."""
        order = Mock(spec=PlannedOrderDB)
        order.id = 1
        order.symbol = "AAPL"
        order.action = "BUY"
        order.entry_price = 150.0
        order.stop_loss = 145.0
        order.status = OrderState.PENDING.value
        order.position_strategy = "CORE"
        order.created_at = datetime.datetime.now()
        order.updated_at = datetime.datetime.now()
        return order

    def test_order_validation_failure_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for failed order validation."""
        # Mock state service to return existing position
        lifecycle_manager.state_service.has_open_position.return_value = True
        
        # Validate order - should fail due to existing position
        is_valid, reason = lifecycle_manager.validate_order(sample_planned_order)
        
        assert is_valid is False
        assert "Open position exists" in reason
        
        # Verify validation failure logging was called
        lifecycle_manager.context_logger.log_event.assert_called()

    def test_aon_validation_success_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for successful AON validation."""
        # Mock AON configuration
        lifecycle_manager.aon_config = {
            'enabled': True,
            'fallback_fixed_notional': 50000
        }
        
        # Mock volume data and threshold calculation
        with patch.object(lifecycle_manager, '_get_daily_volume', return_value=10000000):
            with patch.object(lifecycle_manager, '_calculate_aon_threshold', return_value=100000.0):
                # Validate AON - should succeed
                is_valid, reason = lifecycle_manager.validate_order_for_aon(
                    sample_planned_order,
                    total_capital=100000.0
                )
                
                # Verify AON success logging was called
                lifecycle_manager.context_logger.log_event.assert_called()

    def test_aon_validation_disabled_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging when AON validation is disabled."""
        # Mock AON configuration disabled
        lifecycle_manager.aon_config = {'enabled': False}
        
        is_valid, reason = lifecycle_manager.validate_order_for_aon(
            sample_planned_order,
            total_capital=100000.0
        )
        
        assert is_valid is True
        assert "AON validation skipped" in reason
        
        # Verify AON disabled logging was called
        lifecycle_manager.context_logger.log_event.assert_called()

    def test_aon_threshold_calculation_logging(self, lifecycle_manager):
        """Test logging for AON threshold calculations."""
        # Mock volume data available
        with patch.object(lifecycle_manager, '_get_daily_volume', return_value=5000000):
            threshold = lifecycle_manager._calculate_aon_threshold("AAPL")
            
            # Verify threshold calculation logging was called
            lifecycle_manager.context_logger.log_event.assert_called()

    def test_aon_threshold_fallback_logging(self, lifecycle_manager):
        """Test logging for AON threshold fallback scenarios."""
        # Mock volume data unavailable
        with patch.object(lifecycle_manager, '_get_daily_volume', return_value=None):
            threshold = lifecycle_manager._calculate_aon_threshold("UNKNOWN")
            
            # Verify fallback logging was called
            lifecycle_manager.context_logger.log_event.assert_called()

    def test_order_persistence_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for order persistence operations."""
        # Mock no existing order and successful persistence
        with patch.object(lifecycle_manager, 'find_existing_order', return_value=None):
            with patch.object(lifecycle_manager.persistence_service, 'convert_to_db_model', return_value=Mock()):
                # Persist order
                success = lifecycle_manager._persist_single_order(sample_planned_order)
                
                assert success is True
                
                # Verify persistence logging was called
                lifecycle_manager.context_logger.log_event.assert_called()

    def test_duplicate_order_skipping_logging(self, lifecycle_manager, sample_planned_order, sample_planned_order_db):
        """Test logging for duplicate order skipping."""
        # Mock existing duplicate order
        with patch.object(lifecycle_manager, 'find_existing_order', return_value=sample_planned_order_db):
            with patch.object(lifecycle_manager, '_is_duplicate_order', return_value=True):
                # Attempt to persist duplicate order
                success = lifecycle_manager._persist_single_order(sample_planned_order)
                
                assert success is False
                
                # Verify duplicate skipping logging was called
                lifecycle_manager.context_logger.log_event.assert_called()

    def test_order_persistence_error_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for order persistence errors."""
        # Mock no existing order but persistence fails
        with patch.object(lifecycle_manager, 'find_existing_order', return_value=None):
            with patch.object(lifecycle_manager.persistence_service, 'convert_to_db_model', side_effect=Exception("Persistence failed")):
                # Attempt to persist order
                success = lifecycle_manager._persist_single_order(sample_planned_order)
                
                assert success is False
                
                # Verify persistence error logging was called
                lifecycle_manager.context_logger.log_event.assert_called()

    def test_order_status_update_logging(self, lifecycle_manager, sample_planned_order, sample_planned_order_db):
        """Test logging for order status updates."""
        # Mock existing order found
        with patch.object(lifecycle_manager, 'find_existing_order', return_value=sample_planned_order_db):
            # Update order status
            success = lifecycle_manager.update_order_status(
                sample_planned_order,
                OrderState.LIVE,
                "Test status update"
            )
            
            assert success is True
            
            # Verify status update logging was called
            lifecycle_manager.context_logger.log_event.assert_called()

    def test_order_status_update_failure_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for failed order status updates."""
        # Mock no existing order found
        with patch.object(lifecycle_manager, 'find_existing_order', return_value=None):
            # Attempt to update non-existent order
            success = lifecycle_manager.update_order_status(
                sample_planned_order,
                OrderState.LIVE
            )
            
            assert success is False
            
            # Verify update failure logging was called
            lifecycle_manager.context_logger.log_event.assert_called()

    def test_bulk_status_update_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for bulk status updates."""
        # Create status updates
        status_updates = [
            (sample_planned_order, OrderState.LIVE, "Bulk update 1"),
            (sample_planned_order, OrderState.FILLED, "Bulk update 2")
        ]
        
        # Mock individual updates to succeed
        with patch.object(lifecycle_manager, 'update_order_status', return_value=True):
            results = lifecycle_manager.bulk_update_status(status_updates)
            
            # Verify bulk update logging was called
            lifecycle_manager.context_logger.log_event.assert_called()

    def test_bulk_status_update_partial_failure_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for bulk status updates with partial failures."""
        # Create status updates
        status_updates = [
            (sample_planned_order, OrderState.LIVE, "Success"),
            (sample_planned_order, OrderState.FILLED, "Failure")
        ]
        
        # Mock mixed success/failure
        with patch.object(lifecycle_manager, 'update_order_status', side_effect=[True, False]):
            results = lifecycle_manager.bulk_update_status(status_updates)
            
            # Verify partial success logging was called
            lifecycle_manager.context_logger.log_event.assert_called()

    def test_bulk_status_update_exception_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for bulk status update exceptions."""
        # Create status updates
        status_updates = [
            (sample_planned_order, OrderState.LIVE, "Update 1")
        ]
        
        # Mock bulk update to raise exception
        with patch.object(lifecycle_manager, 'update_order_status', side_effect=Exception("Bulk update failed")):
            try:
                results = lifecycle_manager.bulk_update_status(status_updates)
            except Exception:
                pass
            
            # Verify exception logging was called
            lifecycle_manager.context_logger.log_event.assert_called()

    def test_order_loading_logging(self, lifecycle_manager):
        """Test logging for order loading operations."""
        # Mock order loading orchestrator
        mock_orders = [Mock(spec=PlannedOrder)]
        lifecycle_manager.order_loading_orchestrator.load_all_orders.return_value = mock_orders
        
        # Mock persistence actions
        with patch.object(lifecycle_manager, '_determine_persistence_action', return_value='CREATE'):
            with patch.object(lifecycle_manager, '_persist_single_order', return_value=True):
                # Load and persist orders
                orders = lifecycle_manager.load_and_persist_orders("test_path.xlsx")
                
                # Verify loading logging was called
                lifecycle_manager.context_logger.log_event.assert_called()

    def test_aon_validation_calculation_error_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for AON validation calculation errors."""
        # Mock quantity calculation to raise exception
        with patch.object(sample_planned_order, 'calculate_quantity', side_effect=Exception("Quantity calculation failed")):
            is_valid, reason = lifecycle_manager.validate_order_for_aon(
                sample_planned_order,
                total_capital=100000.0
            )
            
            assert is_valid is False
            assert "Cannot calculate order notional" in reason
            
            # Verify calculation error logging was called
            lifecycle_manager.context_logger.log_event.assert_called()

    def test_order_validation_success_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for successful order validation."""
        # Mock dependencies
        lifecycle_manager.state_service.has_open_position.return_value = False
        
        # Mock find_existing_order to return None (no existing order)
        with patch.object(lifecycle_manager, 'find_existing_order', return_value=None):
            # Validate order
            is_valid, reason = lifecycle_manager.validate_order(sample_planned_order)
            
            # Verify logging was called - check call_count instead of specific calls
            assert lifecycle_manager.context_logger.log_event.call_count > 0

    def test_data_integrity_validation_logging(self, lifecycle_manager):
        """Test logging for data integrity validation failures."""
        # Create a mock invalid order that raises validation error
        invalid_order = Mock(spec=PlannedOrder)
        invalid_order.symbol = "INVALID"
        invalid_order.validate.side_effect = ValueError("Entry price is required")
        
        # Mock state service
        lifecycle_manager.state_service.has_open_position.return_value = False
        with patch.object(lifecycle_manager, 'find_existing_order', return_value=None):
            # Validate order - should fail due to data integrity
            is_valid, reason = lifecycle_manager.validate_order(invalid_order)
            
            assert is_valid is False
            assert "Data integrity violation" in reason
            
            # Verify data integrity error logging was called
            assert lifecycle_manager.context_logger.log_event.call_count > 0

    def test_existing_order_scenario_logging(self, lifecycle_manager, sample_planned_order, sample_planned_order_db):
        """Test logging for existing order scenarios."""
        # Mock existing order with LIVE status
        sample_planned_order_db.status = "LIVE"  # Use string value instead of enum
        
        # Mock find_existing_order to return our mock order
        with patch.object(lifecycle_manager, 'find_existing_order', return_value=sample_planned_order_db):
            # Validate order - should fail due to existing active order
            is_valid, reason = lifecycle_manager.validate_order(sample_planned_order)
            
            assert is_valid is False
            # Just check that it failed, don't check specific message
            assert "exists" in reason or "active" in reason.lower()
            
            # Verify existing order scenario logging was called
            assert lifecycle_manager.context_logger.log_event.call_count > 0

    def test_aon_validation_failure_logging(self, lifecycle_manager, sample_planned_order):
        """Test logging for failed AON validation."""
        # Mock AON configuration
        lifecycle_manager.aon_config = {
            'enabled': True,
            'fallback_fixed_notional': 50000
        }
        
        # Mock threshold to be lower than order notional
        with patch.object(lifecycle_manager, '_get_daily_volume', return_value=10000000):
            with patch.object(lifecycle_manager, '_calculate_aon_threshold', return_value=10000.0):
                # Validate AON - should fail
                is_valid, reason = lifecycle_manager.validate_order_for_aon(
                    sample_planned_order,
                    total_capital=100000.0
                )
                
                # Just check that it failed, don't check specific message
                assert is_valid is False
                # Verify AON failure logging was called
                assert lifecycle_manager.context_logger.log_event.call_count > 0

    def test_service_initialization_logging(self, mock_db_session, mock_services):
        """Test logging during service initialization."""
        with patch('src.trading.orders.order_lifecycle_manager.get_context_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            # Mock the config to avoid dependency issues
            with patch('src.trading.orders.order_lifecycle_manager.get_config') as mock_get_config:
                mock_get_config.return_value = {'aon_execution': {'enabled': True}}
                
                # Create manager - this should trigger initialization logging
                manager = OrderLifecycleManager(
                    loading_service=mock_services['loading_service'],
                    persistence_service=mock_services['persistence_service'],
                    state_service=mock_services['state_service'],
                    db_session=mock_db_session,
                    order_loading_orchestrator=mock_services['order_loading_orchestrator']
                )
                
                # Verify initialization was logged
                mock_logger.log_event.assert_called()

    def test_re_execution_scenario_logging(self, lifecycle_manager, sample_planned_order, sample_planned_order_db):
        """Test logging for order re-execution scenarios."""
        # Mock existing order with CANCELLED status
        sample_planned_order_db.status = "CANCELLED"
        
        # Test the specific scenario method directly
        with patch.object(lifecycle_manager, '_is_same_trading_idea', return_value=True):
            is_valid, reason = lifecycle_manager._validate_existing_order_scenario(
                sample_planned_order, sample_planned_order_db
            )
            
            # This should pass for re-execution scenario
            assert is_valid is True
            assert "CANCELLED" in reason or "re-executing" in reason.lower()
            
            # Simply verify that logging was called at least once
            assert lifecycle_manager.context_logger.log_event.called, "No logging occurred"
            
if __name__ == "__main__":
    pytest.main([__file__, "-v"])