from unittest.mock import MagicMock, Mock, patch
from src.services.order_execution_service import OrderExecutionService
from src.core.planned_order import PlannedOrder, Action, SecurityType

class TestOrderExecutionService:
    """Test suite for OrderExecutionService using pytest"""

    def setup_method(self):
        """Set up test fixtures before each test"""
        self.mock_tm = Mock()
        self.mock_ibkr_client = Mock()
        self.service = OrderExecutionService(self.mock_tm, self.mock_ibkr_client)

        # Mock dependencies that are set via set_dependencies()
        self.order_persistence = Mock()
        self.active_orders = {}
        self.service.set_dependencies(self.order_persistence, self.active_orders)

    def test_place_order_calls_execute_single_order(self):
        """Test that place_order properly calls execute_single_order"""
        test_order = Mock()

        with patch.object(self.service, 'execute_single_order') as mock_execute:
            self.service.place_order(
                test_order, 0.85, 0.0, 100000, 100, 10000, False
            )

            mock_execute.assert_called_once_with(
                test_order, 0.85, 0.0, 100000, 100, 10000, False
            )

    @patch('src.services.order_execution_service.OrderExecutionService._validate_order_margin')
    def test_execute_single_order_simulation_mode(self, mock_validate_margin):
        """Test order execution in simulation mode"""
        mock_validate_margin.return_value = (True, "Validation passed")
        self.mock_ibkr_client.connected = False  # Simulation mode

        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.entry_price = 100.0
        test_order.action.value = "BUY"

        self.mock_tm._find_planned_order_db_id.return_value = 123

        with patch('builtins.print'):
            result = self.service.execute_single_order(
                test_order, 0.85, 100000, 100, 10000, False
            )

        self.order_persistence.update_order_status.assert_called_once()
        self.order_persistence.record_order_execution.assert_called_once()
        assert result is True

    @patch('src.services.order_execution_service.OrderExecutionService._validate_order_margin')
    def test_execute_single_order_live_mode_failure(self, mock_validate_margin):
        """Test failed order execution in live mode"""
        mock_validate_margin.return_value = (True, "Validation passed")
        self.mock_ibkr_client.connected = True
        self.mock_ibkr_client.place_bracket_order.return_value = None

        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.to_ib_contract.return_value = "mock_contract"

        with patch('builtins.print'):
            result = self.service.execute_single_order(
                test_order, 0.85, 100000, 100, 10000, True
            )

        self.mock_ibkr_client.place_bracket_order.assert_called_once()
        self.order_persistence.update_order_status.assert_not_called()
        assert len(self.active_orders) == 0
        assert result is False

    def test_cancel_order_delegates_to_trading_manager(self):
        """Test that cancel_order delegates to trading manager"""
        self.mock_tm._cancel_single_order.return_value = True
        result = self.service.cancel_order(123)

        assert result is True
        self.mock_tm._cancel_single_order.assert_called_once_with(123)

    @patch('src.services.order_persistence_service.OrderPersistenceService.record_order_execution')
    @patch('src.services.order_execution_service.OrderExecutionService._validate_order_margin', return_value=(True, "Validation passed"))
    def test_execute_single_order_live_mode_success(self, mock_validate_margin, mock_record):
        """Test successful live order execution"""
        planned_order = PlannedOrder(
            security_type=SecurityType.STK,
            exchange='SMART',
            currency='USD',
            action=Action.BUY,
            symbol='AAPL',
            entry_price=150.0,
            stop_loss=145.0
        )

        ibkr_client = MagicMock()
        ibkr_client.connected = True
        ibkr_client.place_bracket_order.return_value = [101, 102, 103]

        trading_manager = MagicMock()
        trading_manager._find_planned_order_db_id.return_value = 1

        active_orders = {}
        order_persistence = MagicMock()

        service = OrderExecutionService(trading_manager, ibkr_client)
        service.set_dependencies(order_persistence=order_persistence, active_orders=active_orders)
        service.order_persistence.record_order_execution = mock_record

        result = service.execute_single_order(
            planned_order,
            fill_probability=0.9,
            effective_priority=1.0,
            total_capital=100000,
            quantity=10,
            capital_commitment=1500.0,
            is_live_trading=True
        )

        assert result is True
        mock_record.assert_called_once()
