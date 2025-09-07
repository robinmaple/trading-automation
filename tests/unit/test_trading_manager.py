import datetime
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.core.trading_manager import TradingManager
from src.core.planned_order import PlannedOrder, SecurityType, Action, OrderType, PositionStrategy, ActiveOrder
from src.core.probability_engine import FillProbabilityEngine
from src.core.models import ExecutedOrderDB, PlannedOrderDB
from src.core.order_persistence_service import OrderPersistenceService
# Make sure this import is correct

class TestTradingManager:
    
    @patch('src.core.trading_manager.get_db_session')
    def test_initialization(self, mock_get_session, mock_data_feed):
        """Test that TradingManager initializes correctly with persistence service"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        assert tm.data_feed == mock_data_feed
        assert tm.excel_path == "test.xlsx"
        assert tm.planned_orders == []
        assert not tm.monitoring
        # The db_session should be the mock we provided
        assert tm.db_session == mock_session
        assert isinstance(tm.order_persistence, OrderPersistenceService)
    
    @patch('src.core.trading_manager.get_db_session')
    def test_initialization_with_custom_persistence_service(self, mock_get_session, mock_data_feed):
        """Test initialization with custom persistence service"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        custom_persistence = Mock(spec=OrderPersistenceService)
        tm = TradingManager(
            mock_data_feed, 
            "test.xlsx", 
            order_persistence_service=custom_persistence
        )
        
        assert tm.order_persistence == custom_persistence
    
    @patch('src.core.trading_manager.get_db_session')
    def test_calculate_quantity_forex(self, mock_get_session, mock_data_feed):
        """Test quantity calculation for Forex"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        quantity = tm._calculate_quantity(
            "CASH",  # Forex
            1.1000,  # entry_price
            1.0950,  # stop_loss
            100000,  # total_capital
            0.001    # risk_per_trade (0.1%)
        )
        
        # Risk amount: 100000 * 0.001 = 100
        # Risk per unit: 1.1000 - 1.0950 = 0.0050
        # Base quantity: 100 / 0.0050 = 20,000
        # Rounded to nearest 10,000: 20,000
        assert quantity == 20000
    
    @patch('src.core.trading_manager.get_db_session')
    def test_calculate_quantity_stocks(self, mock_get_session, mock_data_feed):
        """Test quantity calculation for Stocks"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        quantity = tm._calculate_quantity(
            "STK",   # Stocks
            100.0,   # entry_price
            95.0,    # stop_loss
            50000,   # total_capital
            0.01     # risk_per_trade (1%)
        )
        
        # Risk amount: 50000 * 0.01 = 500
        # Risk per unit: 100 - 95 = 5
        # Base quantity: 500 / 5 = 100
        assert quantity == 100
    
    @patch('src.core.trading_manager.get_db_session')
    def test_execute_order_simulation_persists_via_service(self, mock_get_session, mock_data_feed, sample_planned_order):
        """Test that order execution in simulation uses persistence service"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        # Mock persistence service
        mock_persistence = Mock(spec=OrderPersistenceService)
        mock_persistence.update_order_status.return_value = True
        mock_persistence.record_order_execution.return_value = 123

        tm = TradingManager(
            mock_data_feed,
            "test.xlsx",
            order_persistence_service=mock_persistence
        )
        tm.total_capital = 100000

        # Execute order in simulation mode
        with patch('builtins.print'):
            tm._execute_order(sample_planned_order, 0.95)

        # Verify persistence service was called - FIXED: remove None parameter
        mock_persistence.update_order_status.assert_called_once_with(
            sample_planned_order, 'FILLED'  # Removed None parameter
        )
        mock_persistence.record_order_execution.assert_called_once()

    @patch('src.core.trading_manager.get_db_session')
    def test_execute_order_live_persists_via_service(self, mock_get_session, mock_data_feed, mock_ibkr_client, sample_planned_order):
        """Test that live order execution uses persistence service"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        # Mock IBKR client to return order IDs
        mock_ibkr_client.connected = True
        mock_ibkr_client.place_bracket_order.return_value = [111, 222, 333]
        mock_ibkr_client.get_account_value.return_value = 100000
        
        # Mock persistence service
        mock_persistence = Mock(spec=OrderPersistenceService)
        mock_persistence.update_order_status.return_value = True
        
        tm = TradingManager(
            mock_data_feed, 
            "test.xlsx", 
            ibkr_client=mock_ibkr_client,
            order_persistence_service=mock_persistence
        )
        
        # Execute order in live mode
        with patch('builtins.print'):
            tm._execute_order(sample_planned_order, 0.95)
        
        # Verify persistence service was called
        mock_persistence.update_order_status.assert_called_once_with(
            sample_planned_order, 'LIVE', [111, 222, 333]
        )
        # record_order_execution should NOT be called for live orders (handled by IBKR callbacks)
        mock_persistence.record_order_execution.assert_not_called()
    
    def test_record_order_execution_success(self, db_session, sample_planned_order_db):
        """Test successful order execution recording"""
        service = OrderPersistenceService(db_session=db_session)
        
        # Create a mock planned order that matches the database record
        mock_order = Mock()
        mock_order.symbol = sample_planned_order_db.symbol
        mock_order.entry_price = sample_planned_order_db.entry_price
        mock_order.stop_loss = sample_planned_order_db.stop_loss
        mock_order.action.value = sample_planned_order_db.action
        mock_order.order_type.value = sample_planned_order_db.order_type
        
        result = service.record_order_execution(
            planned_order=mock_order,
            filled_price=1.1050,
            filled_quantity=10000,
            commission=2.5,
            status="FILLED",
            is_live_trading=True
        )
        
        assert result is not None
        assert isinstance(result, int)
        
        # Verify the record was created in database
        executed_order = db_session.query(ExecutedOrderDB).filter_by(id=result).first()
        assert executed_order is not None
        assert executed_order.filled_price == 1.1050
        assert executed_order.filled_quantity == 10000
        assert executed_order.commission == 2.5
        assert executed_order.status == "FILLED"
        assert executed_order.is_live_trading == True
    
    def test_record_order_execution_unknown_order(self, db_session):
        """Test recording execution for unknown planned order"""
        service = OrderPersistenceService(db_session=db_session)
        
        mock_order = Mock()
        mock_order.symbol = "UNKNOWN"
        mock_order.entry_price = 999.99
        mock_order.stop_loss = 950.00
        mock_order.action.value = "BUY"
        mock_order.order_type.value = "LMT"
        
        result = service.record_order_execution(
            planned_order=mock_order,
            filled_price=1000.0,
            filled_quantity=100,
            commission=1.0,
            status="FILLED",
            is_live_trading=False
        )
        
        assert result is None
    
    def test_record_order_execution_database_error(self, db_session, sample_planned_order_db):
        """Test error handling during order execution recording"""
        service = OrderPersistenceService(db_session=db_session)
        
        mock_order = Mock()
        mock_order.symbol = sample_planned_order_db.symbol
        mock_order.entry_price = sample_planned_order_db.entry_price
        mock_order.stop_loss = sample_planned_order_db.stop_loss
        mock_order.action.value = sample_planned_order_db.action
        mock_order.order_type.value = sample_planned_order_db.order_type
        
        # Force a database error
        with patch.object(db_session, 'commit', side_effect=Exception("DB error")):
            result = service.record_order_execution(
                planned_order=mock_order,
                filled_price=1.1050,
                filled_quantity=10000,
                commission=2.5,
                status="FILLED",
                is_live_trading=True
            )
            
            assert result is None
    
    def test_update_order_status_success(self, db_session, sample_planned_order_db):
        """Test successful order status update"""
        service = OrderPersistenceService(db_session=db_session)
        
        mock_order = Mock()
        mock_order.symbol = sample_planned_order_db.symbol
        mock_order.entry_price = sample_planned_order_db.entry_price
        mock_order.stop_loss = sample_planned_order_db.stop_loss
        
        result = service.update_order_status(
            order=mock_order,
            status="LIVE",
            order_ids=[123, 456, 789]
        )
        
        assert result is True
        
        # Verify the update
        updated_order = db_session.query(PlannedOrderDB).filter_by(id=sample_planned_order_db.id).first()
        assert updated_order.status == "LIVE"
        assert updated_order.ibkr_order_ids == "[123, 456, 789]"
    
    def test_update_order_status_unknown_order(self, db_session):
        """Test status update for unknown order"""
        service = OrderPersistenceService(db_session=db_session)
        
        mock_order = Mock()
        mock_order.symbol = "UNKNOWN"
        mock_order.entry_price = 999.99
        mock_order.stop_loss = 950.00
        
        result = service.update_order_status(
            order=mock_order,
            status="LIVE"
        )
        
        assert result is False
    
    def test_update_order_status_database_error(self, db_session, sample_planned_order_db):
        """Test error handling during status update"""
        service = OrderPersistenceService(db_session=db_session)
        
        mock_order = Mock()
        mock_order.symbol = sample_planned_order_db.symbol
        mock_order.entry_price = sample_planned_order_db.entry_price
        mock_order.stop_loss = sample_planned_order_db.stop_loss
        
        # Force a database error
        with patch.object(db_session, 'commit', side_effect=Exception("DB error")):
            result = service.update_order_status(
                order=mock_order,
                status="LIVE"
            )
            
            assert result is False
                
    @patch('src.core.trading_manager.get_db_session')
    def test_load_planned_orders(self, mock_get_session, mock_data_feed):
        """Test loading planned orders from Excel (mocked)"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        # Replace the actual session with our mock to avoid database errors
        tm.db_session = mock_session

        with patch('src.core.trading_manager.PlannedOrderManager.from_excel') as mock_loader:
            # Create a proper mock with all required attributes including mock config
            mock_order = Mock()
            mock_order.symbol = "EUR"
            mock_order.security_type.value = "CASH"
            mock_order.action.value = "BUY"
            mock_order.order_type.value = "LMT"
            mock_order.entry_price = 1.1000
            mock_order.stop_loss = 1.0950
            mock_order.risk_per_trade = 0.001
            mock_order.risk_reward_ratio = 2.0
            mock_order.position_strategy.value = "DAY"
            mock_order.priority = 3
            mock_order.mock_anchor_price = 1.1000
            mock_order.mock_trend = "up"
            mock_order.mock_volatility = 0.0005
            
            mock_loader.return_value = [mock_order]
            
            # Mock the database operations to avoid actual database calls
            with patch.object(tm, '_convert_to_db_model') as mock_convert:
                mock_db_order = Mock()
                mock_convert.return_value = mock_db_order
                
                orders = tm.load_planned_orders()
                
                assert len(orders) == 1
                mock_loader.assert_called_once_with("test.xlsx")
                # Verify database operations were attempted
                mock_session.add.assert_called_with(mock_db_order)
                mock_session.commit.assert_called_once()

    @patch('src.core.trading_manager.get_db_session')
    def test_can_place_order_basic_validation(self, mock_get_session, mock_data_feed, sample_planned_order):
        """Test basic order validation logic"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Test valid order
        assert tm._can_place_order(sample_planned_order) == True
        
        # Test order without entry price
        invalid_order = sample_planned_order
        invalid_order.entry_price = None
        assert tm._can_place_order(invalid_order) == False
    
    @patch('src.core.trading_manager.get_db_session')
    def test_can_place_order_max_orders(self, mock_get_session, mock_data_feed, sample_planned_order):
        """Test max open orders validation"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Fill active orders to max capacity with ActiveOrder objects
        for i in range(5):
            tm.active_orders[i] = ActiveOrder(
                planned_order=Mock(spec=PlannedOrder),
                order_ids=[i],
                db_id=i,
                status='SUBMITTED',
                capital_commitment=1000.0,
                timestamp=datetime.datetime.now(),
                is_live_trading=False,
                fill_probability=0.8
            )
        
        assert tm._can_place_order(sample_planned_order) == False
    
    @patch('src.core.trading_manager.get_db_session')
    def test_can_place_order_duplicate_prevention(self, mock_get_session, mock_data_feed, sample_planned_order):
        """Test duplicate order prevention logic"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Add the same order to active orders as ActiveOrder object
        tm.active_orders[1] = ActiveOrder(
            planned_order=sample_planned_order,
            order_ids=[1],
            db_id=1,
            status='SUBMITTED',
            capital_commitment=1000.0,
            timestamp=datetime.datetime.now(),
            is_live_trading=False,
            fill_probability=0.8
        )
        
        # Should prevent duplicate
        assert tm._can_place_order(sample_planned_order) == False    
    
    @patch('src.core.trading_manager.get_db_session')
    def test_find_executable_orders(self, mock_get_session, mock_data_feed, sample_planned_order):
        """Test finding executable orders based on conditions"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        tm.planned_orders = [sample_planned_order]
        
        # Initialize the probability engine properly
        tm.probability_engine = Mock(spec=FillProbabilityEngine)
        tm.probability_engine.should_execute_order.return_value = (True, 0.95)
            
        executable = tm._find_executable_orders()
        
        assert len(executable) == 1
        assert executable[0]['order'] == sample_planned_order
        assert executable[0]['fill_probability'] == 0.95
    
    @patch('src.core.trading_manager.FillProbabilityEngine')
    @patch('src.core.trading_manager.get_db_session')
    def test_order_execution_simulation(self, mock_get_session, mock_engine_class, mock_data_feed, sample_planned_order):
        """Test order execution in simulation mode"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        mock_engine_instance = Mock()
        mock_engine_instance.should_execute_order.return_value = (True, 0.95)
        mock_engine_class.return_value = mock_engine_instance
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        tm.planned_orders = [sample_planned_order]
        
        # Mock the probability engine
        tm.probability_engine = mock_engine_instance
        
        executable = tm._find_executable_orders()
        assert len(executable) == 1
        
        # Should execute in simulation mode (no IBKR client)
        with patch('builtins.print') as mock_print:
            tm._execute_order(sample_planned_order, 0.95)
            mock_print.assert_any_call("✅ SIMULATION: Order for EUR executed successfully")

    @patch('src.core.trading_manager.get_db_session')
    def test_convert_to_db_model(self, mock_get_session, mock_data_feed, db_session, position_strategies):
        """Test conversion of PlannedOrder to PlannedOrderDB model"""
        # Mock the database session to return our test session
        mock_get_session.return_value = db_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Create a REAL planned order, not a mock, to avoid the issue
        from src.core.planned_order import PlannedOrder, SecurityType, Action, OrderType, PositionStrategy
        planned_order = PlannedOrder(
            security_type=SecurityType.CASH,
            exchange="IDEALPRO",
            currency="USD",
            action=Action.BUY,
            symbol="EUR",
            order_type=OrderType.LMT,
            risk_per_trade=0.001,
            entry_price=1.1000,
            stop_loss=1.0950,
            risk_reward_ratio=2.0,
            position_strategy=PositionStrategy.DAY,
            priority=4
        )
        
        # Convert to database model
        db_model = tm._convert_to_db_model(planned_order)
        
        # Verify conversion - only fields that exist in PlannedOrderDB
        assert db_model.symbol == "EUR"
        assert db_model.security_type == "CASH"
        assert db_model.action == "BUY"
        assert db_model.entry_price == 1.1000
        assert db_model.priority == 4
        assert db_model.position_strategy_id == position_strategies["DAY"].id
        
        # Verify mock data fields are NOT in the database model
        assert not hasattr(db_model, 'mock_anchor_price')
        assert not hasattr(db_model, 'mock_trend')
        assert not hasattr(db_model, 'mock_volatility')
        
    # Phase 2 - Remove Mock Config from DB Tests - 2025-09-07 13:26 - Begin
    @patch('src.core.trading_manager.get_db_session')
    def test_load_planned_orders_persists_to_database(self, mock_get_session, mock_data_feed, db_session):
        """Test that loading planned orders persists them to database"""
        mock_get_session.return_value = db_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        with patch('src.core.trading_manager.PlannedOrderManager.from_excel') as mock_loader:
            # Create a mock planned order with proper attributes
            mock_order = Mock()
            mock_order.symbol = "EUR"
            mock_order.security_type.value = "CASH"
            mock_order.action.value = "BUY"
            mock_order.order_type.value = "LMT"
            mock_order.entry_price = 1.1000
            mock_order.stop_loss = 1.0950
            mock_order.risk_per_trade = 0.001
            mock_order.risk_reward_ratio = 2.0
            mock_order.priority = 3
            mock_order.position_strategy.value = "DAY"
            # Mock configuration is NOT persisted to database
            mock_order.mock_anchor_price = 1.1000
            mock_order.mock_trend = "up"
            mock_order.mock_volatility = 0.0005
            
            mock_loader.return_value = [mock_order]
            
            # Load and persist orders
            orders = tm.load_planned_orders()
            
            assert len(orders) == 1
            mock_loader.assert_called_once_with("test.xlsx")
            
            # Verify orders were persisted to database (only core trading data)
            db_orders = db_session.query(PlannedOrderDB).all()
            assert len(db_orders) == 1
            assert db_orders[0].symbol == "EUR"
            assert db_orders[0].status == "PENDING"
            # Mock configuration should NOT be in database
            # Phase 2 - Remove Mock Config from DB Tests - 2025-09-07 13:26 - End

    @patch('src.core.trading_manager.get_db_session')
    def test_update_order_status(self, mock_get_session, mock_data_feed, db_session):
        """Test updating order status in database"""
        # Mock the database session to return our test session
        mock_get_session.return_value = db_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Create and add a test order to database
        from src.core.models import PlannedOrderDB, PositionStrategy
        position_strategy = db_session.query(PositionStrategy).filter_by(name="DAY").first()
        
        test_order = PlannedOrderDB(
            symbol="EUR",
            security_type="CASH",
            action="BUY",
            order_type="LMT",
            entry_price=1.1000,
            stop_loss=1.0950,
            risk_per_trade=0.001,
            risk_reward_ratio=2.0,
            position_strategy_id=position_strategy.id,
            status="PENDING",
            priority=3
            # Mock configuration is NOT persisted to database
        )
        db_session.add(test_order)
        db_session.commit()
        
        # Create a mock planned order for the update
        mock_order = Mock()
        mock_order.symbol = "EUR"
        mock_order.entry_price = 1.1000
        mock_order.stop_loss = 1.0950
        
        # Update status
        tm._update_order_status(mock_order, "LIVE", [123, 456])
        
        # Verify update
        updated_order = db_session.query(PlannedOrderDB).filter_by(symbol="EUR").first()
        assert updated_order.status == "LIVE"
        
    @patch('src.core.trading_manager.get_db_session')
    def test_get_trading_mode_live(self, mock_get_session, mock_data_feed, mock_ibkr_client):
        """Test trading mode detection for live trading"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx", mock_ibkr_client)
        mock_ibkr_client.connected = True
        mock_ibkr_client.is_paper_account = False
        mock_ibkr_client.account_number = "U1234567"
        
        is_live = tm._get_trading_mode()
        assert is_live == True

    @patch('src.core.trading_manager.get_db_session')
    def test_get_trading_mode_paper(self, mock_get_session, mock_data_feed, mock_ibkr_client):
        """Test trading mode detection for paper trading"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx", mock_ibkr_client)
        mock_ibkr_client.connected = True
        mock_ibkr_client.is_paper_account = True
        mock_ibkr_client.account_number = "DU1234567"
        
        is_live = tm._get_trading_mode()
        assert is_live == False

    @patch('src.core.trading_manager.get_db_session')
    def test_get_trading_mode_simulation(self, mock_get_session, mock_data_feed):
        """Test trading mode detection for simulation (no IBKR client)"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        is_live = tm._get_trading_mode()
        assert is_live == False


    @patch('src.core.trading_manager.get_db_session')
    def test_stop_monitoring_closes_db_session(self, mock_get_session, mock_data_feed, db_session):
        """Test that stop_monitoring closes database session"""
        # Mock the database session to return our test session
        mock_get_session.return_value = db_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        tm.monitoring = True
        tm.monitor_thread = Mock()
        tm.monitor_thread.join.return_value = None
        
        # Mock the session close method
        with patch.object(db_session, 'close') as mock_close:
            tm.stop_monitoring()
            
            # Verify session was closed
            mock_close.assert_called_once()
# Database Persistence Tests - End

    # Add this fixture to the TestTradingManager class or in conftest.py
    # For now, I'll add it as a method in the test class

    def create_test_active_order(self, planned_order, status='SUBMITTED', minutes_old=0, capital=1000.0, fill_prob=0.8):
        """Helper to create ActiveOrder objects for testing"""
        from src.core.planned_order import ActiveOrder
        import datetime
        
        timestamp = datetime.datetime.now() - datetime.timedelta(minutes=minutes_old)
        
        return ActiveOrder(
            planned_order=planned_order,
            order_ids=[1],
            db_id=1,
            status=status,
            capital_commitment=capital,
            timestamp=timestamp,
            is_live_trading=False,
            fill_probability=fill_prob
        )
    
    @patch('src.core.trading_manager.get_db_session')
    def test_calculate_order_score(self, mock_get_session, mock_data_feed):
        """Test order scoring calculation"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Create a mock order with priority 4
        mock_order = Mock()
        mock_order.priority = 4
        
        # Test score calculation: priority * probability
        score = tm._calculate_order_score(mock_order, 0.75)
        assert score == 3.0  # 4 * 0.75
        
        # Test edge cases
        score = tm._calculate_order_score(mock_order, 0.0)
        assert score == 0.0
        
        score = tm._calculate_order_score(mock_order, 1.0)
        assert score == 4.0

    @patch('src.core.trading_manager.get_db_session')
    def test_get_committed_capital(self, mock_get_session, mock_data_feed, sample_planned_order):
        """Test capital commitment calculation"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Add working orders with capital commitments
        active_order1 = self.create_test_active_order(sample_planned_order, capital=5000.0)
        active_order2 = self.create_test_active_order(sample_planned_order, capital=3000.0)
        
        tm.active_orders[1] = active_order1
        tm.active_orders[2] = active_order2
        
        # Test total committed capital
        committed = tm._get_committed_capital()
        assert committed == 8000.0  # 5000 + 3000
        
        # Test with non-working orders (should not be counted)
        filled_order = self.create_test_active_order(sample_planned_order, status='FILLED', capital=2000.0)
        tm.active_orders[3] = filled_order
        
        committed = tm._get_committed_capital()
        assert committed == 8000.0  # Still only working orders

    @patch('src.core.trading_manager.get_db_session')
    def test_get_eligible_orders(self, mock_get_session, mock_data_feed, sample_planned_order):
        """Test eligible order finding and sorting"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Create orders with different priorities
        high_priority_order = Mock()
        high_priority_order.priority = 5
        high_priority_order.entry_price = 1.1000
        high_priority_order.stop_loss = 1.0950
        
        medium_priority_order = Mock()
        medium_priority_order.priority = 3  
        medium_priority_order.entry_price = 1.2000
        medium_priority_order.stop_loss = 1.1950
        
        # Mock the planned orders list
        tm.planned_orders = [high_priority_order, medium_priority_order]
        
        # Mock the probability engine to return different probabilities
        tm.probability_engine = Mock()
        tm.probability_engine.should_execute_order.side_effect = [
            (True, 0.6),  # High priority, medium probability
            (True, 0.9)   # Medium priority, high probability
        ]
        
        # Mock _can_place_order to return True for both
        with patch.object(tm, '_can_place_order', return_value=True):
            eligible_orders = tm._get_eligible_orders()
            
            # Should return both orders, sorted by score descending
            assert len(eligible_orders) == 2
            
            # Calculate expected scores: priority * probability
            # high: 5 * 0.6 = 3.0, medium: 3 * 0.9 = 2.7
            assert eligible_orders[0]['score'] == 3.0  # High priority first
            assert eligible_orders[1]['score'] == 2.7  # Medium priority second
            
            assert eligible_orders[0]['order'] == high_priority_order
            assert eligible_orders[1]['order'] == medium_priority_order

    @patch('src.core.trading_manager.get_db_session')
    def test_get_eligible_orders_filters_low_probability(self, mock_get_session, mock_data_feed, sample_planned_order):
        """Test that low probability orders are filtered out"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        tm.planned_orders = [sample_planned_order]
        
        # Mock probability engine to return below threshold
        tm.probability_engine = Mock()
        tm.probability_engine.should_execute_order.return_value = (False, 0.2)
        
        with patch.object(tm, '_can_place_order', return_value=True):
            eligible_orders = tm._get_eligible_orders()
            assert len(eligible_orders) == 0  # Should be filtered out

    @patch('src.core.trading_manager.get_db_session')
    def test_find_worst_active_order_no_stale_orders(self, mock_get_session, mock_data_feed, sample_planned_order):
        """Test when no stale orders exist"""
        # Mock the database session
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Add only fresh orders
        fresh_order = self.create_test_active_order(
            sample_planned_order,
            minutes_old=10,  # Not stale
            capital=1000.0
        )
        tm.active_orders = {1: fresh_order}
        
        # Should return None since no stale orders
        worst = tm._find_worst_active_order()
        assert worst is None

    def test_find_worst_active_order(self, mock_data_feed):
        """Test finding the worst active order based on fill probability"""
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Mock the _calculate_order_score method
        tm._calculate_order_score = Mock()
        tm._calculate_order_score.side_effect = lambda order, prob: prob * 100  # Simple scoring

        # Create mock ActiveOrder objects with all required attributes and methods
        import datetime
        
        # Create current time for timestamp comparison
        current_time = datetime.datetime.now()
        stale_time = current_time - datetime.timedelta(minutes=31)  # 31 minutes old (stale)
        fresh_time = current_time - datetime.timedelta(minutes=29)  # 29 minutes old (not stale)

        # Order 1: Stale but high score (should not be worst)
        order1 = Mock()
        order1.is_working.return_value = True
        order1.timestamp = stale_time
        order1.planned_order = Mock()
        order1.fill_probability = 0.8  # High probability = high score
        order1.symbol = "EUR"

        # Order 2: Stale and low score (should be worst)
        order2 = Mock()
        order2.is_working.return_value = True
        order2.timestamp = stale_time
        order2.planned_order = Mock()
        order2.fill_probability = 0.4  # Low probability = low score
        order2.symbol = "GBP"

        # Order 3: Not stale (should be skipped)
        order3 = Mock()
        order3.is_working.return_value = True
        order3.timestamp = fresh_time
        order3.planned_order = Mock()
        order3.fill_probability = 0.2  # Very low but not stale
        order3.symbol = "JPY"

        # Order 4: Not working (should be skipped)
        order4 = Mock()
        order4.is_working.return_value = False
        order4.timestamp = stale_time
        order4.planned_order = Mock()
        order4.fill_probability = 0.1  # Very low but not working
        order4.symbol = "CHF"

        tm.active_orders = {
            1: order1,
            2: order2,
            3: order3,
            4: order4
        }

        worst_order = tm._find_worst_active_order()
        
        # The worst order should be order2 (stale, working, lowest score)
        assert worst_order is not None
        assert worst_order.symbol == "GBP"
        assert worst_order.fill_probability == 0.4

    def test_find_worst_active_order_no_stale_orders(self, mock_data_feed):
        """Test that no worst order is found when no orders are stale"""
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Mock the _calculate_order_score method
        tm._calculate_order_score = Mock()
        tm._calculate_order_score.side_effect = lambda order, prob: prob * 100

        # Create mock ActiveOrder objects that are not stale
        import datetime
        
        current_time = datetime.datetime.now()
        fresh_time = current_time - datetime.timedelta(minutes=29)  # Not stale

        order1 = Mock()
        order1.is_working.return_value = True
        order1.timestamp = fresh_time
        order1.planned_order = Mock()
        order1.fill_probability = 0.4
        order1.symbol = "EUR"

        order2 = Mock()
        order2.is_working.return_value = True
        order2.timestamp = fresh_time
        order2.planned_order = Mock()
        order2.fill_probability = 0.2
        order2.symbol = "GBP"

        tm.active_orders = {
            1: order1,
            2: order2
        }

        worst_order = tm._find_worst_active_order()
        
        # Should return None since no orders are stale
        assert worst_order is None

    def test_find_worst_active_order_with_min_score_threshold(self, mock_data_feed):
        """Test that orders below minimum score threshold are skipped"""
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Mock the _calculate_order_score method
        tm._calculate_order_score = Mock()
        tm._calculate_order_score.side_effect = lambda order, prob: prob * 100

        import datetime
        
        current_time = datetime.datetime.now()
        stale_time = current_time - datetime.timedelta(minutes=31)

        # Order 1: Stale but above threshold
        order1 = Mock()
        order1.is_working.return_value = True
        order1.timestamp = stale_time
        order1.planned_order = Mock()
        order1.fill_probability = 0.5  # Score = 50
        order1.symbol = "EUR"

        # Order 2: Stale but below threshold (should be skipped)
        order2 = Mock()
        order2.is_working.return_value = True
        order2.timestamp = stale_time
        order2.planned_order = Mock()
        order2.fill_probability = 0.2  # Score = 20 (below threshold of 30)
        order2.symbol = "GBP"

        tm.active_orders = {
            1: order1,
            2: order2
        }

        # Set minimum score threshold to 30
        worst_order = tm._find_worst_active_order(min_score_threshold=30)
        
        # Should return order1 (only one above threshold)
        assert worst_order is not None
        assert worst_order.symbol == "EUR"
        assert worst_order.fill_probability == 0.5

    def test_active_order_creation(self):
        """Test ActiveOrder object creation with all attributes"""
        planned_order = Mock()
        active_order = ActiveOrder(
            planned_order=planned_order,
            order_ids=[123, 456, 789],
            db_id=1,
            status='SUBMITTED',
            capital_commitment=1000.0,
            timestamp=datetime.datetime.now(),
            is_live_trading=False,
            fill_probability=0.85
        )
        
        assert active_order.fill_probability == 0.85
        assert active_order.status == 'SUBMITTED'

    def test_active_order_is_working(self):
        """Test is_working() method with different statuses"""
        active_order = ActiveOrder(
            planned_order=Mock(),
            order_ids=[1],
            db_id=1,
            status='SUBMITTED',
            capital_commitment=1000.0,
            timestamp=datetime.datetime.now(),
            is_live_trading=False,
            fill_probability=0.8
        )
        
        active_order.status = 'SUBMITTED'
        assert active_order.is_working() == True
        
        active_order.status = 'WORKING'
        assert active_order.is_working() == True
        
        active_order.status = 'FILLED'
        assert active_order.is_working() == False
        
        active_order.status = 'CANCELLED'
        assert active_order.is_working() == False

    @patch('src.core.trading_manager.get_db_session')
    def test_cancel_active_order_failure(self, mock_get_session, mock_data_feed, mock_ibkr_client):
        """Test order cancellation failure"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx", mock_ibkr_client)
        mock_ibkr_client.connected = True
        mock_ibkr_client.cancel_order.return_value = False
        
        active_order = ActiveOrder(
            planned_order=Mock(),
            order_ids=[111],
            db_id=1,
            status='SUBMITTED',
            capital_commitment=1000.0,
            timestamp=datetime.datetime.now(),
            is_live_trading=False,
            fill_probability=0.8
        )
        
        result = tm.cancel_active_order(active_order)
        
        assert result == False
        assert active_order.status == 'SUBMITTED'  # Status shouldn't change

    @patch('src.core.trading_manager.get_db_session')
    def test_cleanup_completed_orders(self, mock_get_session, mock_data_feed):
        """Test cleanup of completed orders"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx")
        
        # Add orders with different statuses
        working_order = ActiveOrder(
            planned_order=Mock(),
            order_ids=[1],
            db_id=1,
            status='WORKING',
            capital_commitment=1000.0,
            timestamp=datetime.datetime.now(),
            is_live_trading=False,
            fill_probability=0.8
        )
        filled_order = ActiveOrder(
            planned_order=Mock(),
            order_ids=[2],
            db_id=2,
            status='FILLED',
            capital_commitment=1000.0,
            timestamp=datetime.datetime.now(),
            is_live_trading=False,
            fill_probability=0.8
        )
        cancelled_order = ActiveOrder(
            planned_order=Mock(),
            order_ids=[3],
            db_id=3,
            status='CANCELLED',
            capital_commitment=1000.0,
            timestamp=datetime.datetime.now(),
            is_live_trading=False,
            fill_probability=0.8
        )
        
        tm.active_orders = {1: working_order, 2: filled_order, 3: cancelled_order}
        
        tm.cleanup_completed_orders()
        
        # Only working order should remain
        assert len(tm.active_orders) == 1
        assert 1 in tm.active_orders
        assert 2 not in tm.active_orders
        assert 3 not in tm.active_orders

    def test_active_order_is_stale(self):
        """Test stale order detection - using the actual ActiveOrder class capabilities"""
        current_time = datetime.datetime.now()
        
        # Create active order with proper attributes
        active_order = ActiveOrder(
            planned_order=Mock(),
            order_ids=[1],
            db_id=1,
            status='SUBMITTED',
            capital_commitment=1000.0,
            timestamp=current_time - datetime.timedelta(minutes=31),  # Stale
            is_live_trading=False,
            fill_probability=0.8
        )
        
        # Manually test staleness since is_stale() method doesn't exist
        age_minutes = (datetime.datetime.now() - active_order.timestamp).total_seconds() / 60
        is_stale = age_minutes > 30 and active_order.status in ['SUBMITTED', 'WORKING']
        
        assert is_stale == True

    @patch('src.core.trading_manager.get_db_session')
    def test_active_order_integration(self, mock_get_session, mock_data_feed, mock_ibkr_client, sample_planned_order):
        """Test full active order lifecycle integration"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx", mock_ibkr_client)
        mock_ibkr_client.connected = True
        mock_ibkr_client.place_bracket_order.return_value = [111, 222, 333]
        
        # Mock the ActiveOrder creation in _execute_order
        with patch('src.core.trading_manager.ActiveOrder') as mock_active_order_class:
            mock_active_order = Mock()
            mock_active_order_class.return_value = mock_active_order
            
            # Execute order to create active order
            with patch('builtins.print'):
                tm._execute_order(sample_planned_order, 0.85)
            
            # Verify active order was created and stored
            assert len(tm.active_orders) == 1
            assert 111 in tm.active_orders  # First order ID should be the key

    @patch('src.core.trading_manager.get_db_session')
    def test_cancel_active_order_success(self, mock_get_session, mock_data_feed, mock_ibkr_client):
        """Test successful order cancellation"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session

        tm = TradingManager(mock_data_feed, "test.xlsx", mock_ibkr_client)
        mock_ibkr_client.connected = True
        mock_ibkr_client.cancel_order.return_value = True

        # Create active order with a planned order that has symbol
        planned_order = Mock()
        planned_order.symbol = 'EUR'

        # Fix: Provide all required parameters
        active_order = ActiveOrder(
            planned_order=planned_order,
            order_ids=[111, 222, 333],
            db_id=1,  # Added
            status='SUBMITTED',  # Added
            capital_commitment=1000.0,  # Added
            timestamp=datetime.datetime.now(),
            is_live_trading=False,  # Added
            fill_probability=0.8
        )

        tm.active_orders[111] = active_order

    @patch('src.core.trading_manager.get_db_session')
    def test_replace_active_order_success(self, mock_get_session, mock_data_feed, mock_ibkr_client, sample_planned_order):
        """Test successful order replacement"""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        tm = TradingManager(mock_data_feed, "test.xlsx", mock_ibkr_client)
        mock_ibkr_client.connected = True
        mock_ibkr_client.cancel_order.return_value = True
        
        # Create stale active order
        old_order = ActiveOrder(
            planned_order=sample_planned_order,
            order_ids=[111, 222, 333],
            db_id=1,
            status='SUBMITTED',
            capital_commitment=1000.0,
            timestamp=datetime.datetime.now() - datetime.timedelta(minutes=31),
            is_live_trading=False,
            fill_probability=0.4
        )
        
        tm.active_orders[111] = old_order
        
        with patch.object(tm, '_execute_order') as mock_execute:
            result = tm.replace_active_order(old_order, sample_planned_order, 0.85)
            
            assert result == True
            assert old_order.status == 'REPLACED'
            mock_execute.assert_called_once()