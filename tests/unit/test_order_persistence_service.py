# OrderPersistenceService Test Updates - Begin
from unittest.mock import Mock, patch

import pytest
from core.models import ExecutedOrderDB, PlannedOrderDB
from services.order_persistence_service import OrderPersistenceService


class TestOrderPersistenceService:
    
    def test_initialization(self, db_session):
        """Test that OrderPersistenceService initializes correctly"""
        # Fix: Use correct constructor signature (1-2 args)
        service = OrderPersistenceService(db_session=db_session)
        assert service.db_session == db_session
        
        # Test default initialization
        service2 = OrderPersistenceService()
        assert service2.db_session is not None
    
    def test_record_order_execution_success(self, db_session, sample_planned_order_db):
        """Test successful order execution recording"""
        # Fix: Use correct constructor signature
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
        # Fix: Use correct constructor signature
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
        # Fix: Use correct constructor signature
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
        """Test successful order status update."""
        service = OrderPersistenceService(db_session=db_session)
        
        # Create a mock order that EXACTLY matches the sample_planned_order_db
        mock_order = Mock()
        mock_order.symbol = sample_planned_order_db.symbol
        mock_order.entry_price = sample_planned_order_db.entry_price
        mock_order.stop_loss = sample_planned_order_db.stop_loss
        mock_order.action.value = sample_planned_order_db.action
        mock_order.order_type.value = sample_planned_order_db.order_type  # Exact match
        
        result = service.update_order_status(mock_order, 'LIVE_WORKING')
        
        # Debug: Check what's in the database
        db_orders = db_session.query(PlannedOrderDB).all()
        print(f"Database orders: {len(db_orders)}")
        for order in db_orders:
            print(f"  - {order.symbol}: {order.entry_price}, {order.stop_loss}, {order.action}, {order.order_type}")
        
        # Debug: Check the mock order values
        print(f"Mock order: {mock_order.symbol}, {mock_order.entry_price}, {mock_order.stop_loss}, {mock_order.action.value}, {mock_order.order_type.value}")
        
        assert result is True, f"Expected True but got {result}. Check database matching logic."
        
        # Verify the status was updated in the database
        updated_order = db_session.query(PlannedOrderDB).filter_by(id=sample_planned_order_db.id).first()
        assert updated_order.status == 'LIVE_WORKING'
            
    def test_update_order_status_unknown_order(self, db_session):
        """Test status update for unknown order"""
        # Fix: Use correct constructor signature
        service = OrderPersistenceService(db_session=db_session)
        
        mock_order = Mock()
        mock_order.symbol = "UNKNOWN"
        mock_order.entry_price = 999.99
        mock_order.stop_loss = 950.00
        
        result = service.update_order_status(mock_order, 'LIVE_WORKING')
        
        assert result is False
    
    def test_update_order_status_database_error(self, db_session, sample_planned_order_db):
        """Test error handling during status update"""
        # Fix: Use correct constructor signature
        service = OrderPersistenceService(db_session=db_session)
        
        mock_order = Mock()
        mock_order.symbol = sample_planned_order_db.symbol
        mock_order.entry_price = sample_planned_order_db.entry_price
        mock_order.stop_loss = sample_planned_order_db.stop_loss
        
        # Force a database error
        with patch.object(db_session, 'commit', side_effect=Exception("DB error")):
            result = service.update_order_status(mock_order, 'LIVE_WORKING')
            
            assert result is False

    def test_convert_to_db_model_success(self, db_session, position_strategies):
        """Test successful conversion of PlannedOrder to PlannedOrderDB"""
        # Fix: Use correct constructor signature
        service = OrderPersistenceService(db_session=db_session)
        
        # Create a mock planned order
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
        
        result = service.convert_to_db_model(mock_order)
        
        assert result is not None
        assert result.symbol == "EUR"
        assert result.security_type == "CASH"
        assert result.action == "BUY"
        assert result.entry_price == 1.1000

    def test_convert_to_db_model_strategy_not_found(self, db_session):
        """Test conversion failure when position strategy not found"""
        # Fix: Use correct constructor signature
        service = OrderPersistenceService(db_session=db_session)
        
        # Create a mock planned order with non-existent strategy
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
        mock_order.position_strategy.value = "NON_EXISTENT_STRATEGY"
        
        with pytest.raises(ValueError, match="Position strategy NON_EXISTENT_STRATEGY not found"):
            service.convert_to_db_model(mock_order)

    def test_create_executed_order(self, db_session, sample_planned_order_db):
        """Test creating an executed order record"""
        # Fix: Use correct constructor signature
        service = OrderPersistenceService(db_session=db_session)
        
        # Create a mock planned order
        mock_order = Mock()
        mock_order.symbol = sample_planned_order_db.symbol
        mock_order.entry_price = sample_planned_order_db.entry_price
        mock_order.stop_loss = sample_planned_order_db.stop_loss
        mock_order.action.value = sample_planned_order_db.action
        mock_order.order_type.value = sample_planned_order_db.order_type
        
        # Mock fill info
        fill_info = {
            'price': 1.1050,
            'quantity': 10000,
            'commission': 2.5,
            'pnl': 50.0,
            'status': 'FILLED'
        }
        
        result = service.create_executed_order(mock_order, fill_info)
        
        assert result is not None
        assert result.filled_price == 1.1050
        assert result.filled_quantity == 10000
        assert result.commission == 2.5
# OrderPersistenceService Test Updates - End