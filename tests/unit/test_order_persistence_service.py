import pytest
from unittest.mock import Mock, patch
from src.core.order_persistence_service import OrderPersistenceService
from src.core.models import PlannedOrderDB, ExecutedOrderDB, PositionStrategy

class TestOrderPersistenceService:
    
    def test_initialization(self, db_session):
        """Test that OrderPersistenceService initializes correctly"""
        service = OrderPersistenceService(db_session=db_session)
        assert service.db_session == db_session
        
        # Test default initialization
        service2 = OrderPersistenceService()
        assert service2.db_session is not None
    
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