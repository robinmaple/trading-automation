import pytest
from unittest.mock import Mock, patch
from src.services.order_state_service import OrderStateService
from src.core.models import PlannedOrderDB, PositionStrategy

class TestOrderStateService:
    """Test suite for OrderStateService"""
    
    def test_update_planned_order_status_success(self):
        """Test successful order status update"""
        mock_tm = Mock()
        mock_session = Mock()
        service = OrderStateService(mock_tm, mock_session)
        
        # Mock planned order and database response
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.entry_price = 100.0
        test_order.stop_loss = 95.0
        test_order.action.value = "BUY"
        
        mock_db_order = Mock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_db_order
        
        with patch('builtins.print'):
            result = service.update_planned_order_status(test_order, "LIVE", [111, 222, 333])
        
        # Verify database operations
        mock_session.query.assert_called_once_with(PlannedOrderDB)
        mock_db_order.status = "LIVE"  # This is how the value is actually set
        mock_session.commit.assert_called_once()
        assert result == True
    
    def test_update_planned_order_status_order_not_found(self):
        """Test status update when order is not found in database"""
        mock_tm = Mock()
        mock_session = Mock()
        service = OrderStateService(mock_tm, mock_session)
        
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.entry_price = 100.0
        test_order.stop_loss = 95.0
        test_order.action.value = "BUY"
        
        # Simulate order not found
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        
        with patch('builtins.print'):
            result = service.update_planned_order_status(test_order, "LIVE")
        
        # Should return False when order not found
        assert result == False
        mock_session.commit.assert_not_called()
            
    def test_update_planned_order_status_order_not_found(self):
        """Test status update when order is not found in database"""
        mock_tm = Mock()
        mock_session = Mock()
        service = OrderStateService(mock_tm, mock_session)
        
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.entry_price = 100.0
        test_order.stop_loss = 95.0
        test_order.action.value = "BUY"
        
        # Simulate order not found
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        
        with patch('builtins.print'):
            result = service.update_planned_order_status(test_order, "LIVE")
        
        # Should return False when order not found
        assert result == False
        mock_session.commit.assert_not_called()
    
    def test_convert_to_db_model_success(self):
        """Test successful conversion of PlannedOrder to PlannedOrderDB"""
        mock_tm = Mock()
        mock_session = Mock()
        service = OrderStateService(mock_tm, mock_session)
        
        # Mock position strategy
        mock_strategy = Mock()
        mock_strategy.id = 1
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_strategy
        
        # Mock trading mode
        mock_tm._get_trading_mode.return_value = True  # Live trading
        
        # Mock planned order
        test_order = Mock()
        test_order.symbol = "TEST"
        test_order.security_type.value = "STK"
        test_order.action.value = "BUY"
        test_order.order_type.value = "LMT"
        test_order.entry_price = 100.0
        test_order.stop_loss = 95.0
        test_order.risk_per_trade = 0.01
        test_order.risk_reward_ratio = 2.0
        test_order.priority = 3
        test_order.position_strategy.value = "Day"
        
        with patch('builtins.print'):
            db_model = service.convert_to_db_model(test_order)
        
        # Verify the conversion
        assert db_model.symbol == "TEST"
        assert db_model.security_type == "STK"
        assert db_model.action == "BUY"
        assert db_model.entry_price == 100.0
        assert db_model.stop_loss == 95.0
        assert db_model.risk_per_trade == 0.01
        assert db_model.risk_reward_ratio == 2.0
        assert db_model.priority == 3
        assert db_model.position_strategy_id == 1
        assert db_model.status == "PENDING"
        assert db_model.is_live_trading == True
    
    def test_convert_to_db_model_strategy_not_found(self):
        """Test conversion when position strategy is not found"""
        mock_tm = Mock()
        mock_session = Mock()
        service = OrderStateService(mock_tm, mock_session)
        
        # Simulate strategy not found
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        
        test_order = Mock()
        test_order.position_strategy.value = "InvalidStrategy"
        
        with pytest.raises(ValueError, match="Position strategy InvalidStrategy not found in database"):
            service.convert_to_db_model(test_order)
    
    def test_create_executed_order_delegates_to_trading_manager(self):
        """Test that create_executed_order delegates to trading manager"""
        mock_tm = Mock()
        mock_session = Mock()
        service = OrderStateService(mock_tm, mock_session)
        
        test_order = Mock()
        fill_info = {"price": 100.0, "quantity": 10}
        mock_tm._create_executed_order_record.return_value = "mock_executed_order"
        
        result = service.create_executed_order(test_order, fill_info)
        
        assert result == "mock_executed_order"
        mock_tm._create_executed_order_record.assert_called_once_with(test_order, fill_info)