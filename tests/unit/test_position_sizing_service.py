import pytest
from unittest.mock import Mock, patch
from src.trading.risk.position_sizing_service import PositionSizingService

class TestPositionSizingService:
    """Test suite for PositionSizingService"""
    
    def test_calculate_quantity_stocks(self):
        """Test quantity calculation for stocks"""
        # Mock trading manager (not heavily used in this service)
        mock_tm = Mock()
        service = PositionSizingService(mock_tm)
        
        # Test stock calculation: $100 price, $95 stop, $100k capital, 1% risk
        quantity = service.calculate_quantity("STK", 100.0, 95.0, 100000, 0.01)
        
        # Risk per share = $5, Risk amount = $1000, Quantity = 200 shares
        assert quantity == 200
    
    def test_calculate_quantity_forex(self):
        """Test quantity calculation for forex"""
        mock_tm = Mock()
        service = PositionSizingService(mock_tm)
        
        # Test forex calculation: 1.1000 price, 1.0950 stop, $50k capital, 2% risk
        quantity = service.calculate_quantity("CASH", 1.1000, 1.0950, 50000, 0.02)
        
        # Risk per unit = 0.0050, Risk amount = $1000, Base quantity = 200,000
        # Rounded to nearest 10,000 units = 200,000
        assert quantity == 200000
    
    def test_calculate_quantity_options(self):
        """Test quantity calculation for options"""
        mock_tm = Mock()
        service = PositionSizingService(mock_tm)
        
        # Test option calculation: $5.00 premium, $4.50 stop, $100k capital, 1.5% risk
        quantity = service.calculate_quantity("OPT", 5.00, 4.50, 100000, 0.015)
        
        # Risk per contract = $0.50 * 100 = $50, Risk amount = $1500, Quantity = 30 contracts
        assert quantity == 30
    
    def test_calculate_quantity_zero_risk(self):
        """Test that zero risk per unit raises error"""
        mock_tm = Mock()
        service = PositionSizingService(mock_tm)
        
        with pytest.raises(ValueError, match="Entry price and stop loss cannot be the same"):
            service.calculate_quantity("STK", 100.0, 100.0, 100000, 0.01)
    
    def test_calculate_order_quantity_uses_planned_order(self):
        """Test the higher-level method that takes a PlannedOrder"""
        mock_tm = Mock()
        service = PositionSizingService(mock_tm)
        
        # Mock a planned order
        mock_order = Mock()
        mock_order.security_type.value = "STK"
        mock_order.entry_price = 50.0
        mock_order.stop_loss = 48.0
        mock_order.risk_per_trade = 0.01
        
        # Mock the internal calculate_quantity method
        with patch.object(service, 'calculate_quantity', return_value=100) as mock_calculate:
            result = service.calculate_order_quantity(mock_order, 100000)
            
            # Verify the internal method was called with correct parameters
            mock_calculate.assert_called_once_with("STK", 50.0, 48.0, 100000, 0.01)
            assert result == 100