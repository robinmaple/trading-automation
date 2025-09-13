# Fixed Market Hours Service Tests - Begin
import datetime
from unittest.mock import patch, MagicMock
from src.services.market_hours_service import MarketHoursService

class TestMarketHoursService:
    
    def test_is_market_open_weekday(self):
        """Test market open detection on weekdays"""
        service = MarketHoursService()
        
        # Create a proper timezone-aware datetime object for mocking
        mock_now = datetime.datetime(2024, 1, 10, 10, 0, tzinfo=service.et_timezone)  # Wed 10:00 AM ET
        
        # Patch datetime.now to return our mock value
        with patch('src.services.market_hours_service.datetime') as mock_dt:
            # Create a mock that returns our aware datetime when now() is called
            mock_dt.datetime.now.return_value = mock_now
            mock_dt.datetime.combine = datetime.datetime.combine  # Preserve original combine method
            
            assert service.is_market_open() == True
    
    def test_is_market_open_weekend(self):
        """Test market closed on weekends"""
        service = MarketHoursService()
        
        mock_now = datetime.datetime(2024, 1, 6, 12, 0, tzinfo=service.et_timezone)  # Sat 12:00 PM ET
        
        with patch('src.services.market_hours_service.datetime') as mock_dt:
            mock_dt.datetime.now.return_value = mock_now
            mock_dt.datetime.combine = datetime.datetime.combine
            
            assert service.is_market_open() == False
    
    def test_time_until_market_close(self):
        """Test time until market close calculation"""
        service = MarketHoursService()
        
        mock_now = datetime.datetime(2024, 1, 10, 15, 55, tzinfo=service.et_timezone)  # 3:55 PM ET
        
        with patch('src.services.market_hours_service.datetime') as mock_dt:
            mock_dt.datetime.now.return_value = mock_now
            mock_dt.datetime.combine = datetime.datetime.combine
            
            time_until_close = service.time_until_market_close()
            assert time_until_close is not None
            assert 290 <= time_until_close.total_seconds() <= 300  # Allow some tolerance
    
    def test_should_close_positions(self):
        """Test position closing detection"""
        service = MarketHoursService()
        
        # Test 5 minutes before close
        mock_now_355 = datetime.datetime(2024, 1, 10, 15, 55, tzinfo=service.et_timezone)  # 3:55 PM ET
        with patch('src.services.market_hours_service.datetime') as mock_dt:
            mock_dt.datetime.now.return_value = mock_now_355
            mock_dt.datetime.combine = datetime.datetime.combine
            
            assert service.should_close_positions() == True
        
        # Test 10 minutes before close (should not trigger)
        mock_now_350 = datetime.datetime(2024, 1, 10, 15, 50, tzinfo=service.et_timezone)  # 3:50 PM ET
        with patch('src.services.market_hours_service.datetime') as mock_dt:
            mock_dt.datetime.now.return_value = mock_now_350
            mock_dt.datetime.combine = datetime.datetime.combine
            
            assert service.should_close_positions() == False
# Fixed Market Hours Service Tests - End