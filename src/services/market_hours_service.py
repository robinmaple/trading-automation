# Market Hours Service - Begin
import datetime
from typing import Optional
from zoneinfo import ZoneInfo  # Changed from: import pytz

class MarketHoursService:
    """Service to track market hours and closing times"""
    
    # Standard market hours (ET timezone)
    MARKET_OPEN = datetime.time(9, 30)  # 9:30 AM ET
    MARKET_CLOSE = datetime.time(16, 0)  # 4:00 PM ET
    
    def __init__(self):
        self.et_timezone = ZoneInfo('US/Eastern')  # Changed from: pytz.timezone('US/Eastern')
        
    def is_market_open(self) -> bool:
        """Check if markets are currently open"""
        now_et = datetime.datetime.now(self.et_timezone)
        return (self.MARKET_OPEN <= now_et.time() <= self.MARKET_CLOSE and
                now_et.weekday() < 5)  # Monday-Friday
    
    def time_until_market_close(self) -> Optional[datetime.timedelta]:
        """Return time until market close, or None if market closed"""
        if not self.is_market_open():
            return None
            
        now_et = datetime.datetime.now(self.et_timezone)
        # ZoneInfo changes - Begin
        # Create timezone-aware datetime directly using tzinfo parameter
        close_time = datetime.datetime.combine(
            now_et.date(), 
            self.MARKET_CLOSE, 
            tzinfo=self.et_timezone
        )
        # ZoneInfo changes - End
        return close_time - now_et
    
    def should_close_positions(self, buffer_minutes: int = 5) -> bool:
        """Check if it's time to close positions (5 minutes before close)"""
        time_until_close = self.time_until_market_close()
        if time_until_close is None:
            return False
            
        return time_until_close.total_seconds() <= buffer_minutes * 60
    
    def get_market_status(self) -> str:
        """Get human-readable market status"""
        if not self.is_market_open():
            return "CLOSED"
            
        time_until_close = self.time_until_market_close()
        if time_until_close:
            minutes_until_close = int(time_until_close.total_seconds() / 60)
            return f"OPEN ({minutes_until_close} minutes until close)"
        
        return "OPEN"
# Market Hours Service - End