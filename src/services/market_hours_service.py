# Market Hours Service - Begin
import datetime
from typing import Optional
from zoneinfo import ZoneInfo  # Changed from: import pytz

# Market Hours Service - Begin
import datetime
from typing import Optional
from zoneinfo import ZoneInfo

class MarketHoursService:
    """Service to track market hours and closing times"""
    
    # Standard market hours (ET timezone)
    MARKET_OPEN = datetime.time(9, 30)  # 9:30 AM ET
    MARKET_CLOSE = datetime.time(16, 0)  # 4:00 PM ET
    
    def __init__(self):
        # FIX: Use America/New_York instead of US/Eastern for proper DST handling
        self.et_timezone = ZoneInfo('America/New_York')
        
    def is_market_open(self) -> bool:
        """Check if markets are currently open"""
        now_et = datetime.datetime.now(self.et_timezone)
        current_time = now_et.time()
        current_weekday = now_et.weekday()
        
        print(f"🕒 Market hours check: {current_time} ET, weekday: {current_weekday}")
        print(f"   Market hours: {self.MARKET_OPEN} - {self.MARKET_CLOSE}")
        
        is_open = (self.MARKET_OPEN <= current_time <= self.MARKET_CLOSE and
                  current_weekday < 5)  # Monday-Friday
        print(f"   Market open: {is_open}")
        return is_open
   
    # Market Hours Service - End   
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

    def get_next_market_open(self) -> datetime.datetime:
        """
        Calculate the next market open time.
        
        Returns:
            Timezone-aware datetime of next market open
        """
        now_et = datetime.datetime.now(self.et_timezone)
        current_weekday = now_et.weekday()
        
        # If today is a weekday and before market open, market opens today
        if current_weekday < 5:  # Monday-Friday
            today_open = now_et.replace(hour=self.MARKET_OPEN.hour, minute=self.MARKET_OPEN.minute, 
                                      second=0, microsecond=0)
            if now_et < today_open:
                return today_open
        
        # Otherwise, find next weekday
        days_to_add = 1
        while True:
            next_day = now_et + datetime.timedelta(days=days_to_add)
            if next_day.weekday() < 5:  # Monday-Friday
                next_open = next_day.replace(hour=self.MARKET_OPEN.hour, minute=self.MARKET_OPEN.minute,
                                           second=0, microsecond=0)
                return next_open
            days_to_add += 1
