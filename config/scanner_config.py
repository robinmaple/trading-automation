# src/scanner/scanner_config.py
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import Enum
from datetime import datetime

class ScanMode(Enum):
    DAILY_EOD = "daily_eod"
    REAL_TIME = "real_time"

@dataclass
class ScannerConfig:
    """Fully configurable scanner configuration"""
    
    # Fundamental Criteria Configuration
    min_volume: int = 1_000_000
    min_market_cap: float = 10_000_000_000  # $10B
    min_price: float = 5.0
    max_price: Optional[float] = None  # Add upper price limit if needed
    
    # EMA Period Configuration
    ema_short_term: int = 10    # ST - Configurable
    ema_medium_term: int = 20   # MD - Configurable  
    ema_long_term: int = 50     # LT - Configurable
    
    # Pullback Configuration
    max_pullback_distance_pct: float = 3.0  # Maximum distance from EMA(ST)
    ideal_pullback_range_pct: tuple = (0.5, 2.0)  # Ideal pullback range
    
    # Scan Behavior Configuration
    max_symbols_to_scan: int = 500
    real_time_scan_interval: int = 300  # 5 minutes for real-time scanning
    cache_duration: int = 300  # 5 minutes
    
    # Strategy Configuration
    min_confidence_score: int = 60
    max_candidates: int = 25
    
    # Advanced Configuration
    custom_criteria: List[Dict] = field(default_factory=list)
    excluded_sectors: List[str] = field(default_factory=list)
    included_exchanges: List[str] = field(default_factory=lambda: ['NYSE', 'NASDAQ'])
    
    def __post_init__(self):
        # Validate EMA periods
        if not (self.ema_short_term < self.ema_medium_term < self.ema_long_term):
            raise ValueError("EMA periods must be in ascending order: ST < MD < LT")
        
        # Validate pullback parameters
        if self.max_pullback_distance_pct <= 0:
            raise ValueError("Max pullback distance must be positive")
    
    def to_criteria_parameters(self) -> Dict[str, Any]:
        """Convert scanner config to criteria parameters"""
        return {
            # Fundamental criteria
            'min_volume': self.min_volume,
            'min_market_cap': self.min_market_cap,
            'min_price': self.min_price,
            'max_price': self.max_price,
            
            # EMA criteria
            'ema_short_term': self.ema_short_term,
            'ema_medium_term': self.ema_medium_term, 
            'ema_long_term': self.ema_long_term,
            
            # Pullback criteria
            'max_pullback_distance_pct': self.max_pullback_distance_pct,
            'ideal_pullback_range_pct': self.ideal_pullback_range_pct,
            
            # Advanced
            'excluded_sectors': self.excluded_sectors,
            'included_exchanges': self.included_exchanges
        }

# ADD THE MISSING ScanResult CLASS
@dataclass
class ScanResult:
    """Result of scanning a single stock"""
    symbol: str
    total_score: int
    bull_trend_score: int
    bull_pullback_score: int
    current_price: float
    volume_status: str
    market_cap_status: str
    price_status: str
    ema_values: Dict[int, float]
    last_updated: datetime