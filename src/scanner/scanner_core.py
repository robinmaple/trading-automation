# src/scanner/scanner_core.py
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import pandas as pd
from datetime import datetime
import logging
import time

from config.scanner_config import ScannerConfig
from .technical_scorer import TechnicalScorer
from .integration.ibkr_data_adapter import IBKRDataAdapter

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)

# Tiered Architecture Integration - Begin
@dataclass
class ScanResult:
    """Raw scan result for strategy processing (Tier 1 output)"""
    symbol: str
    current_price: float
    volume: float
    market_cap: float
    ema_values: Dict[str, float]
    historical_data: pd.DataFrame
    last_updated: datetime
    
    # Raw technical data for strategy evaluation
    price_data: Dict[str, Any] = field(default_factory=dict)
    volume_data: Dict[str, Any] = field(default_factory=dict)
    
    # Remove strategy-specific scores - will be calculated by StrategyOrchestrator
    # total_score: float = 0.0
    # bull_trend_score: float = 0.0  
    # bull_pullback_score: float = 0.0
# Tiered Architecture Integration - End

class StockScanner:
    """Tier 1 Scanner: Basic screening and technical data collection"""
    
    def __init__(self, ibkr_data_adapter: IBKRDataAdapter, config: ScannerConfig = None):
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing StockScanner",
            context_provider={
                "ibkr_data_adapter_provided": ibkr_data_adapter is not None,
                "config_provided": config is not None,
                "scanner_type": "Tier 1 - Basic Screening"
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self.data_adapter = ibkr_data_adapter
        self.config = config or ScannerConfig()
        
        # Technical scorer for raw calculations (not strategy scoring)
        self.technical_scorer = TechnicalScorer(
            ema_periods=[self.config.ema_short_term, self.config.ema_medium_term, self.config.ema_long_term, 100],
            pullback_threshold=self.config.max_pullback_distance_pct / 100
        )
        self.logger = logging.getLogger(__name__)
        self.last_scan_time = None
    
    def run_scan(self) -> List[ScanResult]:
        """Execute Tier 1 scanning: basic screening and technical data collection"""
        start_time = time.time()
        self.last_scan_time = datetime.now()
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting Tier 1 Scanner (Basic Screening)",
            context_provider={
                "scan_start_time": self.last_scan_time.isoformat(),
                "config_min_volume": self.config.min_volume,
                "config_min_market_cap": self.config.min_market_cap,
                "config_min_price": self.config.min_price
            }
        )
        # <Context-Aware Logging Integration - End>
        
        # Step 1: Get dynamic universe with basic filters
        filters = {
            'min_volume': self.config.min_volume,
            'min_market_cap': self.config.min_market_cap,
            'min_price': self.config.min_price
        }
        
        qualified_stocks = self.data_adapter.get_dynamic_universe(filters)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Tier 1: Found {len(qualified_stocks)} qualified stocks",
            context_provider={
                "qualified_stocks_count": len(qualified_stocks),
                "filters_applied": filters
            }
        )
        # <Context-Aware Logging Integration - End>
        
        # Step 2: Collect technical data for each stock
        scan_results = []
        for stock_info in qualified_stocks:
            result = self._analyze_stock(stock_info)
            if result:
                scan_results.append(result)
            
            # Small delay between stocks
            time.sleep(0.05)
        
        processing_time = time.time() - start_time
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Tier 1 Scan completed",
            context_provider={
                "processing_time_seconds": processing_time,
                "scan_results_count": len(scan_results),
                "qualified_stocks_count": len(qualified_stocks),
                "success_rate_percentage": (len(scan_results) / len(qualified_stocks) * 100) if qualified_stocks else 0,
                "scan_end_time": datetime.now().isoformat()
            },
            decision_reason="Tier 1 scanning process completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return scan_results
    
    # Enhanced Analysis Method - Begin
    def _analyze_stock(self, stock_info: Dict) -> Optional[ScanResult]:
        """Analyze a single stock and return raw technical data for strategy processing"""
        symbol = stock_info.get('symbol', 'unknown')
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Analyzing stock {symbol}",
            symbol=symbol,
            context_provider={
                "current_price": stock_info.get('price'),
                "volume": stock_info.get('volume', 0),
                "market_cap": stock_info.get('market_cap', 0)
            }
        )
        # <Context-Aware Logging Integration - End>
        
        try:
            # Get historical data
            historical_data = self.data_adapter.get_historical_data(symbol, 100)
            if historical_data is None or historical_data.empty:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"No historical data for {symbol}",
                    symbol=symbol,
                    context_provider={
                        "historical_data_available": False,
                        "data_points_requested": 100
                    },
                    decision_reason="Skipping stock - no historical data"
                )
                # <Context-Aware Logging Integration - End>
                return None
            
            current_price = stock_info['price']
            prices = historical_data['close']
            volumes = historical_data['volume'] if 'volume' in historical_data else None
            
            # Calculate raw technical indicators (no strategy scoring)
            emas = self.technical_scorer.calculate_emas(prices)
            
            # Prepare price and volume data for strategy evaluation
            price_data = {
                'current': current_price,
                'historical': prices.tolist(),
                'highs': historical_data['high'].tolist() if 'high' in historical_data else [],
                'lows': historical_data['low'].tolist() if 'low' in historical_data else [],
                'opens': historical_data['open'].tolist() if 'open' in historical_data else []
            }
            
            volume_data = {
                'current': stock_info.get('volume', 0),
                'historical': volumes.tolist() if volumes is not None else [],
                'average': stock_info.get('average_volume', 0)
            }
            
            result = ScanResult(
                symbol=symbol,
                current_price=current_price,
                volume=stock_info.get('volume', 0),
                market_cap=stock_info.get('market_cap', 0),
                ema_values=emas,
                historical_data=historical_data,
                last_updated=datetime.now(),
                price_data=price_data,
                volume_data=volume_data
            )
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Successfully analyzed stock {symbol}",
                symbol=symbol,
                context_provider={
                    "current_price": current_price,
                    "volume": stock_info.get('volume', 0),
                    "market_cap": stock_info.get('market_cap', 0),
                    "ema_values_count": len(emas),
                    "historical_data_points": len(historical_data),
                    "price_data_points": len(price_data['historical']),
                    "volume_data_points": len(volume_data['historical'])
                },
                decision_reason="Stock analysis completed successfully"
            )
            # <Context-Aware Logging Integration - End>
            
            return result
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error analyzing {symbol}",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "stock_info_keys": list(stock_info.keys()) if stock_info else []
                },
                decision_reason="Stock analysis failed"
            )
            # <Context-Aware Logging Integration - End>
            return None
    
    def run_scan_dataframe(self) -> pd.DataFrame:
        """Legacy method: Run scan and return as DataFrame (for backward compatibility)"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Running legacy DataFrame scan",
            context_provider={}
        )
        # <Context-Aware Logging Integration - End>
        
        scan_results = self.run_scan()
        if scan_results:
            # Convert to dict for DataFrame, excluding historical_data
            results_dict = []
            for result in scan_results:
                result_dict = {
                    'symbol': result.symbol,
                    'current_price': result.current_price,
                    'volume': result.volume,
                    'market_cap': result.market_cap,
                    'last_updated': result.last_updated
                }
                # Add EMA values
                for ema_key, ema_value in result.ema_values.items():
                    result_dict[f'ema_{ema_key}'] = ema_value
                results_dict.append(result_dict)
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Legacy DataFrame conversion completed",
                context_provider={
                    "dataframe_rows": len(results_dict),
                    "dataframe_columns": list(results_dict[0].keys()) if results_dict else [],
                    "scan_results_count": len(scan_results)
                },
                decision_reason="Legacy DataFrame scan completed"
            )
            # <Context-Aware Logging Integration - End>
            
            return pd.DataFrame(results_dict)
        else:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No scan results for DataFrame conversion",
                context_provider={},
                decision_reason="Returning empty DataFrame"
            )
            # <Context-Aware Logging Integration - End>
            return pd.DataFrame()
    # Enhanced Analysis Method - End