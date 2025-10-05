# src/scanner/scanner_core.py
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime
import logging
import time

from config.scanner_config import ScannerConfig, ScanResult
from .technical_scorer import TechnicalScorer
from .integration.ibkr_data_adapter import IBKRDataAdapter

class StockScanner:
    """Main scanner engine that coordinates the scanning process"""
    
    def __init__(self, ibkr_data_adapter: IBKRDataAdapter, config: ScannerConfig = None):
        self.data_adapter = ibkr_data_adapter
        self.config = config or ScannerConfig()
        
        # Use only the available config attributes
        self.technical_scorer = TechnicalScorer(
            ema_periods=[self.config.ema_short_term, self.config.ema_medium_term, self.config.ema_long_term, 100],
            pullback_threshold=self.config.max_pullback_distance_pct / 100
        )
        self.logger = logging.getLogger(__name__)
        self.last_scan_time = None
    
    def run_scan(self) -> pd.DataFrame:
        """Execute complete scanning process"""
        start_time = time.time()
        self.last_scan_time = datetime.now()
        
        self.logger.info("🚀 Starting stock scanner...")
        
        # Step 1: Get dynamic universe
        filters = {
            'min_volume': self.config.min_volume,
            'min_market_cap': self.config.min_market_cap,
            'min_price': self.config.min_price
        }
        
        qualified_stocks = self.data_adapter.get_dynamic_universe(filters)
        self.logger.info(f"📊 Found {len(qualified_stocks)} qualified stocks")
        
        # Step 2: Analyze each stock
        results = []
        for stock_info in qualified_stocks:
            result = self._analyze_stock(stock_info)
            if result:
                results.append(result)
            
            # REMOVED: scan_mode check since it doesn't exist
            # Use a small fixed delay instead
            time.sleep(0.05)  # 50ms delay between stocks
        
        # Step 3: Create ranked output
        if results:
            df = pd.DataFrame([r.__dict__ for r in results])
            df = df.sort_values('total_score', ascending=False)
            
            processing_time = time.time() - start_time
            self.logger.info(f"✅ Scan completed in {processing_time:.2f} seconds")
            self.logger.info(f"📈 Successfully analyzed {len(results)} stocks")
            
            return df
        else:
            self.logger.warning("❌ No results generated from scan")
            return pd.DataFrame()
    
    def _analyze_stock(self, stock_info: Dict) -> Optional[ScanResult]:
        """Analyze a single stock and return scoring results"""
        try:
            symbol = stock_info['symbol']
            
            # Get historical data
            historical_data = self.data_adapter.get_historical_data(symbol, 100)
            if historical_data is None or historical_data.empty:
                self.logger.warning(f"No historical data for {symbol}")
                return None
            
            current_price = stock_info['price']
            prices = historical_data['close']
            
            # Calculate EMAs
            emas = self.technical_scorer.calculate_emas(prices)
            
            # Calculate scores
            trend_score = self.technical_scorer.calculate_bull_trend_score(current_price, emas)
            pullback_score = self.technical_scorer.calculate_bull_pullback_score(current_price, emas)
            total_score = self.technical_scorer.calculate_total_score(trend_score, pullback_score)
            
            return ScanResult(
                symbol=symbol,
                total_score=total_score,
                bull_trend_score=trend_score,
                bull_pullback_score=pullback_score,
                current_price=current_price,
                volume_status='✅' if stock_info['volume'] > self.config.min_volume else '❌',
                market_cap_status='✅' if stock_info['market_cap'] > self.config.min_market_cap else '❌',
                price_status='✅' if stock_info['price'] > self.config.min_price else '❌',
                ema_values=emas,
                last_updated=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing {stock_info.get('symbol', 'unknown')}: {e}")
            return None