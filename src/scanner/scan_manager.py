# src/scanner/scan_manager.py
from typing import List, Dict, Any, Optional
import logging
import pandas as pd
from datetime import datetime

from .scanner_core import StockScanner
from .candidate_generator import CandidateGenerator
from .strategy.strategy_core import StrategyRegistry
from .strategy.bull_trend_pullback_strategy import BullTrendPullbackStrategy
from .strategy.configurable_strategies import create_configurable_bull_trend_pullback_config
from .criteria_setup import create_configurable_criteria_registry
from .scanner_config import ScannerConfig, ScanResult

class ScanManager:
    """
    Main entry point for running scanner operations
    Provides a clean, simple interface for generating candidate lists
    """
    
    def __init__(self, ibkr_data_adapter, scanner_config: Optional[ScannerConfig] = None):
        self.data_adapter = ibkr_data_adapter
        self.scanner_config = scanner_config or ScannerConfig()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self._initialize_components()
        
        self.logger.info("✅ ScanManager initialized successfully")
    
    def _initialize_components(self):
        """Initialize all scanner components"""
        # 1. Create criteria registry
        self.criteria_registry = create_configurable_criteria_registry(self.scanner_config)
        
        # 2. Create strategy registry and register strategies
        self.strategy_registry = StrategyRegistry()
        strategy_config = create_configurable_bull_trend_pullback_config(self.scanner_config)
        self.strategy_registry.register(
            BullTrendPullbackStrategy(strategy_config, self.criteria_registry)
        )
        
        # 3. Create candidate generator
        self.candidate_generator = CandidateGenerator(self.strategy_registry)
        
        # 4. Create main scanner
        self.scanner = StockScanner(self.data_adapter, self.scanner_config)
    
    def generate_bull_trend_pullback_candidates(self) -> List[Dict[str, Any]]:
        """
        Main method: Generate bull trend pullback candidate list
        Simple one-call interface for traders
        """
        self.logger.info("🎯 Generating Bull Trend Pullback Candidates...")
        
        # Step 1: Run base scanner
        scan_results_df = self.scanner.run_scan()
        
        if scan_results_df.empty:
            self.logger.warning("❌ No stocks passed initial filters")
            return []
        
        # Step 2: Convert to ScanResult objects
        scan_results = self._convert_to_scan_results(scan_results_df)
        
        # Step 3: Generate candidates using bull trend pullback strategy
        candidates = self.candidate_generator.generate_candidates(
            scan_results=scan_results,
            strategy_names=["bull_trend_pullback"],
            min_confidence=self.scanner_config.min_confidence_score,
            max_candidates=self.scanner_config.max_candidates
        )
        
        self.logger.info(f"✅ Generated {len(candidates)} bull trend pullback candidates")
        return candidates
    
    def get_scan_statistics(self) -> Dict[str, Any]:
        """Get statistics about current scanner configuration and performance"""
        return {
            'timestamp': datetime.now(),
            'configuration': {
                'min_volume': self.scanner_config.min_volume,
                'min_market_cap': self.scanner_config.min_market_cap,
                'min_price': self.scanner_config.min_price,
                'ema_periods': {
                    'short_term': self.scanner_config.ema_short_term,
                    'medium_term': self.scanner_config.ema_medium_term,
                    'long_term': self.scanner_config.ema_long_term
                },
                'pullback_threshold': f"{self.scanner_config.max_pullback_distance_pct}%",
                'min_confidence': f"{self.scanner_config.min_confidence_score}%"
            },
            'available_strategies': self.candidate_generator.get_available_strategies(),
            'criteria_count': len(self.criteria_registry._criteria)
        }
    
    def update_configuration(self, new_config: ScannerConfig):
        """Update scanner configuration and reload components"""
        self.scanner_config = new_config
        self._initialize_components()  # Re-initialize with new config
        self.logger.info("🔄 Scanner configuration updated and components reloaded")
    
    def _convert_to_scan_results(self, df: pd.DataFrame) -> List[ScanResult]:
        """Convert DataFrame to list of ScanResult objects"""
        scan_results = []
        for _, row in df.iterrows():
            # Handle both dictionary-style and object-style access
            ema_values = getattr(row, 'ema_values', {}) if hasattr(row, 'ema_values') else row.get('ema_values', {})
            
            scan_result = ScanResult(
                symbol=row['symbol'],
                total_score=row['total_score'],
                bull_trend_score=row['bull_trend_score'],
                bull_pullback_score=row['bull_pullback_score'],
                current_price=row['current_price'],
                volume_status=row['volume_status'],
                market_cap_status=row['market_cap_status'],
                price_status=row['price_status'],
                ema_values=ema_values,
                last_updated=getattr(row, 'last_updated', pd.Timestamp.now())
            )
            scan_results.append(scan_result)
        return scan_results