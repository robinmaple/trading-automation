# src/scanner/configurable_orchestrator.py
from typing import List, Dict, Any, Optional
import logging

from src.scanner.scanner_core import StockScanner
from src.scanner.candidate_generator import CandidateGenerator
from src.scanner.criteria_setup import create_configurable_criteria_registry, get_bull_trend_pullback_criteria_list
from src.scanner.strategy.configurable_strategies import create_configurable_bull_trend_pullback_config
from src.scanner.strategy.bull_trend_pullback_strategy import BullTrendPullbackStrategy
from src.scanner.strategy.strategy_core import StrategyRegistry
from config.scanner_config import ScannerConfig

class ConfigurableScannerOrchestrator:
    """Fully configurable scanner orchestrator"""
    
    def __init__(self, ibkr_data_adapter, scanner_config: Optional[ScannerConfig] = None):
        self.data_adapter = ibkr_data_adapter
        self.scanner_config = scanner_config or ScannerConfig()  # Default config
        self.logger = logging.getLogger(__name__)
        self._setup()
    
    def _setup(self):
        """Setup with configurable parameters"""
        # 1. Create configurable criteria registry
        self.criteria_registry = create_configurable_criteria_registry(self.scanner_config)
        
        # 2. Create strategy registry
        self.strategy_registry = StrategyRegistry()
        
        # 3. Register configurable bull trend pullback strategy
        strategy_config = create_configurable_bull_trend_pullback_config(self.scanner_config)
        self.strategy_registry.register(
            BullTrendPullbackStrategy(strategy_config, self.criteria_registry)
        )
        
        # 4. Create candidate generator
        self.candidate_generator = CandidateGenerator(self.strategy_registry)
        
        # 5. Create scanner with configurable parameters
        self.scanner = StockScanner(self.data_adapter, self.scanner_config)
        
        self.logger.info(f"Configurable Scanner initialized with: {self.scanner_config}")
    
    def update_config(self, new_config: ScannerConfig):
        """Update scanner configuration dynamically"""
        self.scanner_config = new_config
        self._setup()  # Re-initialize with new config
        self.logger.info(f"Scanner configuration updated: {new_config}")
    
    def run_scan(self) -> List[Dict[str, Any]]:
        """Run scan with current configuration"""
        self.logger.info(f"🚀 Starting scan with config: {self.scanner_config}")
        
        # Run scanner and generate candidates
        scan_results_df = self.scanner.run_scan()
        
        if scan_results_df.empty:
            self.logger.warning("No scan results generated")
            return []
        
        # Convert and process results
        scan_results = self._process_scan_results(scan_results_df)
        
        candidates = self.candidate_generator.generate_candidates(
            scan_results=scan_results,
            strategy_names=["bull_trend_pullback"],
            min_confidence=self.scanner_config.min_confidence_score,
            max_candidates=self.scanner_config.max_candidates
        )
        
        self.logger.info(f"✅ Scan Complete: {len(candidates)} candidates found")
        return candidates
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get current configuration summary"""
        return {
            'fundamental_criteria': {
                'min_volume': self.scanner_config.min_volume,
                'min_market_cap': self.scanner_config.min_market_cap,
                'min_price': self.scanner_config.min_price,
                'max_price': self.scanner_config.max_price
            },
            'technical_criteria': {
                'ema_periods': {
                    'short_term': self.scanner_config.ema_short_term,
                    'medium_term': self.scanner_config.ema_medium_term,
                    'long_term': self.scanner_config.ema_long_term
                },
                'pullback_parameters': {
                    'max_distance_pct': self.scanner_config.max_pullback_distance_pct,
                    'ideal_range_pct': self.scanner_config.ideal_pullback_range_pct
                }
            },
            'scan_behavior': {
                'max_symbols': self.scanner_config.max_symbols_to_scan,
                'min_confidence': self.scanner_config.min_confidence_score,
                'max_candidates': self.scanner_config.max_candidates
            }
        }