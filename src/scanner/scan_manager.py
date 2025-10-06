# src/scanner/scan_manager.py
from typing import List, Dict, Any, Optional
import logging
import pandas as pd
from datetime import datetime

# Tiered Architecture Integration - Begin
from src.scanner.tiered_scanner import TieredScanner
# Remove direct component imports since TieredScanner handles orchestration
# from .scanner_core import StockScanner
# from .candidate_generator import CandidateGenerator
# from .strategy.strategy_core import StrategyRegistry
# from .strategy.bull_trend_pullback_strategy import BullTrendPullbackStrategy
# from .strategy.configurable_strategies import create_configurable_bull_trend_pullback_config
# from .criteria_setup import create_configurable_criteria_registry
from config.scanner_config import ScannerConfig
# Tiered Architecture Integration - End

class ScanManager:
    """
    Main entry point for running scanner operations
    Provides a clean, simple interface for generating candidate lists
    Now uses TieredScanner for 2-tier architecture with OR logic strategy matching
    """
    
    def __init__(self, ibkr_data_adapter, scanner_config: Optional[ScannerConfig] = None):
        self.data_adapter = ibkr_data_adapter
        self.scanner_config = scanner_config or ScannerConfig()
        self.logger = logging.getLogger(__name__)
        
        # Initialize TieredScanner for 2-tier architecture
        self._initialize_tiered_scanner()
        
        self.logger.info("✅ ScanManager initialized with TieredScanner")
    
    def _initialize_tiered_scanner(self):
        """Initialize TieredScanner for 2-tier architecture"""
        self.tiered_scanner = TieredScanner(self.data_adapter, self.scanner_config)
        
        self.logger.debug("TieredScanner initialized with:")
        self.logger.debug(f"  - Tier 1: Basic screening (AND logic)")
        self.logger.debug(f"  - Tier 2: Strategy matching (OR logic)")
    
    def get_scan_statistics(self) -> Dict[str, Any]:
        """Get statistics about current scanner configuration and performance"""
        config_summary = self.tiered_scanner.get_config_summary()
        
        return {
            'timestamp': datetime.now(),
            'architecture': '2-Tier (Basic Screening + Strategy OR Logic)',
            'configuration': config_summary.get('fundamental_criteria', {}),
            'technical_configuration': config_summary.get('technical_criteria', {}),
            'scan_behavior': config_summary.get('scan_behavior', {}),
            'available_strategies': ['bull_trend_pullback']  # Will be dynamic when more strategies added
        }
    
    def update_configuration(self, new_config: ScannerConfig):
        """Update scanner configuration and reload components"""
        self.scanner_config = new_config
        self.tiered_scanner.update_config(new_config)
        self.logger.info("🔄 Scanner configuration updated via TieredScanner")
        
    def generate_bull_trend_pullback_candidates(self, save_to_excel: bool = False, 
                                            excel_output_dir: str = "scanner_results") -> List[Dict[str, Any]]:
        """Generate candidates with option to save to Excel"""
        self.logger.info("🎯 Generating Bull Trend Pullback Candidates...")
        
        candidates = self.tiered_scanner.run_scan()
        bull_trend_candidates = [
            candidate for candidate in candidates 
            if candidate.get('identified_by') == 'bull_trend_pullback'
        ]
        
        if save_to_excel and bull_trend_candidates:
            # Note: You'll need to access the candidate_generator from tiered_scanner
            # This might require exposing it or adding save method to TieredScanner
            pass
        
        return bull_trend_candidates

    def generate_all_candidates(self, save_to_excel: bool = False,
                            excel_output_dir: str = "scanner_results") -> List[Dict[str, Any]]:
        """Generate all candidates with option to save to Excel"""
        self.logger.info("🎯 Generating All Candidates (OR Logic)...")
        
        candidates = self.tiered_scanner.run_scan()
        
        if save_to_excel and candidates:
            # Note: Similar to above - need to handle Excel saving
            pass
        
        return candidates