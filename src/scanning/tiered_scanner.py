# src/scanner/tiered_scanner.py
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

from src.scanning.scanner_core import StockScanner
from src.scanning.candidate_generator import CandidateGenerator
from src.scanning.criteria_setup import create_configurable_criteria_registry
from src.scanning.strategy.configurable_strategies import create_configurable_bull_trend_pullback_config
from src.scanning.strategy.bull_trend_pullback_strategy import BullTrendPullbackStrategy
from src.scanning.strategy.strategy_core import StrategyRegistry, StrategyOrchestrator
from config.scanner_config import ScannerConfig

class TieredScanner:
    """
    Tiered Scanner implementing 2-layer architecture:
    Tier 1: Basic screening (AND logic) - volume, market cap, price
    Tier 2: Strategy matching (OR logic) - any strategy can identify candidates
    Results include strategy identification for each candidate
    """
    
    def __init__(self, ibkr_data_adapter, scanner_config: Optional[ScannerConfig] = None):
        self.data_adapter = ibkr_data_adapter
        self.scanner_config = scanner_config or ScannerConfig()  # Default config
        self.logger = logging.getLogger(__name__)
        self._setup_tiered_architecture()
    
    def _setup_tiered_architecture(self):
        """Setup the 2-tier scanning architecture"""
        # Tier 1: Basic screening components
        self._setup_basic_screening()
        
        # Tier 2: Strategy matching with OR logic
        self._setup_strategy_orchestration()
        
        # Results processing
        self.candidate_generator = CandidateGenerator(self.strategy_orchestrator)
        
        self.logger.info(f"Tiered Scanner initialized with {len(self.scanner_config.enabled_strategies)} enabled strategies")
    
    def _setup_basic_screening(self):
        """Setup Tier 1: Basic screening criteria (AND logic)"""
        # Use existing criteria registry for basic screening
        self.criteria_registry = create_configurable_criteria_registry(self.scanner_config)
        self.scanner = StockScanner(self.data_adapter, self.scanner_config)
        self.logger.debug("Tier 1: Basic screening criteria configured")
    
    def _setup_strategy_orchestration(self):
        """Setup Tier 2: Strategy orchestration with OR logic - only enabled strategies"""
        self.strategy_registry = StrategyRegistry()
        
        # Only register enabled strategies
        enabled_strategies = []
        
        if 'bull_trend_pullback' in self.scanner_config.enabled_strategies:
            strategy_config = create_configurable_bull_trend_pullback_config(self.scanner_config)
            bull_strategy = BullTrendPullbackStrategy(strategy_config, self.criteria_registry)
            self.strategy_registry.register(bull_strategy)
            enabled_strategies.append(bull_strategy)
            self.logger.info(f"Registered enabled strategy: bull_trend_pullback")
        
        # Note: Additional strategies will be added here when implemented
        # if 'momentum_breakout' in self.scanner_config.enabled_strategies:
        #     momentum_config = create_momentum_breakout_config(self.scanner_config)
        #     momentum_strategy = MomentumBreakoutStrategy(momentum_config, self.criteria_registry)
        #     self.strategy_registry.register(momentum_strategy)
        #     enabled_strategies.append(momentum_strategy)
        #     self.logger.info(f"Registered enabled strategy: momentum_breakout")
        
        # Create StrategyOrchestrator only with enabled strategies
        self.strategy_orchestrator = StrategyOrchestrator(enabled_strategies)
        
        self.logger.info(f"Tier 2: {len(enabled_strategies)} strategies enabled: {[s.config.name for s in enabled_strategies]}")
    
    def update_config(self, new_config: ScannerConfig):
        """Update scanner configuration dynamically"""
        self.scanner_config = new_config
        self._setup_tiered_architecture()  # Re-initialize with new config
        self.logger.info(f"Tiered Scanner configuration updated: {new_config}")
    
    def run_scan(self) -> List[Dict[str, Any]]:
        """Run scanning process - returns Tier 1 results if no strategies enabled"""
        self.logger.info(f"🚀 Starting scan with {len(self.scanner_config.enabled_strategies)} enabled strategies")
        
        # Tier 1: Basic screening - get scan results
        scan_results = self.scanner.run_scan()
        self.logger.info(f"📊 Tier 1: {len(scan_results)} symbols passed basic screening")
        
        # If no strategies enabled, return Tier 1 results only
        if not self.scanner_config.enabled_strategies:
            self.logger.info("🔄 No strategies enabled - returning Tier 1 results only")
            return self._format_tier1_results(scan_results)
        
        # Tier 2: Strategy matching with OR logic
        candidates = self.candidate_generator.generate_candidates(
            scan_results=scan_results,
            min_confidence=self.scanner_config.min_confidence_score,
            max_candidates=self.scanner_config.max_candidates
        )
        
        self.logger.info(f"🎉 Scan Complete: {len(candidates)} candidates found ({len(self.scanner_config.enabled_strategies)} strategies)")
        return candidates
    
    def _format_tier1_results(self, scan_results: List[Any]) -> List[Dict[str, Any]]:
        """Return Tier 1 results when no strategies are enabled"""
        tier1_results = []
        for result in scan_results:
            tier1_results.append({
                'symbol': result.symbol,
                'identified_by': 'tier1_screening',
                'confidence': 100.0,  # All passed basic criteria
                'current_price': result.current_price,
                'volume': result.volume,
                'market_cap': result.market_cap,
                'total_score': 100.0,  # Perfect score for basic screening
                'ema_values': result.ema_values,
                'matching_strategies': ['basic_screening'],
                'scan_timestamp': datetime.now(),
                'criteria_details': {
                    'volume_ok': result.volume >= self.scanner_config.min_volume,
                    'market_cap_ok': result.market_cap >= self.scanner_config.min_market_cap,
                    'price_ok': result.current_price >= self.scanner_config.min_price
                },
                'metadata': {
                    'historical_data_points': len(result.historical_data),
                    'ema_calculated': len(result.ema_values) > 0
                }
            })
        
        self.logger.info(f"📋 Tier 1 only: {len(tier1_results)} candidates passed basic screening")
        return tier1_results
    
    def save_results_to_excel(self, candidates: List[Dict[str, Any]], 
                            output_dir: str = "scanner_results",
                            filename: str = None) -> str:
        """
        Save candidates to Excel file with timestamp
        Returns the full file path where Excel was saved
        """
        return self.candidate_generator.save_candidates_to_excel(candidates, output_dir, filename)
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get current configuration summary"""
        enabled_strategies = self.scanner_config.enabled_strategies or ['tier1_screening_only']
        
        return {
            'tiered_architecture': {
                'tier_1': 'Basic screening (AND logic)',
                'tier_2': f'Strategy matching (OR logic) - {len(enabled_strategies)} strategies enabled'
            },
            'enabled_strategies': enabled_strategies,
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
                'max_candidates': self.scanner_config.max_candidates,
                'strategy_logic': 'OR (symbols match if ANY strategy identifies them)'
            }
        }

# Legacy class for backward compatibility
class ConfigurableScannerOrchestrator:
    """LEGACY: Fully configurable scanner orchestrator - Replaced by TieredScanner"""
    
    def __init__(self, ibkr_data_adapter, scanner_config: Optional[ScannerConfig] = None):
        self.logger = logging.getLogger(__name__)
        self.logger.warning("ConfigurableScannerOrchestrator is deprecated. Use TieredScanner instead.")
        # Delegate to new TieredScanner for backward compatibility
        self.tiered_scanner = TieredScanner(ibkr_data_adapter, scanner_config)
    
    def update_config(self, new_config: ScannerConfig):
        """Update scanner configuration dynamically"""
        self.tiered_scanner.update_config(new_config)
    
    def run_scan(self) -> List[Dict[str, Any]]:
        """Run scanning process - returns Tier 1 results if no strategies enabled"""
        print(f"🎯 TieredScanner.run_scan() started with {len(self.scanner_config.enabled_strategies)} enabled strategies")
        
        # Tier 1: Basic screening - get scan results
        print("🔄 Running Tier 1: Basic screening...")
        scan_results = self.scanner.run_scan()
        print(f"📊 Tier 1: {len(scan_results)} symbols passed basic screening")
        
        # If no strategies enabled, return Tier 1 results only
        if not self.scanner_config.enabled_strategies:
            print("🔄 No strategies enabled - returning Tier 1 results only")
            return self._format_tier1_results(scan_results)
        
        # Tier 2: Strategy matching with OR logic
        print("🔄 Running Tier 2: Strategy matching...")
        candidates = self.candidate_generator.generate_candidates(
            scan_results=scan_results,
            min_confidence=self.scanner_config.min_confidence_score,
            max_candidates=self.scanner_config.max_candidates
        )
        
        print(f"🎉 Scan Complete: {len(candidates)} candidates found ({len(self.scanner_config.enabled_strategies)} strategies)")
        return candidates
        
    def get_config_summary(self) -> Dict[str, Any]:
        """Get current configuration summary"""
        return self.tiered_scanner.get_config_summary()