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

# <Tiered Scanner Architecture - Begin>
class TieredScanner:
    """
    Tiered Scanner implementing 3-layer architecture:
    Tier 1: Basic screening (AND logic) - volume, market cap, price
    Tier 2: Strategy matching (OR logic) - any strategy can identify candidates
    Tier 3: Results with strategy highlighting - each candidate shows which strategy identified it
    """
    
    def __init__(self, ibkr_data_adapter, scanner_config: Optional[ScannerConfig] = None):
        self.data_adapter = ibkr_data_adapter
        self.scanner_config = scanner_config or ScannerConfig()  # Default config
        self.logger = logging.getLogger(__name__)
        self._setup_tiered_architecture()
    
    def _setup_tiered_architecture(self):
        """Setup the 3-tier scanning architecture"""
        # Tier 1: Basic screening components
        self._setup_basic_screening()
        
        # Tier 2: Strategy matching with OR logic
        self._setup_strategy_orchestration()
        
        # Tier 3: Results processing
        self.candidate_generator = CandidateGenerator(self.strategy_registry)
        
        self.logger.info(f"Tiered Scanner initialized with: {self.scanner_config}")
    
    def _setup_basic_screening(self):
        """Setup Tier 1: Basic screening criteria (AND logic)"""
        # Use existing criteria registry for basic screening
        self.criteria_registry = create_configurable_criteria_registry(self.scanner_config)
        self.logger.debug("Tier 1: Basic screening criteria configured")
    
    def _setup_strategy_orchestration(self):
        """Setup Tier 2: Strategy orchestration with OR logic"""
        self.strategy_registry = StrategyRegistry()
        
        # Register configurable bull trend pullback strategy
        strategy_config = create_configurable_bull_trend_pullback_config(self.scanner_config)
        self.strategy_registry.register(
            BullTrendPullbackStrategy(strategy_config, self.criteria_registry)
        )
        
        # Note: Additional strategies will be added here for OR logic
        self.logger.debug("Tier 2: Strategy orchestration configured with OR logic")
    
    def update_config(self, new_config: ScannerConfig):
        """Update scanner configuration dynamically"""
        self.scanner_config = new_config
        self._setup_tiered_architecture()  # Re-initialize with new config
        self.logger.info(f"Tiered Scanner configuration updated: {new_config}")
    
    def run_scan(self) -> List[Dict[str, Any]]:
        """Run 3-tier scanning process with OR logic for strategy matching"""
        self.logger.info(f"🚀 Starting 3-tier scan with config: {self.scanner_config}")
        
        # Tier 1: Basic screening - get initial symbol universe
        symbol_universe = self._get_symbol_universe()
        self.logger.info(f"📊 Tier 1: {len(symbol_universe)} symbols in initial universe")
        
        # Tier 2: Strategy matching with OR logic
        scan_results_df = self.scanner.run_scan()
        
        if scan_results_df.empty:
            self.logger.warning("No scan results generated from basic screening")
            return []
        
        # Convert and process results through strategy matching
        scan_results = self._process_scan_results(scan_results_df)
        
        # Apply OR logic: candidates match if ANY strategy identifies them
        candidates = self.candidate_generator.generate_candidates(
            scan_results=scan_results,
            strategy_names=["bull_trend_pullback"],  # Will be expanded for multiple strategies
            min_confidence=self.scanner_config.min_confidence_score,
            max_candidates=self.scanner_config.max_candidates
        )
        
        # Tier 3: Format results with strategy highlighting
        tiered_results = self._format_tiered_results(candidates)
        
        self.logger.info(f"🎉 3-Tier Scan Complete: {len(tiered_results)} candidates found with strategy highlighting")
        return tiered_results
    
    def _get_symbol_universe(self) -> List[str]:
        """Get initial symbol universe for Tier 1 screening"""
        # This will be enhanced to use actual basic screening
        # For now, use existing scanner to get initial symbols
        scanner = StockScanner(self.data_adapter, self.scanner_config)
        # Return placeholder - actual implementation will filter symbols
        return []  # To be implemented
    
    def _process_scan_results(self, scan_results_df) -> List[Dict[str, Any]]:
        """Process scan results for strategy matching (Tier 2)"""
        # Convert DataFrame to list of dictionaries for processing
        return scan_results_df.to_dict('records')
    
    def _format_tiered_results(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Tier 3: Format results with strategy highlighting"""
        formatted_results = []
        
        for candidate in candidates:
            # Enhance candidate with strategy highlighting
            enhanced_candidate = {
                **candidate,
                'identified_by': candidate.get('primary_strategy', 'unknown'),
                'strategy_confidence': candidate.get('confidence_score', 0),
                'matching_strategies': [candidate.get('primary_strategy', 'unknown')]  # Single strategy for now
            }
            formatted_results.append(enhanced_candidate)
        
        self.logger.debug(f"Tier 3: Formatted {len(formatted_results)} results with strategy highlighting")
        return formatted_results
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get current configuration summary"""
        return {
            'tiered_architecture': {
                'tier_1': 'Basic screening (AND logic)',
                'tier_2': 'Strategy matching (OR logic)', 
                'tier_3': 'Results with strategy highlighting'
            },
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
# <Tiered Scanner Architecture - End>

# <Legacy Class - Begin>
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
        """Run scan with current configuration"""
        return self.tiered_scanner.run_scan()
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get current configuration summary"""
        return self.tiered_scanner.get_config_summary()
# <Legacy Class - End>