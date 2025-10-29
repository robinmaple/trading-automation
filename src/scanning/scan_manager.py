# src/scanner/scan_manager.py
from typing import List, Dict, Any, Optional
import logging
import pandas as pd
from datetime import datetime

# Tiered Architecture Integration - Begin
from src.scanning.tiered_scanner import TieredScanner
# Remove direct component imports since TieredScanner handles orchestration
# from src.scanning.scanner_core import StockScanner
# from .candidate_generator import CandidateGenerator
# from src.scanning.strategy.strategy_core import StrategyRegistry
# from src.scanning.strategy.bull_trend_pullback_strategy import BullTrendPullbackStrategy
# from src.scanning.strategy.configurable_strategies import create_configurable_bull_trend_pullback_config
# from .criteria_setup import create_configurable_criteria_registry
from config.scanner_config import ScannerConfig
# Tiered Architecture Integration - End

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)

class ScanManager:
    """
    Main entry point for running scanner operations
    Provides a clean, simple interface for generating candidate lists
    Now uses TieredScanner for 2-tier architecture with OR logic strategy matching
    """
    
    def __init__(self, ibkr_data_adapter, scanner_config: Optional[ScannerConfig] = None):
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing ScanManager",
            context_provider={
                "ibkr_data_adapter_provided": ibkr_data_adapter is not None,
                "scanner_config_provided": scanner_config is not None,
                "architecture": "2-Tier (Basic Screening + Strategy OR Logic)"
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self.data_adapter = ibkr_data_adapter
        self.scanner_config = scanner_config or ScannerConfig()
        self.logger = logging.getLogger(__name__)
        
        # Initialize TieredScanner for 2-tier architecture
        self._initialize_tiered_scanner()
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "ScanManager initialized with TieredScanner",
            context_provider={},
            decision_reason="ScanManager initialization completed"
        )
        # <Context-Aware Logging Integration - End>
    
    def _initialize_tiered_scanner(self):
        """Initialize TieredScanner for 2-tier architecture"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing TieredScanner",
            context_provider={
                "data_adapter_provided": self.data_adapter is not None,
                "scanner_config_provided": self.scanner_config is not None
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self.tiered_scanner = TieredScanner(self.data_adapter, self.scanner_config)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "TieredScanner architecture initialized",
            context_provider={
                "tier_1_description": "Basic screening (AND logic)",
                "tier_2_description": "Strategy matching (OR logic)",
                "tiered_scanner_type": type(self.tiered_scanner).__name__
            }
        )
        # <Context-Aware Logging Integration - End>
    
    def get_scan_statistics(self) -> Dict[str, Any]:
        """Get statistics about current scanner configuration and performance"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Retrieving scan statistics",
            context_provider={}
        )
        # <Context-Aware Logging Integration - End>
        
        config_summary = self.tiered_scanner.get_config_summary()
        
        statistics = {
            'timestamp': datetime.now(),
            'architecture': '2-Tier (Basic Screening + Strategy OR Logic)',
            'configuration': config_summary.get('fundamental_criteria', {}),
            'technical_configuration': config_summary.get('technical_criteria', {}),
            'scan_behavior': config_summary.get('scan_behavior', {}),
            'available_strategies': ['bull_trend_pullback']  # Will be dynamic when more strategies added
        }
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scan statistics retrieved",
            context_provider={
                "statistics_timestamp": statistics['timestamp'].isoformat(),
                "available_strategies_count": len(statistics['available_strategies']),
                "configuration_keys": list(statistics['configuration'].keys()) if statistics['configuration'] else [],
                "technical_configuration_keys": list(statistics['technical_configuration'].keys()) if statistics['technical_configuration'] else []
            },
            decision_reason="Statistics retrieval completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return statistics
    
    def update_configuration(self, new_config: ScannerConfig):
        """Update scanner configuration and reload components"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Updating scanner configuration",
            context_provider={
                "new_config_provided": new_config is not None,
                "new_config_type": type(new_config).__name__
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self.scanner_config = new_config
        self.tiered_scanner.update_config(new_config)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner configuration updated via TieredScanner",
            context_provider={},
            decision_reason="Configuration update completed"
        )
        # <Context-Aware Logging Integration - End>
        
    def generate_bull_trend_pullback_candidates(self, save_to_excel: bool = False, 
                                            excel_output_dir: str = "scanner_results") -> List[Dict[str, Any]]:
        """Generate candidates with option to save to Excel"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Generating Bull Trend Pullback Candidates",
            context_provider={
                "save_to_excel": save_to_excel,
                "excel_output_dir": excel_output_dir,
                "strategy": "bull_trend_pullback"
            }
        )
        # <Context-Aware Logging Integration - End>
        
        candidates = self.tiered_scanner.run_scan()
        bull_trend_candidates = [
            candidate for candidate in candidates 
            if candidate.get('identified_by') == 'bull_trend_pullback'
        ]
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Bull Trend Pullback candidate generation completed",
            context_provider={
                "total_candidates_found": len(candidates),
                "bull_trend_candidates_count": len(bull_trend_candidates),
                "other_strategy_candidates_count": len(candidates) - len(bull_trend_candidates),
                "save_to_excel_attempted": save_to_excel,
                "excel_output_dir": excel_output_dir
            },
            decision_reason="Strategy-specific candidate generation completed"
        )
        # <Context-Aware Logging Integration - End>
        
        # <Timestamped Excel Output - Begin>
        if save_to_excel and bull_trend_candidates:
            try:
                timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
                filename = f"scanner_bull_trend_{timestamp}.xlsx"
                filepath = f"{excel_output_dir}/{filename}"
                
                # Create DataFrame and save to Excel
                df = pd.DataFrame(bull_trend_candidates)
                df.to_excel(filepath, index=False)
                
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Bull Trend Pullback candidates saved to timestamped Excel file",
                    context_provider={
                        "filepath": filepath,
                        "candidates_count": len(bull_trend_candidates),
                        "timestamp_format": "YYMMDD_HHMMSS"
                    },
                    decision_reason="Excel file saved successfully"
                )
                # <Context-Aware Logging Integration - End>
                
                print(f"💾 Bull Trend Pullback results saved to: {filepath}")
                
            except Exception as e:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Failed to save Bull Trend Pullback candidates to Excel",
                    context_provider={
                        "error": str(e),
                        "candidates_count": len(bull_trend_candidates)
                    },
                    decision_reason="Excel save operation failed"
                )
                # <Context-Aware Logging Integration - End>
                print(f"❌ Failed to save Excel file: {e}")
        # <Timestamped Excel Output - End>
        
        return bull_trend_candidates

    def generate_all_candidates(self, save_to_excel: bool = False,
                            excel_output_dir: str = "scanner_results") -> List[Dict[str, Any]]:
        """Generate all candidates with option to save to Excel"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Generating All Candidates (OR Logic)",
            context_provider={
                "save_to_excel": save_to_excel,
                "excel_output_dir": excel_output_dir,
                "strategy": "all_candidates"
            }
        )
        # <Context-Aware Logging Integration - End>
        
        candidates = self.tiered_scanner.run_scan()
        
        # <Context-Aware Logging Integration - Begin>
        strategy_breakdown = {}
        for candidate in candidates:
            strategy = candidate.get('identified_by', 'unknown')
            strategy_breakdown[strategy] = strategy_breakdown.get(strategy, 0) + 1
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "All candidate generation completed",
            context_provider={
                "total_candidates_found": len(candidates),
                "strategy_breakdown": strategy_breakdown,
                "save_to_excel_attempted": save_to_excel,
                "excel_output_dir": excel_output_dir
            },
            decision_reason="All candidate generation completed"
        )
        # <Context-Aware Logging Integration - End>
        
        # <Timestamped Excel Output - Begin>
        if save_to_excel and candidates:
            try:
                timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
                filename = f"scanner_all_candidates_{timestamp}.xlsx"
                filepath = f"{excel_output_dir}/{filename}"
                
                # Create DataFrame and save to Excel
                df = pd.DataFrame(candidates)
                df.to_excel(filepath, index=False)
                
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "All candidates saved to timestamped Excel file",
                    context_provider={
                        "filepath": filepath,
                        "candidates_count": len(candidates),
                        "timestamp_format": "YYMMDD_HHMMSS"
                    },
                    decision_reason="Excel file saved successfully"
                )
                # <Context-Aware Logging Integration - End>
                
                print(f"💾 All scanner results saved to: {filepath}")
                
            except Exception as e:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Failed to save all candidates to Excel",
                    context_provider={
                        "error": str(e),
                        "candidates_count": len(candidates)
                    },
                    decision_reason="Excel save operation failed"
                )
                # <Context-Aware Logging Integration - End>
                print(f"❌ Failed to save Excel file: {e}")
        # <Timestamped Excel Output - End>
        
        return candidates