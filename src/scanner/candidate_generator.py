# src/scanner/candidate_generator.py
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import os
import pandas as pd

# Strategy Orchestrator Integration - Begin
from .strategy.strategy_core import StrategyOrchestrator, StrategyMatch
from .scanner_core import ScanResult  # Updated import for new ScanResult format

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)

# Minimal safe logging import for fallback
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)


class CandidateGenerator:
    """
    Generates trading candidates using StrategyOrchestrator with OR logic
    Each candidate shows which strategy identified it
    """
    
    def __init__(self, strategy_orchestrator: StrategyOrchestrator):
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing CandidateGenerator",
            context_provider={
                "strategy_orchestrator_provided": strategy_orchestrator is not None,
                "strategy_orchestrator_type": type(strategy_orchestrator).__name__,
                "logic_type": "OR logic - symbols match if ANY strategy identifies them"
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self.strategy_orchestrator = strategy_orchestrator
        self.logger = logging.getLogger(__name__)
    
    def generate_candidates(self, 
                          scan_results: List[ScanResult],
                          min_confidence: int = 60,
                          max_candidates: int = 25) -> List[Dict[str, Any]]:
        """
        Generate candidates using OR logic - symbols match if ANY strategy identifies them
        Each candidate includes strategy identification
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting candidate generation with OR logic",
            context_provider={
                "scan_results_count": len(scan_results),
                "min_confidence": min_confidence,
                "max_candidates": max_candidates
            }
        )
        # <Context-Aware Logging Integration - End>
        
        if not scan_results:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No scan results to process",
                context_provider={},
                decision_reason="Candidate generation skipped - no input data"
            )
            # <Context-Aware Logging Integration - End>
            return []
        
        all_candidates = []
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Applying OR logic strategy matching to {len(scan_results)} scan results",
            context_provider={
                "scan_results_count": len(scan_results),
                "min_confidence_threshold": min_confidence
            }
        )
        # <Context-Aware Logging Integration - End>
        
        for scan_result in scan_results:
            try:
                # Use StrategyOrchestrator to evaluate with OR logic
                strategy_matches = self.strategy_orchestrator.evaluate_symbol(scan_result)
                
                if strategy_matches:
                    # Create candidate for each strategy match (symbol can match multiple strategies)
                    for strategy_match in strategy_matches:
                        if strategy_match.confidence >= min_confidence:
                            candidate = self._format_candidate(scan_result, strategy_match)
                            all_candidates.append(candidate)
                            
            except Exception as e:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Error evaluating {scan_result.symbol}",
                    symbol=scan_result.symbol,
                    context_provider={
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    decision_reason="Symbol evaluation failed"
                )
                # <Context-Aware Logging Integration - End>
                continue
        
        # Sort by confidence and limit results
        all_candidates.sort(key=lambda x: x['confidence'], reverse=True)
        final_candidates = all_candidates[:max_candidates]
        
        # Log strategy distribution
        self._log_strategy_distribution(final_candidates)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Candidate generation completed",
            context_provider={
                "total_candidates_generated": len(final_candidates),
                "max_candidates_limit": max_candidates,
                "original_candidates_count": len(all_candidates),
                "confidence_threshold_applied": min_confidence
            },
            decision_reason="Candidate generation process completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return final_candidates
    
    def _format_candidate(self, scan_result: ScanResult, strategy_match: StrategyMatch) -> Dict[str, Any]:
        """Format candidate with strategy identification"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Formatting candidate for {scan_result.symbol}",
            symbol=scan_result.symbol,
            context_provider={
                "strategy_name": strategy_match.strategy_name,
                "strategy_type": strategy_match.strategy_type.value,
                "confidence": strategy_match.confidence,
                "total_score": strategy_match.total_score
            }
        )
        # <Context-Aware Logging Integration - End>
        
        candidate = {
            'symbol': scan_result.symbol,
            'identified_by': strategy_match.strategy_name,
            'strategy_type': strategy_match.strategy_type.value,
            'confidence': strategy_match.confidence,
            'current_price': scan_result.current_price,
            'volume': scan_result.volume,
            'market_cap': scan_result.market_cap,
            'total_score': strategy_match.total_score,
            'base_criteria_score': strategy_match.base_criteria_score,
            'strategy_confidence': strategy_match.strategy_confidence,
            'ema_values': scan_result.ema_values,
            'matching_strategies': [strategy_match.strategy_name],  # Single strategy for now
            'scan_timestamp': datetime.now(),
            'metadata': strategy_match.metadata,
            'criteria_details': strategy_match.criteria_details
        }
        
        return candidate
    
    def _log_strategy_distribution(self, candidates: List[Dict[str, Any]]):
        """Log distribution of candidates by strategy"""
        strategy_counts = {}
        for candidate in candidates:
            strategy = candidate.get('identified_by', 'unknown')
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Candidate strategy distribution analysis",
            context_provider={
                "total_candidates": len(candidates),
                "strategy_distribution": strategy_counts,
                "unique_strategies_count": len(strategy_counts)
            },
            decision_reason="Strategy distribution analysis completed"
        )
        # <Context-Aware Logging Integration - End>
    
    def generate_candidates_by_strategy(self, 
                                      scan_results: List[ScanResult],
                                      strategy_names: List[str] = None,
                                      min_confidence: int = 60,
                                      max_candidates: int = 25) -> List[Dict[str, Any]]:
        """
        LEGACY METHOD: Remove this method as it's causing the error
        The StrategyOrchestrator doesn't have get_strategy method
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Deprecated method generate_candidates_by_strategy called",
            context_provider={
                "scan_results_count": len(scan_results),
                "strategy_names_requested": strategy_names,
                "min_confidence": min_confidence,
                "max_candidates": max_candidates
            },
            decision_reason="Falling back to generate_candidates method"
        )
        # <Context-Aware Logging Integration - End>
        
        return self.generate_candidates(scan_results, min_confidence, max_candidates)
    
    def get_available_strategies(self) -> List[str]:
        """Get list of available strategy names from orchestrator"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Retrieving available strategies",
            context_provider={}
        )
        # <Context-Aware Logging Integration - End>
        
        strategies = [strategy.config.name for strategy in self.strategy_orchestrator.strategies]
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Available strategies retrieved",
            context_provider={
                "available_strategies_count": len(strategies),
                "available_strategies": strategies
            },
            decision_reason="Strategy list retrieval completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return strategies
    
    def save_candidates_to_excel(self, candidates: List[Dict[str, Any]], 
                               output_dir: str = "scanner_results",
                               filename: str = None) -> str:
        """
        Save candidates to Excel file with timestamp
        Returns the full file path where Excel was saved
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting Excel export for candidates",
            context_provider={
                "candidates_count": len(candidates),
                "output_dir": output_dir,
                "filename_provided": filename is not None
            }
        )
        # <Context-Aware Logging Integration - End>
        
        if not candidates:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No candidates to save to Excel",
                context_provider={},
                decision_reason="Excel export skipped - no candidates"
            )
            # <Context-Aware Logging Integration - End>
            return ""
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            strategies = "_".join(sorted(set(c.get('identified_by', 'tier1') for c in candidates)))
            filename = f"scanner_candidates_{timestamp}_{strategies}.xlsx"
        
        filepath = os.path.join(output_dir, filename)
        
        try:
            # Generate Excel data structure
            excel_data = self.generate_excel_output(candidates)
            
            # Create DataFrame and save to Excel
            df = pd.DataFrame(excel_data['data'], columns=excel_data['columns'])
            df.to_excel(filepath, index=False, engine='openpyxl')
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Candidates successfully saved to Excel",
                context_provider={
                    "file_path": filepath,
                    "candidates_saved": len(candidates),
                    "excel_columns_count": len(excel_data['columns']),
                    "excel_rows_count": len(excel_data['data']),
                    "file_size_bytes": os.path.getsize(filepath) if os.path.exists(filepath) else 0
                },
                decision_reason="Excel export completed successfully"
            )
            # <Context-Aware Logging Integration - End>
            
            return filepath
            
        except Exception as e:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Failed to save Excel file",
                context_provider={
                    "file_path": filepath,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "candidates_count": len(candidates)
                },
                decision_reason="Excel export failed"
            )
            # <Context-Aware Logging Integration - End>
            return ""

    def generate_excel_output(self, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Format candidates for Excel output with strategy identification
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Generating Excel output structure",
            context_provider={
                "candidates_count": len(candidates)
            }
        )
        # <Context-Aware Logging Integration - End>
        
        if not candidates:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No candidates for Excel output generation",
                context_provider={},
                decision_reason="Excel output generation skipped"
            )
            # <Context-Aware Logging Integration - End>
            return {'columns': [], 'data': [], 'timestamp': datetime.now(), 'candidate_count': 0}
        
        # Enhanced columns with strategy info
        columns = [
            'Symbol', 
            'Strategy',
            'Confidence %',
            'Current Price',
            'Volume',
            'Market Cap',
            'Total Score', 
            'Base Criteria Score',
            'Strategy Confidence',
            'EMA Values',
            'Matching Strategies',
            'Scan Timestamp'
        ]
        
        excel_data = []
        for candidate in candidates:
            row = [
                candidate['symbol'],
                candidate.get('identified_by', 'tier1_screening'),
                f"{candidate['confidence']:.1f}%",
                f"${candidate['current_price']:.2f}",
                f"{candidate.get('volume', 0):,}",
                f"${candidate.get('market_cap', 0):,}",
                candidate.get('total_score', 'N/A'),
                candidate.get('base_criteria_score', 'N/A'),
                candidate.get('strategy_confidence', 'N/A'),
                str(candidate.get('ema_values', {})),
                ', '.join(candidate.get('matching_strategies', ['basic_screening'])),
                candidate.get('scan_timestamp', datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
            ]
            excel_data.append(row)
        
        result = {
            'columns': columns,
            'data': excel_data,
            'timestamp': datetime.now(),
            'candidate_count': len(candidates)
        }
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Excel output structure generated",
            context_provider={
                "excel_columns_count": len(columns),
                "excel_rows_count": len(excel_data),
                "candidate_count": len(candidates)
            },
            decision_reason="Excel output generation completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return result