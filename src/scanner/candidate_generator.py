# src/scanner/candidate_generator.py
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import os
import pandas as pd

# Strategy Orchestrator Integration - Begin
from .strategy.strategy_core import StrategyOrchestrator, StrategyMatch
from .scanner_core import ScanResult  # Updated import for new ScanResult format

class CandidateGenerator:
    """
    Generates trading candidates using StrategyOrchestrator with OR logic
    Each candidate shows which strategy identified it
    """
    
    def __init__(self, strategy_orchestrator: StrategyOrchestrator):
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
        if not scan_results:
            self.logger.warning("No scan results to process")
            return []
        
        all_candidates = []
        
        self.logger.info(f"Applying OR logic strategy matching to {len(scan_results)} scan results")
        
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
                self.logger.error(f"Error evaluating {scan_result.symbol}: {e}")
                continue
        
        # Sort by confidence and limit results
        all_candidates.sort(key=lambda x: x['confidence'], reverse=True)
        final_candidates = all_candidates[:max_candidates]
        
        # Log strategy distribution
        self._log_strategy_distribution(final_candidates)
        
        self.logger.info(f"Generated {len(final_candidates)} total candidates (max: {max_candidates})")
        return final_candidates
    
    def _format_candidate(self, scan_result: ScanResult, strategy_match: StrategyMatch) -> Dict[str, Any]:
        """Format candidate with strategy identification"""
        return {
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
    
    def _log_strategy_distribution(self, candidates: List[Dict[str, Any]]):
        """Log distribution of candidates by strategy"""
        strategy_counts = {}
        for candidate in candidates:
            strategy = candidate.get('identified_by', 'unknown')
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
        
        for strategy, count in strategy_counts.items():
            self.logger.info(f"  - {strategy}: {count} candidates")
    
    def generate_candidates_by_strategy(self, 
                                      scan_results: List[ScanResult],
                                      strategy_names: List[str] = None,
                                      min_confidence: int = 60,
                                      max_candidates: int = 25) -> List[Dict[str, Any]]:
        """
        LEGACY METHOD: Remove this method as it's causing the error
        The StrategyOrchestrator doesn't have get_strategy method
        """
        self.logger.warning("generate_candidates_by_strategy is deprecated - use generate_candidates instead")
        return self.generate_candidates(scan_results, min_confidence, max_candidates)
    
    def get_available_strategies(self) -> List[str]:
        """Get list of available strategy names from orchestrator"""
        return [strategy.config.name for strategy in self.strategy_orchestrator.strategies]
    
    def save_candidates_to_excel(self, candidates: List[Dict[str, Any]], 
                               output_dir: str = "scanner_results",
                               filename: str = None) -> str:
        """
        Save candidates to Excel file with timestamp
        Returns the full file path where Excel was saved
        """
        if not candidates:
            self.logger.warning("No candidates to save to Excel")
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
            
            self.logger.info(f"💾 Saved {len(candidates)} candidates to: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Failed to save Excel file: {e}")
            return ""

    def generate_excel_output(self, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Format candidates for Excel output with strategy identification
        """
        if not candidates:
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
        
        return {
            'columns': columns,
            'data': excel_data,
            'timestamp': datetime.now(),
            'candidate_count': len(candidates)
        }