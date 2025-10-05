# src/scanner/candidate_generator.py
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from config.scanner_config import ScanResult
from .strategy.strategy_core import StrategyRegistry

class CandidateGenerator:
    """
    Generates trading candidates by applying strategies to scan results
    This is the missing piece that connects scan results to trading signals
    """
    
    def __init__(self, strategy_registry: StrategyRegistry):
        self.strategy_registry = strategy_registry
        self.logger = logging.getLogger(__name__)
    
    def generate_candidates(self, 
                          scan_results: List[ScanResult],
                          strategy_names: List[str] = None,
                          min_confidence: int = 60,
                          max_candidates: int = 25) -> List[Dict[str, Any]]:
        """
        Generate candidates by applying specified strategies to scan results
        """
        if not scan_results:
            self.logger.warning("No scan results to process")
            return []
        
        strategy_names = strategy_names or ["bull_trend_pullback"]
        all_candidates = []
        
        self.logger.info(f"Applying {len(strategy_names)} strategies to {len(scan_results)} scan results")
        
        for strategy_name in strategy_names:
            strategy = self.strategy_registry.get_strategy(strategy_name)
            if not strategy:
                self.logger.warning(f"Strategy '{strategy_name}' not found in registry")
                continue
                
            self.logger.info(f"Evaluating {strategy_name} strategy...")
            strategy_candidates = 0
            
            for scan_result in scan_results:
                try:
                    signal = strategy.evaluate(scan_result)
                    if signal and signal.get('confidence', 0) >= min_confidence:
                        candidate = {
                            'symbol': scan_result.symbol,
                            'strategy': strategy_name,
                            'confidence': signal['confidence'],
                            'total_score': scan_result.total_score,
                            'trend_score': scan_result.bull_trend_score,
                            'pullback_score': scan_result.bull_pullback_score,
                            'current_price': scan_result.current_price,
                            'signal_details': signal,
                            'scan_timestamp': datetime.now(),
                            'volume_status': scan_result.volume_status,
                            'market_cap_status': scan_result.market_cap_status,
                            'price_status': scan_result.price_status
                        }
                        all_candidates.append(candidate)
                        strategy_candidates += 1
                        
                except Exception as e:
                    self.logger.error(f"Error evaluating {scan_result.symbol} with {strategy_name}: {e}")
                    continue
            
            self.logger.info(f"Strategy {strategy_name} found {strategy_candidates} candidates")
        
        # Sort by confidence and limit results
        all_candidates.sort(key=lambda x: x['confidence'], reverse=True)
        final_candidates = all_candidates[:max_candidates]
        
        self.logger.info(f"Generated {len(final_candidates)} total candidates (max: {max_candidates})")
        return final_candidates
    
    def get_available_strategies(self) -> List[str]:
        """Get list of available strategy names from registry"""
        return list(self.strategy_registry._strategies.keys())
    
    def generate_excel_output(self, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Format candidates for Excel output as specified in requirements
        """
        if not candidates:
            return {
                'columns': [],
                'data': [],
                'timestamp': datetime.now(),
                'candidate_count': 0
            }
        
        # Define Excel columns as per requirements
        columns = [
            'Symbol', 
            'Total Score', 
            'Bull Trend Score', 
            'Bull Pullback Score',
            'Current Price',
            'Volume Status',
            'Market Cap Status', 
            'Price Status',
            'Confidence',
            'Strategy',
            'Signal Type',
            'Risk Level'
        ]
        
        excel_data = []
        for candidate in candidates:
            signal_details = candidate.get('signal_details', {})
            row = [
                candidate['symbol'],
                candidate['total_score'],
                candidate['trend_score'], 
                candidate['pullback_score'],
                f"${candidate['current_price']:.2f}",
                candidate['volume_status'],
                candidate['market_cap_status'],
                candidate['price_status'],
                f"{candidate['confidence']:.1f}%",
                candidate['strategy'],
                signal_details.get('entry_signal', 'N/A'),
                signal_details.get('risk_level', 'N/A')
            ]
            excel_data.append(row)
        
        return {
            'columns': columns,
            'data': excel_data,
            'timestamp': datetime.now(),
            'candidate_count': len(candidates)
        }