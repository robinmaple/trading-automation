# src/scanner/strategy/strategy_core.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

# Add criteria imports - FIXED IMPORT
from src.scanner.criteria.criteria_core import CriteriaRegistry, CriteriaType

class StrategyType(Enum):
    BULL_TREND = "bull_trend"
    BULL_PULLBACK = "bull_pullback" 
    MOMENTUM_BREAKOUT = "momentum_breakout"
    MEAN_REVERSION = "mean_reversion"
    BREAKOUT = "breakout"
    CUSTOM = "custom"

@dataclass
class StrategyConfig:
    """Flexible configuration for any strategy"""
    name: str
    strategy_type: StrategyType
    # Base criteria that ALL strategies must pass
    required_criteria: List[str] = field(default_factory=lambda: [
        'min_volume', 'min_price', 'min_market_cap'
    ])
    # Strategy-specific criteria
    additional_criteria: List[str] = field(default_factory=list)
    # Scoring weights
    weights: Dict[str, float] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)

class Strategy(ABC):
    """Base strategy class with built-in criteria evaluation"""
    
    def __init__(self, config: StrategyConfig, criteria_registry: CriteriaRegistry):
        self.config = config
        self.criteria_registry = criteria_registry
        self.logger = logging.getLogger(__name__)
    
    def evaluate_base_criteria(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate all base criteria that every strategy must pass
        """
        all_criteria = self.config.required_criteria + self.config.additional_criteria
        return self.criteria_registry.evaluate_all(stock_data, all_criteria)
    
    def passes_base_requirements(self, stock_data: Dict[str, Any]) -> bool:
        """Quick check if stock passes all base criteria"""
        criteria_result = self.evaluate_base_criteria(stock_data)
        return criteria_result['meets_requirements']
    
    @abstractmethod
    def evaluate_strategy_specific(self, scan_result, stock_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Evaluate strategy-specific conditions (to be implemented by subclasses)"""
        pass
    
    @abstractmethod
    def calculate_strategy_confidence(self, scan_result, stock_data: Dict[str, Any]) -> float:
        """Calculate strategy-specific confidence (to be implemented by subclasses)"""
        pass
    
    def evaluate(self, scan_result) -> Optional[Dict[str, Any]]:
        """Complete evaluation: base criteria + strategy-specific"""
        # Convert scan_result to stock_data format for criteria
        stock_data = self._scan_result_to_stock_data(scan_result)
        
        # First, check base criteria
        if not self.passes_base_requirements(stock_data):
            return None
        
        # Then, evaluate strategy-specific conditions
        strategy_result = self.evaluate_strategy_specific(scan_result, stock_data)
        if not strategy_result:
            return None
        
        # Calculate overall confidence
        base_criteria_result = self.evaluate_base_criteria(stock_data)
        strategy_confidence = self.calculate_strategy_confidence(scan_result, stock_data)
        
        # Combine base criteria score with strategy confidence
        overall_confidence = self._combine_confidence(
            base_criteria_result['overall_score'], 
            strategy_confidence
        )
        
        if overall_confidence >= 50:  # Minimum threshold
            signal = self.generate_signal(scan_result, stock_data, overall_confidence)
            signal.update({
                'base_criteria_score': base_criteria_result['overall_score'],
                'strategy_confidence': strategy_confidence,
                'criteria_details': base_criteria_result
            })
            return signal
        return None
    
    def _scan_result_to_stock_data(self, scan_result) -> Dict[str, Any]:
        """Convert scan result to stock data format for criteria"""
        return {
            'symbol': scan_result.symbol,
            'price': scan_result.current_price,
            'volume': getattr(scan_result, 'volume', 1_500_000),  # Mock for now
            'market_cap': getattr(scan_result, 'market_cap', 15_000_000_000),  # Mock
            'exchange': 'NASDAQ',  # Mock - you'd get this from your data
            # Add other required fields as needed
        }
    
    def _combine_confidence(self, base_score: float, strategy_score: float) -> float:
        """Combine base criteria score with strategy confidence"""
        base_weight = self.config.weights.get('base_criteria', 0.3)
        strategy_weight = self.config.weights.get('strategy', 0.7)
        
        return (base_score * base_weight) + (strategy_score * strategy_weight)
    
    def generate_signal(self, scan_result, stock_data: Dict[str, Any], confidence: float) -> Dict[str, Any]:
        """Generate trading signal for qualified candidates"""
        return {
            'symbol': scan_result.symbol,
            'strategy': self.config.name,
            'strategy_type': self.config.strategy_type.value,
            'confidence': confidence,
            'current_price': scan_result.current_price,
            'total_score': scan_result.total_score,
            'trend_score': scan_result.bull_trend_score,
            'pullback_score': scan_result.bull_pullback_score,
            'timestamp': scan_result.last_updated,
            'metadata': self._generate_metadata(scan_result, stock_data)
        }
    
    def _generate_metadata(self, scan_result, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate strategy-specific metadata"""
        return {
            'ema_values': scan_result.ema_values,
            'base_criteria_passed': self.evaluate_base_criteria(stock_data)['passed_criteria'],
            'setup_quality': self._assess_setup_quality(scan_result)
        }
    
    def _assess_setup_quality(self, scan_result) -> str:
        """Assess overall setup quality"""
        # This can now consider both base criteria and strategy strength
        if scan_result.total_score >= 85:
            return 'EXCELLENT'
        elif scan_result.total_score >= 70:
            return 'GOOD'
        elif scan_result.total_score >= 60:
            return 'FAIR'
        else:
            return 'POOR'

# ADD THE MISSING CLASS - COPY THIS EXACTLY
class StrategyRegistry:
    """Registry to manage all available strategies"""
    
    def __init__(self):
        self._strategies: Dict[str, Strategy] = {}
    
    def register(self, strategy: Strategy):
        """Register a strategy"""
        self._strategies[strategy.config.name] = strategy
        logging.info(f"Registered strategy: {strategy.config.name}")
    
    def get_strategy(self, name: str) -> Optional[Strategy]:
        """Get strategy by name"""
        return self._strategies.get(name)
    
    def list_strategies(self) -> List[str]:
        """List all available strategy names"""
        return list(self._strategies.keys())
    
    def get_strategies_by_type(self, strategy_type: StrategyType) -> List[Strategy]:
        """Get all strategies of a specific type"""
        return [s for s in self._strategies.values() 
                if s.config.strategy_type == strategy_type]