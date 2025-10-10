# src/scanner/strategy/strategy_core.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

# Add criteria imports - FIXED IMPORT
from src.scanner.criteria.criteria_core import CriteriaRegistry, CriteriaType

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)

# Minimal safe logging import for fallback
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)

# Strategy Matching System - Begin
@dataclass
class StrategyMatch:
    """Represents a strategy match with detailed information"""
    symbol: str
    strategy_name: str
    strategy_type: 'StrategyType'
    confidence: float
    current_price: float
    total_score: float
    metadata: Dict[str, Any]
    criteria_details: Dict[str, Any] = field(default_factory=dict)
    base_criteria_score: float = 0.0
    strategy_confidence: float = 0.0

class StrategyOrchestrator:
    """Coordinates multiple strategies with OR logic"""
    
    def __init__(self, strategies: List['Strategy']):
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing StrategyOrchestrator",
            context_provider={
                "strategies_count": len(strategies),
                "strategy_names": [strategy.config.name for strategy in strategies],
                "logic_type": "OR logic - symbols match if ANY strategy identifies them"
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self.strategies = strategies
        self.logger = logging.getLogger(__name__)
    
    def evaluate_symbol(self, scan_result) -> List[StrategyMatch]:
        """
        Evaluate symbol against all strategies using OR logic
        Returns all strategy matches (empty list if no matches)
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Evaluating symbol {scan_result.symbol} against all strategies",
            symbol=scan_result.symbol,
            context_provider={
                "strategies_count": len(self.strategies),
                "current_price": scan_result.current_price,
                "symbol": scan_result.symbol
            }
        )
        # <Context-Aware Logging Integration - End>
        
        matches = []
        
        for strategy in self.strategies:
            try:
                match = strategy.evaluate_with_details(scan_result)
                if match:
                    matches.append(match)
                    # <Context-Aware Logging Integration - Begin>
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Strategy {strategy.config.name} matched symbol {scan_result.symbol}",
                        symbol=scan_result.symbol,
                        context_provider={
                            "strategy_name": strategy.config.name,
                            "confidence": match.confidence,
                            "base_criteria_score": match.base_criteria_score,
                            "strategy_confidence": match.strategy_confidence
                        }
                    )
                    # <Context-Aware Logging Integration - End>
                    
            except Exception as e:
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Strategy {strategy.config.name} failed for {scan_result.symbol}",
                    symbol=scan_result.symbol,
                    context_provider={
                        "strategy_name": strategy.config.name,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    decision_reason="Strategy evaluation failed"
                )
                # <Context-Aware Logging Integration - End>
                continue
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Strategy evaluation completed for {scan_result.symbol}",
            symbol=scan_result.symbol,
            context_provider={
                "total_matches_found": len(matches),
                "matching_strategies": [match.strategy_name for match in matches],
                "strategies_evaluated": len(self.strategies)
            },
            decision_reason="Multi-strategy evaluation completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return matches
    
    def get_matching_strategies(self, scan_result) -> List[str]:
        """Get list of strategy names that match this symbol"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Getting matching strategies for {scan_result.symbol}",
            symbol=scan_result.symbol,
            context_provider={}
        )
        # <Context-Aware Logging Integration - End>
        
        matches = self.evaluate_symbol(scan_result)
        matching_strategies = [match.strategy_name for match in matches]
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Matching strategies retrieved for {scan_result.symbol}",
            symbol=scan_result.symbol,
            context_provider={
                "matching_strategies_count": len(matching_strategies),
                "matching_strategies": matching_strategies
            },
            decision_reason="Matching strategies retrieval completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return matching_strategies
# Strategy Matching System - End

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
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Initializing Strategy: {config.name}",
            context_provider={
                "strategy_name": config.name,
                "strategy_type": config.strategy_type.value,
                "required_criteria_count": len(config.required_criteria),
                "additional_criteria_count": len(config.additional_criteria),
                "criteria_registry_provided": criteria_registry is not None
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self.config = config
        self.criteria_registry = criteria_registry
        self.logger = logging.getLogger(__name__)
    
    def evaluate_base_criteria(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate all base criteria that every strategy must pass
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Evaluating base criteria for {stock_data.get('symbol', 'unknown')}",
            symbol=stock_data.get('symbol'),
            context_provider={
                "strategy_name": self.config.name,
                "required_criteria": self.config.required_criteria,
                "additional_criteria": self.config.additional_criteria
            }
        )
        # <Context-Aware Logging Integration - End>
        
        all_criteria = self.config.required_criteria + self.config.additional_criteria
        criteria_result = self.criteria_registry.evaluate_all(stock_data, all_criteria)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Base criteria evaluation completed for {stock_data.get('symbol', 'unknown')}",
            symbol=stock_data.get('symbol'),
            context_provider={
                "strategy_name": self.config.name,
                "meets_requirements": criteria_result['meets_requirements'],
                "overall_score": criteria_result['overall_score'],
                "passed_criteria_count": len(criteria_result['passed_criteria']),
                "failed_criteria_count": len(criteria_result['failed_criteria'])
            },
            decision_reason="Base criteria evaluation completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return criteria_result
    
    def passes_base_requirements(self, stock_data: Dict[str, Any]) -> bool:
        """Quick check if stock passes all base criteria"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Quick base requirements check for {stock_data.get('symbol', 'unknown')}",
            symbol=stock_data.get('symbol'),
            context_provider={
                "strategy_name": self.config.name
            }
        )
        # <Context-Aware Logging Integration - End>
        
        criteria_result = self.evaluate_base_criteria(stock_data)
        passes = criteria_result['meets_requirements']
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Base requirements check result for {stock_data.get('symbol', 'unknown')}: {passes}",
            symbol=stock_data.get('symbol'),
            context_provider={
                "strategy_name": self.config.name,
                "passes_base_requirements": passes
            },
            decision_reason="Base requirements check completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return passes
    
    @abstractmethod
    def evaluate_strategy_specific(self, scan_result, stock_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Evaluate strategy-specific conditions (to be implemented by subclasses)"""
        pass
    
    @abstractmethod
    def calculate_strategy_confidence(self, scan_result, stock_data: Dict[str, Any]) -> float:
        """Calculate strategy-specific confidence (to be implemented by subclasses)"""
        pass
    
    # Enhanced Evaluation Methods - Begin
    def evaluate_with_details(self, scan_result) -> Optional[StrategyMatch]:
        """Complete evaluation returning detailed StrategyMatch object"""
        symbol = scan_result.symbol
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Starting detailed evaluation for {symbol}",
            symbol=symbol,
            context_provider={
                "strategy_name": self.config.name,
                "current_price": scan_result.current_price
            }
        )
        # <Context-Aware Logging Integration - End>
        
        # Convert scan_result to stock_data format for criteria
        stock_data = self._scan_result_to_stock_data(scan_result)
        
        # First, check base criteria
        if not self.passes_base_requirements(stock_data):
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Symbol {symbol} failed base requirements",
                symbol=symbol,
                context_provider={
                    "strategy_name": self.config.name
                },
                decision_reason="Strategy evaluation failed - base requirements not met"
            )
            # <Context-Aware Logging Integration - End>
            return None
        
        # Then, evaluate strategy-specific conditions
        strategy_result = self.evaluate_strategy_specific(scan_result, stock_data)
        if not strategy_result:
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Symbol {symbol} failed strategy-specific conditions",
                symbol=symbol,
                context_provider={
                    "strategy_name": self.config.name
                },
                decision_reason="Strategy evaluation failed - strategy conditions not met"
            )
            # <Context-Aware Logging Integration - End>
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
            match = StrategyMatch(
                symbol=scan_result.symbol,
                strategy_name=self.config.name,
                strategy_type=self.config.strategy_type,
                confidence=overall_confidence,
                current_price=scan_result.current_price,
                total_score=scan_result.total_score,
                metadata=self._generate_metadata(scan_result, stock_data),
                criteria_details=base_criteria_result,
                base_criteria_score=base_criteria_result['overall_score'],
                strategy_confidence=strategy_confidence
            )
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Strategy match found for {symbol}",
                symbol=symbol,
                context_provider={
                    "strategy_name": self.config.name,
                    "overall_confidence": overall_confidence,
                    "base_criteria_score": base_criteria_result['overall_score'],
                    "strategy_confidence": strategy_confidence,
                    "minimum_threshold": 50
                },
                decision_reason="Strategy evaluation successful - match found"
            )
            # <Context-Aware Logging Integration - End>
            
            return match
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Symbol {symbol} below confidence threshold",
            symbol=symbol,
            context_provider={
                "strategy_name": self.config.name,
                "overall_confidence": overall_confidence,
                "minimum_threshold": 50
            },
            decision_reason="Strategy evaluation failed - confidence below threshold"
        )
        # <Context-Aware Logging Integration - End>
        
        return None
    
    def evaluate(self, scan_result) -> Optional[Dict[str, Any]]:
        """Legacy evaluate method - now uses new detailed system"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Using legacy evaluate method for {scan_result.symbol}",
            symbol=scan_result.symbol,
            context_provider={
                "strategy_name": self.config.name
            }
        )
        # <Context-Aware Logging Integration - End>
        
        match = self.evaluate_with_details(scan_result)
        if match:
            result = {
                'symbol': match.symbol,
                'strategy': match.strategy_name,
                'strategy_type': match.strategy_type.value,
                'confidence': match.confidence,
                'current_price': match.current_price,
                'total_score': match.total_score,
                'trend_score': getattr(scan_result, 'bull_trend_score', 0),
                'pullback_score': getattr(scan_result, 'bull_pullback_score', 0),
                'timestamp': getattr(scan_result, 'last_updated', None),
                'metadata': match.metadata,
                'base_criteria_score': match.base_criteria_score,
                'strategy_confidence': match.strategy_confidence,
                'criteria_details': match.criteria_details
            }
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Legacy evaluation successful for {scan_result.symbol}",
                symbol=scan_result.symbol,
                context_provider={
                    "strategy_name": self.config.name,
                    "confidence": match.confidence
                },
                decision_reason="Legacy evaluation completed successfully"
            )
            # <Context-Aware Logging Integration - End>
            
            return result
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Legacy evaluation failed for {scan_result.symbol}",
            symbol=scan_result.symbol,
            context_provider={
                "strategy_name": self.config.name
            },
            decision_reason="Legacy evaluation failed - no match"
        )
        # <Context-Aware Logging Integration - End>
        
        return None
    # Enhanced Evaluation Methods - End
    
    def _scan_result_to_stock_data(self, scan_result) -> Dict[str, Any]:
        """Convert scan result to stock data format for criteria"""
        stock_data = {
            'symbol': scan_result.symbol,
            'price': scan_result.current_price,
            'volume': getattr(scan_result, 'volume', 1_500_000),  # Mock for now
            'market_cap': getattr(scan_result, 'market_cap', 15_000_000_000),  # Mock
            'exchange': 'NASDAQ',  # Mock - you'd get this from your data
            # Add other required fields as needed
        }
        return stock_data
    
    def _combine_confidence(self, base_score: float, strategy_score: float) -> float:
        """Combine base criteria score with strategy confidence"""
        base_weight = self.config.weights.get('base_criteria', 0.3)
        strategy_weight = self.config.weights.get('strategy', 0.7)
        
        combined_confidence = (base_score * base_weight) + (strategy_score * strategy_weight)
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Combining confidence scores",
            context_provider={
                "strategy_name": self.config.name,
                "base_score": base_score,
                "strategy_score": strategy_score,
                "base_weight": base_weight,
                "strategy_weight": strategy_weight,
                "combined_confidence": combined_confidence
            }
        )
        # <Context-Aware Logging Integration - End>
        
        return combined_confidence
    
    def generate_signal(self, scan_result, stock_data: Dict[str, Any], confidence: float) -> Dict[str, Any]:
        """Generate trading signal for qualified candidates"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Generating trading signal for {scan_result.symbol}",
            symbol=scan_result.symbol,
            context_provider={
                "strategy_name": self.config.name,
                "confidence": confidence
            }
        )
        # <Context-Aware Logging Integration - End>
        
        signal = {
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
        
        return signal
    
    def _generate_metadata(self, scan_result, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate strategy-specific metadata"""
        metadata = {
            'ema_values': scan_result.ema_values,
            'base_criteria_passed': self.evaluate_base_criteria(stock_data)['passed_criteria'],
            'setup_quality': self._assess_setup_quality(scan_result)
        }
        return metadata
    
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

class StrategyRegistry:
    """Registry to manage all available strategies"""
    
    def __init__(self):
        # <Context-Aware Logging Integration - Begin>
        self.context_logger = get_context_logger()
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing StrategyRegistry",
            context_provider={}
        )
        # <Context-Aware Logging Integration - End>
        
        self._strategies: Dict[str, Strategy] = {}
    
    def register(self, strategy: Strategy):
        """Register a strategy"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Registering strategy: {strategy.config.name}",
            context_provider={
                "strategy_name": strategy.config.name,
                "strategy_type": strategy.config.strategy_type.value,
                "total_strategies_registered": len(self._strategies) + 1
            }
        )
        # <Context-Aware Logging Integration - End>
        
        self._strategies[strategy.config.name] = strategy
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Strategy registered: {strategy.config.name}",
            context_provider={
                "strategy_name": strategy.config.name,
                "current_registry_size": len(self._strategies)
            },
            decision_reason="Strategy registration completed"
        )
        # <Context-Aware Logging Integration - End>
    
    def get_strategy(self, name: str) -> Optional[Strategy]:
        """Get strategy by name"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Retrieving strategy: {name}",
            context_provider={
                "requested_strategy": name,
                "available_strategies": list(self._strategies.keys())
            }
        )
        # <Context-Aware Logging Integration - End>
        
        strategy = self._strategies.get(name)
        
        # <Context-Aware Logging Integration - Begin>
        if strategy:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Strategy found: {name}",
                context_provider={
                    "strategy_name": name,
                    "strategy_type": strategy.config.strategy_type.value
                },
                decision_reason="Strategy retrieval successful"
            )
        else:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Strategy not found: {name}",
                context_provider={
                    "requested_strategy": name,
                    "available_strategies": list(self._strategies.keys())
                },
                decision_reason="Strategy retrieval failed - not found"
            )
        # <Context-Aware Logging Integration - End>
        
        return strategy
    
    def list_strategies(self) -> List[str]:
        """List all available strategy names"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Listing all available strategies",
            context_provider={}
        )
        # <Context-Aware Logging Integration - End>
        
        strategies = list(self._strategies.keys())
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Strategy list retrieved",
            context_provider={
                "total_strategies": len(strategies),
                "available_strategies": strategies
            },
            decision_reason="Strategy listing completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return strategies
    
    def get_strategies_by_type(self, strategy_type: StrategyType) -> List[Strategy]:
        """Get all strategies of a specific type"""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Getting strategies by type: {strategy_type.value}",
            context_provider={
                "requested_type": strategy_type.value
            }
        )
        # <Context-Aware Logging Integration - End>
        
        strategies = [s for s in self._strategies.values() 
                if s.config.strategy_type == strategy_type]
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Strategies by type retrieved: {strategy_type.value}",
            context_provider={
                "requested_type": strategy_type.value,
                "strategies_found": len(strategies),
                "strategy_names": [s.config.name for s in strategies]
            },
            decision_reason="Strategy type filtering completed"
        )
        # <Context-Aware Logging Integration - End>
        
        return strategies