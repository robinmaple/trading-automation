# src/scanner/criteria/criteria_core.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

class CriteriaType(Enum):
    FUNDAMENTAL = "fundamental"
    TECHNICAL = "technical" 
    LIQUIDITY = "liquidity"
    VOLATILITY = "volatility"
    CUSTOM = "custom"

@dataclass
class CriteriaConfig:
    """Configuration for any criteria"""
    name: str
    criteria_type: CriteriaType
    enabled: bool = True
    weight: float = 1.0
    parameters: Dict[str, Any] = field(default_factory=dict)

class BaseCriteria(ABC):
    """Base class for all criteria that strategies can use"""
    
    def __init__(self, config: CriteriaConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    @abstractmethod
    def evaluate(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate criteria against stock data
        Returns: {
            'passed': bool,
            'score': float (0-100),
            'message': str,
            'metadata': Dict
        }
        """
        pass
    
    @abstractmethod
    def get_required_fields(self) -> List[str]:
        """List of data fields required for evaluation"""
        pass

class CriteriaRegistry:
    """Registry to manage all available criteria"""
    
    def __init__(self):
        self._criteria: Dict[str, BaseCriteria] = {}
    
    def register(self, criteria: BaseCriteria):
        """Register a criteria"""
        self._criteria[criteria.config.name] = criteria
        logging.info(f"Registered criteria: {criteria.config.name}")
    
    def get_criteria(self, name: str) -> Optional[BaseCriteria]:
        """Get criteria by name"""
        return self._criteria.get(name)
    
    def get_criteria_by_type(self, criteria_type: CriteriaType) -> List[BaseCriteria]:
        """Get all criteria of a specific type"""
        return [c for c in self._criteria.values() 
                if c.config.criteria_type == criteria_type]
    
    def evaluate_all(self, stock_data: Dict[str, Any], 
                    criteria_names: List[str] = None) -> Dict[str, Any]:
        """
        Evaluate multiple criteria against stock data
        """
        results = {}
        total_score = 0
        total_weight = 0
        passed_criteria = []
        failed_criteria = []
        
        criteria_to_evaluate = self._get_criteria_to_evaluate(criteria_names)
        
        for criteria in criteria_to_evaluate:
            if not criteria.config.enabled:
                continue
                
            result = criteria.evaluate(stock_data)
            results[criteria.config.name] = result
            
            if result['passed']:
                passed_criteria.append(criteria.config.name)
                total_score += result['score'] * criteria.config.weight
                total_weight += criteria.config.weight
            else:
                failed_criteria.append(criteria.config.name)
        
        # Calculate weighted average score
        overall_score = total_score / total_weight if total_weight > 0 else 0
        
        return {
            'overall_score': overall_score,
            'passed_criteria': passed_criteria,
            'failed_criteria': failed_criteria,
            'detailed_results': results,
            'meets_requirements': len(failed_criteria) == 0
        }
    
    def _get_criteria_to_evaluate(self, criteria_names: List[str] = None) -> List[BaseCriteria]:
        """Get criteria based on names or all if None"""
        if criteria_names:
            return [self.get_criteria(name) for name in criteria_names 
                   if self.get_criteria(name)]
        else:
            return list(self._criteria.values())