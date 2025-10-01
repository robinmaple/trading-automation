# src/scanner/criteria/liquidity_criteria.py
from .criteria_core import BaseCriteria, CriteriaConfig, CriteriaType
from typing import Dict, Any, List

class LiquidityCriteria(BaseCriteria):
    """Liquidity-focused criteria for stable trading"""
    
    def evaluate(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        if self.config.name == "bid_ask_spread":
            return self._evaluate_bid_ask_spread(stock_data)
        elif self.config.name == "average_dollar_volume":
            return self._evaluate_avg_dollar_volume(stock_data)
        elif self.config.name == "volume_consistency":
            return self._evaluate_volume_consistency(stock_data)
        else:
            return {'passed': False, 'score': 0, 'message': 'Unknown criteria'}
    
    def get_required_fields(self) -> List[str]:
        if self.config.name == "bid_ask_spread":
            return ['bid_price', 'ask_price', 'price']
        elif self.config.name == "average_dollar_volume":
            return ['volume', 'price']
        elif self.config.name == "volume_consistency":
            return ['volume_history']
        return []
    
    def _evaluate_bid_ask_spread(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """Bid-Ask spread should be reasonable"""
        max_spread_pct = self.config.parameters.get('max_spread_pct', 0.02)  # 2%
        bid = stock_data.get('bid_price', 0)
        ask = stock_data.get('ask_price', 0)
        price = stock_data.get('price', 0)
        
        if bid == 0 or ask == 0 or price == 0:
            return {'passed': False, 'score': 0, 'message': 'Missing bid/ask data'}
        
        spread_pct = (ask - bid) / price
        passed = spread_pct <= max_spread_pct
        score = max(0, 100 - (spread_pct / max_spread_pct) * 100)
        
        return {
            'passed': passed,
            'score': score,
            'message': f"Spread: {spread_pct:.4f} vs Max: {max_spread_pct:.4f}",
            'metadata': {
                'bid_price': bid,
                'ask_price': ask,
                'spread_pct': spread_pct,
                'max_spread_pct': max_spread_pct
            }
        }
    
    def _evaluate_avg_dollar_volume(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """Minimum average dollar volume for liquidity"""
        min_dollar_volume = self.config.parameters.get('min_dollar_volume', 10_000_000)  # $10M
        volume = stock_data.get('volume', 0)
        price = stock_data.get('price', 0)
        
        dollar_volume = volume * price
        passed = dollar_volume > min_dollar_volume
        score = min(100, (dollar_volume / min_dollar_volume) * 100) if passed else 0
        
        return {
            'passed': passed,
            'score': score,
            'message': f"Dollar Volume: ${dollar_volume:,.0f} vs Required: ${min_dollar_volume:,.0f}",
            'metadata': {
                'dollar_volume': dollar_volume,
                'required_dollar_volume': min_dollar_volume
            }
        }
    
    def _evaluate_volume_consistency(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """Volume should be consistent (not too volatile)"""
        volume_history = stock_data.get('volume_history', [])
        if len(volume_history) < 5:
            return {'passed': False, 'score': 0, 'message': 'Insufficient volume history'}
        
        # Calculate coefficient of variation
        avg_volume = sum(volume_history) / len(volume_history)
        if avg_volume == 0:
            return {'passed': False, 'score': 0, 'message': 'Zero average volume'}
        
        std_dev = (sum((v - avg_volume) ** 2 for v in volume_history) / len(volume_history)) ** 0.5
        cv = std_dev / avg_volume
        
        max_cv = self.config.parameters.get('max_cv', 0.5)  # Maximum coefficient of variation
        passed = cv <= max_cv
        score = max(0, 100 - (cv / max_cv) * 100)
        
        return {
            'passed': passed,
            'score': score,
            'message': f"Volume CV: {cv:.3f} vs Max: {max_cv:.3f}",
            'metadata': {
                'volume_std_dev': std_dev,
                'volume_avg': avg_volume,
                'coefficient_variation': cv
            }
        }