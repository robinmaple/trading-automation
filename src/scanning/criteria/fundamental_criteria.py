# src/scanner/criteria/fundamental_criteria.py
from .criteria_core import BaseCriteria, CriteriaConfig, CriteriaType
from typing import Dict, Any, List

class FundamentalCriteria(BaseCriteria):
    """Base fundamental criteria for high-quality, liquid stocks"""
    
    def evaluate(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        if self.config.name == "min_volume":
            return self._evaluate_min_volume(stock_data)
        elif self.config.name == "min_price":
            return self._evaluate_min_price(stock_data)
        elif self.config.name == "min_market_cap":
            return self._evaluate_min_market_cap(stock_data)
        elif self.config.name == "exchange_listed":
            return self._evaluate_exchange_listed(stock_data)
        else:
            return {'passed': False, 'score': 0, 'message': 'Unknown criteria'}
    
    def get_required_fields(self) -> List[str]:
        if self.config.name == "min_volume":
            return ['volume']
        elif self.config.name == "min_price":
            return ['price']
        elif self.config.name == "min_market_cap":
            return ['market_cap']
        elif self.config.name == "exchange_listed":
            return ['exchange']
        return []
    
    def _evaluate_min_volume(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """Average Daily Volume > 1,000,000"""
        min_volume = self.config.parameters.get('min_volume', 1_000_000)
        volume = stock_data.get('volume', 0)
        
        passed = volume > min_volume
        score = min(100, (volume / min_volume) * 100) if passed else 0
        
        return {
            'passed': passed,
            'score': score,
            'message': f"Volume: {volume:,.0f} vs Required: {min_volume:,.0f}",
            'metadata': {
                'actual_volume': volume,
                'required_volume': min_volume,
                'volume_ratio': volume / min_volume
            }
        }
    
    def _evaluate_min_price(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """Current Price > $5"""
        min_price = self.config.parameters.get('min_price', 5.0)
        price = stock_data.get('price', 0)
        
        passed = price > min_price
        score = 100 if passed else 0
        
        return {
            'passed': passed,
            'score': score,
            'message': f"Price: ${price:.2f} vs Required: ${min_price:.2f}",
            'metadata': {
                'actual_price': price,
                'required_price': min_price
            }
        }
    
    def _evaluate_min_market_cap(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """Market Cap > $10B"""
        min_market_cap = self.config.parameters.get('min_market_cap', 10_000_000_000)
        market_cap = stock_data.get('market_cap', 0)
        
        passed = market_cap > min_market_cap
        score = min(100, (market_cap / min_market_cap) * 100) if passed else 0
        
        return {
            'passed': passed,
            'score': score,
            'message': f"Market Cap: ${market_cap:,.0f} vs Required: ${min_market_cap:,.0f}",
            'metadata': {
                'actual_market_cap': market_cap,
                'required_market_cap': min_market_cap,
                'market_cap_ratio': market_cap / min_market_cap
            }
        }
    
    def _evaluate_exchange_listed(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        """Must be listed on major exchange (NYSE, NASDAQ)"""
        exchange = stock_data.get('exchange', '').upper()
        major_exchanges = self.config.parameters.get('major_exchanges', ['NYSE', 'NASDAQ'])
        
        passed = exchange in major_exchanges
        score = 100 if passed else 0
        
        return {
            'passed': passed,
            'score': score,
            'message': f"Exchange: {exchange} vs Required: {major_exchanges}",
            'metadata': {
                'actual_exchange': exchange,
                'required_exchanges': major_exchanges
            }
        }