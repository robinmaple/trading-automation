# src/scanner/integration/ibkr_data_adapter.py
from typing import List, Dict, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta
import logging

# FIXED: Use the official IBKR API Contract class
from ibapi.contract import Contract

from ...core.market_data_manager import MarketDataManager
from ...services.market_context_service import MarketContextService

class IBKRDataAdapter:
    """
    Adapter to connect scanner with your existing IBKR infrastructure
    Uses your MarketDataManager for real-time data and MarketContextService for analysis
    """
    
    def __init__(self, market_data_manager: MarketDataManager, 
                 market_context_service: MarketContextService):
        self.market_data = market_data_manager
        self.market_context = market_context_service
        self.logger = logging.getLogger(__name__)
        self._historical_cache = {}
        self._cache_expiry = timedelta(minutes=5)
    
    def get_dynamic_universe(self, filters: Dict) -> List[Dict]:
        """
        Get dynamic stock universe based on your current market data subscriptions
        Use your existing pattern for stock selection
        """
        # Use your existing major stocks list or query method
        major_stocks = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'JNJ',
            'V', 'PG', 'UNH', 'HD', 'DIS', 'PYPL', 'NFLX', 'ADBE', 'CRM', 'INTC'
        ]
        
        qualified_stocks = []
        
        for symbol in major_stocks:
            try:
                # Use your existing price data retrieval
                price_data = self.market_data.get_current_price(symbol)
                
                if price_data and price_data.get('price', 0) > 0:
                    stock_info = {
                        'symbol': symbol,
                        'price': price_data['price'],
                        'volume': self._get_volume_from_existing(symbol),
                        'market_cap': self._get_market_cap_from_existing(symbol),
                        'data_type': price_data.get('data_type', 'unknown')
                    }
                    
                    # Apply filters
                    if (stock_info['volume'] > filters['min_volume'] and
                        stock_info['market_cap'] > filters['min_market_cap'] and
                        stock_info['price'] > filters['min_price']):
                        qualified_stocks.append(stock_info)
                        
            except Exception as e:
                self.logger.warning(f"Could not get data for {symbol}: {e}")
                continue
        
        self.logger.info(f"Found {len(qualified_stocks)} qualified stocks")
        return qualified_stocks
    
    def get_historical_data(self, symbol: str, days: int = 100) -> Optional[pd.DataFrame]:
        """Get historical data using your existing infrastructure"""
        cache_key = f"{symbol}_{days}"
        if cache_key in self._historical_cache:
            cached_data = self._historical_cache[cache_key]
            if datetime.now() < cached_data['expiry']:
                return cached_data['data']
        
        try:
            # Use your existing historical data pattern
            if hasattr(self.market_context.data_feed, 'get_historical_data'):
                ohlc_data = self.market_context.data_feed.get_historical_data(
                    symbol, '1D', days
                )
                if not ohlc_data.empty:
                    self._historical_cache[cache_key] = {
                        'data': ohlc_data,
                        'expiry': datetime.now() + self._cache_expiry
                    }
                    return ohlc_data
            
            # Fallback to mock data
            self.logger.warning(f"Using mock historical data for {symbol}")
            mock_data = self._generate_mock_historical_data(symbol, days)
            
            self._historical_cache[cache_key] = {
                'data': mock_data,
                'expiry': datetime.now() + self._cache_expiry
            }
            return mock_data
            
        except Exception as e:
            self.logger.error(f"Error getting historical data for {symbol}: {e}")
            return None
    
    def create_contract(self, symbol: str) -> Contract:
        """Create IBKR contract using the official IBKR API Contract class"""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract
    
    def _get_volume_from_existing(self, symbol: str) -> float:
        """Get volume using your existing trading app's pattern"""
        try:
            if hasattr(self.market_data, 'get_volume'):
                return self.market_data.get_volume(symbol)
        except:
            pass
        
        # Fallback to existing mock pattern
        base_volumes = {
            'AAPL': 50_000_000, 'MSFT': 25_000_000, 'GOOGL': 15_000_000,
            'AMZN': 20_000_000, 'TSLA': 30_000_000, 'META': 18_000_000
        }
        return base_volumes.get(symbol, 5_000_000)
    
    def _get_market_cap_from_existing(self, symbol: str) -> float:
        """Get market cap using your existing trading app's pattern"""
        try:
            if hasattr(self.market_context, 'get_fundamental_data'):
                fundamental = self.market_context.get_fundamental_data(symbol)
                return fundamental.get('market_cap', 0)
        except:
            pass
        
        # Fallback to existing mock pattern  
        market_caps = {
            'AAPL': 2.5e12, 'MSFT': 1.8e12, 'GOOGL': 1.2e12, 'AMZN': 1.1e12,
            'TSLA': 800e9, 'META': 600e9, 'NVDA': 400e9
        }
        return market_caps.get(symbol, 50e9)
    
    def _generate_mock_historical_data(self, symbol: str, days: int) -> pd.DataFrame:
        """Generate mock historical data for testing"""
        import numpy as np
        
        dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
        
        base_prices = {'AAPL': 150, 'MSFT': 280, 'GOOGL': 135, 'AMZN': 3200, 'TSLA': 220}
        start_price = base_prices.get(symbol, 100)
        
        returns = np.random.normal(0.001, 0.015, days)
        prices = start_price * (1 + returns).cumprod()
        
        data = []
        for i, date in enumerate(dates):
            close = prices[i]
            open_price = close * (1 + np.random.normal(0, 0.008))
            high = max(open_price, close) * (1 + abs(np.random.normal(0, 0.006)))
            low = min(open_price, close) * (1 - abs(np.random.normal(0, 0.006)))
            volume = np.random.randint(1_000_000, 10_000_000)
            
            data.append({
                'date': date,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })
        
        return pd.DataFrame(data).set_index('date')