# src/scanner/integration/eod_data_provider.py
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime, timedelta
import logging
import numpy as np

class EODDataProvider:
    """
    Optimized data provider for scanner EOD (End-of-Day) data needs
    Provides batch data access for scanning, separate from real-time trading data
    """
    
    def __init__(self, ibkr_data_feed=None):
        self.ibkr_data_feed = ibkr_data_feed
        self.logger = logging.getLogger(__name__)
        self._price_cache = {}
        self._cache_expiry = timedelta(minutes=30)
        
        print("✅ EODDataProvider initialized - optimized for batch scanning")
    
    def get_symbol_universe(self) -> List[str]:
        """
        Get dynamic list of symbols to scan
        TODO: Replace with actual IBKR symbol lookup
        """
        # For now, return major stocks + some additional symbols
        # Later: Query IBKR for symbols meeting basic criteria
        return [
            # Major tech
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'ADBE', 'CRM', 'INTC',
            # Financials
            'JPM', 'V', 'MA', 'BAC', 'WFC', 'GS', 'MS', 'AXP',
            # Healthcare
            'JNJ', 'UNH', 'PFE', 'ABT', 'TMO', 'LLY', 'MRK',
            # Consumer
            'PG', 'KO', 'PEP', 'WMT', 'HD', 'MCD', 'NKE', 'DIS',
            # Industrials & Energy
            'CAT', 'BA', 'MMM', 'XOM', 'CVX',
            # Additional active stocks
            'NFLX', 'PYPL', 'UBER', 'SHOP', 'SQ'
        ]
    
    def get_universe_prices(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Get EOD prices for multiple symbols in one batch
        Returns: {symbol: {price: float, volume: float, market_cap: float, timestamp: datetime}}
        """
        print(f"📊 Getting EOD prices for {len(symbols)} symbols in batch...")
        
        results = {}
        successful_symbols = 0
        
        for symbol in symbols:
            try:
                price_data = self._get_single_symbol_data(symbol)
                if price_data and price_data.get('price', 0) > 0:
                    results[symbol] = price_data
                    successful_symbols += 1
                    print(f"✅ {symbol}: ${price_data['price']:.2f}")
                else:
                    print(f"❌ {symbol}: No EOD data available")
                    
            except Exception as e:
                print(f"❌ {symbol}: Error - {e}")
                continue
        
        print(f"🎯 EOD data retrieval: {successful_symbols}/{len(symbols)} symbols successful")
        return results
    
    def _get_single_symbol_data(self, symbol: str) -> Optional[Dict]:
        """Get EOD data for a single symbol with caching"""
        cache_key = f"price_{symbol}"
        
        # Check cache first
        if cache_key in self._price_cache:
            cached_data = self._price_cache[cache_key]
            if datetime.now() < cached_data['expiry']:
                return cached_data['data']
        
        try:
            # Try IBKR delayed data first
            price_data = self._get_ibkr_delayed_data(symbol)
            if price_data:
                self._price_cache[cache_key] = {
                    'data': price_data,
                    'expiry': datetime.now() + self._cache_expiry
                }
                return price_data
            
            # Fallback to realistic mock data
            price_data = self._get_realistic_mock_data(symbol)
            self._price_cache[cache_key] = {
                'data': price_data,
                'expiry': datetime.now() + self._cache_expiry
            }
            return price_data
            
        except Exception as e:
            self.logger.error(f"Error getting EOD data for {symbol}: {e}")
            return None
    
    def _get_ibkr_delayed_data(self, symbol: str) -> Optional[Dict]:
        """Attempt to get delayed data from IBKR (free, no subscription needed)"""
        if not self.ibkr_data_feed:
            return None
            
        try:
            # Try to get price from existing data feed
            price_data = self.ibkr_data_feed.get_current_price(symbol)
            
            if price_data and price_data.get('price', 0) > 0:
                return {
                    'price': price_data['price'],
                    'volume': self._estimate_volume(symbol, price_data['price']),
                    'market_cap': self._estimate_market_cap(symbol, price_data['price']),
                    'timestamp': price_data.get('timestamp', datetime.now()),
                    'data_type': 'ibkr_delayed',
                    'source': 'IBKR'
                }
                
        except Exception as e:
            self.logger.debug(f"IBKR delayed data not available for {symbol}: {e}")
        
        return None
    
    def _get_realistic_mock_data(self, symbol: str) -> Dict:
        """Provide realistic mock EOD data for scanner testing"""
        mock_prices = {
            'AAPL': 182.50, 'MSFT': 345.67, 'GOOGL': 145.23, 'AMZN': 178.90,
            'TSLA': 245.60, 'META': 498.75, 'NVDA': 125.45, 'JPM': 198.30,
            'JNJ': 165.80, 'V': 275.40, 'PG': 155.25, 'UNH': 520.30,
            'HD': 350.75, 'DIS': 95.60, 'PYPL': 62.30, 'NFLX': 485.20,
            'ADBE': 525.80, 'CRM': 215.40, 'INTC': 44.50, 'MA': 420.10,
            'BAC': 35.20, 'WFC': 48.90, 'GS': 385.60, 'MS': 85.40,
            'AXP': 215.80, 'PFE': 28.90, 'ABT': 115.60, 'TMO': 525.40,
            'LLY': 620.80, 'MRK': 105.30, 'KO': 60.45, 'PEP': 175.80,
            'WMT': 165.20, 'MCD': 285.60, 'NKE': 95.40, 'CAT': 235.80,
            'BA': 205.60, 'MMM': 105.20, 'XOM': 105.80, 'CVX': 155.60,
            'UBER': 45.30, 'SHOP': 75.60, 'SQ': 65.40
        }
        
        price = mock_prices.get(symbol, 100.0)
        
        return {
            'price': price,
            'volume': self._estimate_volume(symbol, price),
            'market_cap': self._estimate_market_cap(symbol, price),
            'timestamp': datetime.now(),
            'data_type': 'mock_eod',
            'source': 'Mock Data'
        }
    
    def _estimate_volume(self, symbol: str, price: float) -> float:
        """Estimate realistic volume based on symbol and price"""
        typical_volumes = {
            'AAPL': 50_000_000, 'MSFT': 25_000_000, 'GOOGL': 15_000_000,
            'AMZN': 20_000_000, 'TSLA': 30_000_000, 'META': 18_000_000,
            'NVDA': 45_000_000, 'JPM': 15_000_000, 'JNJ': 8_000_000,
            'V': 12_000_000, 'PG': 7_000_000, 'UNH': 3_000_000,
            'HD': 8_000_000, 'DIS': 15_000_000, 'PYPL': 20_000_000,
            'NFLX': 10_000_000, 'ADBE': 4_000_000, 'CRM': 8_000_000,
            'INTC': 25_000_000, 'MA': 8_000_000, 'BAC': 40_000_000,
            'WFC': 25_000_000, 'GS': 5_000_000, 'MS': 10_000_000,
            'AXP': 6_000_000, 'PFE': 30_000_000, 'ABT': 8_000_000,
            'TMO': 3_000_000, 'LLY': 5_000_000, 'MRK': 15_000_000,
            'KO': 20_000_000, 'PEP': 12_000_000, 'WMT': 15_000_000,
            'MCD': 8_000_000, 'NKE': 10_000_000, 'CAT': 6_000_000,
            'BA': 10_000_000, 'MMM': 4_000_000, 'XOM': 25_000_000,
            'CVX': 15_000_000, 'UBER': 25_000_000, 'SHOP': 12_000_000,
            'SQ': 18_000_000
        }
        return typical_volumes.get(symbol, 5_000_000)
    
    def _estimate_market_cap(self, symbol: str, price: float) -> float:
        """Estimate market cap using typical shares outstanding"""
        shares_outstanding = {
            'AAPL': 16_000, 'MSFT': 7_500, 'GOOGL': 12_500,
            'AMZN': 10_300, 'TSLA': 3_200, 'META': 2_500,
            'NVDA': 2_500, 'JPM': 3_000, 'JNJ': 2_600,
            'V': 2_000, 'PG': 2_400, 'UNH': 900,
            'HD': 1_800, 'DIS': 1_800, 'PYPL': 1_100,
            'NFLX': 450, 'ADBE': 450, 'CRM': 1_000,
            'INTC': 4_200, 'MA': 1_900, 'BAC': 8_000,
            'WFC': 3_800, 'GS': 340, 'MS': 1_600,
            'AXP': 750, 'PFE': 5_600, 'ABT': 1_700,
            'TMO': 400, 'LLY': 950, 'MRK': 2_500,
            'KO': 4_300, 'PEP': 1_700, 'WMT': 2_700,
            'MCD': 730, 'NKE': 1_500, 'CAT': 500,
            'BA': 600, 'MMM': 550, 'XOM': 4_000,
            'CVX': 1_900, 'UBER': 2_000, 'SHOP': 1_300,
            'SQ': 600
        }
        
        shares = shares_outstanding.get(symbol, 1_000)  # in millions
        return price * shares * 1_000_000  # Convert to full market cap
    
    def get_historical_data(self, symbol: str, days: int = 100) -> Optional[pd.DataFrame]:
        """Get historical EOD data for strategy analysis"""
        print(f"📈 Getting historical EOD data for {symbol} ({days} days)")
        return self._generate_historical_mock_data(symbol, days)
    
    def _generate_historical_mock_data(self, symbol: str, days: int) -> pd.DataFrame:
        """Generate realistic historical mock data"""
        # Get current price to base historical data on
        current_data = self._get_single_symbol_data(symbol)
        current_price = current_data['price'] if current_data else 100.0
        
        dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
        
        # Generate realistic price movement
        np.random.seed(hash(symbol) % 10000)  # Consistent per symbol
        returns = np.random.normal(0.0005, 0.015, days)
        
        prices = [current_price]
        for ret in returns[1:]:
            new_price = prices[-1] * (1 + ret)
            prices.append(max(new_price, 1.0))
        
        # Reverse to show progression from past to present
        prices = prices[::-1]
        
        # Create OHLC data
        data = []
        for i, date in enumerate(dates):
            close = prices[i]
            open_price = close * (1 + np.random.normal(0, 0.005))
            high = max(open_price, close) * (1 + abs(np.random.normal(0, 0.008)))
            low = min(open_price, close) * (1 - abs(np.random.normal(0, 0.008)))
            volume = np.random.randint(1_000_000, 50_000_000)
            
            data.append({
                'date': date,
                'open': open_price,
                'high': high, 
                'low': low,
                'close': close,
                'volume': volume
            })
        
        df = pd.DataFrame(data)
        df.set_index('date', inplace=True)
        return df