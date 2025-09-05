from src.core.abstract_data_feed import AbstractDataFeed
from ibapi.contract import Contract
from typing import Dict, Any, Optional, List
import yfinance as yf
import pandas as pd
import datetime
import time
from collections import deque
import threading

class YFinanceHistoricalFeed(AbstractDataFeed):
    """
    Historical data feed implementation using yfinance for replay.
    Loads historical data and replays it in sequence.
    """
    
    def __init__(self, start_date: str = None, 
                 end_date: str = None, 
                 interval: str = '1m'):
        self._connected = False
        self.data: Dict[str, pd.DataFrame] = {}
        self.iterators: Dict[str, iter] = {}
        self.current_prices: Dict[str, Dict[str, Any]] = {}
        
        # Set default dates if not provided
        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            # For intraday data, use recent period (last 7 days)
            if interval in ['1m', '2m', '5m', '15m', '30m', '60m', '90m']:
                start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
            else:
                # For daily/weekly/monthly data, use longer period
                start_date = '2024-01-01'
                
        self.config = {
            'start_date': start_date,
            'end_date': end_date,
            'interval': interval
        }
        self.lock = threading.RLock()
        
    def connect(self) -> bool:
        """
        Initialize the historical data feed.
        For historical mode, this just marks the feed as ready.
        Actual data loading happens during subscribe().
        """
        try:
            self._connected = True
            print(f"Historical data feed initialized for period: {self.config['start_date']} to {self.config['end_date']}")
            print(f"Data interval: {self.config['interval']}")
            return True
        except Exception as e:
            print(f"Failed to initialize historical data feed: {e}")
            return False
    
    def is_connected(self) -> bool:
        return self._connected
    
    def _get_yfinance_symbol(self, symbol: str, contract: Contract) -> str:
        """
        Convert IBKR symbol to yfinance format.
        Handles forex pairs by adding currency and =X suffix.
        """
        # For forex pairs in IBKR, symbol is like 'EUR' and currency is 'USD'
        # yfinance expects 'EURUSD=X' for forex pairs
        if contract.secType == "CASH" and contract.currency:
            return f"{symbol}{contract.currency}=X"
        return symbol
    
    def subscribe(self, symbol: str, contract: Contract) -> bool:
        """
        Subscribe to a symbol by loading its historical data.
        """
        try:
            with self.lock:
                if symbol in self.data:
                    return True  # Already subscribed
                
                print(f"Loading historical data for {symbol}...")
                
                # Convert symbol to yfinance format
                yf_symbol = self._get_yfinance_symbol(symbol, contract)
                print(f"Using yfinance symbol: {yf_symbol}")
                
                # Fetch historical data
                ticker = yf.Ticker(yf_symbol)
                
                try:
                    df = ticker.history(
                        start=self.config['start_date'],
                        end=self.config['end_date'],
                        interval=self.config['interval']
                    )
                except Exception as e:
                    print(f"Error fetching {self.config['interval']} data for {yf_symbol}: {e}")
                    print("Trying with daily data instead...")
                    # Fallback to daily data if specific interval fails
                    df = ticker.history(
                        start=self.config['start_date'],
                        end=self.config['end_date'],
                        interval='1d'
                    )
                
                if df.empty:
                    print(f"No historical data found for {yf_symbol}")
                    print("Available info:", ticker.info)
                    return False
                
                # Store data and create iterator
                self.data[symbol] = df
                self.iterators[symbol] = df.itertuples()
                self.current_prices[symbol] = {
                    'price': 0.0,
                    'timestamp': None,
                    'data_type': 'HISTORICAL',
                    'updates': 0,
                    'history': []
                }
                
                print(f"Loaded {len(df)} bars for {symbol} ({self.config['interval']} interval)")
                return True
                
        except Exception as e:
            print(f"Failed to load historical data for {symbol}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        if symbol == 'EUR' and not self.data:  # If no yfinance data was loaded
            if not hasattr(self, '_hardcoded_eur_price'):
                self._hardcoded_eur_price = 1.16455  # Start at anchor price
            else:
                self._hardcoded_eur_price += 0.001  # Increment by 1 pip each call

            price_data = {
                'price': self._hardcoded_eur_price,
                'timestamp': datetime.datetime.now(),
                'data_type': 'HARDCODED',
                'updates': 0,
                'history': []
            }
            return price_data
        
        """
        Get the next historical data point for the symbol.
        Advances the iterator each time called.
        """
        
        with self.lock:
            if symbol not in self.iterators:
                return None
            
            try:
                # Get next bar from historical data
                bar = next(self.iterators[symbol])
                
                # Update current price data
                price_data = self.current_prices[symbol]
                price_data['price'] = bar.Close  # Use Close price as current
                price_data['timestamp'] = bar.Index.to_pydatetime()
                price_data['updates'] += 1
                price_data['history'].append(bar.Close)
                
                # Keep limited history
                if len(price_data['history']) > 100:
                    price_data['history'].pop(0)
                
                return price_data.copy()
                
            except StopIteration:
                # End of historical data
                print(f"Historical data exhausted for {symbol}")
                return self.current_prices[symbol].copy()
            except Exception as e:
                print(f"Error getting historical data for {symbol}: {e}")
                return None