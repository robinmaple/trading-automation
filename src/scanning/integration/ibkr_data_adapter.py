from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime

# <Consolidated EOD Provider Integration - Begin>
from .historical_eod_provider import HistoricalEODProvider
# <Consolidated EOD Provider Integration - End>

# <Historical Data Manager Integration - Begin>
from src.market_data.managers.historical_data_manager import HistoricalDataManager
# <Historical Data Manager Integration - End>

# <Context-Aware Logger Integration - Begin>
from src.core.context_aware_logger import (
    get_context_logger, 
    TradingEventType,
    SafeContext
)
# <Context-Aware Logger Integration - End>

class IBKRDataAdapter:
    """
    Enhanced adapter using HistoricalEODProvider for scanner data needs
    Provides dynamic symbol universe and batch EOD data processing
    Maintains same interface for backward compatibility
    Now uses real IBKR historical data instead of mock data
    """
    
    def __init__(self, ibkr_data_feed):
        # <Context-Aware Logger Initialization - Begin>
        self.context_logger = get_context_logger()
        # <Context-Aware Logger Initialization - End>
        
        # <Adapter Initialization Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKRDataAdapter initializing with HistoricalEODProvider",
            context_provider={
                "ibkr_data_feed_provided": ibkr_data_feed is not None,
                "data_provider_type": "HistoricalEODProvider",
                "mock_data_usage": False
            }
        )
        # <Adapter Initialization Logging - End>
        
        print("🔄 IBKRDataAdapter initializing with HistoricalEODProvider...")
        
        self.ibkr_data_feed = ibkr_data_feed
        
        # <Historical EOD Provider Integration - Begin>
        # Use the new consolidated provider with real IBKR historical data
        self.eod_provider = HistoricalEODProvider(ibkr_data_feed)
        # <Historical EOD Provider Integration - End>
        
        # <Historical Data Manager Integration - Begin>
        # Connect HistoricalDataManager to IBKR client for proper callback routing
        self._connect_historical_manager()
        # <Historical Data Manager Integration - End>
        
        # <Adapter Ready Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKRDataAdapter initialization completed",
            context_provider={
                "eod_provider_initialized": self.eod_provider is not None,
                "architecture": "real_historical_data_only",
                "historical_manager_connected": hasattr(self, '_historical_manager_connected') and self._historical_manager_connected
            },
            decision_reason="Adapter ready with real historical EOD data provider"
        )
        # <Adapter Ready Logging - End>
        
        print("✅ IBKRDataAdapter ready with real historical EOD data provider")
    
    def _connect_historical_manager(self):
        """Connect HistoricalDataManager to IBKR client for proper callback routing"""
        try:
            if (self.ibkr_data_feed and 
                hasattr(self.ibkr_data_feed, 'ibkr_client') and 
                self.ibkr_data_feed.ibkr_client and
                hasattr(self.eod_provider, 'historical_manager')):
                
                ibkr_client = self.ibkr_data_feed.ibkr_client
                historical_manager = self.eod_provider.historical_manager
                
                # Connect historical manager to IBKR client
                ibkr_client.set_historical_data_manager(historical_manager)
                
                # Ensure historical manager has IBKR client reference
                historical_manager.set_ibkr_client(ibkr_client)
                
                self._historical_manager_connected = True
                
                # <Historical Manager Connection Success Logging - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "HistoricalDataManager connected to IBKR client",
                    context_provider={
                        "historical_manager_connected": True,
                        "ibkr_client_available": True,
                        "connection_method": "direct_integration"
                    },
                    decision_reason="HistoricalDataManager successfully connected to IBKR client for callback routing"
                )
                # <Historical Manager Connection Success Logging - End>
                
                print("✅ HistoricalDataManager connected to IBKR client for callback routing")
                
            else:
                self._historical_manager_connected = False
                
                # <Historical Manager Connection Failed Logging - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Cannot connect HistoricalDataManager - components not available",
                    context_provider={
                        "ibkr_data_feed_available": self.ibkr_data_feed is not None,
                        "ibkr_client_available": hasattr(self.ibkr_data_feed, 'ibkr_client') if self.ibkr_data_feed else False,
                        "historical_manager_available": hasattr(self.eod_provider, 'historical_manager') if self.eod_provider else False,
                        "historical_manager_connected": False
                    },
                    decision_reason="Required components not available for HistoricalDataManager connection"
                )
                # <Historical Manager Connection Failed Logging - End>
                print("⚠️  Cannot connect HistoricalDataManager - required components not available")
                
        except Exception as e:
            self._historical_manager_connected = False
            
            # <Historical Manager Connection Error Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error connecting HistoricalDataManager",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "historical_manager_connected": False
                },
                decision_reason=f"HistoricalDataManager connection failed: {e}"
            )
            # <Historical Manager Connection Error Logging - End>
            print(f"❌ Error connecting HistoricalDataManager: {e}")
    # <Historical Data Manager Integration - End>
    
    def get_dynamic_universe(self, filters: Dict) -> List[Dict]:
        """
        Get dynamic stock universe using real historical EOD data
        Fetches symbols dynamically and applies filters
        Returns empty list if no real data available (no mock fallbacks)
        """
        # <Universe Request Start Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting dynamic universe generation with historical EOD data",
            context_provider={
                "filter_min_volume": filters.get('min_volume', 1_000_000),
                "filter_min_market_cap": filters.get('min_market_cap', 1_000_000_000),
                "filter_min_price": filters.get('min_price', 10),
                "filter_count": len(filters),
                "historical_manager_connected": getattr(self, '_historical_manager_connected', False)
            }
        )
        # <Universe Request Start Logging - End>
        
        print(f"🎯 Getting dynamic universe with REAL historical EOD data, filters: {filters}")
        
        # Get dynamic symbol universe (not hardcoded)
        symbols = self.eod_provider.get_symbol_universe()
        
        # <Symbol Universe Retrieved Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Symbol universe retrieved from EOD provider",
            context_provider={
                "symbol_count": len(symbols),
                "symbol_source": "ibkr_scanner",
                "universe_generation_method": "dynamic_scanner"
            }
        )
        # <Symbol Universe Retrieved Logging - End>
        
        print(f"📋 Dynamic symbol universe: {len(symbols)} symbols")
        
        # Get batch EOD data for all symbols using real historical data
        eod_data = self.eod_provider.get_universe_prices(symbols)
        
        # <EOD Data Retrieved Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "EOD price data retrieved for universe",
            context_provider={
                "total_symbols": len(symbols),
                "symbols_with_data": len(eod_data),
                "data_retrieval_rate": len(eod_data) / len(symbols) if symbols else 0,
                "data_source": "ibkr_historical"
            }
        )
        # <EOD Data Retrieved Logging - End>
        
        # Apply filters
        qualified_stocks = []
        min_volume = filters.get('min_volume', 1_000_000)
        min_market_cap = filters.get('min_market_cap', 1_000_000_000)
        min_price = filters.get('min_price', 10)
        
        # Track filtering statistics
        filtering_stats = {
            'total_considered': 0,
            'volume_passed': 0,
            'market_cap_passed': 0,
            'price_passed': 0,
            'all_filters_passed': 0
        }
        
        for symbol, data in eod_data.items():
            # Only include symbols with real price data
            if data and data.get('price', 0) > 0:
                filtering_stats['total_considered'] += 1
                
                volume_ok = data['volume'] >= min_volume
                market_cap_ok = data['market_cap'] >= min_market_cap
                price_ok = data['price'] >= min_price
                
                if volume_ok:
                    filtering_stats['volume_passed'] += 1
                if market_cap_ok:
                    filtering_stats['market_cap_passed'] += 1
                if price_ok:
                    filtering_stats['price_passed'] += 1
                
                if volume_ok and market_cap_ok and price_ok:
                    filtering_stats['all_filters_passed'] += 1
                    qualified_stocks.append({
                        'symbol': symbol,
                        'price': data['price'],
                        'volume': data['volume'],
                        'market_cap': data['market_cap'],
                        'data_type': data.get('data_type', 'historical_eod'),
                        'timestamp': data.get('timestamp'),
                        'source': data.get('source', 'IBKR Historical')
                    })
        
        # <Filtering Complete Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Universe filtering completed",
            context_provider={
                "total_symbols": len(symbols),
                "symbols_with_data": len(eod_data),
                "qualified_stocks": len(qualified_stocks),
                "filtering_efficiency": len(qualified_stocks) / len(eod_data) if eod_data else 0,
                "filtering_stats": filtering_stats,
                "filter_criteria": {
                    "min_volume": min_volume,
                    "min_market_cap": min_market_cap,
                    "min_price": min_price
                }
            },
            decision_reason=f"Filtered {len(qualified_stocks)} qualified stocks from {len(eod_data)} with data"
        )
        # <Filtering Complete Logging - End>
        
        print(f"🎯 Historical EOD filtering: {len(qualified_stocks)}/{len(symbols)} stocks qualified")
        print(f"   - Min volume: {min_volume:,}")
        print(f"   - Min market cap: ${min_market_cap:,.0f}")
        print(f"   - Min price: ${min_price}")
        
        # <No Qualified Stocks Logging - Begin>
        if not qualified_stocks:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No stocks qualified after filtering",
                context_provider={
                    "total_symbols_processed": len(symbols),
                    "symbols_with_valid_data": len(eod_data),
                    "filter_criteria": {
                        "min_volume": min_volume,
                        "min_market_cap": min_market_cap,
                        "min_price": min_price
                    },
                    "filtering_stats": filtering_stats
                },
                decision_reason="No stocks met all filtering criteria with real historical data"
            )
            print("⚠️  No stocks qualified with real historical EOD data")
            print("💡 Check IBKR connection and data subscriptions")
        else:
            # <Qualified Stocks Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Qualified stocks ready for scanning",
                context_provider={
                    "qualified_count": len(qualified_stocks),
                    "qualification_rate": len(qualified_stocks) / len(eod_data) if eod_data else 0,
                    "sample_qualified_symbols": [s['symbol'] for s in qualified_stocks[:5]]  # First 5 as sample
                },
                decision_reason=f"Successfully qualified {len(qualified_stocks)} stocks for scanning"
            )
            # <Qualified Stocks Logging - End>
        # <No Qualified Stocks Logging - End>
        
        return qualified_stocks
    
    def get_historical_data(self, symbol: str, days: int = 100) -> Optional[pd.DataFrame]:
        """Get historical data via historical EOD provider with simplified error handling"""
        # <Historical Data Request Logging - Begin>
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Requesting historical data via adapter",
            symbol=symbol,
            context_provider={
                "days_requested": days,
                "data_provider": "HistoricalEODProvider",
                "request_type": "multi_day_historical",
                "historical_manager_connected": getattr(self, '_historical_manager_connected', False)
            }
        )
        # <Historical Data Request Logging - End>
        
        # <Historical Manager Check - Begin>
        if not getattr(self, '_historical_manager_connected', False):
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Warning: HistoricalDataManager not connected",
                symbol=symbol,
                context_provider={
                    "historical_manager_connected": False,
                    "potential_issue": "callback_routing",
                    "recovery_attempted": True
                },
                decision_reason="Proceeding with historical data request despite manager connection issue"
            )
            print(f"⚠️  Warning: HistoricalDataManager not connected for {symbol}")
        # <Historical Manager Check - End>
        
        try:
            # Delegate to EOD provider which now uses HistoricalDataManager
            result = self.eod_provider.get_historical_data(symbol, days)
            
            # <Historical Data Response Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Historical data request completed via adapter",
                symbol=symbol,
                context_provider={
                    "days_requested": days,
                    "data_available": result is not None,
                    "result_type": type(result).__name__ if result else "None",
                    "dataframe_shape": result.shape if hasattr(result, 'shape') and result is not None else "N/A",
                    "data_points": len(result) if result is not None else 0,
                    "historical_manager_used": True
                },
                decision_reason=f"Historical data {'available' if result else 'not available'} for {symbol}"
            )
            # <Historical Data Response Logging - End>
            
            return result
            
        except Exception as e:
            # <Historical Data Error Logging - Begin>
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Historical data request failed in adapter",
                symbol=symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "days_requested": days,
                    "historical_manager_connected": getattr(self, '_historical_manager_connected', False)
                },
                decision_reason=f"Historical data adapter exception: {e}"
            )
            # <Historical Data Error Logging - End>
            
            print(f"❌ Historical data adapter error for {symbol}: {e}")
            return None