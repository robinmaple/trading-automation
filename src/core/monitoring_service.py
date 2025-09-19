# src/core/monitoring_service.py
"""
Manages the continuous monitoring loop for market data and order execution.
Handles market data subscriptions, periodic checks, and error recovery.
Provides robust monitoring with configurable intervals and error handling.
"""

import threading
import time
import datetime
from typing import Set, Dict, List, Optional, Callable
from src.core.abstract_data_feed import AbstractDataFeed
from src.core.planned_order import PlannedOrder


class MonitoringService:
    """Service for continuous market monitoring and order execution checks."""
    
    def __init__(self, data_feed: AbstractDataFeed, interval_seconds: int = 5):
        """Initialize the monitoring service with data feed and monitoring interval."""
        self.data_feed = data_feed
        self.interval_seconds = interval_seconds
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.subscribed_symbols: Set[str] = set()
        self.market_data_updates: Dict[str, int] = {}
        self.last_labeling_time: Optional[datetime.datetime] = None
        self.error_count = 0
        self.max_errors = 10
        self._check_callback: Optional[Callable] = None
        self._label_callback: Optional[Callable] = None
        
    def start_monitoring(self, check_callback: Callable, label_callback: Callable) -> bool:
        """Start the monitoring loop with provided order check and labeling callbacks."""
        if not self.data_feed.is_connected():
            print("❌ Cannot start monitoring - data feed not connected")
            return False
            
        self._check_callback = check_callback
        self._label_callback = label_callback
        self.monitoring = True
        
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True,
            name="MonitoringServiceThread"
        )
        self.monitor_thread.start()
        
        print(f"✅ Monitoring started with {self.interval_seconds}s interval")
        return True
        
    def stop_monitoring(self) -> None:
        """Stop the monitoring loop and clean up resources."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
            if self.monitor_thread.is_alive():
                print("⚠️  Monitoring thread did not terminate cleanly")
        print("✅ Monitoring stopped")
        
    def _monitoring_loop(self) -> None:
        """Main monitoring loop with error handling and recovery mechanisms."""
        print("🔄 Monitoring loop started")
        
        while self.monitoring and self.error_count < self.max_errors:
            try:
                # Execute the main check callback
                if self._check_callback:
                    self._check_callback()
                
                # Handle periodic labeling
                self._handle_periodic_labeling()
                
                # Reset error counter on successful iteration
                self.error_count = 0
                
                # Sleep for the configured interval
                time.sleep(self.interval_seconds)
                
            except Exception as e:
                self._handle_monitoring_error(e)
                
        if self.error_count >= self.max_errors:
            print(f"❌ Too many errors ({self.error_count}), stopping monitoring")
            self.monitoring = False
            
    def _handle_monitoring_error(self, error: Exception) -> None:
        """Handle monitoring errors with exponential backoff."""
        self.error_count += 1
        print(f"⚠️  Monitoring error ({self.error_count}/{self.max_errors}): {error}")
        
        # Calculate backoff time (exponential with max of 5 minutes)
        backoff_time = min(60 * self.error_count, 300)
        print(f"⏳ Backing off for {backoff_time}s due to error")
        time.sleep(backoff_time)
        
    def _handle_periodic_labeling(self) -> None:
        """Handle periodic order labeling based on configured interval."""
        if not self._label_callback:
            return
            
        current_time = datetime.datetime.now()
        labeling_interval = datetime.timedelta(minutes=10)  # Label every 10 minutes
        
        if (self.last_labeling_time is None or 
            current_time - self.last_labeling_time >= labeling_interval):
            try:
                self._label_callback()
                self.last_labeling_time = current_time
                print("✅ Periodic order labeling completed")
            except Exception as e:
                print(f"❌ Periodic labeling failed: {e}")
                
    def subscribe_to_symbols(self, orders: List[PlannedOrder]) -> Dict[str, bool]:
        """Subscribe to market data for all symbols in planned orders."""
        if not orders:
            print("💡 No orders provided for subscription")
            return {}
            
        subscription_results = {}
        successful_subscriptions = 0
        
        print(f"\n📡 Subscribing to {len(orders)} symbols")
        print("-" * 40)
        
        for order in orders:
            symbol = order.symbol
            if symbol in self.subscribed_symbols:
                subscription_results[symbol] = True
                successful_subscriptions += 1
                continue
                
            try:
                contract = order.to_ib_contract()
                success = self.data_feed.subscribe(symbol, contract)
                
                if success:
                    self.subscribed_symbols.add(symbol)
                    self.market_data_updates[symbol] = 0
                    subscription_results[symbol] = True
                    successful_subscriptions += 1
                    print(f"✅ {symbol}: Subscription successful")
                else:
                    subscription_results[symbol] = False
                    print(f"❌ {symbol}: Subscription failed")
                    
            except Exception as e:
                subscription_results[symbol] = False
                print(f"❌ {symbol}: Subscription error - {e}")
                
        print("-" * 40)
        print(f"📊 Subscription results: {successful_subscriptions}/{len(orders)} successful")
        
        return subscription_results
        
    def unsubscribe_from_symbol(self, symbol: str) -> bool:
        """Unsubscribe from market data for a specific symbol."""
        if symbol not in self.subscribed_symbols:
            return True
            
        try:
            success = self.data_feed.unsubscribe(symbol)
            if success:
                self.subscribed_symbols.remove(symbol)
                if symbol in self.market_data_updates:
                    del self.market_data_updates[symbol]
                print(f"✅ Unsubscribed from {symbol}")
            return success
        except Exception as e:
            print(f"❌ Failed to unsubscribe from {symbol}: {e}")
            return False
            
    def unsubscribe_all(self) -> None:
        """Unsubscribe from all currently subscribed symbols."""
        symbols_to_unsubscribe = list(self.subscribed_symbols)
        successful_unsubscribes = 0
        
        for symbol in symbols_to_unsubscribe:
            if self.unsubscribe_from_symbol(symbol):
                successful_unsubscribes += 1
                
        print(f"📊 Unsubscribed from {successful_unsubscribes}/{len(symbols_to_unsubscribe)} symbols")
        
    def get_subscription_stats(self) -> Dict[str, any]:
        """Get statistics about current subscriptions and market data updates."""
        total_updates = sum(self.market_data_updates.values())
        avg_updates = total_updates / len(self.market_data_updates) if self.market_data_updates else 0
        
        return {
            'total_subscriptions': len(self.subscribed_symbols),
            'subscribed_symbols': list(self.subscribed_symbols),
            'total_market_data_updates': total_updates,
            'average_updates_per_symbol': avg_updates,
            'most_active_symbol': self._get_most_active_symbol(),
            'monitoring_active': self.monitoring,
            'error_count': self.error_count
        }
        
    def _get_most_active_symbol(self) -> Optional[str]:
        """Get the symbol with the most market data updates."""
        if not self.market_data_updates:
            return None
            
        return max(self.market_data_updates.items(), key=lambda x: x[1])[0]
        
    def record_market_data_update(self, symbol: str) -> None:
        """Record a market data update for a symbol (called by data feed callbacks)."""
        if symbol in self.market_data_updates:
            self.market_data_updates[symbol] += 1
            
    def is_symbol_subscribed(self, symbol: str) -> bool:
        """Check if a symbol is currently subscribed for market data."""
        return symbol in self.subscribed_symbols
        
    def get_market_data_for_symbol(self, symbol: str) -> Optional[Dict]:
        """Get current market data for a subscribed symbol."""
        if not self.is_symbol_subscribed(symbol):
            return None
            
        try:
            return self.data_feed.get_current_price(symbol)
        except Exception as e:
            print(f"❌ Failed to get market data for {symbol}: {e}")
            return None
            
    def set_monitoring_interval(self, interval_seconds: int) -> None:
        """Update the monitoring interval (takes effect on next iteration)."""
        if interval_seconds < 1:
            print("⚠️  Monitoring interval must be at least 1 second")
            return
            
        old_interval = self.interval_seconds
        self.interval_seconds = interval_seconds
        print(f"⏰ Monitoring interval changed from {old_interval}s to {interval_seconds}s")
        
    def reset_error_count(self) -> None:
        """Reset the error counter for monitoring recovery."""
        self.error_count = 0
        print("✅ Error counter reset")
        
    def is_healthy(self) -> bool:
        """Check if the monitoring service is healthy and functioning properly."""
        return (self.monitoring and 
                self.error_count < self.max_errors and 
                self.data_feed.is_connected())