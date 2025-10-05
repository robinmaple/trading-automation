"""
Integration tests for the complete event-driven trading system.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
import datetime
import gc
import sys
import threading
import time

from src.core.event_bus import EventBus
from src.core.market_data_manager import MarketDataManager
from src.core.trading_manager import TradingManager
from src.core.events import PriceUpdateEvent, EventType
from src.data_feeds.ibkr_data_feed import IBKRDataFeed


class TestCompleteEventSystem:
    """Integration tests for the complete event-driven system."""
    
    def test_end_to_end_event_flow(self, mock_data_feed, mock_ibkr_client):
        """Test complete event flow from IBKR tick to order execution."""
        # Create event bus
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        # Create MarketDataManager with event bus
        market_data_manager = MarketDataManager(mock_ibkr_client, event_bus)
        
        # Create IBKRDataFeed with MarketDataManager
        ibkr_data_feed = IBKRDataFeed(mock_ibkr_client, event_bus)
        ibkr_data_feed.market_data_manager = market_data_manager
        
        # Create TradingManager with event bus
        trading_manager = TradingManager(
            data_feed=ibkr_data_feed,
            ibkr_client=mock_ibkr_client,
            event_bus=event_bus
        )
        
        # Set up planned orders
        trading_manager.planned_orders = [Mock(symbol="AAPL")]
        
        # Mock state service to avoid warnings
        trading_manager.state_service.get_all_positions = Mock(return_value=[])
        
        trading_manager._update_monitored_symbols()
        
        # Mock order execution
        trading_manager._check_and_execute_orders = Mock()
        
        # Set up MarketDataManager subscription
        market_data_manager.subscriptions["AAPL"] = 9001
        market_data_manager.prices["AAPL"] = {
            'price': 150.00,
            'timestamp': None,
            'history': [],
            'type': 'PENDING',
            'updates': 0
        }
        
        # Simulate IBKR price tick (this should trigger the complete flow)
        market_data_manager.on_tick_price(9001, 4, 160.00, {})  # $10 change to bypass filtering
        
        # Verify the complete flow:
        # 1. MarketDataManager should publish event
        # 2. TradingManager should receive event and trigger order execution
        trading_manager._check_and_execute_orders.assert_called_once()
        
    def test_filtering_reduces_event_volume(self, mock_data_feed, mock_ibkr_client):
        """Test that filtering significantly reduces event volume."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        market_data_manager = MarketDataManager(mock_ibkr_client, event_bus)
        ibkr_data_feed = IBKRDataFeed(mock_ibkr_client, event_bus)
        ibkr_data_feed.market_data_manager = market_data_manager
        
        trading_manager = TradingManager(
            data_feed=ibkr_data_feed,
            ibkr_client=mock_ibkr_client,
            event_bus=event_bus
        )
        
        # Monitor only AAPL
        trading_manager.planned_orders = [Mock(symbol="AAPL")]
        
        # Mock state service
        trading_manager.state_service.get_all_positions = Mock(return_value=[])
        trading_manager._update_monitored_symbols()
        
        trading_manager._check_and_execute_orders = Mock()
        
        # Set up multiple symbol subscriptions
        symbols = ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN"]
        for i, symbol in enumerate(symbols):
            market_data_manager.subscriptions[symbol] = 9000 + i
            market_data_manager.prices[symbol] = {
                'price': 100.00,
                'timestamp': None,
                'history': [],
                'type': 'PENDING',
                'updates': 0
            }
        
        # Simulate many small price changes across all symbols
        event_count = 0
        def count_events(event):
            nonlocal event_count
            event_count += 1
            
        # Count events received by TradingManager
        event_bus.subscribe(EventType.PRICE_UPDATE, count_events)
        
        # Generate price ticks for all symbols - use small changes that should be filtered
        for symbol in symbols:
            market_data_manager.on_tick_price(
                market_data_manager.subscriptions[symbol], 
                4,  # LAST tick
                100.01,  # Tiny change that should be filtered
                {}
            )
            
        # Should have far fewer events than total ticks due to filtering
        # Only monitored symbols (AAPL) with significant changes would generate events
        assert event_count < len(symbols)  # Most ticks should be filtered
        
    def test_event_system_with_real_components(self):
        """Test event system with minimally mocked components."""
        # Create real event bus
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        # Mock only the essential external dependencies
        mock_ibkr_client = Mock()
        mock_ibkr_client.connected = True
        mock_ibkr_client.is_paper_account = True
        
        mock_data_feed = Mock()
        mock_data_feed.is_connected.return_value = True
        
        # Create real MarketDataManager
        market_data_manager = MarketDataManager(mock_ibkr_client, event_bus)
        
        # Create real TradingManager
        trading_manager = TradingManager(
            data_feed=mock_data_feed,
            ibkr_client=mock_ibkr_client,
            event_bus=event_bus
        )
        
        # Set up test scenario
        trading_manager.planned_orders = [Mock(symbol="TEST")]
        
        # Mock state service
        trading_manager.state_service.get_all_positions = Mock(return_value=[])
        
        # Mock the internal method we want to test
        trading_manager._check_and_execute_orders = Mock()
        
        # Update monitored symbols
        trading_manager._update_monitored_symbols()
        
        # Manually set up MarketDataManager state (bypassing IBKR subscription)
        market_data_manager.monitored_symbols = {"TEST"}
        market_data_manager.prices["TEST"] = {
            'price': 100.00,
            'timestamp': None,
            'history': [],
            'type': 'PENDING',
            'updates': 0
        }
        market_data_manager.subscriptions["TEST"] = 9001
        
        # Simulate price update directly - use significant change to bypass filtering
        market_data_manager.on_tick_price(9001, 4, 110.00, {})  # $10 change
        
        # Verify event flow worked
        trading_manager._check_and_execute_orders.assert_called_once()
        
    def test_error_handling_in_event_chain(self, mock_data_feed, mock_ibkr_client):
        """Test that errors in event chain don't break the system."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        market_data_manager = MarketDataManager(mock_ibkr_client, event_bus)
        ibkr_data_feed = IBKRDataFeed(mock_ibkr_client, event_bus)
        ibkr_data_feed.market_data_manager = market_data_manager
        
        trading_manager = TradingManager(
            data_feed=ibkr_data_feed,
            ibkr_client=mock_ibkr_client,
            event_bus=event_bus
        )
        
        # Set up planned orders
        trading_manager.planned_orders = [Mock(symbol="AAPL")]
        
        # Mock state service
        trading_manager.state_service.get_all_positions = Mock(return_value=[])
        trading_manager._update_monitored_symbols()
        
        # Make order execution fail
        trading_manager._check_and_execute_orders = Mock(side_effect=Exception("Execution error"))
        
        # Set up MarketDataManager
        market_data_manager.subscriptions["AAPL"] = 9001
        market_data_manager.prices["AAPL"] = {
            'price': 150.00,
            'timestamp': None,
            'history': [],
            'type': 'PENDING',
            'updates': 0
        }
        
        # Simulate price tick - should not raise exception
        market_data_manager.on_tick_price(9001, 4, 160.00, {})  # Significant change
        
        # Execution should have been attempted despite error
        trading_manager._check_and_execute_orders.assert_called_once()

class TestEventSystemPerformance:
    """Performance tests for the event system."""
    
    def test_event_throughput(self, mock_data_feed, mock_ibkr_client):
        """Test event system throughput under load."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        market_data_manager = MarketDataManager(mock_ibkr_client, event_bus)
        ibkr_data_feed = IBKRDataFeed(mock_ibkr_client, event_bus)
        ibkr_data_feed.market_data_manager = market_data_manager
        
        trading_manager = TradingManager(
            data_feed=ibkr_data_feed,
            ibkr_client=mock_ibkr_client,
            event_bus=event_bus
        )
        
        # Monitor multiple symbols
        num_symbols = 20
        symbols = [f"SYM{i}" for i in range(num_symbols)]
        trading_manager.planned_orders = [Mock(symbol=symbol) for symbol in symbols]
        
        # Mock state service
        trading_manager.state_service.get_all_positions = Mock(return_value=[])
        trading_manager._update_monitored_symbols()
        
        trading_manager._check_and_execute_orders = Mock()
        
        # Set up all symbol subscriptions
        for i, symbol in enumerate(symbols):
            market_data_manager.subscriptions[symbol] = 9000 + i
            market_data_manager.prices[symbol] = {
                'price': 100.00,
                'timestamp': None,
                'history': [],
                'type': 'PENDING',
                'updates': 0
            }
        
        # DISABLE FILTERING temporarily to ensure all events pass through
        original_should_publish = market_data_manager._should_publish_price_update
        market_data_manager._should_publish_price_update = lambda symbol, new_price, old_price: True
        
        try:
            # Measure throughput
            num_events_per_symbol = 50
            total_events = num_symbols * num_events_per_symbol
            
            start_time = time.time()
            
            for event_num in range(num_events_per_symbol):
                for i, symbol in enumerate(symbols):
                    market_data_manager.on_tick_price(
                        9000 + i,
                        4,
                        100.00 + event_num * 0.01,  # Any change will work since filtering is disabled
                        {}
                    )
                    
            end_time = time.time()
            total_time = end_time - start_time
            
            # Calculate events per second
            events_per_second = total_events / total_time
            
            # Should process at least 100 events per second
            assert events_per_second > 100, f"Only achieved {events_per_second:.1f} events/second"
            
            # All events should have been processed
            assert trading_manager._check_and_execute_orders.call_count == total_events
            
        finally:
            # Restore original method
            market_data_manager._should_publish_price_update = original_should_publish
        
    def test_event_system_stress_test(self, mock_data_feed, mock_ibkr_client):
        """Stress test the event system with high volume and rapid events."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        market_data_manager = MarketDataManager(mock_ibkr_client, event_bus)
        ibkr_data_feed = IBKRDataFeed(mock_ibkr_client, event_bus)
        ibkr_data_feed.market_data_manager = market_data_manager
        
        trading_manager = TradingManager(
            data_feed=ibkr_data_feed,
            ibkr_client=mock_ibkr_client,
            event_bus=event_bus
        )
        
        # Monitor multiple symbols
        num_symbols = 10
        symbols = [f"STRESS{i}" for i in range(num_symbols)]
        trading_manager.planned_orders = [Mock(symbol=symbol) for symbol in symbols]
        
        # Mock state service
        trading_manager.state_service.get_all_positions = Mock(return_value=[])
        trading_manager._update_monitored_symbols()
        
        # Use a thread-safe counter for order execution checks
        execution_count = 0
        execution_lock = threading.Lock()
        
        def counting_execution():
            nonlocal execution_count
            with execution_lock:
                execution_count += 1
                
        trading_manager._check_and_execute_orders = counting_execution
        
        # Set up all symbol subscriptions
        for i, symbol in enumerate(symbols):
            market_data_manager.subscriptions[symbol] = 9000 + i
            market_data_manager.prices[symbol] = {
                'price': 100.00,
                'timestamp': None,
                'history': [],
                'type': 'PENDING',
                'updates': 0
            }
        
        # DISABLE FILTERING temporarily to ensure all events pass through
        original_should_publish = market_data_manager._should_publish_price_update
        market_data_manager._should_publish_price_update = lambda symbol, new_price, old_price: True
        
        try:
            # Create multiple threads to simulate concurrent market data
            def stress_thread(thread_id, num_events):
                for i in range(num_events):
                    symbol = symbols[thread_id % num_symbols]
                    req_id = market_data_manager.subscriptions[symbol]
                    # Any price change will work since filtering is disabled
                    price = 100.00 + (thread_id * 0.1) + (i * 0.01)
                    market_data_manager.on_tick_price(req_id, 4, price, {})
            
            # Run stress test
            num_threads = 5
            events_per_thread = 20
            total_expected_events = num_threads * events_per_thread
            
            threads = []
            start_time = time.time()
            
            for i in range(num_threads):
                thread = threading.Thread(target=stress_thread, args=(i, events_per_thread))
                threads.append(thread)
                thread.start()
                
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
                
            end_time = time.time()
            total_time = end_time - start_time
            
            # Verify all events were processed
            assert execution_count == total_expected_events, \
                f"Processed {execution_count} events, expected {total_expected_events}"
                
            # Verify reasonable performance
            events_per_second = total_expected_events / total_time
            assert events_per_second > 50, f"Only achieved {events_per_second:.1f} events/second under stress"
            
        finally:
            # Restore original method
            market_data_manager._should_publish_price_update = original_should_publish

class TestEventSystemFilteringEffectiveness:
    """Tests specifically for filtering effectiveness."""
    
    def test_filtering_blocks_small_changes(self, mock_data_feed, mock_ibkr_client):
        """Test that small price changes are filtered out."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        market_data_manager = MarketDataManager(mock_ibkr_client, event_bus)
        ibkr_data_feed = IBKRDataFeed(mock_ibkr_client, event_bus)
        ibkr_data_feed.market_data_manager = market_data_manager
        
        trading_manager = TradingManager(
            data_feed=ibkr_data_feed,
            ibkr_client=mock_ibkr_client,
            event_bus=event_bus
        )
        
        # Set up single symbol
        trading_manager.planned_orders = [Mock(symbol="AAPL")]
        
        # Mock state service
        trading_manager.state_service.get_all_positions = Mock(return_value=[])
        trading_manager._update_monitored_symbols()
        
        trading_manager._check_and_execute_orders = Mock()
        
        # Set up subscription
        market_data_manager.subscriptions["AAPL"] = 9001
        market_data_manager.prices["AAPL"] = {
            'price': 150.00,
            'timestamp': None,
            'history': [],
            'type': 'PENDING',
            'updates': 0
        }
        
        # Test small change that should be filtered
        market_data_manager.on_tick_price(9001, 4, 150.01, {})  # $0.01 change
        
        # Should not trigger execution (filtered out)
        trading_manager._check_and_execute_orders.assert_not_called()
        
        # Test large change that should pass through
        market_data_manager.on_tick_price(9001, 4, 160.00, {})  # $10.00 change
        
        # Should trigger execution
        trading_manager._check_and_execute_orders.assert_called_once()
        
    def test_filtering_respects_monitored_symbols(self, mock_data_feed, mock_ibkr_client):
        """Test that only monitored symbols generate events."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        market_data_manager = MarketDataManager(mock_ibkr_client, event_bus)
        ibkr_data_feed = IBKRDataFeed(mock_ibkr_client, event_bus)
        ibkr_data_feed.market_data_manager = market_data_manager
        
        trading_manager = TradingManager(
            data_feed=ibkr_data_feed,
            ibkr_client=mock_ibkr_client,
            event_bus=event_bus
        )
        
        # Only monitor AAPL
        trading_manager.planned_orders = [Mock(symbol="AAPL")]
        
        # Mock state service
        trading_manager.state_service.get_all_positions = Mock(return_value=[])
        trading_manager._update_monitored_symbols()
        
        trading_manager._check_and_execute_orders = Mock()
        
        # Set up multiple symbol subscriptions
        symbols = ["AAPL", "TSLA", "MSFT"]
        for i, symbol in enumerate(symbols):
            market_data_manager.subscriptions[symbol] = 9000 + i
            market_data_manager.prices[symbol] = {
                'price': 100.00,
                'timestamp': None,
                'history': [],
                'type': 'PENDING',
                'updates': 0
            }
        
        # Generate significant price changes for all symbols
        for symbol in symbols:
            market_data_manager.on_tick_price(
                market_data_manager.subscriptions[symbol],
                4,
                110.00,  # $10 change - significant enough to pass filtering
                {}
            )
        
        # Should only execute for monitored symbol (AAPL)
        assert trading_manager._check_and_execute_orders.call_count == 1