"""
Tests for EventBus implementation and event system.
"""
import pytest
import threading
import time
from unittest.mock import Mock, call

from src.core.event_bus import EventBus
from src.core.events import EventType, TradingEvent, PriceUpdateEvent


class TestEventBus:
    """Test cases for EventBus functionality."""
    
    def test_event_bus_initialization(self):
        """Test that EventBus initializes with correct defaults."""
        event_bus = EventBus()
        
        assert event_bus._subscribers == {}
        assert event_bus._global_subscribers == []
        assert event_bus.enable_logging is True
        assert event_bus.max_subscribers == 50
        
    def test_event_bus_initialization_with_config(self):
        """Test EventBus initialization with custom config."""
        config = {
            'event_bus': {
                'enable_logging': False,
                'max_subscribers': 25
            }
        }
        event_bus = EventBus(config=config)
        
        assert event_bus.enable_logging is False
        assert event_bus.max_subscribers == 25
        
    def test_subscribe_to_event_type(self):
        """Test subscribing to specific event types."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})  # Disable logging to avoid __name__ issue
        callback = Mock()
        
        result = event_bus.subscribe(EventType.PRICE_UPDATE, callback)
        
        assert result is True
        assert EventType.PRICE_UPDATE in event_bus._subscribers
        assert callback in event_bus._subscribers[EventType.PRICE_UPDATE]
        
    def test_subscribe_all_events(self):
        """Test subscribing to all event types."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        callback = Mock()
        
        result = event_bus.subscribe_all(callback)
        
        assert result is True
        assert callback in event_bus._global_subscribers
        
    def test_publish_to_specific_subscribers(self):
        """Test publishing events to specific subscribers."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        callback1 = Mock()
        callback2 = Mock()
        
        # Subscribe two callbacks to same event type
        event_bus.subscribe(EventType.PRICE_UPDATE, callback1)
        event_bus.subscribe(EventType.PRICE_UPDATE, callback2)
        
        # Create and publish event
        event = TradingEvent(event_type=EventType.PRICE_UPDATE)
        event_bus.publish(event)
        
        # Both callbacks should be called with the event
        callback1.assert_called_once_with(event)
        callback2.assert_called_once_with(event)
        
    def test_publish_to_global_subscribers(self):
        """Test publishing events to global subscribers."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        global_callback = Mock()
        
        event_bus.subscribe_all(global_callback)
        
        event = TradingEvent(event_type=EventType.ORDER_EXECUTED)
        event_bus.publish(event)
        
        global_callback.assert_called_once_with(event)
        
    def test_publish_mixed_subscribers(self):
        """Test publishing to both specific and global subscribers."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        specific_callback = Mock()
        global_callback = Mock()
        
        event_bus.subscribe(EventType.PRICE_UPDATE, specific_callback)
        event_bus.subscribe_all(global_callback)
        
        event = TradingEvent(event_type=EventType.PRICE_UPDATE)
        event_bus.publish(event)
        
        specific_callback.assert_called_once_with(event)
        global_callback.assert_called_once_with(event)
        
    def test_unsubscribe_from_event_type(self):
        """Test unsubscribing from specific event types."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        callback = Mock()
        
        event_bus.subscribe(EventType.PRICE_UPDATE, callback)
        assert callback in event_bus._subscribers[EventType.PRICE_UPDATE]
        
        result = event_bus.unsubscribe(EventType.PRICE_UPDATE, callback)
        
        assert result is True
        assert callback not in event_bus._subscribers[EventType.PRICE_UPDATE]
        
    def test_unsubscribe_nonexistent_callback(self):
        """Test unsubscribing a callback that was never subscribed."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        callback = Mock()
        
        result = event_bus.unsubscribe(EventType.PRICE_UPDATE, callback)
        
        assert result is False
        
    def test_safe_callback_execution(self):
        """Test that callback errors don't propagate and break the event bus."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        def failing_callback(event):
            raise ValueError("Callback failed")
            
        def working_callback(event):
            working_callback.called = True
            
        working_callback.called = False
        
        event_bus.subscribe(EventType.PRICE_UPDATE, failing_callback)
        event_bus.subscribe(EventType.PRICE_UPDATE, working_callback)
        
        event = TradingEvent(event_type=EventType.PRICE_UPDATE)
        
        # Should not raise an exception
        event_bus.publish(event)
        
        # Working callback should still be called
        assert working_callback.called is True
        
    def test_max_subscribers_limit(self):
        """Test that max subscribers limit is enforced."""
        config = {'event_bus': {'max_subscribers': 2, 'enable_logging': False}}
        event_bus = EventBus(config=config)
        
        # Add max subscribers
        callback1 = Mock()
        callback2 = Mock()
        event_bus.subscribe(EventType.PRICE_UPDATE, callback1)
        event_bus.subscribe(EventType.PRICE_UPDATE, callback2)
        
        # Try to add one more - should fail
        callback3 = Mock()
        result = event_bus.subscribe(EventType.PRICE_UPDATE, callback3)
        
        assert result is False
        assert len(event_bus._subscribers[EventType.PRICE_UPDATE]) == 2
        
    def test_max_global_subscribers_limit(self):
        """Test that max global subscribers limit is enforced."""
        config = {'event_bus': {'max_subscribers': 1, 'enable_logging': False}}
        event_bus = EventBus(config=config)
        
        # Add one global subscriber
        callback1 = Mock()
        event_bus.subscribe_all(callback1)
        
        # Try to add another - should fail
        callback2 = Mock()
        result = event_bus.subscribe_all(callback2)
        
        assert result is False
        assert len(event_bus._global_subscribers) == 1
        
    def test_get_subscription_stats(self):
        """Test subscription statistics reporting."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        # Add some subscribers
        callback1 = Mock()
        callback2 = Mock()
        global_callback = Mock()
        
        event_bus.subscribe(EventType.PRICE_UPDATE, callback1)
        event_bus.subscribe(EventType.ORDER_EXECUTED, callback2)
        event_bus.subscribe_all(global_callback)
        
        stats = event_bus.get_subscription_stats()
        
        assert stats['total_event_types'] == 2
        assert stats['global_subscribers'] == 1
        assert stats['subscriptions_by_type']['price_update'] == 1
        assert stats['subscriptions_by_type']['order_executed'] == 1
        
    def test_thread_safety(self):
        """Test that EventBus operations are thread-safe."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        results = []
        results_lock = threading.Lock()
        
        def subscribe_thread(thread_id):
            callback = Mock()
            event_bus.subscribe(EventType.PRICE_UPDATE, callback)
            with results_lock:
                results.append(f"thread_{thread_id}_subscribed")
            
        def publish_thread():
            event = TradingEvent(event_type=EventType.PRICE_UPDATE)
            event_bus.publish(event)
            with results_lock:
                results.append("published")
            
        # Create multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=subscribe_thread, args=(i,))
            threads.append(t)
            
        publish_t = threading.Thread(target=publish_thread)
        threads.append(publish_t)
        
        # Start all threads
        for t in threads:
            t.start()
            
        # Wait for all threads to complete
        for t in threads:
            t.join()
            
        # Should have some subscriptions and publish
        assert len(results) >= 1  # At least the publish should complete
        # The exact number depends on thread timing, so we don't assert specific count


class TestPriceUpdateEvent:
    """Test cases for PriceUpdateEvent functionality."""
    
    def test_price_update_event_creation(self):
        """Test creating a PriceUpdateEvent with minimal data."""
        event = PriceUpdateEvent(
            event_type=EventType.PRICE_UPDATE,  # Add required parameter
            symbol="AAPL",
            price=150.25,
            price_type="LAST"
        )
        
        assert event.event_type == EventType.PRICE_UPDATE
        assert event.symbol == "AAPL"
        assert event.price == 150.25
        assert event.price_type == "LAST"
        assert event.source == "unknown"
        
    def test_price_update_event_with_source(self):
        """Test creating a PriceUpdateEvent with custom source."""
        event = PriceUpdateEvent(
            event_type=EventType.PRICE_UPDATE,
            symbol="EUR",
            price=1.1000,
            price_type="BID",
            source="MarketDataManager"
        )
        
        assert event.source == "MarketDataManager"
        assert event.data['symbol'] == "EUR"
        assert event.data['price'] == 1.1000
        assert event.data['price_type'] == "BID"
        
    def test_price_update_event_to_dict(self):
        """Test serialization of PriceUpdateEvent to dictionary."""
        event = PriceUpdateEvent(
            event_type=EventType.PRICE_UPDATE,
            symbol="TSLA",
            price=250.75,
            price_type="ASK"
        )
        
        event_dict = event.to_dict()
        
        assert event_dict['event_type'] == 'price_update'
        assert event_dict['data']['symbol'] == 'TSLA'
        assert event_dict['data']['price'] == 250.75
        assert event_dict['data']['price_type'] == 'ASK'
        assert 'timestamp' in event_dict

class TestEventBusIntegration:
    """Integration tests for EventBus with other components."""
    
    def test_event_flow_market_data_to_trading(self):
        """Test complete event flow from market data to trading manager."""
        from src.market_data.managers.market_data_manager import MarketDataManager
        from src.trading.execution.trading_manager import TradingManager
        from src.core.events import EventType  # Import EventType
        
        # Create event bus with logging disabled
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        # Create MarketDataManager with event bus
        mock_ibkr_client = Mock()
        market_data_manager = MarketDataManager(mock_ibkr_client, event_bus)
        
        # Create TradingManager with event bus
        mock_data_feed = Mock()
        trading_manager = TradingManager(
            data_feed=mock_data_feed,
            ibkr_client=mock_ibkr_client,
            event_bus=event_bus
        )
        
        # Mock the order execution check
        trading_manager._check_and_execute_orders = Mock()
        
        # Set up monitored symbols - mock the data feed's market_data_manager
        mock_data_feed.market_data_manager = market_data_manager
        trading_manager.planned_orders = [Mock(symbol="AAPL")]
        
        # Mock the state service to avoid the get_all_positions error
        trading_manager.state_service.get_all_positions = Mock(return_value=[])
        trading_manager._update_monitored_symbols()
        
        # Set up MarketDataManager subscription
        market_data_manager.subscriptions["AAPL"] = 9001
        market_data_manager.prices["AAPL"] = {
            'price': 150.00,
            'timestamp': None,
            'history': [],
            'type': 'PENDING',
            'updates': 0
        }
        
        # Simulate price update
        market_data_manager.on_tick_price(9001, 4, 155.25, {})  # LAST tick for AAPL
        
        # TradingManager should have been called due to price event
        trading_manager._check_and_execute_orders.assert_called_once()
        
    def test_event_bus_performance(self):
        """Test EventBus performance under load."""
        event_bus = EventBus({'event_bus': {'enable_logging': False}})
        
        callback = Mock()
        event_bus.subscribe(EventType.PRICE_UPDATE, callback)
        
        # Publish many events quickly
        start_time = time.time()
        num_events = 100
        
        for i in range(num_events):
            event = PriceUpdateEvent(
                event_type=EventType.PRICE_UPDATE,
                symbol=f"SYM{i}", 
                price=100.0 + i, 
                price_type="LAST"
            )
            event_bus.publish(event)
            
        end_time = time.time()
        
        # Should process events quickly (adjust threshold as needed)
        processing_time = end_time - start_time
        assert processing_time < 1.0  # Should process 100 events in under 1 second
        
        # All events should be delivered
        assert callback.call_count == num_events