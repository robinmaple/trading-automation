# tests/unit/test_monitoring_service.py

import pytest
import datetime
import time
from unittest.mock import MagicMock, patch
from src.trading.monitoring.monitoring_service import MonitoringService

class FakeDataFeed:
    """Simple fake data feed for testing monitoring service."""

    def __init__(self, connected=True):
        self.connected = connected
        self.subscribed = {}
        self.unsubscribed = []

    def is_connected(self):
        return self.connected

    def subscribe(self, symbol, contract):
        self.subscribed[symbol] = contract
        return True

    def unsubscribe(self, symbol):
        self.unsubscribed.append(symbol)
        return True

    def get_current_price(self, symbol):
        return {"symbol": symbol, "price": 123.45}


class FakeOrder:
    def __init__(self, symbol):
        self.symbol = symbol
        self.to_ib_contract_called = False
        
    def to_ib_contract(self):
        self.to_ib_contract_called = True
        
        # Create a proper mock contract with all required attributes
        contract = MagicMock()
        contract.symbol = self.symbol
        contract.secType = "STK"  # Required attribute
        contract.exchange = "SMART"  # Required attribute  
        contract.currency = "USD"  # Required attribute
        
        return contract

@pytest.fixture
def data_feed():
    return FakeDataFeed()


@pytest.fixture
def service(data_feed):
    return MonitoringService(data_feed, interval_seconds=1)


def test_start_monitoring_success(service):
    check_cb = MagicMock()
    label_cb = MagicMock()
    result = service.start_monitoring(check_cb, label_cb)
    assert result is True
    assert service.monitoring is True
    assert service._check_callback == check_cb
    assert service._label_callback == label_cb
    service.stop_monitoring()


def test_start_monitoring_fail_if_not_connected():
    df = FakeDataFeed(connected=False)
    service = MonitoringService(df)
    result = service.start_monitoring(MagicMock(), MagicMock())
    assert result is False
    assert service.monitoring is False


def test_stop_monitoring(service):
    service.monitoring = True
    service.monitor_thread = MagicMock()
    service.monitor_thread.is_alive.return_value = False
    service.stop_monitoring()
    assert service.monitoring is False


def test_handle_monitoring_error_backoff(service):
    with patch("time.sleep") as mock_sleep:
        service._handle_monitoring_error(Exception("boom"))
        assert service.error_count == 1
        mock_sleep.assert_called_once()


def test_handle_periodic_labeling(service):
    label_cb = MagicMock()
    service._label_callback = label_cb

    # First call should trigger
    service._handle_periodic_labeling()
    assert label_cb.called
    last_time = service.last_labeling_time

    # Within 10 minutes should not trigger again
    service._handle_periodic_labeling()
    assert service.last_labeling_time == last_time

def test_subscribe_to_symbols_success(service):
    service.data_feed = MagicMock()
    service.data_feed.is_connected.return_value = True
    service.data_feed.subscribe.side_effect = lambda symbol, contract: True  # Always return True
    
    orders = [FakeOrder("AAPL"), FakeOrder("TSLA")]
    results = service.subscribe_to_symbols(orders)
    
    print(f"Results: {results}")
    
    assert results["AAPL"] is True
    assert results["TSLA"] is True
    
def test_subscribe_to_symbols_empty(service):
    results = service.subscribe_to_symbols([])
    assert results == {}


def test_unsubscribe_from_symbol(service):
    service.subscribed_symbols.add("AAPL")
    service.market_data_updates["AAPL"] = 1
    result = service.unsubscribe_from_symbol("AAPL")
    assert result is True
    assert "AAPL" not in service.subscribed_symbols


def test_unsubscribe_from_symbol_not_subscribed(service):
    assert service.unsubscribe_from_symbol("XYZ") is True


def test_unsubscribe_all(service):
    service.subscribed_symbols.update(["AAPL", "TSLA"])
    service.market_data_updates.update({"AAPL": 1, "TSLA": 2})
    service.unsubscribe_all()
    assert service.subscribed_symbols == set()


def test_get_subscription_stats(service):
    service.subscribed_symbols.add("AAPL")
    service.market_data_updates["AAPL"] = 5
    stats = service.get_subscription_stats()
    assert stats["total_subscriptions"] == 1
    assert stats["most_active_symbol"] == "AAPL"


def test_record_market_data_update(service):
    service.market_data_updates["AAPL"] = 0
    service.record_market_data_update("AAPL")
    assert service.market_data_updates["AAPL"] == 1


def test_is_symbol_subscribed(service):
    service.subscribed_symbols.add("AAPL")
    assert service.is_symbol_subscribed("AAPL") is True
    assert service.is_symbol_subscribed("TSLA") is False


def test_get_market_data_for_symbol(service):
    service.subscribed_symbols.add("AAPL")
    service.market_data_updates["AAPL"] = 0
    price = service.get_market_data_for_symbol("AAPL")
    assert price["price"] == 123.45


def test_get_market_data_for_symbol_not_subscribed(service):
    assert service.get_market_data_for_symbol("MSFT") is None


def test_set_monitoring_interval(service):
    service.set_monitoring_interval(10)
    assert service.interval_seconds == 10


def test_set_monitoring_interval_too_low(service):
    service.set_monitoring_interval(0)
    assert service.interval_seconds == 1  # unchanged


def test_reset_error_count(service):
    service.error_count = 5
    service.reset_error_count()
    assert service.error_count == 0


def test_is_healthy(service):
    service.monitoring = True
    service.error_count = 0
    assert service.is_healthy() is True

    service.error_count = service.max_errors
    assert service.is_healthy() is False
