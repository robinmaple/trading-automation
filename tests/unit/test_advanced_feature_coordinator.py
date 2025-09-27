# tests/unit/test_advanced_feature_coordinator.py
import pytest
from unittest.mock import Mock, create_autospec
import datetime

from src.core.advanced_feature_coordinator import AdvancedFeatureCoordinator
from src.core.planned_order import PlannedOrder, Action
from src.core.abstract_data_feed import AbstractDataFeed
from src.services.position_sizing_service import PositionSizingService


# ------------------------
# Fixtures
# ------------------------
# In conftest.py or the test file, find the valid_order fixture and update it:

@pytest.fixture
def valid_order():
    from src.core.planned_order import PlannedOrder, SecurityType, Action, PositionStrategy
    
    return PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,
        symbol="AAPL",
        entry_price=150.0,  # ← ADD REQUIRED FIELD
        stop_loss=145.0,    # ← ADD REQUIRED FIELD
        overall_trend="Bull", # ← ADD REQUIRED FIELD
        # Include other required fields as needed
        risk_per_trade=0.01,
        risk_reward_ratio=2.0,
        priority=3,
        position_strategy=PositionStrategy.DAY
    )

@pytest.fixture
def mock_data_feed():
    return create_autospec(AbstractDataFeed)


@pytest.fixture
def mock_sizing_service():
    return create_autospec(PositionSizingService)


@pytest.fixture
def mock_order_persistence():
    return Mock()


@pytest.fixture
def mock_market_context_service():
    svc = Mock()
    svc.analyze_symbol.return_value = {"trend": "bullish"}
    svc.cleanup = Mock()
    return svc


@pytest.fixture
def mock_historical_performance_service(mock_order_persistence):
    svc = Mock()
    svc.analyze_symbol_performance.return_value = {"score": 0.9}
    svc.cleanup = Mock()
    return svc


@pytest.fixture
def mock_outcome_labeling_service():
    svc = Mock()
    svc.label_completed_orders.return_value = {
        "total_orders": 5,
        "labeled_orders": 5,
        "labels_created": 5,
        "errors": 0,
    }
    svc.export_training_data.return_value = True
    return svc


@pytest.fixture
def mock_prioritization_service():
    svc = Mock()
    svc.prioritize_orders.side_effect = lambda orders, total, working: [
        {**o, "advanced_features": True} for o in orders
    ]
    svc.get_prioritization_summary.side_effect = lambda orders: {
        "total_orders": len(orders),
        "advanced_features_enabled": True,
    }
    return svc


@pytest.fixture
def coordinator_enabled(
    mock_data_feed,
    mock_sizing_service,
    mock_order_persistence,
    mock_market_context_service,
    mock_historical_performance_service,
    mock_outcome_labeling_service,
    mock_prioritization_service,
):
    coordinator = AdvancedFeatureCoordinator(enabled=True)

    # Inject mocks directly
    coordinator.market_context_service = mock_market_context_service
    coordinator.historical_performance_service = mock_historical_performance_service
    coordinator.outcome_labeling_service = mock_outcome_labeling_service
    coordinator.prioritization_service = mock_prioritization_service
    coordinator.initialized = True
    return coordinator


@pytest.fixture
def coordinator_disabled():
    return AdvancedFeatureCoordinator(enabled=False)


# ------------------------
# Tests
# ------------------------
def test_initialize_services_disabled(coordinator_disabled):
    result = coordinator_disabled.initialize_services(None, None, None, {})
    assert result is False


def test_label_completed_orders_success(coordinator_enabled):
    summary = coordinator_enabled.label_completed_orders(12)
    assert summary["total_orders"] == 5
    assert summary["labeled_orders"] == 5
    assert summary["labels_created"] == 5
    assert summary["errors"] == 0


def test_label_completed_orders_failure(coordinator_enabled):
    coordinator_enabled.outcome_labeling_service.label_completed_orders.side_effect = Exception("db error")
    summary = coordinator_enabled.label_completed_orders(12)
    assert summary["errors"] == 1


def test_generate_training_data_success(coordinator_enabled):
    result = coordinator_enabled.generate_training_data("file.csv")
    assert result is True


def test_generate_training_data_failure(coordinator_enabled):
    coordinator_enabled.outcome_labeling_service.export_training_data.side_effect = Exception("disk full")
    result = coordinator_enabled.generate_training_data("file.csv")
    assert result is False


def test_enhance_order_prioritization_success(coordinator_enabled):
    orders = [{"id": 1}]
    enhanced = coordinator_enabled.enhance_order_prioritization(orders, 1000, [])
    assert "advanced_features" in enhanced[0]


def test_enhance_order_prioritization_failure(coordinator_enabled):
    coordinator_enabled.prioritization_service.prioritize_orders.side_effect = Exception("bad prio")
    orders = [{"id": 1}]
    enhanced = coordinator_enabled.enhance_order_prioritization(orders, 1000, [])
    assert enhanced == orders  # fallback to input


def test_get_prioritization_summary_success(coordinator_enabled):
    summary = coordinator_enabled.get_prioritization_summary([{"viable": True}])
    assert summary["advanced_features_enabled"] is True


def test_get_prioritization_summary_failure(coordinator_enabled):
    coordinator_enabled.prioritization_service.get_prioritization_summary.side_effect = Exception("fail")
    summary = coordinator_enabled.get_prioritization_summary([{"viable": True}])
    assert summary["advanced_features_enabled"] is False


def test_analyze_market_context_success(coordinator_enabled):
    result = coordinator_enabled.analyze_market_context("AAPL")
    assert result == {"trend": "bullish"}


def test_get_historical_performance_success(coordinator_enabled):
    result = coordinator_enabled.get_historical_performance("AAPL", 10)
    assert result == {"score": 0.9}


def test_get_feature_engineering_report(coordinator_enabled):
    report = coordinator_enabled.get_feature_engineering_report()
    assert report["advanced_features_enabled"] is True
    assert report["services_initialized"] is True


def test_validate_advanced_configuration_all_good(coordinator_enabled):
    ok, issues = coordinator_enabled.validate_advanced_configuration()
    assert ok is True
    assert issues == []


def test_validate_advanced_configuration_with_issues(coordinator_disabled):
    ok, issues = coordinator_disabled.validate_advanced_configuration()
    assert ok is True
    assert issues == ["Advanced features are disabled"]


def test_shutdown_calls_cleanup(coordinator_enabled):
    coordinator_enabled.shutdown()
    coordinator_enabled.market_context_service.cleanup.assert_called_once()
    coordinator_enabled.historical_performance_service.cleanup.assert_called_once()


def test_precompute_features_success(coordinator_enabled, valid_order):
    features = coordinator_enabled.precompute_features([valid_order])
    assert features[0]["symbol"] == "AAPL"
    assert "basic_features" in features[0]
    assert "market_context" in features[0]
    assert "historical_performance" in features[0]
