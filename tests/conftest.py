import sys
import os
from pathlib import Path

from src.trading.orders.order_persistence_service import OrderPersistenceService
from src.services.state_service import StateService

# Add the src directory to Python path for imports
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

import pandas as pd
import pytest
from unittest.mock import Mock, MagicMock, patch

# Database Testing - Begin
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from src.core.database import DatabaseManager, init_database
from src.core.models import Base, PlannedOrderDB, PositionStrategy
# Database Testing - End


# Global database manager for tests
_test_db_manager = None

@pytest.fixture(scope="session", autouse=True)
def initialize_test_database():
    """Initialize test database once per test session"""
    global _test_db_manager
    
    # Use in-memory SQLite for tests
    _test_db_manager = DatabaseManager(":memory:")
    _test_db_manager.engine = create_engine("sqlite:///:memory:")
    _test_db_manager.Session = scoped_session(sessionmaker(bind=_test_db_manager.engine))
    
    # Create all tables
    Base.metadata.create_all(_test_db_manager.engine)
    
    # Initialize the global database manager
    from src.core.database import db_manager
    db_manager.engine = _test_db_manager.engine
    db_manager.Session = _test_db_manager.Session
    
    yield
    
    # Cleanup
    _test_db_manager.Session.remove()
    _test_db_manager.engine.dispose()

@pytest.fixture
def mock_data_feed():
    """Fixture for mocking AbstractDataFeed"""
    mock_feed = Mock()
    mock_feed.is_connected.return_value = True
    mock_feed.get_current_price.return_value = {
        'price': 100.0,
        'timestamp': '2024-01-01 12:00:00',
        'data_type': 'MOCK'
    }
    return mock_feed

@pytest.fixture
def mock_ibkr_client():
    """Fixture for mocking IbkrClient"""
    mock_client = Mock()
    mock_client.connected = True
    mock_client.get_account_value.return_value = 100000.0
    mock_client.place_bracket_order.return_value = [1, 2, 3]  # Mock order IDs
    mock_client.is_paper_account = False
    mock_client.account_number = "U1234567"
    return mock_client

@pytest.fixture
def sample_planned_order():
    """Fixture for creating a sample planned order"""
    from src.trading.orders.planned_order import PlannedOrder, SecurityType, Action, OrderType, PositionStrategy
    
    return PlannedOrder(
        security_type=SecurityType.CASH,
        exchange="IDEALPRO",
        currency="USD",
        action=Action.BUY,
        symbol="EUR",
        order_type=OrderType.LMT,
        risk_per_trade=0.001,
        entry_price=1.1000,
        stop_loss=1.0950,
        risk_reward_ratio=2.0,
        position_strategy=PositionStrategy.DAY,
        priority=3,
        # Phase B Additions - Begin
        trading_setup="Breakout",
        core_timeframe="15min"
        # Phase B Additions - End
    )

# Database Testing - Begin
@pytest.fixture(scope="function")
def test_db():
    """Create a test database in memory for isolated testing"""
    # Use in-memory SQLite for tests
    db_manager = DatabaseManager(":memory:")
    db_manager.engine = create_engine("sqlite:///:memory:")
    db_manager.Session = scoped_session(sessionmaker(bind=db_manager.engine))
    
    # Create all tables
    Base.metadata.create_all(db_manager.engine)
    
    yield db_manager
    
    # Cleanup
    db_manager.Session.remove()
    db_manager.engine.dispose()

@pytest.fixture(scope="function")
def db_session(test_db):
    """Provide a database session for tests"""
    session = test_db.get_session()
    
    # Initialize default position strategies - check if they exist first
    strategy_names = ["DAY", "CORE", "HYBRID"]
    for name in strategy_names:
        existing = session.query(PositionStrategy).filter_by(name=name).first()
        if not existing:
            session.add(PositionStrategy(name=name))
    session.commit()
    
    yield session
    
    # Cleanup
    session.rollback()
    session.close()

@pytest.fixture
def position_strategies(db_session):
    """Fixture providing access to position strategies"""
    return {
        "DAY": db_session.query(PositionStrategy).filter_by(name="DAY").first(),
        "CORE": db_session.query(PositionStrategy).filter_by(name="CORE").first(),
        "HYBRID": db_session.query(PositionStrategy).filter_by(name="HYBRID").first()
    }
# Database Testing - End

@pytest.fixture
def sample_planned_order_db(db_session, position_strategies):
    """Fixture for creating a sample planned order in database"""
    planned_order = PlannedOrderDB(
        symbol="EUR",
        security_type="CASH",
        action="BUY",
        order_type="LMT",
        entry_price=1.1000,
        stop_loss=1.0950,
        risk_per_trade=0.001,
        risk_reward_ratio=2.0,
        position_strategy_id=position_strategies["DAY"].id,
        status="PENDING",
        is_live_trading=False,
        priority=3,
        # Phase B Additions - Begin
        core_timeframe="15min"
        # trading_setup would be set via relationship if needed
        # Phase B Additions - End
    )
    db_session.add(planned_order)
    db_session.commit()
    return planned_order

# Phase B Additions - Begin
@pytest.fixture
def mock_probability_engine():
    """Fixture for mocking ProbabilityEngine with Phase B features"""
    mock_engine = Mock()
    
    # Mock the Phase B enhanced score_fill method
    mock_features = {
        'timestamp': '2024-01-01T12:00:00',
        'time_of_day_seconds': 43200,
        'day_of_week': 0,
        'current_price': 100.0,
        'bid': 99.95,
        'ask': 100.05,
        'spread_absolute': 0.10,
        'symbol': 'TEST',
        'order_side': 'BUY',
        'priority_manual': 3,
        'trading_setup': 'Breakout',
        'core_timeframe': '15min'
    }
    mock_engine.score_fill.return_value = (0.85, mock_features)  # (probability, features)
    mock_engine.should_execute_order.return_value = (True, 0.85)
    
    return mock_engine

@pytest.fixture
def phase_b_test_data():
    """Fixture providing sample Phase B feature data for testing"""
    return {
        'timestamp': '2024-01-01T12:00:00',
        'time_of_day_seconds': 43200,  # 12:00:00
        'day_of_week': 0,  # Monday
        'seconds_since_midnight': 43200,
        'current_price': 150.25,
        'bid': 150.20,
        'ask': 150.30,
        'bid_size': 1000,
        'ask_size': 1500,
        'last_price': 150.25,
        'volume': 25000,
        'spread_absolute': 0.10,
        'spread_relative': 0.000665,
        'symbol': 'AAPL',
        'order_side': 'BUY',
        'order_type': 'LMT',
        'entry_price': 150.00,
        'stop_loss': 148.50,
        'priority_manual': 3,
        'trading_setup': 'Breakout',
        'core_timeframe': '15min',
        'price_diff_absolute': 0.25,
        'price_diff_relative': 0.001667,
        'volatility_estimate': 0.001
    }

@pytest.fixture
def sample_planned_order_with_phase_b():
    """Fixture for creating a sample planned order with Phase B fields"""
    from src.trading.orders.planned_order import PlannedOrder, SecurityType, Action, OrderType, PositionStrategy
    
    return PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,
        symbol="AAPL",
        order_type=OrderType.LMT,
        risk_per_trade=0.005,
        entry_price=150.00,
        stop_loss=148.50,
        risk_reward_ratio=2.0,
        position_strategy=PositionStrategy.CORE,
        priority=3,
        trading_setup="Breakout",
        core_timeframe="15min"
    )
# Phase B Additions - End

# Phase B Additions - Begin (continued)
@pytest.fixture
def mock_execution_service():
    """Fixture for mocking OrderExecutionService with Phase B attempt tracking."""
    mock_service = Mock()
    
    # Mock the Phase B enhanced methods
    mock_service.execute_single_order.return_value = True
    mock_service.place_order.return_value = True
    mock_service.cancel_order.return_value = True
    
    # Mock attempt tracking method
    mock_service._record_order_attempt = Mock()
    mock_service._record_order_attempt.return_value = 999  # Mock attempt ID
    
    return mock_service

@pytest.fixture
def sample_order_attempt_data():
    """Fixture providing sample order attempt data for testing."""
    return {
        'planned_order_id': 1,
        'attempt_type': 'PLACEMENT',
        'fill_probability': 0.85,
        'effective_priority': 0.75,
        'quantity': 100,
        'capital_commitment': 15000.0,
        'status': 'SUBMITTED',
        'ib_order_ids': [101, 102, 103],
        'details': {'message': 'Order placed successfully'}
    }

@pytest.fixture
def mock_order_persistence():
    """Fixture for mocking OrderPersistenceService with Phase B support."""
    mock_persistence = Mock()
    
    # Mock database session for attempt tracking
    mock_persistence.db_session = Mock()
    mock_persistence.db_session.add = Mock()
    mock_persistence.db_session.commit = Mock()
    
    # Mock other persistence methods
    mock_persistence.validate_sufficient_margin.return_value = (True, "Margin OK")
    mock_persistence.record_order_execution.return_value = 123
    mock_persistence.update_order_status.return_value = True
    mock_persistence.handle_order_rejection.return_value = True
    
    return mock_persistence
# Phase B Additions - End

@pytest.fixture
def mock_historical_performance_service():
    """Create a properly mocked historical performance service."""
    mock_service = Mock()
    mock_service.get_symbol_performance.return_value = {
        'success_rate': 75.0,
        'total_trades': 20,
        'winning_trades': 15,
        'total_pnl': 2500.0
    }
    mock_service.calculate_performance_score.return_value = 85.0
    mock_service.get_top_performing_symbols.return_value = [
        {'symbol': 'AAPL', 'performance_score': 90.0},
        {'symbol': 'MSFT', 'performance_score': 85.0}
    ]
    return mock_service

@pytest.fixture
def mock_market_context_service():
    """Create a properly mocked market context service."""
    mock_service = Mock()
    
    # Mock market context data
    mock_context_data = {
        'trend': 'bullish',
        'volatility': 'medium',
        'volume_profile': 'normal',
        'support_levels': [145.0, 142.5],
        'resistance_levels': [155.0, 157.5],
        'rsi': 65.0,
        'macd_signal': 'bullish'
    }
    
    mock_service.analyze_market_context.return_value = mock_context_data
    mock_service.get_market_context.return_value = pd.DataFrame({
        'close': [150 + i for i in range(20)]
    })
    
    return mock_service

@pytest.fixture
def mock_prioritization_service(mock_historical_performance_service, mock_market_context_service):
    """Create a properly mocked prioritization service."""
    mock_service = Mock()
    
    # Mock the calculate_priority method
    mock_service.calculate_priority.return_value = {
        'symbol': 'AAPL',
        'priority_score': 85.0,
        'factors': {
            'technical_score': 80.0,
            'performance_score': 85.0,
            'market_context_score': 90.0
        }
    }
    
    # Mock the properties that might be accessed
    mock_service.historical_performance_service = mock_historical_performance_service
    mock_service.market_context_service = mock_market_context_service
    
    return mock_service

@pytest.fixture
def mock_state_service():
    """Mock StateService for testing."""
    mock_service = Mock(spec=StateService)
    # Add any necessary mock methods or return values
    mock_service.get_current_state.return_value = {'trading_enabled': True}
    return mock_service

@pytest.fixture
def mock_persistence_service():
    """Mock OrderPersistenceService for testing."""
    mock_service = Mock(spec=OrderPersistenceService)
    # Add any necessary mock methods or return values
    mock_service.save_order.return_value = True
    mock_service.load_orders.return_value = []
    return mock_service
