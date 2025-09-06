import sys
import os
from pathlib import Path

# Add the src directory to Python path for imports
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

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
    from src.core.planned_order import PlannedOrder, SecurityType, Action, OrderType, PositionStrategy
    
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
        position_strategy=PositionStrategy.DAY
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
        is_live_trading=False
    )
    db_session.add(planned_order)
    db_session.commit()
    return planned_order