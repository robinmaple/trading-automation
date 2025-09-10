import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import Mock

from src.core.events import OrderState
from src.services.state_service import StateService
from src.core.models import Base, PlannedOrderDB, ExecutedOrderDB, PositionStrategy


@pytest.fixture(scope="function")
def test_db_session():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Add a position strategy
    strategy = PositionStrategy(name="DAY")
    session.add(strategy)
    session.commit()
    
    yield session
    session.close()


class TestStateServiceIntegration:
    """Integration tests for StateService with actual database."""
    
    def test_state_persistence(self, test_db_session):
        """Test that state changes are properly persisted to database."""
        # Create a planned order - use string 'PENDING' instead of OrderState.PENDING
        strategy = test_db_session.query(PositionStrategy).first()
        order = PlannedOrderDB(
            symbol="AAPL",
            entry_price=150.0,
            stop_loss=145.0,
            action="BUY",
            order_type="LMT",
            security_type="STK",
            risk_per_trade=0.01,
            risk_reward_ratio=2.0,
            priority=3,
            position_strategy_id=strategy.id,
            status='PENDING'  # Use string instead of OrderState.PENDING
        )
        test_db_session.add(order)
        test_db_session.commit()
        
        # Create StateService and update state
        state_service = StateService(test_db_session)
        result = state_service.update_planned_order_state(
            order.id, OrderState.LIVE_WORKING, "test"
        )
        
        assert result is True
        
        # Verify state was persisted
        updated_order = test_db_session.query(PlannedOrderDB).filter_by(id=order.id).first()
        assert updated_order.status == 'LIVE_WORKING'  # Should be string value
    
    def test_open_position_tracking(self, test_db_session):
        """Test open position tracking functionality."""
        # Create a planned order and executed order - use string 'FILLED'
        strategy = test_db_session.query(PositionStrategy).first()
        planned_order = PlannedOrderDB(
            symbol="AAPL",
            entry_price=150.0,
            stop_loss=145.0,
            action="BUY",
            order_type="LMT",
            security_type="STK",
            risk_per_trade=0.01,
            risk_reward_ratio=2.0,
            priority=3,
            position_strategy_id=strategy.id,
            status='FILLED'  # Use string instead of OrderState.FILLED
        )
        test_db_session.add(planned_order)
        test_db_session.commit()
        
        executed_order = ExecutedOrderDB(
            planned_order_id=planned_order.id,
            filled_price=150.0,
            filled_quantity=10,
            commission=1.0,
            pnl=0.0,
            status="FILLED",
            is_open=True
        )
        test_db_session.add(executed_order)
        test_db_session.commit()
        
        # Test open position detection - FIX: Use planned_order.symbol instead of executed_order.symbol
        state_service = StateService(test_db_session)
        open_positions = state_service.get_open_positions("AAPL")  # Filter by symbol
        assert len(open_positions) == 1
        
        # Test has_open_position
        has_position = state_service.has_open_position("AAPL")
        assert has_position is True
        
        # Test closing position
        result = state_service.close_position(executed_order.id, 155.0, 10, 1.0)
        assert result is True
        
        # Verify position is closed
        closed_position = test_db_session.query(ExecutedOrderDB).filter_by(id=executed_order.id).first()
        assert closed_position.is_open is False
        assert closed_position.pnl == (155.0 - 150.0) * 10 - 1.0 - 1.0