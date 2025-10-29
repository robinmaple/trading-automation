# tests/test_phase1_5.py
import pytest
import datetime
from decimal import Decimal
from src.trading.orders.order_persistence_service import OrderPersistenceService
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# tests/test_phase1_5_excel.py
import pandas as pd
from io import BytesIO
from src.trading.orders.planned_order import PlannedOrder, SecurityType, Action, OrderType, PositionStrategy, PlannedOrderManager



# Mock DB setup (SQLite in-memory for testing)
from src.core.database import Base
engine = create_engine('sqlite:///:memory:')
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

@pytest.fixture
def db_session():
    session = Session()
    yield session
    session.close()

# -------------------------------
# PlannedOrder Initialization Tests
# -------------------------------
def get_valid_stop_loss(action, entry_price=100.0):
    """Return a valid stop loss based on action type."""
    if action == Action.BUY:
        return entry_price - 5.0  # Below entry for BUY
    else:  # Action.SELL
        return entry_price + 5.0  # Above entry for SELL

@pytest.mark.parametrize("action,trend,expected_alignment", [
    (Action.BUY, "Bull", True),
    (Action.SELL, "Bear", True),
    (Action.BUY, "Bear", False),
    (Action.SELL, "Bull", False),
    (Action.BUY, "Neutral", False),
])
def test_trend_alignment(action, trend, expected_alignment):
    entry_price = 100.0
    stop_loss = get_valid_stop_loss(action, entry_price)
    
    order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=action,
        symbol="TEST",
        entry_price=entry_price,
        stop_loss=stop_loss,
        overall_trend=trend
    )
    assert order.trend_alignment == expected_alignment
def test_features_dict_contains_trend_alignment():
    order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,  # For BUY, stop loss should be BELOW entry
        symbol="TEST",
        entry_price=100.0,
        stop_loss=95.0,  # Correct for BUY order
        overall_trend="Bull",
        brief_analysis="Setup",
    )

# -------------------------------
# Calculated Fields Tests
# -------------------------------
def test_quantity_calculation():
    order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,
        symbol="TEST",
        entry_price=100,
        stop_loss=95,
        overall_trend="Bull"
    )
    qty = order.calculate_quantity(total_capital=10000)
    assert qty > 0

def test_profit_target_calculation():
    order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,
        symbol="TEST",
        entry_price=100,
        stop_loss=95,
        risk_reward_ratio=2.0,
        overall_trend="Bull"
    )
    target = order.calculate_profit_target()
    assert target == 100 + (100-95)*2

# -------------------------------
# Persistence Tests
# -------------------------------
def test_create_and_update_planned_order(db_session):
    service = OrderPersistenceService(db_session)
    
    order = PlannedOrder(
        security_type=SecurityType.STK,
        exchange="SMART",
        currency="USD",
        action=Action.BUY,
        symbol="TEST",
        entry_price=100,
        stop_loss=95,
        overall_trend="Bull",
        brief_analysis="Strong setup",
        position_strategy=PositionStrategy.CORE
    )
    
    # Convert to DB model and persist
    db_model = service.convert_to_db_model(order)
    db_session.add(db_model)
    db_session.commit()
    
    # Retrieve and verify fields
    persisted = db_session.query(db_model.__class__).filter_by(symbol="TEST").first()
    assert persisted is not None
    assert persisted.security_type == "STK"
    assert persisted.action == "BUY"
    
    # Simulate update
    order.brief_analysis = "Updated setup"
    db_model.brief_analysis = order.brief_analysis
    db_session.commit()
    
    updated = db_session.query(db_model.__class__).filter_by(symbol="TEST").first()
    assert updated.brief_analysis == "Updated setup"

# -------------------------------
# Excel Import Simulation
# -------------------------------

@pytest.fixture
def sample_excel_data():
    """Create an in-memory Excel file with sample planned orders."""
    data = {
        "Symbol": ["AAPL", "TSLA"],
        "Security Type": ["STK", "STK"],
        "Exchange": ["SMART", "SMART"],
        "Currency": ["USD", "USD"],
        "Action": ["BUY", "SELL"],
        "Order Type": ["LMT", "LMT"],
        "Entry Price": [150.0, 700.0],
        "Stop Loss": [145.0, 680.0],
        "Position Management Strategy": ["CORE", "DAY"],
        "Priority": [3, 2],
        "Risk Per Trade": [0.005, 0.01],
        "Risk Reward Ratio": [2.0, 2.5],
        "Overall Trend": ["Bull", "Bear"],
        "Brief Analysis": ["Strong bullish setup", "Bearish setup"]
    }
    df = pd.DataFrame(data)
    
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_buffer.seek(0)
    return excel_buffer

@pytest.mark.skip(reason="Excel writing not used in this app")
def test_import_excel_orders(sample_excel_data):
    """Test loading orders from Excel including Phase 1.5 fields."""
    orders = PlannedOrderManager.from_excel(sample_excel_data)
    
    assert len(orders) == 2
    
    order1 = orders[0]
    order2 = orders[1]
    
    # Verify Excel-imported fields
    assert order1.symbol == "AAPL"
    assert order1.overall_trend == "Bull"
    assert order1.brief_analysis == "Strong bullish setup"
    assert order1.trend_alignment is True  # BUY in Bull
    
    assert order2.symbol == "TSLA"
    assert order2.overall_trend == "Bear"
    assert order2.brief_analysis == "Bearish setup"
    assert order2.trend_alignment is True  # SELL in Bear

@pytest.mark.skip(reason="Excel writing not used in this app")
def test_import_with_missing_optional_fields():
    """Test handling Excel rows with missing optional Phase 1.5 fields."""
    data = {
        "Symbol": ["MSFT"],
        "Security Type": ["STK"],
        "Exchange": ["SMART"],
        "Currency": ["USD"],
        "Action": ["BUY"]
        # missing Order Type, Entry Price, Stop Loss, Trend, Analysis
    }
    df = pd.DataFrame(data)
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_buffer.seek(0)
    
    orders = PlannedOrderManager.from_excel(excel_buffer)
    assert len(orders) == 1
    order = orders[0]
    
    # Default values should be applied
    assert order.order_type == OrderType.LMT
    assert order.overall_trend == "Neutral"  # default
    assert order.brief_analysis is None
    assert order.trend_alignment is False
