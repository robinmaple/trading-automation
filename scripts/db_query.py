"""
Quick script to inspect the DB schema and see what `order_type` stores
for PlannedOrderDB.
"""

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from src.core.models import Base, PlannedOrderDB
from src.core.planned_order import OrderType

# Use in-memory SQLite for a quick check
engine = create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine)

# Inspect schema
insp = inspect(engine)
print("=== Table Schema ===")
for col in insp.get_columns("planned_order_db"):
    print(f"{col['name']} -> {col['type']}")

# Insert a sample order
Session = sessionmaker(bind=engine)
session = Session()

sample_order = PlannedOrderDB(
    symbol="AAPL",
    entry_price=150.0,
    stop_loss=145.0,
    action="BUY",
    order_type=OrderType.LMT,  # 👈 critical part
    status="NEW"
)

session.add(sample_order)
session.commit()

# Read back from raw SQL
print("\n=== Stored Values ===")
rows = engine.execute(text("SELECT symbol, order_type FROM planned_order_db")).fetchall()
for row in rows:
    print(dict(row._mapping))
