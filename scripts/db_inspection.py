# scripts/inspect_db_schema.py
import sys
import os
import sqlite3

# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from src.core.database import get_db_session, Base, db_manager, init_database
from src.core.models import PlannedOrderDB

def inspect_current_schema(db_path="trading_automation.db"):
    """Connect to the DB directly via sqlite3 and print the schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=== EXISTING TABLES (Raw SQLite) ===")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table in tables:
        print(f"Table: {table[0]}")
        
        cursor.execute(f"PRAGMA table_info({table[0]});")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  Column: {col[1]} (Type: {col[2]}, Nullable: {not col[3]}, PK: {col[5]})")
        print()
    conn.close()

def check_model_vs_db():
    """Use SQLAlchemy metadata to see expected schema."""
    print("=== SQLALCHEMY METADATA (Expected Schema) ===")
    for table_name, table_obj in Base.metadata.tables.items():
        print(f"Table: {table_name}")
        for column in table_obj.columns:
            print(f"  Column: {column.name} (Type: {column.type}, "
                  f"Nullable: {column.nullable}, PK: {column.primary_key})")
        print()

    if db_manager.engine:
        print("\n✅ Engine is initialized and ready")
    else:
        print("\n⚠️ Engine not initialized (call init_database() first)")

if __name__ == "__main__":
    print("Inspecting current database state...\n")

    # Initialize DB (ensures engine + tables + default data)
    init_database()

    inspect_current_schema("trading_automation.db")
    print("\n" + "="*50 + "\n")
    check_model_vs_db()