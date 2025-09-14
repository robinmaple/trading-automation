# scripts/sql_runner.py
import sqlite3
import sys

DB_PATH = "trading_automation.db"  # Adjust if your DB is elsewhere


def run_query(query: str, db_path: str = DB_PATH):
    """Run any SQL query against the SQLite database and print results."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(query)

        if query.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            print("=== QUERY RESULTS ===")
            print(" | ".join(col_names))
            print("-" * (len(col_names) * 15))
            for row in rows:
                print(" | ".join(str(v) if v is not None else "NULL" for v in row))
        else:
            conn.commit()
            print(f"✅ Query executed successfully: {query}")

    except Exception as e:
        print(f"❌ Error executing query: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.sql_runner \"SQL_QUERY\"")
        sys.exit(1)

    query = sys.argv[1]
    run_query(query)

# python -m scripts.sql_runner "ALTER TABLE planned_orders ADD COLUMN core_timeframe VARCHAR(50);"
# python -m scripts.sql_runner ""
# python -m scripts.sql_runner ""
# python -m scripts.sql_runner ""
# python -m scripts.sql_runner ""