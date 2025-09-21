# run_sql.py
import sqlite3

# Path to your SQLite database file
db_path = "trading_automation.db"  # Change this as needed

# --- Paste your multi-line SQL script below ---
sql_script = """
SELECT * FROM position_strategies;
"""
# --- End of SQL script ---

def run_sql(db_path, sql_script):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Executescript handles multiple statements
        cursor.executescript(sql_script)
        conn.commit()
        print(f"SQL script executed successfully on '{db_path}'")
        
        # Optional: print results of the last SELECT statement
        try:
            cursor.execute(sql_script)  # Adjust if your script uses a different SELECT
            rows = cursor.fetchall()
            print("Query results:")
            for row in rows:
                print(row)
        except sqlite3.Error:
            pass  # No SELECT statement to fetch
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_sql(db_path, sql_script)
