import sqlite3
conn = sqlite3.connect('trading_automation.db')
cursor = conn.cursor()

# Check all status values in planned_orders
cursor.execute('SELECT DISTINCT status FROM planned_orders')
statuses = cursor.fetchall()
print('Current status values in planned_orders:')
for status in statuses:
    print(f'  {status[0]}')

# Check all status values in executed_orders  
cursor.execute('SELECT DISTINCT status FROM executed_orders')
executed_statuses = cursor.fetchall()
print('Current status values in executed_orders:')
for status in executed_statuses:
    print(f'  {status[0]}')

conn.close()