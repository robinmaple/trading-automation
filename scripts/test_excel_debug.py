# scripts/test_excel_debug.py
import pandas as pd
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trading.orders.planned_order import PlannedOrderManager

def test_directly():
    """Test the excel parsing directly without any mocks"""
    print("Testing PlannedOrderManager.from_excel directly...")
    
    # Create the exact same data structure as your test
    mock_data = {
        'Security Type': ['STK'],
        'Exchange': ['NASDAQ'],
        'Currency': ['USD'],
        'Action': ['BUY'],
        'Symbol': ['AAPL'],
        'Order Type': ['LMT'],
        'Entry Price': [150.0],
        'Stop Loss': [145.0],
        'Position Management Strategy': ['CORE'],
        'Risk Per Trade': [None],
        'Risk Reward Ratio': [None],
        'Priority': [None],
        'Trading Setup': [None],
        'Core Timeframe': [None],
        'Overall Trend': ['Neutral'],
        'Brief Analysis': ['Test analysis']
    }
    
    # Save to temporary Excel file
    df = pd.DataFrame(mock_data)
    test_file = 'debug_test.xlsx'
    df.to_excel(test_file, index=False)
    
    print("Test data:")
    print(df)
    
    # Test with config
    test_config = {
        'order_defaults': {
            'risk_per_trade': 0.01,
            'risk_reward_ratio': 2.5,
            'priority': 4
        }
    }
    
    print("\nCalling from_excel...")
    try:
        orders = PlannedOrderManager.from_excel(test_file, test_config)
        print(f"Result: {len(orders)} orders")
        
        if orders:
            order = orders[0]
            print(f"risk_per_trade: {order.risk_per_trade}")
            print(f"risk_reward_ratio: {order.risk_reward_ratio}")
            print(f"priority: {order.priority}")
        else:
            print("No orders returned - checking for error logs...")
            
    except Exception as e:
        print(f"Exception occurred: {e}")
        import traceback
        traceback.print_exc()
    
    # Clean up
    if os.path.exists(test_file):
        os.remove(test_file)

if __name__ == "__main__":
    test_directly()