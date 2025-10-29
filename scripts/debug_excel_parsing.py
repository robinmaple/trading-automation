# scripts/debug_excel_parsing.py
import pandas as pd
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.trading.orders.planned_order import PlannedOrderManager

def test_excel_parsing():
    """Test the excel parsing directly without test mocks"""
    # Create test data that matches what your tests use
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
        'Overall Trend': ['Bull'],
        'Brief Analysis': [None]
    }
    
    # Save to temporary Excel file
    df = pd.DataFrame(mock_data)
    test_file = 'test_debug.xlsx'
    df.to_excel(test_file, index=False)
    
    print("Testing with data:")
    print(df.to_string())
    
    # Test with config
    test_config = {
        'order_defaults': {
            'risk_per_trade': 0.01,
            'risk_reward_ratio': 2.5,
            'priority': 4
        }
    }
    
    print("\nCalling from_excel with config...")
    orders = PlannedOrderManager.from_excel(test_file, test_config)
    print(f"Result: {len(orders)} orders")
    
    if orders:
        print("First order details:")
        for attr in ['risk_per_trade', 'risk_reward_ratio', 'priority']:
            print(f"  {attr}: {getattr(orders[0], attr)}")
    
    # Clean up
    if os.path.exists(test_file):
        os.remove(test_file)

if __name__ == "__main__":
    test_excel_parsing()