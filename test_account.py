#!/usr/bin/env python3
"""
Quick test script for IBKR account value retrieval
Run this to diagnose the account value issue
"""

import sys
import os
import time

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.core.ibkr_client import IbkrClient

def enhanced_diagnostic(client):
    print("🔍 ENHANCED DIAGNOSTIC")
    
    # Test 1: Get managed accounts
    print("1. Testing managed accounts...")
    time.sleep(2)
    # This often triggers account data flow
    
    # Test 2: Request specific currency
    print("2. Testing USD-specific requests...")
    client.reqAccountSummary(2, "All", "NetLiquidation;TotalCashValue")
    time.sleep(5)
    
    # Test 3: Check if it's a permissions issue
    print("3. Testing basic portfolio...")
    client.reqAccountUpdates(True, "U20131583")

def debug_account_values(self):
    """Debug what's actually in account_values"""
    print("🔍 DEBUG: CURRENT account_values CONTENTS:")
    for key, value in list(self.account_values.items()):  # Create a copy with list()
        if isinstance(value, dict):
            print(f"   {key}: {value} (DICT)")
            for subkey, subval in value.items():
                print(f"     {subkey}: {subval} (type: {type(subval)})")
        else:
            print(f"   {key}: {value} (type: {type(value)})")
    
    # Check for our processed values
    print("🔍 DEBUG: Looking for processed numeric values...")
    numeric_fields = ['NetLiquidation_CAD', 'AvailableFunds_CAD', 'BuyingPower_CAD', 'TotalCashValue_CAD']
    for field in numeric_fields:
        if field in self.account_values:
            print(f"✅ FOUND {field}: {self.account_values[field]}")
        else:
            print(f"❌ MISSING {field}")

def main():
    print("🚀 STARTING IBKR ACCOUNT RETRIEVAL TEST")
    
    # Create client instance
    client = IbkrClient(host='127.0.0.1', port=7496, client_id=1, mode='live')
    
    print("🔗 Connecting to IBKR...")
    if client.connect():
        print("✅ Connected to IBKR successfully!")
        
        # Wait a moment for connection to stabilize
        time.sleep(2)
        
        # Run the diagnostic test
        client.test_account_retrieval()
        
        # Run enhanced diagnostic
        enhanced_diagnostic(client)
        debug_account_values(client)

        # Disconnect
        print("\n🔌 Disconnecting from IBKR...")
        client.disconnect()
        
    else:
        print("❌ Failed to connect to IBKR")
        
    print("🏁 TEST COMPLETED")

if __name__ == "__main__":
    main()