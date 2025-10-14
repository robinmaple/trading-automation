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
        
        # Disconnect
        print("\n🔌 Disconnecting from IBKR...")
        client.disconnect()
        
    else:
        print("❌ Failed to connect to IBKR")
        
    print("🏁 TEST COMPLETED")

if __name__ == "__main__":
    main()