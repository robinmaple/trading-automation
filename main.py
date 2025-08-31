from order_executor import OrderExecutor
import time

def main():
    # Create and configure the order executor
    executor = OrderExecutor()
    
    # Connect to IB
    if not executor.connect_to_ib('127.0.0.1', 7497, 0):
        print("Failed to connect to IB")
        return
    
    # Wait for connection to be ready
    if not executor.wait_for_connection(timeout=10):
        print("Connection timeout")
        return
    
    print("Successfully connected to IB API")
    
    # Create Forex contract
    eurusd_contract = executor.create_forex_contract('EUR', 'USD')
    
    # For testing - use a fixed price. In real usage, get from market data
    entry_price = 1.0850
    
    try:
        # Place bracket order
        order_ids = executor.place_bracket_order(
            contract=eurusd_contract,
            entry_price=entry_price,
            quantity=20000,      # Minimum for IdealPro
            profit_pct=0.005,    # 0.5% profit target
            loss_pct=0.005       # 0.5% stop loss
        )
        
        if order_ids:
            print(f"Successfully placed bracket order with IDs: {order_ids}")
        else:
            print("Failed to place bracket order")
            
    except Exception as e:
        print(f"Error placing order: {e}")
    
    # Keep the program running to monitor order status
    try:
        time.sleep(20)
        print("Monitoring completed")
    except KeyboardInterrupt:
        print("Interrupted by user")
    
    # Clean disconnect
    executor.disconnect()

if __name__ == "__main__":
    main()