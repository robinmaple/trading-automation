import pytest
from src.core.order_executor import OrderExecutor
from src.core.planned_order import SecurityType, Action, OrderType

class TestOrderExecutor:
    
    def test_calculate_quantity_forex(self):
        """Test Forex position sizing calculation"""
        executor = OrderExecutor()
        
        # Test Forex (CASH) quantity calculation
        quantity = executor._calculate_quantity(
            SecurityType.CASH.value,  # entry_price=1.1000, stop_loss=1.0950
            1.1000, 1.0950, 100000, 0.001  # 0.1% risk on $100k account
        )
        
        # Risk per share: 0.0050 (50 pips)
        # Risk amount: $100 (0.1% of $100k)
        # Base quantity: $100 / 0.0050 = 20,000
        # Rounded to nearest 10,000: 20,000
        assert quantity == 20000
    
    def test_calculate_quantity_stocks(self):
        """Test stock position sizing calculation"""
        executor = OrderExecutor()
        
        # Test stock quantity calculation
        quantity = executor._calculate_quantity(
            SecurityType.STK.value,  # entry_price=100, stop_loss=95
            100, 95, 100000, 0.01  # 1% risk on $100k account
        )
        
        # Risk per share: $5
        # Risk amount: $1,000 (1% of $100k)
        # Quantity: $1,000 / $5 = 200 shares
        assert quantity == 200
    
    def test_calculate_profit_target_buy(self):
        """Test profit target calculation for BUY orders"""
        executor = OrderExecutor()
        
        # BUY order: entry=100, stop=95, risk_reward=2.0
        profit_target = executor._calculate_profit_target(
            Action.BUY.value, 100, 95, 2.0
        )
        
        # Risk amount: $5 per share
        # Profit target: $100 + ($5 * 2) = $110
        assert profit_target == 110.0
    
    def test_calculate_profit_target_sell(self):
        """Test profit target calculation for SELL orders"""
        executor = OrderExecutor()
        
        # SELL order: entry=100, stop=105, risk_reward=2.0
        profit_target = executor._calculate_profit_target(
            Action.SELL.value, 100, 105, 2.0
        )
        
        # Risk amount: $5 per share
        # Profit target: $100 - ($5 * 2) = $90
        assert profit_target == 90.0
    
    def test_create_bracket_order_buy(self):
        """Test bracket order creation for BUY orders"""
        executor = OrderExecutor()
        
        orders = executor.create_native_bracket_order(
            Action.BUY.value, OrderType.LMT.value, SecurityType.CASH.value,
            1.1000, 1.0950, 0.001, 2.0, 100000, 1
        )
        
        assert len(orders) == 3
        assert orders[0].action == "BUY"  # Parent order
        assert orders[1].action == "SELL"  # Take profit
        assert orders[2].action == "SELL"  # Stop loss
        assert orders[0].lmtPrice == 1.1000
        assert orders[2].auxPrice == 1.0950
    
    def test_create_bracket_order_sell(self):
        """Test bracket order creation for SELL orders"""
        executor = OrderExecutor()
        
        orders = executor.create_native_bracket_order(
            Action.SELL.value, OrderType.LMT.value, SecurityType.CASH.value,
            1.1000, 1.1050, 0.001, 2.0, 100000, 1
        )
        
        assert len(orders) == 3
        assert orders[0].action == "SELL"  # Parent order
        assert orders[1].action == "BUY"   # Take profit
        assert orders[2].action == "BUY"   # Stop loss
        assert orders[0].lmtPrice == 1.1000
        assert orders[2].auxPrice == 1.1050