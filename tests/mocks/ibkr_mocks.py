from unittest.mock import Mock, MagicMock
from ibapi.contract import Contract
from ibapi.order import Order

def mock_contract(symbol="EUR", sec_type="CASH", exchange="IDEALPRO", currency="USD"):
    """Create a mock IBKR Contract"""
    contract = Mock(spec=Contract)
    contract.symbol = symbol
    contract.secType = sec_type
    contract.exchange = exchange
    contract.currency = currency
    return contract

def mock_order(order_id=1, action="BUY", order_type="LMT", quantity=10000, price=1.1000):
    """Create a mock IBKR Order"""
    order = Mock(spec=Order)
    order.orderId = order_id
    order.action = action
    order.orderType = order_type
    order.totalQuantity = quantity
    order.lmtPrice = price
    order.transmit = False
    return order

def mock_bracket_orders(parent_id=1, action="BUY", entry_price=1.1000, quantity=10000, 
                       profit_target=1.1100, stop_loss=1.0950):
    """Create mock bracket orders"""
    parent = mock_order(parent_id, action, "LMT", quantity, entry_price)
    
    closing_action = "SELL" if action == "BUY" else "BUY"
    take_profit = mock_order(parent_id + 1, closing_action, "LMT", quantity, profit_target)
    stop_loss_order = mock_order(parent_id + 2, closing_action, "STP", quantity, stop_loss)
    
    return [parent, take_profit, stop_loss_order]