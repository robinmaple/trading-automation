# src/brokers/ibkr/ibkr_client.py (Corrected Version)

"""
High-level, test-friendly client for Interactive Brokers (IBKR) TWS API.
Acts as a facade coordinating specialized managers for different functionalities.
"""

from typing import Optional, List, Any, Dict
from ibapi.contract import Contract
from ibapi.client import EClient
from ibapi.wrapper import EWrapper

from src.core.context_aware_logger import get_context_logger, TradingEventType
from src.brokers.ibkr.types.ibkr_types import IbkrOrder, IbkrPosition

# Import all managers
from src.brokers.ibkr.core.connection_manager import ConnectionManager
from src.brokers.ibkr.core.order_manager import OrderManager
from src.brokers.ibkr.core.market_data_handler import MarketDataHandler
from src.brokers.ibkr.core.account_manager import AccountManager
from src.brokers.ibkr.core.historical_data_handler import HistoricalDataHandler


class IbkrClient(EClient, EWrapper):
    """
    Facade class that coordinates specialized managers for IBKR API operations.
    Provides a clean, high-level interface while delegating to specialized components.
    """

    def __init__(self, host='127.0.0.1', port=None, client_id=1, mode='auto'):
        """Initialize the client with all specialized managers."""
        
        self.context_logger = get_context_logger()
        
        # Initialize all managers
        self.connection_manager = ConnectionManager(host, port, client_id, mode)
        self.order_manager = OrderManager(self.connection_manager)
        self.market_data_handler = MarketDataHandler(self.connection_manager)
        self.account_manager = AccountManager(self.connection_manager)
        self.historical_data_handler = HistoricalDataHandler(self.connection_manager)

        # Initialize EClient first with self as wrapper
        EClient.__init__(self, wrapper=self)

        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IbkrClient facade initialized with all managers",
            context_provider={
                "managers_initialized": [
                    "ConnectionManager", "OrderManager", "MarketDataHandler", 
                    "AccountManager", "HistoricalDataHandler"
                ]
            }
        )
    
    # ===== CONNECTION DELEGATION =====
    def connect(self, host: Optional[str] = None, port: Optional[int] = None, 
                client_id: Optional[int] = None) -> bool:
        return self.connection_manager.connect(host, port, client_id)
    
    def disconnect(self) -> None:
        self.connection_manager.disconnect()
    
    @property
    def connected(self) -> bool:
        return self.connection_manager.connected
    
    @property 
    def next_valid_id(self) -> Optional[int]:
        return self.connection_manager.next_valid_id
    
    @property
    def account_number(self) -> Optional[str]:
        return self.connection_manager.account_number
    
    @property
    def account_name(self) -> Optional[str]:
        return self.connection_manager.account_name
    
    @property
    def is_paper_account(self) -> bool:
        return self.connection_manager.is_paper_account
    
    # ===== ORDER DELEGATION =====
    def place_bracket_order(self, contract, action, order_type, security_type, 
                           entry_price, stop_loss, risk_per_trade, risk_reward_ratio,
                           total_capital, account_number=None):
        return self.order_manager.place_bracket_order(
            contract, action, order_type, security_type, entry_price, stop_loss,
            risk_per_trade, risk_reward_ratio, total_capital, account_number
        )
    
    def cancel_order(self, order_id: int) -> bool:
        return self.order_manager.cancel_order(order_id)
    
    def get_open_orders(self) -> List[IbkrOrder]:
        return self.order_manager.get_open_orders()
    
    # ===== ACCOUNT DELEGATION =====
    def get_account_value(self) -> float:
        return self.account_manager.get_account_value()
    
    def get_positions(self) -> List[IbkrPosition]:
        return self.account_manager.get_positions()
    
    def get_simple_account_value(self, key: str) -> Optional[float]:
        return self.account_manager.get_simple_account_value(key)
    
    # ===== MARKET DATA DELEGATION =====
    def set_market_data_manager(self, manager) -> None:
        self.market_data_handler.set_market_data_manager(manager)
    
    def set_historical_data_manager(self, manager) -> None:
        self.market_data_handler.set_historical_data_manager(manager)
    
    def set_historical_eod_provider(self, eod_provider) -> None:
        self.market_data_handler.set_historical_eod_provider(eod_provider)
    
    def get_market_data_health(self) -> dict:
        return self.market_data_handler.get_market_data_health()
    
    # ===== HISTORICAL DATA DELEGATION =====
    def reqHistoricalData(self, reqId: int, contract: Contract, endDateTime: str,
                         durationStr: str, barSizeSetting: str, whatToShow: str,
                         useRTH: int, formatDate: int, keepUpToDate: bool,
                         chartOptions: list) -> None:
        self.historical_data_handler.req_historical_data(
            reqId, contract, endDateTime, durationStr, barSizeSetting,
            whatToShow, useRTH, formatDate, keepUpToDate, chartOptions
        )
    
    def cancelHistoricalData(self, reqId: int) -> None:
        self.historical_data_handler.cancel_historical_data(reqId)
    
    # ===== CALLBACK DELEGATION =====
    # These methods will be called by IBKR API and delegate to appropriate managers
    
    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        self.connection_manager.error(reqId, errorCode, errorString, advancedOrderRejectJson)
    
    def nextValidId(self, orderId: int):
        self.connection_manager.nextValidId(orderId)
    
    def managedAccounts(self, accountsList: str):
        self.connection_manager.managedAccounts(accountsList)
    
    def tickPrice(self, reqId: int, tickType: int, price: float, attrib):
        self.market_data_handler.tickPrice(reqId, tickType, price, attrib)
    
    def historicalData(self, reqId: int, bar):
        self.market_data_handler.historicalData(reqId, bar)
    
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        self.market_data_handler.historicalDataEnd(reqId, start, end)
    
    def openOrder(self, orderId, contract, order, orderState):
        self.order_manager.openOrder(orderId, contract, order, orderState)
    
    def openOrderEnd(self):
        self.order_manager.openOrderEnd()
    
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                   permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        self.order_manager.orderStatus(orderId, status, filled, remaining, avgFillPrice,
                                     permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
    
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        self.account_manager.updateAccountValue(key, val, currency, accountName)
    
    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost,
                       unrealizedPNL, realizedPNL, accountName):
        self.account_manager.updatePortfolio(contract, position, marketPrice, marketValue,
                                           averageCost, unrealizedPNL, realizedPNL, accountName)
    
    def updateAccountTime(self, timeStamp: str):
        self.account_manager.updateAccountTime(timeStamp)
    
    def accountDownloadEnd(self, accountName: str):
        self.account_manager.accountDownloadEnd(accountName)
    
    def position(self, account: str, contract, position: float, avgCost: float):
        self.account_manager.position(account, contract, position, avgCost)
    
    def positionEnd(self):
        self.account_manager.positionEnd()
    
    def scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        self.market_data_handler.scannerData(reqId, rank, contractDetails, distance, 
                                           benchmark, projection, legsStr)
    
    def scannerDataEnd(self, reqId):
        self.market_data_handler.scannerDataEnd(reqId)
    
    def scannerParameters(self, xml: str):
        self.historical_data_handler.scannerParameters(xml)
    
    # ===== UTILITY METHODS =====
    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status of all managers."""
        return {
            'connection': self.connection_manager.get_connection_status(),
            'market_data': self.market_data_handler.get_market_data_health(),
            'account': self.account_manager.get_account_summary(),
            'historical_requests': self.historical_data_handler.get_historical_requests_status(),
            'order_history_count': len(self.order_manager.order_history),
            'open_orders_count': len(self.order_manager.open_orders)
        }
    
    def setConnState(self, state):
        """
        Set connection state - compatibility method for IBAPI EClient
        This method is called by the underlying EClient to manage connection state.
        """
        self.connState = state
        # Also propagate to connection manager if it exists
        if hasattr(self, 'connection_manager'):
            self.connection_manager.connState = state
        
        self.context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            f"Connection state updated: {state}",
            context_provider={"connection_state": state}
        )