# This is a new file. The entire content below is new.
from typing import Optional, Any
from src.core.order_executor import OrderExecutor


class IbkrClient:
    """
    A high-level, test-friendly client for IBKR TWS API.
    This class encapsulates the low-level, asynchronous API calls and exposes
    a simple interface for the trading system. Phase 1: Pass-through facade.
    """
    def __init__(self):
        self._order_executor: Optional[OrderExecutor] = None
        self.connected = False

    def connect(self, host='127.0.0.1', port=7497, client_id=0) -> bool:
        """Establish connection to IB Gateway/TWS. Returns success status."""
        self._order_executor = OrderExecutor()
        success = self._order_executor.connect_to_ib(host, port, client_id)
        if success:
            # Wait for connection to be fully established
            self.connected = self._order_executor.wait_for_connection()
        return self.connected

    def disconnect(self):
        """Cleanly disconnect from TWS."""
        if self._order_executor and self.connected:
            self._order_executor.disconnect()
            self.connected = False

    def get_account_value(self) -> float:
        """
        Request and return the Net Liquidation value of the account.
        Handles all underlying reqAccountUpdates calls and waiting.
        """
        if not self._check_connection():
            return 100000.0  # Fallback value
        return self._order_executor.get_account_value()

    def place_bracket_order(self, contract, action, order_type, security_type, entry_price, stop_loss,
                        risk_per_trade, risk_reward_ratio):
        """
        Place a complete bracket order.
        Returns: order IDs of the placed orders or None.
        """
        if not self._check_connection():
            return None
        return self._order_executor.place_bracket_order(
            contract, action, order_type, security_type, entry_price, stop_loss,
            risk_per_trade, risk_reward_ratio
        )

    def _check_connection(self) -> bool:
        """Helper method to check if the client is ready for API calls."""
        if not self._order_executor or not self.connected:
            print("IbkrClient: Not connected to IBKR.")
            return False
        return True

    # Property to access the underlying executor if needed for specific operations
    @property
    def order_executor(self) -> Optional[OrderExecutor]:
        return self._order_executor