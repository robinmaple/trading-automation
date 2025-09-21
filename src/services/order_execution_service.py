"""
Service responsible for all order execution interactions with the brokerage client.
Handles placement, cancellation, monitoring of orders, validation, live/simulation mode,
and persistence of execution results, including Phase B: fill probability and unified execution tracking.
"""

import datetime
from typing import Any, Dict, Optional, List
from ibapi.contract import Contract
from ibapi.order import Order

from src.core.planned_order import ActiveOrder
# Phase B Additions - Begin
from src.core.models import OrderAttemptDB
# Phase B Additions - End


class OrderExecutionService:
    """Encapsulates all logic for executing orders and interacting with the broker."""

    def __init__(self, trading_manager, ibkr_client):
        """Initialize the service with references to the trading manager and IBKR client."""
        self._trading_manager = trading_manager
        self._ibkr_client = ibkr_client
        self.order_persistence = None
        self.active_orders = None

    def set_dependencies(self, order_persistence, active_orders) -> None:
        """Inject dependencies for order execution and tracking."""
        self.order_persistence = order_persistence
        self.active_orders = active_orders

    # Phase B Additions - Begin
    def _record_order_attempt(self, planned_order, attempt_type, fill_probability=None,
                            effective_priority=None, quantity=None, capital_commitment=None,
                            status=None, ib_order_ids=None, details=None,
                            account_number: Optional[str] = None):
        """
        Record an order attempt to the database for Phase B tracking.
        
        Args:
            planned_order: The planned order being attempted
            attempt_type: Type of attempt ('PLACEMENT', 'CANCELLATION', 'REPLACEMENT')
            fill_probability: Fill probability at time of attempt
            effective_priority: Effective priority score
            quantity: Quantity attempted
            capital_commitment: Capital commitment for the order
            status: Status of the attempt
            ib_order_ids: IBKR order IDs if available
            details: Additional details or error messages
            account_number: Account number for the attempt
        """
        if not self.order_persistence or not hasattr(self.order_persistence, 'db_session'):
            return None
            
        try:
            db_id = self._trading_manager._find_planned_order_db_id(planned_order)
            
            attempt = OrderAttemptDB(
                planned_order_id=db_id,
                attempt_ts=datetime.datetime.now(),
                attempt_type=attempt_type,
                fill_probability=fill_probability,
                effective_priority=effective_priority,
                quantity=quantity,
                capital_commitment=capital_commitment,
                status=status,
                ib_order_ids=ib_order_ids,
                details=details,
                account_number=account_number  # Store account number
            )
            
            self.order_persistence.db_session.add(attempt)
            self.order_persistence.db_session.commit()
            
            return attempt.id
            
        except Exception as e:
            print(f"❌ Failed to record order attempt: {e}")
            return None
    # Phase B Additions - End

    # Account Context Integration - Begin
    def place_order(
        self,
        planned_order,
        fill_probability=0.0,
        effective_priority=0.0,
        total_capital=None,
        quantity=None,
        capital_commitment=None,
        is_live_trading=False,
        account_number: Optional[str] = None
    ) -> bool:
        """Place an order for a PlannedOrder, tracking fill probability (Phase B)."""
        return self.execute_single_order(
            planned_order,
            fill_probability,
            effective_priority,
            total_capital,
            quantity,
            capital_commitment,
            is_live_trading,
            account_number  # Pass account number
        )
    # Account Context Integration - End

    def _validate_order_margin(self, order, quantity, total_capital) -> tuple[bool, str]:
        """Validate if the order has sufficient margin before execution."""
        try:
            is_valid, message = self.order_persistence.validate_sufficient_margin(
                order.symbol, quantity, order.entry_price
            )
            if not is_valid:
                print(f"❌ Order rejected due to margin: {message}")
                return False, message
            return True, "Margin validation passed"
        except Exception as e:
            return False, f"Margin validation error: {e}"

    # Account Context Integration - Begin
    def execute_single_order(
        self,
        order,
        fill_probability=0.0,
        effective_priority=0.0,
        total_capital=None,
        quantity=None,
        capital_commitment=None,
        is_live_trading=False,
        account_number: Optional[str] = None
    ) -> bool:
        """
        Execute a single order while incorporating fill probability into ActiveOrder tracking.
        Phase B: Supports unified execution record for entry/SL/PT.
        """
        margin_valid, margin_message = self._validate_order_margin(order, quantity, total_capital)
        if not margin_valid:
            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                self.order_persistence.handle_order_rejection(db_id, margin_message)
            else:
                print(f"❌ Cannot mark order as canceled: Database ID not found for {order.symbol}")
            
            # Phase B Additions - Begin
            # Record failed attempt due to margin validation
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'FAILED', None, margin_message,
                account_number  # Pass account number
            )
            # Phase B Additions - End
            return False

        ibkr_connected = self._ibkr_client and self._ibkr_client.connected

        # === LIVE ORDER PATH ===
        if ibkr_connected:
            print(f"   Taking LIVE order execution path... FillProb={fill_probability:.3f}")
            
            # Phase B Additions - Begin
            # Record placement attempt before execution
            attempt_id = self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'SUBMITTING', None, None,
                account_number  # Pass account number
            )
            # Phase B Additions - End
            
            contract = order.to_ib_contract()
            order_ids = self._ibkr_client.place_bracket_order(
                contract,
                order.action.value,
                order.order_type.value,
                order.security_type.value,
                order.entry_price,
                order.stop_loss,
                order.risk_per_trade,
                order.risk_reward_ratio,
                total_capital
            )

            if not order_ids:
                print("❌ Failed to place real order through IBKR")
                rejection_reason = "IBKR order placement failed - no order IDs returned"
                db_id = self._trading_manager._find_planned_order_db_id(order)
                if db_id:
                    self.order_persistence.handle_order_rejection(db_id, rejection_reason)
                
                # Phase B Additions - Begin
                # Update attempt with failure
                self._record_order_attempt(
                    order, 'PLACEMENT', fill_probability, effective_priority,
                    quantity, capital_commitment, 'FAILED', None, rejection_reason,
                    account_number  # Pass account number
                )
                # Phase B Additions - End
                return False

            # Account Context Integration - Begin
            # Pass account number to persistence service
            execution_id = self.order_persistence.record_order_execution(
                order,
                order.entry_price,
                quantity,
                account_number,  # Pass account number
                status='SUBMITTED',
                is_live_trading=is_live_trading
            )
            # Account Context Integration - End

            # Create ActiveOrder with unified tracking
            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                active_order = ActiveOrder(
                    planned_order=order,
                    order_ids=order_ids,
                    db_id=db_id,
                    status='SUBMITTED',
                    capital_commitment=capital_commitment,
                    timestamp=datetime.datetime.now(),
                    is_live_trading=is_live_trading,
                    fill_probability=fill_probability
                )
                self.active_orders[order_ids[0]] = active_order
            else:
                print("⚠️  Could not create ActiveOrder - database ID not found")
            
            # Phase B Additions - Begin
            # Update attempt with success and order IDs
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'SUBMITTED', order_ids, None,
                account_number  # Pass account number
            )
            # Phase B Additions - End
            
            print(f"✅ REAL ORDER PLACED: Order IDs {order_ids} sent to IBKR (Account: {account_number})")
            return True

        # === SIMULATION PATH ===
        else:
            print(f"   Taking SIMULATION order execution path... FillProb={fill_probability:.3f}")
            
            # Phase B Additions - Begin
            # Record simulation attempt
            self._record_order_attempt(
                order, 'PLACEMENT', fill_probability, effective_priority,
                quantity, capital_commitment, 'SIMULATION', None, None,
                account_number  # Pass account number
            )
            # Phase B Additions - End
            
            update_success = self.order_persistence.update_order_status(order, 'FILLED')
            
            # Account Context Integration - Begin
            # Pass account number to persistence service
            execution_id = self.order_persistence.record_order_execution(
                order,
                order.entry_price,
                quantity,
                account_number,  # Pass account number
                status='FILLED',
                is_live_trading=is_live_trading
            )
            # Account Context Integration - End

            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                active_order = ActiveOrder(
                    planned_order=order,
                    order_ids=[f"SIM-{db_id}"],
                    db_id=db_id,
                    status='FILLED',
                    capital_commitment=capital_commitment,
                    timestamp=datetime.datetime.now(),
                    is_live_trading=is_live_trading,
                    fill_probability=fill_probability
                )
                self.active_orders[active_order.order_ids[0]] = active_order
            return True
    # Account Context Integration - End

    def cancel_order(self, order_id) -> bool:
        """Cancel a working order by delegating to the trading manager's logic."""
        # Phase B Additions - Begin
        # Find the active order to get details for tracking
        active_order = None
        for ao in self.active_orders.values():
            if order_id in ao.order_ids:
                active_order = ao
                break
        
        # Record cancellation attempt
        if active_order:
            self._record_order_attempt(
                active_order.planned_order, 'CANCELLATION',
                active_order.fill_probability, None, None, None,
                'ATTEMPTING', [order_id], None,
                active_order.account_number if hasattr(active_order, 'account_number') else None
            )
        # Phase B Additions - End
        
        success = self._trading_manager._cancel_single_order(order_id)
        
        # Phase B Additions - Begin
        # Update attempt with result
        if active_order:
            status = 'SUCCESS' if success else 'FAILED'
            details = f"Cancellation {'succeeded' if success else 'failed'}"
            self._record_order_attempt(
                active_order.planned_order, 'CANCELLATION',
                active_order.fill_probability, None, None, None,
                status, [order_id], details,
                active_order.account_number if hasattr(active_order, 'account_number') else None
            )
        # Phase B Additions - End
        
        return success

    def close_position(self, position_data: Dict, account_number: Optional[str] = None) -> Optional[int]:
        """Close an open position by placing a market order through IBKR."""
        if not self._ibkr_client or not self._ibkr_client.connected:
            print(f"✅ Simulation: Would close {position_data['symbol']} position (Account: {account_number})")
            return None
        try:
            contract = Contract()
            contract.symbol = position_data['symbol']
            contract.secType = position_data['security_type']
            contract.exchange = position_data.get('exchange', 'SMART')
            contract.currency = position_data.get('currency', 'USD')

            order = Order()
            order.action = position_data['action']
            order.orderType = "MKT"
            order.totalQuantity = position_data['quantity']
            order.tif = "DAY"

            order_id = self._ibkr_client.next_valid_id
            self._ibkr_client.placeOrder(order_id, contract, order)
            self._ibkr_client.next_valid_id += 1

            print(f"✅ Closing market order placed for {position_data['symbol']} (ID: {order_id}, Account: {account_number})")
            return order_id
        except Exception as e:
            print(f"❌ Failed to close position {position_data['symbol']}: {e}")
            return None

    def cancel_orders_for_symbol(self, symbol: str) -> bool:
        """Cancel all active open orders for a specific symbol."""
        if not self._ibkr_client or not self._ibkr_client.connected:
            print(f"✅ Simulation: Would cancel orders for {symbol}")
            return True
        try:
            orders = self._ibkr_client.get_open_orders()
            symbol_orders = [
                o for o in orders
                if o.symbol == symbol and o.status in ['Submitted', 'PreSubmitted', 'PendingSubmit']
            ]
            if not symbol_orders:
                print(f"ℹ️  No active orders found to cancel for {symbol}")
                return True

            success = True
            for order in symbol_orders:
                print(f"❌ Cancelling order {order.order_id} for {symbol}")
                if not self._ibkr_client.cancel_order(order.order_id):
                    success = False
                    print(f"⚠️  Failed to cancel order {order.order_id}")
            return success
        except Exception as e:
            print(f"❌ Error cancelling orders for {symbol}: {e}")
            return False

    def find_orders_by_symbol(self, symbol: str) -> List[Any]:
        """Find all open orders for a specific symbol from IBKR."""
        if not self._ibkr_client or not self._ibkr_client.connected:
            print(f"✅ Simulation: Would find orders for {symbol}")
            return []
        try:
            orders = self._ibkr_client.get_open_orders()
            return [o for o in orders if o.symbol == symbol]
        except Exception as e:
            print(f"❌ Error finding orders for {symbol}: {e}")
            return []