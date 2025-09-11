import datetime
from src.core.planned_order import ActiveOrder


class OrderExecutionService:
    """
    Service responsible for all interactions with the brokerage client.
    Handles placing, cancelling, and monitoring orders.
    """

    def __init__(self, trading_manager, ibkr_client):
        # Phase 1: We need references to the data this service requires
        self._trading_manager = trading_manager
        self._ibkr_client = ibkr_client
        # These will be set after TradingManager initialization
        self.order_persistence = None
        self.active_orders = None

    def set_dependencies(self, order_persistence, active_orders):
        """Set the dependencies required for order execution"""
        self.order_persistence = order_persistence
        self.active_orders = active_orders

    def place_order(self, planned_order, fill_probability, total_capital, quantity, capital_commitment, is_live_trading):
        """
        Places an order for the given PlannedOrder.
        Args:
            planned_order (PlannedOrder): The order to place.
            fill_probability (float): The calculated fill probability.
            total_capital (float): The total account capital.
            quantity (int): The calculated quantity/shares.
            capital_commitment (float): The total capital commitment.
            is_live_trading (bool): Flag indicating live trading mode.
        Returns:
            bool: True if the order was successfully placed, False otherwise.
        """
        # Phase 1: Use our own internal implementation instead of delegating back.
        return self.execute_single_order(
            planned_order, fill_probability, total_capital, quantity, capital_commitment, is_live_trading
        )

    # Add margin validation before order execution - Begin
    def _validate_order_margin(self, order, quantity, total_capital) -> tuple:
        """Validate if order has sufficient margin before execution"""
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
    # Add margin validation before order execution - End

    def execute_single_order(self, order, fill_probability, total_capital, quantity, capital_commitment, is_live_trading):
        """
        Core order execution logic - extracted from TradingManager.
        This method contains the actual order placement implementation.
        """
        # Validate margin before order execution - Begin
        margin_valid, margin_message = self._validate_order_margin(order, quantity, total_capital)
        if not margin_valid:
            # Mark order as CANCELED due to insufficient margin
            db_id = self._trading_manager._find_planned_order_db_id(order)
            if db_id:
                self.order_persistence.handle_order_rejection(db_id, margin_message)
            else:
                print(f"❌ Cannot mark order as canceled: Database ID not found for {order.symbol}")
            return False
        # Validate margin before order execution - End

        # Real order execution if executor is available and connected
        ibkr_connected = self._ibkr_client and self._ibkr_client.connected
        if ibkr_connected:
            print("   Taking LIVE order execution path...")
            
            contract = order.to_ib_contract()
            
            # Place real order through IBKR
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
            
            if order_ids:
                print(f"✅ REAL ORDER PLACED: Order IDs {order_ids} sent to IBKR")
                
                # Update order status to LIVE in database
                update_success = self.order_persistence.update_order_status(order, 'LIVE', order_ids)
                print(f"   Order status update success: {update_success}")
                
                # Record execution for live orders - use 'SUBMITTED' status since not filled yet
                execution_id = self.order_persistence.record_order_execution(
                    order, 
                    order.entry_price,  # This is the intended price, not actual fill price
                    quantity, 
                    0.0,  # Commission will be updated via IBKR callbacks
                    'SUBMITTED',  # Changed from 'LIVE' to match order lifecycle
                    is_live_trading
                )
                
                if execution_id:
                    print(f"✅ Execution recorded for live order: ID {execution_id}")
                else:
                    print("❌ FAILED to record execution for live order")
                
                # Create ActiveOrder object for tracking
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
                
            else:
                # Add order rejection handling when IBKR fails - Begin
                print("❌ Failed to place real order through IBKR")
                rejection_reason = "IBKR order placement failed - no order IDs returned"
                db_id = self._trading_manager._find_planned_order_db_id(order)
                if db_id:
                    self.order_persistence.handle_order_rejection(db_id, rejection_reason)
                else:
                    print(f"❌ Cannot mark order as canceled: Database ID not found for {order.symbol}")
                # Add order rejection handling when IBKR fails - End
                
        else:
            print("   Taking SIMULATION order execution path...")
            # Fall back to simulation
            print(f"✅ SIMULATION: Order for {order.symbol} executed successfully")
            
            # Update order status to FILLED in database
            update_success = self.order_persistence.update_order_status(order, 'FILLED')
            print(f"   Order status update success: {update_success}")
            
            # Record simulated execution
            execution_id = self.order_persistence.record_order_execution(
                order, 
                order.entry_price, 
                quantity, 
                 0.0, 
                'FILLED', 
                is_live_trading
            )
            
            if execution_id:
                print(f"✅ Simulation execution recorded: ID {execution_id}")
            else:
                print("❌ FAILED to record simulation execution")

    def cancel_order(self, order_id):
        """
        Cancels a working order.
        Args:
            order_id (int): The ID of the order to cancel.
        Returns:
            bool: True if the cancel request was successful, False otherwise.
        """
        # Phase 1: Delegate to existing logic. This will be refactored later.
        return self._trading_manager._cancel_single_order(order_id)