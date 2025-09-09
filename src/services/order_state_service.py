from src.core.models import PlannedOrderDB, PositionStrategy


class OrderStateService:
    """
    Service responsible for managing the persistence and state transitions
    of PlannedOrder and ExecutedOrder models in the database.
    """

    def __init__(self, trading_manager, db_session):
        # Phase 1: Hold references for delegation during refactoring.
        self._trading_manager = trading_manager
        self._db_session = db_session

    def update_planned_order_status(self, planned_order, new_status, order_ids=None):
        """
        Updates the status of a PlannedOrder and persists it to the database.
        Args:
            planned_order (PlannedOrder): The order to update.
            new_status (str): The new status (e.g., 'LIVE_WORKING', 'CANCELLED').
            order_ids (list, optional): List of order IDs from the broker.
        Returns:
            bool: True if successful, False if order not found
        """
        try:
            # Find the order in database
            db_order = self._db_session.query(PlannedOrderDB).filter_by(
                symbol=planned_order.symbol,
                entry_price=planned_order.entry_price,
                stop_loss=planned_order.stop_loss,
                action=planned_order.action.value
            ).first()
            
            if db_order:
                db_order.status = new_status
                if order_ids:
                    db_order.ibkr_order_ids = str(order_ids)
                self._db_session.commit()
                print(f"✅ Updated order status to {new_status} in database")
                return True
            else:
                print(f"❌ Order not found in database: {planned_order.symbol}")
                return False
                
        except Exception as e:
            self._db_session.rollback()
            print(f"❌ Failed to update order status: {e}")
            return False
        
    def create_executed_order(self, planned_order, fill_info):
        """
        Creates a new ExecutedOrder record from a PlannedOrder and fill information.
        Args:
            planned_order (PlannedOrder): The source planned order.
            fill_info (dict): Fill details from the broker.
        Returns:
            ExecutedOrder: The newly created executed order object.
        """
        # Phase 1: Delegate to existing logic.
        return self._trading_manager._create_executed_order_record(planned_order, fill_info)
    
    def convert_to_db_model(self, planned_order):
        """
        Convert PlannedOrder to PlannedOrderDB for database persistence.
        Extracted from TradingManager._convert_to_db_model.
        """
        # Find position strategy in database
        position_strategy = self._db_session.query(PositionStrategy).filter_by(
            name=planned_order.position_strategy.value
        ).first()
        
        if not position_strategy:
            raise ValueError(f"Position strategy {planned_order.position_strategy.value} not found in database")
        
        # Live/Paper trading tracking - Get mode from the trading manager
        is_live_trading = self._trading_manager._get_trading_mode()

        db_model = PlannedOrderDB(
            symbol=planned_order.symbol,
            security_type=planned_order.security_type.value,
            action=planned_order.action.value,
            order_type=planned_order.order_type.value,
            entry_price=planned_order.entry_price,
            stop_loss=planned_order.stop_loss,
            risk_per_trade=planned_order.risk_per_trade,
            risk_reward_ratio=planned_order.risk_reward_ratio,
            priority=planned_order.priority,
            position_strategy_id=position_strategy.id,
            status='PENDING',
            is_live_trading=is_live_trading
        )

        print(f"DEBUG: Created DB model - entry_price: {db_model.entry_price}, stop_loss: {db_model.stop_loss}")

        return db_model