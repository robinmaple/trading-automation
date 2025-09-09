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
        """
        # Phase 1: Delegate to existing logic.
        self._trading_manager._update_order_status(planned_order, new_status, order_ids)

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