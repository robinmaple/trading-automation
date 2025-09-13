"""
Service for handling all order-related database persistence operations.
Consolidates logic for creating, updating, and querying PlannedOrderDB and ExecutedOrderDB records.
Acts as the gateway between business logic and the persistence layer.
"""

from decimal import Decimal
import datetime
from typing import Optional, Tuple
from sqlalchemy.orm import Session

from src.core.events import OrderState
from src.core.database import get_db_session
from src.core.models import ExecutedOrderDB, PlannedOrderDB, PositionStrategy


class OrderPersistenceService:
    """Encapsulates all database operations for order persistence and validation."""

    def __init__(self, db_session: Optional[Session] = None):
        """Initialize the service with an optional database session."""
        self.db_session = db_session or get_db_session()

    def record_order_execution(self, planned_order, filled_price: float,
                             filled_quantity: float, commission: float = 0.0,
                             status: str = 'FILLED', is_live_trading: bool = False) -> Optional[int]:
        """Record an order execution in the database. Returns the ID of the new record or None."""
        try:
            planned_order_id = self._find_planned_order_id(planned_order)
            if planned_order_id is None:
                print(f"❌ Cannot record execution: Planned order not found in database for {planned_order.symbol}")
                print(f"   Searching for: {planned_order.symbol}, {planned_order.entry_price}, {planned_order.stop_loss}")
                existing_orders = self.db_session.query(PlannedOrderDB).filter_by(symbol=planned_order.symbol).all()
                print(f"   Existing orders for {planned_order.symbol}: {len(existing_orders)}")
                for order in existing_orders:
                    print(f"     - {order.symbol}: entry={order.entry_price}, stop={order.stop_loss}, status={order.status}")
                return None

            executed_order = ExecutedOrderDB(
                planned_order_id=planned_order_id,
                filled_price=filled_price,
                filled_quantity=filled_quantity,
                commission=commission,
                status=status,
                executed_at=datetime.datetime.now(),
                is_live_trading=is_live_trading
            )

            if planned_order.position_strategy.value == 'HYBRID':
                expiration_date = datetime.datetime.now() + datetime.timedelta(days=10)
                executed_order.expiration_date = expiration_date
                print(f"📅 HYBRID order expiration set: {expiration_date.strftime('%Y-%m-%d %H:%M')}")

            self.db_session.add(executed_order)
            self.db_session.commit()

            print(f"✅ Execution recorded for {planned_order.symbol}: "
                  f"{filled_quantity} @ {filled_price}, Status: {status}")

            return executed_order.id

        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to record order execution: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _find_planned_order_id(self, planned_order) -> Optional[int]:
        """Find the database ID for a planned order based on its parameters."""
        try:
            db_order = self.db_session.query(PlannedOrderDB).filter_by(
                symbol=planned_order.symbol,
                entry_price=planned_order.entry_price,
                stop_loss=planned_order.stop_loss,
                action=planned_order.action.value,
                order_type=planned_order.order_type.value
            ).first()
            return db_order.id if db_order else None
        except Exception as e:
            print(f"❌ Error finding planned order in database: {e}")
            return None

    def convert_to_db_model(self, planned_order) -> PlannedOrderDB:
        """Convert a domain PlannedOrder object to a PlannedOrderDB database entity."""
        try:
            position_strategy = self.db_session.query(PositionStrategy).filter_by(
                name=planned_order.position_strategy.value
            ).first()

            if not position_strategy:
                raise ValueError(f"Position strategy {planned_order.position_strategy.value} not found in database")

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
                status='PENDING'
            )
            return db_model

        except Exception as e:
            print(f"❌ Failed to convert planned order to DB model: {e}")
            raise

    def create_executed_order(self, planned_order, fill_info) -> Optional[ExecutedOrderDB]:
        """Create an ExecutedOrderDB record from a PlannedOrder and fill information."""
        try:
            planned_order_id = self._find_planned_order_id(planned_order)
            if not planned_order_id:
                print(f"❌ Cannot create executed order: Planned order not found for {planned_order.symbol}")
                return None

            executed_order = ExecutedOrderDB(
                planned_order_id=planned_order_id,
                filled_price=fill_info.get('price', 0),
                filled_quantity=fill_info.get('quantity', 0),
                commission=fill_info.get('commission', 0),
                pnl=fill_info.get('pnl', 0),
                status=fill_info.get('status', 'FILLED'),
                executed_at=datetime.datetime.now()
            )

            self.db_session.add(executed_order)
            self.db_session.commit()

            print(f"✅ Created executed order for {planned_order.symbol}")
            return executed_order

        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to create executed order: {e}")
            return None

    def handle_order_rejection(self, planned_order_id: int, rejection_reason: str) -> bool:
        """Mark a planned order as CANCELLED with a rejection reason in the database."""
        try:
            order = self.db_session.query(PlannedOrderDB).filter_by(id=planned_order_id).first()
            if order:
                order.status = 'CANCELLED'
                order.rejection_reason = rejection_reason
                order.updated_at =datetime.datetime.now()
                self.db_session.commit()
                print(f"✅ Order {planned_order_id} canceled due to rejection: {rejection_reason}")
                return True
            else:
                print(f"❌ Order {planned_order_id} not found for rejection handling")
                return False
        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to cancel rejected order {planned_order_id}: {e}")
            return False

    def validate_sufficient_margin(self, symbol: str, quantity: float, entry_price: float,
                                currency: str = 'USD') -> Tuple[bool, str]:
        """Validate if the account has sufficient margin for a proposed trade. Returns (is_valid, message)."""
        try:
            account_value = self.get_account_value()
            trade_value = quantity * entry_price

            if symbol in ['EUR', 'AUD', 'GBP', 'JPY', 'CAD']:
                margin_requirement = trade_value * 0.02
            else:
                margin_requirement = trade_value * 0.5

            max_allowed_margin = account_value * 0.8

            if margin_requirement > max_allowed_margin:
                message = (f"Insufficient margin. Required: ${margin_requirement:,.2f}, "
                        f"Available: ${max_allowed_margin:,.2f}, "
                        f"Account Value: ${account_value:,.2f}")
                return False, message

            return True, "Sufficient margin available"

        except Exception as e:
            return False, f"Margin validation error: {e}"

    def get_account_value(self, account_id: str = None) -> float:
        """Get the current account value. Currently a mock implementation."""
        try:
            mock_account_value = 100000.0
            print(f"📊 Current account value (mock): ${mock_account_value:,.2f}")
            return mock_account_value
        except Exception as e:
            print(f"❌ Failed to get account value: {e}")
            return 50000.0

    def update_order_status(self, order, status: str, reason: str = "", order_ids=None) -> bool:
        """Update the status of an order in the database with an optional reason."""
        try:
            valid_statuses = ['PENDING', 'LIVE', 'LIVE_WORKING', 'FILLED', 'CANCELLED',
                             'EXPIRED', 'LIQUIDATED', 'REPLACED']

            if status not in valid_statuses:
                print(f"❌ Invalid order status: '{status}'. Valid values: {valid_statuses}")
                return False

            db_order = self._find_planned_order_db_record(order)

            if db_order:
                db_order.status = status
                if order_ids:
                    db_order.ibkr_order_ids = str(order_ids)
                if reason:
                    db_order.status_reason = reason[:255]
                db_order.updated_at = datetime.datetime.now()

                self.db_session.commit()
                print(f"✅ Updated {order.symbol} status to {status}: {reason}")
                return True
            else:
                print(f"❌ Order not found in database: {order.symbol}")
                try:
                    db_model = self.convert_to_db_model(order)
                    db_model.status = status
                    if reason:
                        db_model.status_reason = reason[:255]
                    self.db_session.add(db_model)
                    self.db_session.commit()
                    print(f"✅ Created new order record for {order.symbol} with status {status}")
                    return True
                except Exception as create_error:
                    print(f"❌ Failed to create order record: {create_error}")
                    return False

        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to update order status: {e}")
            return False

    def _find_planned_order_db_record(self, order) -> Optional[PlannedOrderDB]:
        """Find a PlannedOrderDB record in the database based on its parameters."""
        try:
            return self.db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value,
                order_type=order.order_type.value
            ).first()
        except Exception as e:
            print(f"❌ Error finding planned order in database: {e}")
            return None