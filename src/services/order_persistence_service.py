"""
Service for handling all order-related database persistence operations.
Consolidates logic for creating, updating, and querying PlannedOrderDB and ExecutedOrderDB records.
Acts as the gateway between business logic and the persistence layer.
"""

from decimal import Decimal
import datetime
from typing import Optional, Tuple, List, Dict
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.events import OrderState
from src.core.database import get_db_session
from src.core.models import ExecutedOrderDB, PlannedOrderDB, PositionStrategy
from src.core.shared_enums import OrderState as SharedOrderState
from src.core.planned_order import PlannedOrder, Action, OrderType, SecurityType, PositionStrategy as PositionStrategyEnum

class OrderPersistenceService:
    """Encapsulates all database operations for order persistence and validation."""

    def __init__(self, db_session: Optional[Session] = None):
        """Initialize the service with an optional database session."""
        self.db_session = db_session or get_db_session()

    def get_active_orders(self) -> List[PlannedOrderDB]:
        """
        Get all active orders from database that should be resumed.
        
        Returns:
            List of PlannedOrderDB objects with active status
        """
        try:
            active_db_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.status.in_([
                    SharedOrderState.PENDING.value,
                    SharedOrderState.LIVE.value, 
                    SharedOrderState.LIVE_WORKING.value
                ])
            ).all()
            
            return active_db_orders
            
        except Exception as e:
            print(f"❌ Failed to get active orders from database: {e}")
            return []
    # <Active Orders Query - End>

    # Account Tracking Implementation - Begin
    def record_order_execution(self, planned_order, filled_price: float,
                             filled_quantity: float, account_number: str,
                             commission: float = 0.0, status: str = 'FILLED') -> Optional[int]:
        """Record an order execution in the database with account context. Returns the ID of the new record or None."""
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
                is_live_trading=account_number.startswith('U'),  # Live accounts start with 'U'
                account_number=account_number  # Store which account executed this
            )

            if planned_order.position_strategy.value == 'HYBRID':
                expiration_date = datetime.datetime.now() + datetime.timedelta(days=10)
                executed_order.expiration_date = expiration_date
                print(f"📅 HYBRID order expiration set: {expiration_date.strftime('%Y-%m-%d %H:%M')}")

            self.db_session.add(executed_order)
            self.db_session.commit()

            print(f"✅ Execution recorded for {planned_order.symbol} (Account: {account_number}): "
                  f"{filled_quantity} @ {filled_price}, Status: {status}")

            return executed_order.id

        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to record order execution: {e}")
            import traceback
            traceback.print_exc()
            return None

    def create_executed_order(self, planned_order, fill_info, account_number: str) -> Optional[ExecutedOrderDB]:
        """Create an ExecutedOrderDB record from a PlannedOrder and fill information with account context."""
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
                executed_at=datetime.datetime.now(),
                account_number=account_number,  # Store which account executed this
                is_live_trading=account_number.startswith('U')  # Live accounts start with 'U'
            )

            self.db_session.add(executed_order)
            self.db_session.commit()

            print(f"✅ Created executed order for {planned_order.symbol} (Account: {account_number})")
            return executed_order

        except Exception as e:
            self.db_session.rollback()
            print(f"❌ Failed to create executed order: {e}")
            return None

    def get_realized_pnl_period(self, account_number: str, days: int) -> Decimal:
        """Get realized P&L for specific account for the last N calendar days."""
        start_date = datetime.datetime.now() - datetime.timedelta(days=days)
        
        result = self.db_session.execute(
            text("""
                SELECT COALESCE(SUM(realized_pnl), 0) 
                FROM executed_orders 
                WHERE exit_time >= :start_date AND account_number = :account_number
            """),
            {'start_date': start_date, 'account_number': account_number}
        ).scalar()
        
        return Decimal(str(result or '0'))

    def record_realized_pnl(self, order_id: int, symbol: str, pnl: Decimal, 
                          exit_date: datetime, account_number: str):
        """Record realized P&L for a closed trade with account context."""
        self.db_session.execute(
            text("""
                INSERT INTO executed_orders (order_id, symbol, realized_pnl, exit_time, account_number)
                VALUES (:order_id, :symbol, :pnl, :exit_time, :account_number)
            """),
            {
                'order_id': order_id,
                'symbol': symbol,
                'pnl': float(pnl),
                'exit_time': exit_date,
                'account_number': account_number
            }
        )
        self.db_session.commit()
    # Account Tracking Implementation - End

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
        """Convert a domain PlannedOrder object to a PlannedOrderDB entity."""

        # Resolve DB row; create if missing
        strategy_name = getattr(planned_order.position_strategy, 'value', str(planned_order.position_strategy))
        position_strategy = self.db_session.query(PositionStrategy).filter_by(name=strategy_name).first()

        if not position_strategy:
            print(f"⚠ Position strategy '{strategy_name}' not found in DB. Auto-creating.")
            position_strategy = PositionStrategy(name=strategy_name)
            self.db_session.add(position_strategy)
            self.db_session.commit()

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
            overall_trend=planned_order.overall_trend,       # store human-entered value
            brief_analysis=planned_order.brief_analysis
        )
        return db_model

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
        """
        Update the status of a PlannedOrder in the database and synchronize all relevant fields.
        Always overwrites with current PlannedOrder values (blind update).
        
        Args:
            order: PlannedOrder domain object
            status: New status string ('PENDING', 'LIVE', etc.)
            reason: Optional reason for status update
            order_ids: Optional list of IBKR order IDs

        Returns:
            True if update/create succeeded, False otherwise
        """
        try:
            valid_statuses = ['PENDING', 'LIVE', 'LIVE_WORKING', 'FILLED', 'CANCELLED',
                            'EXPIRED', 'LIQUIDATED', 'REPLACED']

            if status not in valid_statuses:
                print(f"❌ Invalid order status: '{status}'. Valid values: {valid_statuses}")
                return False

            db_order = self._find_planned_order_db_record(order)

            if db_order:
                # Blindly update all relevant fields
                db_order.status = status
                db_order.overall_trend = getattr(order, 'overall_trend', db_order.overall_trend)
                db_order.brief_analysis = getattr(order, 'brief_analysis', db_order.brief_analysis)
                db_order.risk_per_trade = getattr(order, 'risk_per_trade', db_order.risk_per_trade)
                db_order.risk_reward_ratio = getattr(order, 'risk_reward_ratio', db_order.risk_reward_ratio)
                db_order.priority = getattr(order, 'priority', db_order.priority)
                db_order.entry_price = getattr(order, 'entry_price', db_order.entry_price)
                db_order.stop_loss = getattr(order, 'stop_loss', db_order.stop_loss)
                db_order.action = getattr(order.action, 'value', db_order.action)
                db_order.order_type = getattr(order.order_type, 'value', db_order.order_type)
                db_order.security_type = getattr(order.security_type, 'value', db_order.security_type)
                db_order.position_strategy_id = self.db_session.query(PositionStrategy).filter_by(
                    name=order.position_strategy.value).first().id
                if order_ids:
                    db_order.ibkr_order_ids = str(order_ids)
                if reason:
                    db_order.status_reason = reason[:255]
                db_order.updated_at = datetime.datetime.now()

                self.db_session.commit()
                print(f"✅ Updated {order.symbol} status to {status}: {reason}")
                return True

            else:
                # Record does not exist, create new
                try:
                    db_model = self.convert_to_db_model(order)
                    db_model.status = status
                    db_model.overall_trend = getattr(order, 'overall_trend', None)
                    db_model.brief_analysis = getattr(order, 'brief_analysis', None)
                    if reason:
                        db_model.status_reason = reason[:255]
                    if order_ids:
                        db_model.ibkr_order_ids = str(order_ids)

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

    # <Advanced Feature Integration - Begin>
    # Updated methods for account-specific performance analysis
    def get_trades_by_setup(self, setup_name: str, account_number: str, days_back: int = 90) -> List[Dict]:
        """
        Get all completed trades for a specific trading setup and account within time period.
        
        Args:
            setup_name: Name of the trading setup (e.g., 'Breakout', 'Reversal')
            account_number: Specific account to filter by
            days_back: Number of days to look back (default: 90 days)
            
        Returns:
            List of trade dictionaries with performance data
        """
        try:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_back)
            
            # Join PlannedOrderDB with ExecutedOrderDB to get setup information
            query = self.db_session.query(
                ExecutedOrderDB,
                PlannedOrderDB.trading_setup,
                PlannedOrderDB.timeframe,
                PlannedOrderDB.risk_reward_ratio
            ).join(
                PlannedOrderDB, ExecutedOrderDB.planned_order_id == PlannedOrderDB.id
            ).filter(
                PlannedOrderDB.trading_setup == setup_name,
                ExecutedOrderDB.executed_at >= cutoff_date,
                ExecutedOrderDB.status == 'FILLED',
                ExecutedOrderDB.filled_price.isnot(None),
                ExecutedOrderDB.filled_quantity.isnot(None),
                ExecutedOrderDB.account_number == account_number  # Account-specific filter
            )
            
            results = query.all()
            
            trades = []
            for executed_order, trading_setup, timeframe, risk_reward_ratio in results:
                # Calculate PnL if not already stored
                pnl = executed_order.pnl
                if pnl is None and executed_order.filled_price and executed_order.filled_quantity:
                    # Simple PnL calculation (can be enhanced based on your actual logic)
                    pnl = executed_order.filled_quantity * (executed_order.filled_price - 
                                                          executed_order.planned_order.entry_price)
                
                trade_data = {
                    'order_id': executed_order.id,
                    'symbol': executed_order.planned_order.symbol,
                    'entry_price': executed_order.planned_order.entry_price,
                    'exit_price': executed_order.filled_price,
                    'quantity': executed_order.filled_quantity,
                    'pnl': pnl,
                    'commission': executed_order.commission or 0.0,
                    'entry_time': executed_order.planned_order.created_at,
                    'exit_time': executed_order.executed_at,
                    'trading_setup': trading_setup,
                    'timeframe': timeframe,
                    'risk_reward_ratio': risk_reward_ratio,
                    'account_number': executed_order.account_number  # Include account info
                }
                
                # Calculate PnL percentage if possible
                if (executed_order.planned_order.entry_price and 
                    executed_order.filled_price and 
                    executed_order.planned_order.entry_price > 0):
                    if executed_order.planned_order.action.value == 'BUY':
                        pnl_percentage = ((executed_order.filled_price - executed_order.planned_order.entry_price) / 
                                        executed_order.planned_order.entry_price) * 100
                    else:  # SELL
                        pnl_percentage = ((executed_order.planned_order.entry_price - executed_order.filled_price) / 
                                        executed_order.planned_order.entry_price) * 100
                    trade_data['pnl_percentage'] = pnl_percentage
                
                trades.append(trade_data)
                
            return trades
            
        except Exception as e:
            print(f"Error getting trades for setup {setup_name} on account {account_number}: {e}")
            return []

    def get_all_trading_setups(self, account_number: str, days_back: int = 90) -> List[str]:
        """
        Get all unique trading setups with recent trading activity for specific account.
        
        Args:
            account_number: Specific account to filter by
            days_back: Number of days to look back (default: 90 days)
            
        Returns:
            List of unique setup names
        """
        try:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_back)
            
            query = self.db_session.query(
                PlannedOrderDB.trading_setup
            ).join(
                ExecutedOrderDB, PlannedOrderDB.id == ExecutedOrderDB.planned_order_id
            ).filter(
                ExecutedOrderDB.executed_at >= cutoff_date,
                ExecutedOrderDB.status == 'FILLED',
                ExecutedOrderDB.account_number == account_number,  # Account-specific filter
                PlannedOrderDB.trading_setup.isnot(None),
                PlannedOrderDB.trading_setup != ''
            ).distinct()
            
            results = query.all()
            return [result[0] for result in results if result[0]]
            
        except Exception as e:
            print(f"Error getting trading setups for account {account_number}: {e}")
            return []

    def get_setup_performance_summary(self, account_number: str, days_back: int = 90) -> Dict[str, Dict]:
        """
        Get performance summary for all trading setups for specific account.
        
        Args:
            account_number: Specific account to filter by
            days_back: Number of days to look back
            
        Returns:
            Dictionary with setup names as keys and performance metrics as values
        """
        try:
            setups = self.get_all_trading_setups(account_number, days_back)
            performance_summary = {}
            
            for setup in setups:
                trades = self.get_trades_by_setup(setup, account_number, days_back)
                if trades:
                    performance = self._calculate_setup_performance(trades)
                    performance_summary[setup] = performance
                    
            return performance_summary
            
        except Exception as e:
            print(f"Error getting setup performance summary for account {account_number}: {e}")
            return {}
    # <Advanced Feature Integration - End>

    # <Database to Domain Conversion - Begin>
    def convert_to_planned_order(self, db_order: PlannedOrderDB) -> PlannedOrder:
        """
        Convert a PlannedOrderDB entity back to a PlannedOrder domain object.
        
        Args:
            db_order: Database order entity
            
        Returns:
            PlannedOrder domain object
        """
        try:
            # Convert string values back to enums using proper enum lookup
            action = self._string_to_enum(Action, db_order.action)
            order_type = self._string_to_enum(OrderType, db_order.order_type)
            security_type = self._string_to_enum(SecurityType, db_order.security_type)
            
            # Get position strategy from database - use SQLAlchemy model
            position_strategy_entity = self.db_session.query(PositionStrategy).filter_by(id=db_order.position_strategy_id).first()
            position_strategy = self._string_to_enum(PositionStrategyEnum, position_strategy_entity.name) if position_strategy_entity else PositionStrategyEnum.CORE
            
            # Create PlannedOrder with all required fields
            planned_order = PlannedOrder(
                symbol=db_order.symbol,
                security_type=security_type,
                action=action,
                order_type=order_type,
                entry_price=db_order.entry_price,
                stop_loss=db_order.stop_loss,
                risk_per_trade=db_order.risk_per_trade,
                risk_reward_ratio=db_order.risk_reward_ratio,
                priority=db_order.priority,
                position_strategy=position_strategy,
                overall_trend=db_order.overall_trend,
                brief_analysis=db_order.brief_analysis,
                exchange=getattr(db_order, 'exchange', 'SMART'),
                currency=getattr(db_order, 'currency', 'USD')
            )
            
            return planned_order
            
        except Exception as e:
            print(f"❌ Failed to convert DB order to PlannedOrder: {e}")
            raise

    def _string_to_enum(self, enum_class, value: str):
        """Convert string value to enum member, handling case variations and spaces."""
        if value is None:
            return enum_class(list(enum_class)[0])  # Return first enum member as default
            
        # Clean up the string value
        clean_value = str(value).strip().upper().replace(' ', '_')
        
        # Try direct match first
        try:
            return enum_class[clean_value]
        except KeyError:
            pass
            
        # Try value-based lookup
        for member in enum_class:
            if member.value.upper() == clean_value or member.name.upper() == clean_value:
                return member
                
        # Fallback to first member
        print(f"⚠️  Could not map '{value}' to {enum_class.__name__}, using default")
        return enum_class(list(enum_class)[0])
    # <Database to Domain Conversion - End>