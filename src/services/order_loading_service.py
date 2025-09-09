from src.core.models import PlannedOrderDB
from src.core.planned_order import PlannedOrderManager


class OrderLoadingService:
    """
    Service responsible for loading, validating, and preparing planned orders.
    Handles Excel parsing, validation, duplicate detection, and persistence.
    """

    def __init__(self, trading_manager, db_session):
        self._trading_manager = trading_manager
        self._db_session = db_session

    def load_and_validate_orders(self, excel_path):
        """
        Load and validate planned orders from Excel.
        Returns: List of valid PlannedOrder objects.
        """
        try:
            # Load from Excel first
            excel_orders = PlannedOrderManager.from_excel(excel_path)
            print(f"Loaded {len(excel_orders)} planned orders from Excel")
            
            valid_orders = []
            invalid_count = 0
            duplicate_count = 0
            
            for excel_order in excel_orders:
                # Basic sanity checks
                if excel_order.entry_price is None:
                    print(f"❌ Skipping order {excel_order.symbol}: missing entry price")
                    invalid_count += 1
                    continue
                    
                if excel_order.stop_loss is None:
                    print(f"❌ Skipping order {excel_order.symbol}: missing stop loss")  
                    invalid_count += 1
                    continue
                    
                if excel_order.entry_price == excel_order.stop_loss:
                    print(f"❌ Skipping order {excel_order.symbol}: entry price equals stop loss")
                    invalid_count += 1
                    continue
                    
                # Business-rule validation
                if not self._trading_manager._validate_order_basic(excel_order):
                    print(f"❌ Skipping order {excel_order.symbol}: basic validation failed")
                    invalid_count += 1
                    continue
                    
                # Check for duplicates in current Excel load
                is_duplicate = False
                for valid_order in valid_orders:
                    if (valid_order.symbol == excel_order.symbol and
                        valid_order.entry_price == excel_order.entry_price and
                        valid_order.stop_loss == excel_order.stop_loss and
                        valid_order.action == excel_order.action):
                        print(f"❌ Skipping duplicate order in Excel: {excel_order.symbol} @ {excel_order.entry_price}")
                        duplicate_count += 1
                        is_duplicate = True
                        break
                
                if is_duplicate:
                    continue
                    
                # Check for duplicates in existing database
                existing_order = self._find_existing_planned_order(excel_order)
                if existing_order:
                    print(f"❌ Skipping duplicate order in database: {excel_order.symbol} @ {excel_order.entry_price}")
                    duplicate_count += 1
                    continue
                    
                valid_orders.append(excel_order)
                print(f"✅ Valid order: {excel_order.symbol} - Entry: ${excel_order.entry_price:.4f}, Stop: ${excel_order.stop_loss:.4f}")
            
            print(f"✅ Found {len(valid_orders)} valid orders")
            if invalid_count > 0:
                print(f"⚠️  Skipped {invalid_count} invalid orders")
            if duplicate_count > 0:
                print(f"⚠️  Skipped {duplicate_count} duplicate orders")
            
            return valid_orders
            
        except Exception as e:
            self._db_session.rollback()
            print(f"Error loading planned orders: {e}")
            return []

    def _find_existing_planned_order(self, order):
        """
        Check if an order already exists in the database.
        Returns: The existing PlannedOrderDB if found, None otherwise.
        """
        try:
            existing_order = self._db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value
            ).first()
            
            return existing_order
            
        except Exception as e:
            print(f"❌ Error checking for existing order {order.symbol}: {e}")
            return None