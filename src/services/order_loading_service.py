"""
Service responsible for loading, validating, and preparing planned orders from Excel.
Handles parsing, validation, duplicate detection (in-file and in-database), and filtering.
"""

from typing import Any, Dict, Optional
from src.core.models import PlannedOrderDB
from src.core.planned_order import PlannedOrderManager


class OrderLoadingService:
    """Orchestrates the loading and validation of planned orders from an Excel template."""

    def __init__(self, trading_manager, db_session, config: Optional[Dict[str, Any]] = None):
        """Initialize the service with a trading manager, database session, and configuration."""
        self._trading_manager = trading_manager
        self._db_session = db_session
        self.config = config or {}  # <-- STORE CONFIGURATION
        
    def load_and_validate_orders(self, excel_path) -> list:
        """
        Load orders from Excel, validate them, and filter out duplicates/invalid entries.
        Returns a list of valid PlannedOrder objects.
        """
        try:
            excel_orders = PlannedOrderManager.from_excel(excel_path, self.config)
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
                if not self._validate_order_basic_fields(excel_order):
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

    def _find_existing_planned_order(self, order) -> Optional[PlannedOrderDB]:
        """Check if an order with identical parameters already exists in the database."""
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

    def _validate_order_basic_fields(self, order) -> tuple[bool, str]:
        """Layer 1: Basic field validation for data integrity before persistence."""
        try:
            # Symbol validation
            symbol_str = str(order.symbol).strip() if order.symbol else ""
            if not symbol_str or symbol_str in ['', '0', 'nan', 'None', 'null']:
                return False, f"Invalid symbol: '{order.symbol}'"
            
            # Symbol should be meaningful (at least 1 character, not just numbers)
            if len(symbol_str) < 1 or (symbol_str.isdigit() and len(symbol_str) < 2):
                return False, f"Symbol too short or invalid: '{order.symbol}'"
                
            # Price validation
            if order.entry_price is None or order.entry_price <= 0:
                return False, f"Invalid entry price: {order.entry_price}"
                
            # Stop loss validation (basic syntax only - business logic comes later)
            if order.stop_loss is not None and order.stop_loss <= 0:
                return False, f"Invalid stop loss price: {order.stop_loss}"
                
            # Action validation
            if order.action not in ['BUY', 'SELL']:
                return False, f"Invalid action: {order.action}"
                
            # Security type validation
            if not order.security_type:
                return False, "Security type is required"
                
            # Exchange validation
            if not order.exchange:
                return False, "Exchange is required"
                
            return True, "Basic validation passed"
            
        except Exception as e:
            return False, f"Basic validation error: {e}"
        