"""
Service responsible for loading, validating, and preparing planned orders from Excel.
Handles parsing, validation, duplicate detection (in-file and in-database), and filtering.
"""

from typing import Any, Dict, Optional
from src.core.models import PlannedOrderDB
from src.core.planned_order import PlannedOrderManager

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)


class OrderLoadingService:
    """Orchestrates the loading and validation of planned orders from an Excel template."""

    def __init__(self, trading_manager, db_session, config: Optional[Dict[str, Any]] = None):
        """Initialize the service with a trading manager, database session, and configuration."""
        if logger:
            logger.debug("Initializing OrderLoadingService")
            
        self._trading_manager = trading_manager
        self._db_session = db_session
        self.config = config or {}  # <-- STORE CONFIGURATION
        
        if logger:
            logger.info("OrderLoadingService initialized successfully")
        
    def load_and_validate_orders(self, excel_path) -> list:
        """
        Load orders from Excel, validate them, and filter out duplicates/invalid entries.
        Returns a list of valid PlannedOrder objects.
        """
        if logger:
            logger.info(f"Loading and validating orders from Excel: {excel_path}")
            
        try:
            excel_orders = PlannedOrderManager.from_excel(excel_path, self.config)
            if logger:
                logger.info(f"Loaded {len(excel_orders)} planned orders from Excel")

            valid_orders = []
            invalid_count = 0
            duplicate_count = 0

            for excel_order in excel_orders:
                # Basic sanity checks
                if excel_order.entry_price is None:
                    if logger:
                        logger.warning(f"Skipping order {excel_order.symbol}: missing entry price")
                    invalid_count += 1
                    continue

                if excel_order.stop_loss is None:
                    if logger:
                        logger.warning(f"Skipping order {excel_order.symbol}: missing stop loss")
                    invalid_count += 1
                    continue

                if excel_order.entry_price == excel_order.stop_loss:
                    if logger:
                        logger.warning(f"Skipping order {excel_order.symbol}: entry price equals stop loss")
                    invalid_count += 1
                    continue

                # Business-rule validation
                if not self._validate_order_basic_fields(excel_order):
                    if logger:
                        logger.warning(f"Skipping order {excel_order.symbol}: basic validation failed")
                    invalid_count += 1
                    continue

                # Check for duplicates in current Excel load
                is_duplicate = False
                for valid_order in valid_orders:
                    if (valid_order.symbol == excel_order.symbol and
                        valid_order.entry_price == excel_order.entry_price and
                        valid_order.stop_loss == excel_order.stop_loss and
                        valid_order.action == excel_order.action):
                        if logger:
                            logger.warning(f"Skipping duplicate order in Excel: {excel_order.symbol} @ {excel_order.entry_price}")
                        duplicate_count += 1
                        is_duplicate = True
                        break

                if is_duplicate:
                    continue

                # Check for duplicates in existing database
                existing_order = self._find_existing_planned_order(excel_order)
                if existing_order:
                    if logger:
                        logger.warning(f"Skipping duplicate order in database: {excel_order.symbol} @ {excel_order.entry_price}")
                    duplicate_count += 1
                    continue

                valid_orders.append(excel_order)
                if logger:
                    logger.debug(f"Valid order: {excel_order.symbol} - Entry: ${excel_order.entry_price:.4f}, Stop: ${excel_order.stop_loss:.4f}")

            if logger:
                logger.info(f"Found {len(valid_orders)} valid orders")
                if invalid_count > 0:
                    logger.warning(f"Skipped {invalid_count} invalid orders")
                if duplicate_count > 0:
                    logger.warning(f"Skipped {duplicate_count} duplicate orders")

            return valid_orders

        except Exception as e:
            self._db_session.rollback()
            if logger:
                logger.error(f"Error loading planned orders: {e}")
            return []

    def _find_existing_planned_order(self, order) -> Optional[PlannedOrderDB]:
        """Check if an order with identical parameters already exists in the database."""
        if logger:
            logger.debug(f"Checking for existing order in database: {order.symbol}")
            
        try:
            existing_order = self._db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value
            ).first()
            
            if logger:
                if existing_order:
                    logger.debug(f"Found existing order in database: {order.symbol}")
                else:
                    logger.debug(f"No existing order found in database: {order.symbol}")
                    
            return existing_order
        except Exception as e:
            if logger:
                logger.error(f"Error checking for existing order {order.symbol}: {e}")
            return None

    def _validate_order_basic_fields(self, order) -> bool:
        """Layer 1: Basic field validation for data integrity before persistence."""
        if logger:
            logger.debug(f"Validating basic fields for order: {order.symbol}")
            
        try:
            # Symbol validation
            symbol_str = str(order.symbol).strip() if order.symbol else ""
            if not symbol_str or symbol_str in ['', '0', 'nan', 'None', 'null']:
                if logger:
                    logger.warning(f"Invalid symbol: '{order.symbol}'")
                return False
            
            # Symbol should be meaningful (at least 1 character, not just numbers)
            if len(symbol_str) < 1 or (symbol_str.isdigit() and len(symbol_str) < 2):
                if logger:
                    logger.warning(f"Symbol too short or invalid: '{order.symbol}'")
                return False
                
            # Price validation
            if order.entry_price is None or order.entry_price <= 0:
                if logger:
                    logger.warning(f"Invalid entry price: {order.entry_price}")
                return False
                
            # Stop loss validation (basic syntax only - business logic comes later)
            if order.stop_loss is not None and order.stop_loss <= 0:
                if logger:
                    logger.warning(f"Invalid stop loss price: {order.stop_loss}")
                return False
                
            # FIXED: Action validation - handle both enum objects and strings
            from src.core.planned_order import Action
            if isinstance(order.action, Action):
                # Enum comparison
                if order.action not in [Action.BUY, Action.SELL, Action.SSHORT]:
                    if logger:
                        logger.warning(f"Invalid action: {order.action}")
                    return False
            else:
                # String comparison (fallback)
                if order.action not in ['BUY', 'SELL', 'SSHORT']:
                    if logger:
                        logger.warning(f"Invalid action: {order.action}")
                    return False
                
            # FIXED: Security type validation - handle enum objects
            from src.core.planned_order import SecurityType
            if not order.security_type:
                if logger:
                    logger.warning("Security type is required")
                return False
            elif isinstance(order.security_type, SecurityType):
                # Valid enum - no need to check further
                pass
            elif not isinstance(order.security_type, str):
                if logger:
                    logger.warning(f"Invalid security type: {order.security_type}")
                return False
                
            # Exchange validation
            if not order.exchange:
                if logger:
                    logger.warning("Exchange is required")
                return False
                
            if logger:
                logger.debug(f"Basic validation passed for order: {order.symbol}")
            return True
            
        except Exception as e:
            if logger:
                logger.error(f"Basic validation error for {order.symbol}: {e}")
            return False