"""
Service responsible for loading, validating, and preparing planned orders from Excel.
Handles parsing, validation, duplicate detection (in-file and in-database), and filtering.
"""

import datetime
from typing import Any, Dict, Optional
from src.core.models import PlannedOrderDB
from src.core.planned_order import PlannedOrderManager

# Context-aware logging import - replacing simple_logger
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()


class OrderLoadingService:
    """Orchestrates the loading and validation of planned orders from an Excel template."""

    def __init__(self, trading_manager, db_session, config: Optional[Dict[str, Any]] = None):
        """Initialize the service with a trading manager, database session, and configuration."""
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing OrderLoadingService",
            context_provider={
                "trading_manager_provided": trading_manager is not None,
                "db_session_provided": db_session is not None,
                "config_provided": config is not None,
                "config_keys": list(config.keys()) if config else []
            }
        )
            
        self._trading_manager = trading_manager
        self._db_session = db_session
        self.config = config or {}  # <-- STORE CONFIGURATION
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "OrderLoadingService initialized successfully",
            context_provider={
                "session_id": context_logger.session_id,
                "config_entries": len(self.config)
            }
        )
        
    def load_and_validate_orders(self, excel_path) -> list:
        """
        Load orders from Excel, validate them, and filter out duplicates/invalid entries.
        Returns a list of valid PlannedOrder objects.
        """
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Loading and validating orders from Excel",
            context_provider={
                "excel_path": excel_path,
                "config_entries": len(self.config)
            }
        )
            
        try:
            excel_orders = PlannedOrderManager.from_excel(excel_path, self.config)
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                f"Loaded planned orders from Excel",
                context_provider={
                    "excel_path": excel_path,
                    "orders_loaded": len(excel_orders),
                    "order_symbols": [order.symbol for order in excel_orders]
                }
            )

            valid_orders = []
            invalid_count = 0
            duplicate_count = 0

            for excel_order in excel_orders:
                # Basic sanity checks
                if excel_order.entry_price is None:
                    context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        f"Skipping order: missing entry price",
                        symbol=excel_order.symbol,
                        context_provider={
                            "validation_failure": "missing_entry_price",
                            "symbol": excel_order.symbol,
                            "order_details": {
                                "action": excel_order.action.value,
                                "order_type": excel_order.order_type.value
                            }
                        },
                        decision_reason="INVALID_ORDER_MISSING_ENTRY_PRICE"
                    )
                    invalid_count += 1
                    continue

                if excel_order.stop_loss is None:
                    context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        f"Skipping order: missing stop loss",
                        symbol=excel_order.symbol,
                        context_provider={
                            "validation_failure": "missing_stop_loss",
                            "symbol": excel_order.symbol,
                            "entry_price": excel_order.entry_price
                        },
                        decision_reason="INVALID_ORDER_MISSING_STOP_LOSS"
                    )
                    invalid_count += 1
                    continue

                if excel_order.entry_price == excel_order.stop_loss:
                    context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        f"Skipping order: entry price equals stop loss",
                        symbol=excel_order.symbol,
                        context_provider={
                            "validation_failure": "entry_equals_stop_loss",
                            "symbol": excel_order.symbol,
                            "entry_price": excel_order.entry_price,
                            "stop_loss": excel_order.stop_loss
                        },
                        decision_reason="INVALID_ORDER_EQUAL_PRICES"
                    )
                    invalid_count += 1
                    continue

                # Business-rule validation
                if not self._validate_order_basic_fields(excel_order):
                    context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        f"Skipping order: basic validation failed",
                        symbol=excel_order.symbol,
                        context_provider={
                            "validation_failure": "basic_validation_failed",
                            "symbol": excel_order.symbol,
                            "entry_price": excel_order.entry_price,
                            "stop_loss": excel_order.stop_loss
                        },
                        decision_reason="INVALID_ORDER_BASIC_VALIDATION_FAILED"
                    )
                    invalid_count += 1
                    continue

                # Check for duplicates in current Excel load
                is_duplicate = False
                for valid_order in valid_orders:
                    if (valid_order.symbol == excel_order.symbol and
                        valid_order.entry_price == excel_order.entry_price and
                        valid_order.stop_loss == excel_order.stop_loss and
                        valid_order.action == excel_order.action):
                        context_logger.log_event(
                            TradingEventType.ORDER_VALIDATION,
                            f"Skipping duplicate order in Excel",
                            symbol=excel_order.symbol,
                            context_provider={
                                "duplicate_type": "in_file_duplicate",
                                "symbol": excel_order.symbol,
                                "entry_price": excel_order.entry_price,
                                "stop_loss": excel_order.stop_loss,
                                "action": excel_order.action.value,
                                "matching_order_index": valid_orders.index(valid_order)
                            },
                            decision_reason="DUPLICATE_ORDER_IN_FILE"
                        )
                        duplicate_count += 1
                        is_duplicate = True
                        break

                if is_duplicate:
                    continue

                # Check for duplicates in existing database
                existing_order = self._find_existing_planned_order(excel_order)
                if existing_order:
                    context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        f"Skipping duplicate order in database",
                        symbol=excel_order.symbol,
                        context_provider={
                            "duplicate_type": "database_duplicate",
                            "symbol": excel_order.symbol,
                            "entry_price": excel_order.entry_price,
                            "stop_loss": excel_order.stop_loss,
                            "action": excel_order.action.value,
                            "existing_order_id": existing_order.id,
                            "existing_order_status": existing_order.status
                        },
                        decision_reason="DUPLICATE_ORDER_IN_DATABASE"
                    )
                    duplicate_count += 1
                    continue

                valid_orders.append(excel_order)
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Order validated successfully",
                    symbol=excel_order.symbol,
                    context_provider={
                        "validation_result": "valid",
                        "symbol": excel_order.symbol,
                        "entry_price": excel_order.entry_price,
                        "stop_loss": excel_order.stop_loss,
                        "action": excel_order.action.value,
                        "order_type": excel_order.order_type.value,
                        "security_type": excel_order.security_type.value,
                        "valid_order_index": len(valid_orders) - 1
                    },
                    decision_reason="ORDER_VALIDATION_PASSED"
                )

            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                f"Excel order loading completed",
                context_provider={
                    "excel_path": excel_path,
                    "total_orders_processed": len(excel_orders),
                    "valid_orders_found": len(valid_orders),
                    "invalid_orders_skipped": invalid_count,
                    "duplicate_orders_skipped": duplicate_count,
                    "validation_success_rate": len(valid_orders) / len(excel_orders) if excel_orders else 0,
                    "valid_order_symbols": [order.symbol for order in valid_orders]
                },
                decision_reason="EXCEL_ORDER_LOADING_COMPLETED"
            )
            return valid_orders

        except Exception as e:
            self._db_session.rollback()
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error loading planned orders from Excel",
                context_provider={
                    "excel_path": excel_path,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "load_and_validate_orders"
                },
                decision_reason="EXCEL_ORDER_LOADING_FAILED"
            )
            return []

    def _find_existing_planned_order(self, order) -> Optional[PlannedOrderDB]:
        """Check if an order with identical parameters already exists in the database."""
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            f"Checking for existing order in database",
            symbol=order.symbol,
            context_provider={
                "symbol": order.symbol,
                "entry_price": order.entry_price,
                "stop_loss": order.stop_loss,
                "action": order.action.value,
                "query_type": "duplicate_check"
            }
        )
            
        try:
            existing_order = self._db_session.query(PlannedOrderDB).filter_by(
                symbol=order.symbol,
                entry_price=order.entry_price,
                stop_loss=order.stop_loss,
                action=order.action.value
            ).first()
            
            context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                f"Existing order check completed",
                symbol=order.symbol,
                context_provider={
                    "order_exists": existing_order is not None,
                    "existing_order_id": existing_order.id if existing_order else None,
                    "existing_order_status": existing_order.status if existing_order else None,
                    "query_execution_time": datetime.datetime.now().isoformat()
                },
                decision_reason="DUPLICATE_CHECK_COMPLETED"
            )
                    
            return existing_order
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Error checking for existing order in database",
                symbol=order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "symbol": order.symbol,
                    "operation": "_find_existing_planned_order"
                },
                decision_reason="DUPLICATE_CHECK_FAILED"
            )
            return None

    def _validate_order_basic_fields(self, order) -> bool:
        """Layer 1: Basic field validation for data integrity before persistence."""
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            f"Validating basic fields for order",
            symbol=order.symbol,
            context_provider={
                "symbol": order.symbol,
                "entry_price": order.entry_price,
                "stop_loss": order.stop_loss,
                "action": order.action.value if hasattr(order.action, 'value') else str(order.action),
                "order_type": order.order_type.value if hasattr(order.order_type, 'value') else str(order.order_type),
                "security_type": order.security_type.value if hasattr(order.security_type, 'value') else str(order.security_type)
            }
        )
            
        try:
            # Symbol validation
            symbol_str = str(order.symbol).strip() if order.symbol else ""
            if not symbol_str or symbol_str in ['', '0', 'nan', 'None', 'null']:
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Symbol validation failed: invalid symbol",
                    symbol=order.symbol,
                    context_provider={
                        "validation_field": "symbol",
                        "validation_failure": "invalid_symbol_format",
                        "symbol_value": order.symbol,
                        "cleaned_symbol": symbol_str
                    },
                    decision_reason="SYMBOL_VALIDATION_FAILED"
                )
                return False
            
            # Symbol should be meaningful (at least 1 character, not just numbers)
            if len(symbol_str) < 1 or (symbol_str.isdigit() and len(symbol_str) < 2):
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Symbol validation failed: symbol too short or invalid",
                    symbol=order.symbol,
                    context_provider={
                        "validation_field": "symbol",
                        "validation_failure": "symbol_too_short",
                        "symbol_length": len(symbol_str),
                        "is_numeric": symbol_str.isdigit(),
                        "symbol_value": symbol_str
                    },
                    decision_reason="SYMBOL_LENGTH_VALIDATION_FAILED"
                )
                return False
                
            # Price validation
            if order.entry_price is None or order.entry_price <= 0:
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Entry price validation failed",
                    symbol=order.symbol,
                    context_provider={
                        "validation_field": "entry_price",
                        "validation_failure": "invalid_entry_price",
                        "entry_price_value": order.entry_price,
                        "price_condition": "<= 0" if order.entry_price else "None"
                    },
                    decision_reason="ENTRY_PRICE_VALIDATION_FAILED"
                )
                return False
                
            # Stop loss validation (basic syntax only - business logic comes later)
            if order.stop_loss is not None and order.stop_loss <= 0:
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Stop loss validation failed",
                    symbol=order.symbol,
                    context_provider={
                        "validation_field": "stop_loss",
                        "validation_failure": "invalid_stop_loss",
                        "stop_loss_value": order.stop_loss,
                        "price_condition": "<= 0"
                    },
                    decision_reason="STOP_LOSS_VALIDATION_FAILED"
                )
                return False
                
            # <Fix Enum Validation - Begin>
            # Action validation - handle enum objects properly
            from src.core.planned_order import Action
            if isinstance(order.action, Action):
                # Enum comparison - this is the correct way
                if order.action not in [Action.BUY, Action.SELL, Action.SSHORT]:
                    context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        f"Action validation failed: invalid enum value",
                        symbol=order.symbol,
                        context_provider={
                            "validation_field": "action",
                            "validation_failure": "invalid_action_enum",
                            "action_value": order.action.value,
                            "valid_actions": [action.value for action in [Action.BUY, Action.SELL, Action.SSHORT]]
                        },
                        decision_reason="ACTION_ENUM_VALIDATION_FAILED"
                    )
                    return False
            else:
                # String comparison (fallback for robustness)
                if order.action not in ['BUY', 'SELL', 'SSHORT']:
                    context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        f"Action validation failed: invalid string value",
                        symbol=order.symbol,
                        context_provider={
                            "validation_field": "action",
                            "validation_failure": "invalid_action_string",
                            "action_value": order.action,
                            "valid_actions": ['BUY', 'SELL', 'SSHORT']
                        },
                        decision_reason="ACTION_STRING_VALIDATION_FAILED"
                    )
                    return False
                
            # Security type validation - handle enum objects properly
            from src.core.planned_order import SecurityType
            if not order.security_type:
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Security type validation failed: missing value",
                    symbol=order.symbol,
                    context_provider={
                        "validation_field": "security_type",
                        "validation_failure": "missing_security_type",
                        "security_type_value": order.security_type
                    },
                    decision_reason="SECURITY_TYPE_MISSING_VALIDATION_FAILED"
                )
                return False
            elif isinstance(order.security_type, SecurityType):
                # Valid enum - no need to check further
                pass
            elif not isinstance(order.security_type, str):
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Security type validation failed: invalid type",
                    symbol=order.symbol,
                    context_provider={
                        "validation_field": "security_type",
                        "validation_failure": "invalid_security_type",
                        "security_type_value": order.security_type,
                        "security_type_actual_type": type(order.security_type).__name__
                    },
                    decision_reason="SECURITY_TYPE_TYPE_VALIDATION_FAILED"
                )
                return False
            # <Fix Enum Validation - End>
                
            # Exchange validation
            if not order.exchange:
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Exchange validation failed: missing value",
                    symbol=order.symbol,
                    context_provider={
                        "validation_field": "exchange",
                        "validation_failure": "missing_exchange",
                        "exchange_value": order.exchange
                    },
                    decision_reason="EXCHANGE_MISSING_VALIDATION_FAILED"
                )
                return False
                
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                f"Basic field validation passed",
                symbol=order.symbol,
                context_provider={
                    "validation_result": "passed",
                    "symbol": order.symbol,
                    "fields_validated": ["symbol", "entry_price", "stop_loss", "action", "security_type", "exchange"]
                },
                decision_reason="BASIC_FIELD_VALIDATION_PASSED"
            )
            return True
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Basic validation error occurred",
                symbol=order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "symbol": order.symbol,
                    "operation": "_validate_order_basic_fields"
                },
                decision_reason="BASIC_VALIDATION_ERROR"
            )
            return False