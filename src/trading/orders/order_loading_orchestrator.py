"""
Orchestrates order loading from multiple sources including database resumption and Excel input.
Handles multi-source order loading with proper deduplication and error resilience.
Provides comprehensive order loading capabilities for production trading systems.
"""

import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session

from src.trading.orders.planned_order import PlannedOrder
from src.core.models import PlannedOrderDB
from src.trading.orders.order_loading_service import OrderLoadingService
from src.trading.orders.order_persistence_service import OrderPersistenceService
from src.services.state_service import StateService

# <IBKR Integration - Begin>
from src.brokers.ibkr.ibkr_client import IbkrClient
from src.brokers.ibkr.types.ibkr_types import IbkrOrder
# <IBKR Integration - End>

# <Session Awareness Integration - Begin>
from src.core.shared_enums import OrderState as SharedOrderState
# <Session Awareness Integration - End>

# Context-aware logging import - replacing simple_logger
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()


class OrderLoadingOrchestrator:
    """
    Orchestrates order loading from multiple sources with proper deduplication
    and error handling. Supports database order resumption and Excel order loading.
    """
    
    def __init__(self, 
                 loading_service: OrderLoadingService,
                 persistence_service: OrderPersistenceService,
                 state_service: StateService,
                 db_session: Session,
                 # <IBKR Integration - Begin>
                 ibkr_client: Optional[IbkrClient] = None
                 # <IBKR Integration - End>
                 ):
        """
        Initialize the order loading orchestrator with required services.
        
        Args:
            loading_service: Service for loading orders from Excel
            persistence_service: Service for order persistence operations
            state_service: Service for checking system state
            db_session: Database session for querying orders
            ibkr_client: IBKR client for order discovery (optional)
        """
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing OrderLoadingOrchestrator",
            context_provider={
                "loading_service_provided": loading_service is not None,
                "persistence_service_provided": persistence_service is not None,
                "state_service_provided": state_service is not None,
                "db_session_provided": db_session is not None,
                "ibkr_client_provided": ibkr_client is not None,
                "ibkr_client_connected": ibkr_client.connected if ibkr_client else False
            }
        )
            
        self.loading_service = loading_service
        self.persistence_service = persistence_service
        self.state_service = state_service
        self.db_session = db_session
        # <IBKR Integration - Begin>
        self.ibkr_client = ibkr_client
        # <IBKR Integration - End>
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "OrderLoadingOrchestrator initialized successfully",
            context_provider={
                "session_id": context_logger.session_id,
                "multi_source_loading_enabled": True
            }
        )
        
    def load_all_orders(self, excel_path: str) -> List[PlannedOrder]:
        """
        Load orders from all available sources and merge results.
        
        Args:
            excel_path: Path to Excel file containing new orders
            
        Returns:
            List of PlannedOrder objects from all sources after deduplication
        """
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Loading orders from all sources",
            context_provider={
                "excel_path": excel_path,
                "sources_available": ["database", "excel", "ibkr"],
                "ibkr_integration_enabled": self.ibkr_client is not None
            }
        )
        
        all_orders = []
        sources_loaded = 0
        
        try:
            # Load from database (order resumption) - FIXED: Load active orders FIRST
            db_orders = self._load_from_database()
            if db_orders:
                all_orders.extend(db_orders)
                sources_loaded += 1
                context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    "Database orders loaded successfully",
                    context_provider={
                        "database_orders_count": len(db_orders),
                        "source_priority": "first"
                    },
                    decision_reason="DATABASE_ORDERS_RESUMED"
                )
            else:
                context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    "No active orders to resume from database",
                    context_provider={
                        "database_orders_count": 0
                    },
                    decision_reason="NO_ACTIVE_DATABASE_ORDERS"
                )
                
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Database loading failed",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_load_from_database"
                },
                decision_reason="DATABASE_LOADING_FAILED"
            )
            # Continue with other sources despite database failure
            
        try:
            # Load from Excel (new orders and updates)
            excel_orders = self._load_from_excel(excel_path)
            if excel_orders:
                all_orders.extend(excel_orders)
                sources_loaded += 1
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "Excel orders loaded successfully",
                    context_provider={
                        "excel_orders_count": len(excel_orders),
                        "source_priority": "second"
                    },
                    decision_reason="EXCEL_ORDERS_LOADED"
                )
            else:
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "No orders found in Excel file",
                    context_provider={
                        "excel_path": excel_path,
                        "excel_orders_count": 0
                    },
                    decision_reason="NO_EXCEL_ORDERS"
                )
                
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Excel loading failed",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "excel_path": excel_path,
                    "operation": "_load_from_excel"
                },
                decision_reason="EXCEL_LOADING_FAILED"
            )
            # Continue with available orders despite Excel failure

        # <IBKR Order Discovery - Begin>
        try:
            # Load from IBKR (working orders discovery)
            ibkr_orders = self._discover_ibkr_orders()
            if ibkr_orders:
                all_orders.extend(ibkr_orders)
                sources_loaded += 1
                context_logger.log_event(
                    TradingEventType.STATE_TRANSITION,
                    "IBKR orders discovered successfully",
                    context_provider={
                        "ibkr_orders_count": len(ibkr_orders),
                        "source_priority": "third",
                        "ibkr_client_connected": self.ibkr_client.connected if self.ibkr_client else False
                    },
                    decision_reason="IBKR_ORDERS_DISCOVERED"
                )
            else:
                context_logger.log_event(
                    TradingEventType.STATE_TRANSITION,
                    "No working orders discovered from IBKR",
                    context_provider={
                        "ibkr_orders_count": 0,
                        "ibkr_client_connected": self.ibkr_client.connected if self.ibkr_client else False
                    },
                    decision_reason="NO_IBKR_ORDERS"
                )
                
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR discovery failed",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_discover_ibkr_orders"
                },
                decision_reason="IBKR_DISCOVERY_FAILED"
            )
            # Graceful degradation: continue without IBKR orders
        # <IBKR Order Discovery - End>
            
        # Merge and deduplicate orders from all sources with session awareness
        merged_orders = self._merge_orders(all_orders)
        
        # <Conflict Logging - Begin>
        # Log any conflicts between sources for manual review
        self._log_order_conflicts(all_orders, merged_orders)
        # <Conflict Logging - End>
        
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Multi-source order loading completed",
            context_provider={
                "sources_loaded": sources_loaded,
                "total_sources_available": 3,
                "total_orders_before_merge": len(all_orders),
                "total_orders_after_merge": len(merged_orders),
                "duplicates_removed": len(all_orders) - len(merged_orders),
                "merge_efficiency_percent": round((len(merged_orders) / len(all_orders)) * 100, 2) if all_orders else 0
            },
            decision_reason="MULTI_SOURCE_ORDER_LOADING_COMPLETED"
        )
              
        return merged_orders

    # <Session-Aware Database Loading - Begin>
    def _load_from_database(self) -> List[PlannedOrder]:
        """
        Load and resume active orders from database with session awareness.
        
        Returns:
            List of PlannedOrder objects representing active database orders
        """
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            "Loading orders from database",
            context_provider={
                "target_statuses": [
                    SharedOrderState.PENDING.value,
                    SharedOrderState.LIVE.value, 
                    SharedOrderState.LIVE_WORKING.value
                ],
                "session_awareness_enabled": True
            }
        )
            
        try:
            # Query for active orders that should be resumed
            # FIXED: Include LIVE_WORKING status and filter by expiration
            active_db_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.status.in_([
                    SharedOrderState.PENDING.value,
                    SharedOrderState.LIVE.value, 
                    SharedOrderState.LIVE_WORKING.value
                ])
            ).all()
            
            if not active_db_orders:
                context_logger.log_event(
                    TradingEventType.DATABASE_STATE,
                    "No active orders found in database",
                    context_provider={
                        "query_result": "empty"
                    },
                    decision_reason="NO_ACTIVE_DATABASE_ORDERS_FOUND"
                )
                return []
                
            # Convert to PlannedOrder objects and filter out expired orders
            planned_orders = []
            resumed_count = 0
            expired_count = 0
            
            for db_order in active_db_orders:
                try:
                    planned_order = self.persistence_service.convert_to_planned_order(db_order)
                    
                    # Check if order should be resumed based on session and expiration
                    should_resume, reason = self._should_resume_order(db_order, planned_order)
                    
                    if should_resume:
                        planned_orders.append(planned_order)
                        resumed_count += 1
                        context_logger.log_event(
                            TradingEventType.STATE_TRANSITION,
                            "Order resumed from database",
                            symbol=db_order.symbol,
                            context_provider={
                                "order_id": db_order.id,
                                "resume_reason": reason,
                                "position_strategy": db_order.position_strategy,
                                "created_date": db_order.created_at.isoformat() if db_order.created_at else None
                            },
                            decision_reason="DATABASE_ORDER_RESUMED"
                        )
                    else:
                        expired_count += 1
                        context_logger.log_event(
                            TradingEventType.STATE_TRANSITION,
                            "Order not resumed - expired/skipped",
                            symbol=db_order.symbol,
                            context_provider={
                                "order_id": db_order.id,
                                "skip_reason": reason,
                                "position_strategy": db_order.position_strategy,
                                "created_date": db_order.created_at.isoformat() if db_order.created_at else None
                            },
                            decision_reason="DATABASE_ORDER_SKIPPED"
                        )
                        
                except Exception as e:
                    context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "Failed to convert DB order",
                        symbol=db_order.symbol,
                        context_provider={
                            "order_id": db_order.id,
                            "error_type": type(e).__name__,
                            "error_details": str(e),
                            "operation": "convert_to_planned_order"
                        },
                        decision_reason="DB_ORDER_CONVERSION_FAILED"
                    )
                    continue
                    
            context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                "Database order loading summary",
                context_provider={
                    "total_database_orders": len(active_db_orders),
                    "orders_resumed": resumed_count,
                    "orders_expired_skipped": expired_count,
                    "resume_rate_percent": round((resumed_count / len(active_db_orders)) * 100, 2) if active_db_orders else 0
                },
                decision_reason="DATABASE_ORDER_LOADING_SUMMARY"
            )
            return planned_orders
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Database order loading failed",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_load_from_database"
                },
                decision_reason="DATABASE_ORDER_LOADING_FAILED"
            )
            return []
            
    def _should_resume_order(self, db_order: PlannedOrderDB, planned_order: PlannedOrder) -> tuple[bool, str]:
        """
        Determine if a database order should be resumed based on session and expiration.
        
        Args:
            db_order: Database order record
            planned_order: Converted PlannedOrder object
            
        Returns:
            Tuple of (should_resume, reason)
        """
        # Check if order is expired based on position strategy
        if self._is_order_expired(db_order):
            return False, f"Expired {db_order.position_strategy} strategy"
            
        # Check if this is a cross-session scenario (order from previous day)
        is_cross_session = self._is_cross_session_order(db_order)
        
        if is_cross_session:
            # <Fix PositionStrategy Enum Handling - Begin>
            # Cross-session rules - handle PositionStrategy enum properly
            from src.trading.orders.planned_order import PositionStrategy
            
            # Get strategy as string for comparison
            if isinstance(db_order.position_strategy, PositionStrategy):
                strategy_str = db_order.position_strategy.value.upper()
            else:
                strategy_str = str(db_order.position_strategy).upper()
                
            if strategy_str == 'DAY':
                return False, "DAY strategy expired (cross-session)"
            elif strategy_str == 'HYBRID':
                # HYBRID orders can resume across sessions within 10-day window
                days_old = (datetime.datetime.now().date() - db_order.created_at.date()).days
                if days_old <= 10:
                    return True, f"HYBRID strategy valid ({days_old}/10 days)"
                else:
                    return False, f"HYBRID strategy expired ({days_old}/10 days)"
            else:  # CORE strategy
                return True, "CORE strategy (cross-session)"
            # <Fix PositionStrategy Enum Handling - End>
        else:
            # Same-session rules - resume all non-expired orders
            return True, "Active order (same-session)"
            
    def _is_cross_session_order(self, db_order: PlannedOrderDB) -> bool:
        """
        Check if an order is from a previous trading session.
        
        Args:
            db_order: Database order to check
            
        Returns:
            True if order is from previous session, False if same session
        """
        if not db_order.created_at:
            return False
            
        order_date = db_order.created_at.date()
        current_date = datetime.datetime.now().date()
        
        # Simple implementation: consider cross-session if created before today
        # In production, you might want more sophisticated session detection
        return order_date < current_date
            
    def _is_cross_session_order(self, db_order: PlannedOrderDB) -> bool:
        """
        Check if an order is from a previous trading session.
        
        Args:
            db_order: Database order to check
            
        Returns:
            True if order is from previous session, False if same session
        """
        if not db_order.created_at:
            return False
            
        order_date = db_order.created_at.date()
        current_date = datetime.datetime.now().date()
        
        # Simple implementation: consider cross-session if created before today
        # In production, you might want more sophisticated session detection
        return order_date < current_date
    # <Session-Aware Database Loading - End>
            
    def _load_from_excel(self, excel_path: str) -> List[PlannedOrder]:
        """
        Load new orders from Excel file.
        
        Args:
            excel_path: Path to Excel file
            
        Returns:
            List of PlannedOrder objects from Excel
        """
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Loading orders from Excel file",
            context_provider={
                "excel_path": excel_path
            }
        )
            
        try:
            orders = self.loading_service.load_and_validate_orders(excel_path)
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Excel order loading completed",
                context_provider={
                    "excel_path": excel_path,
                    "orders_loaded": len(orders),
                    "loading_service_used": True
                },
                decision_reason="EXCEL_ORDER_LOADING_SUCCESS"
            )
            return orders
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Excel order loading failed",
                context_provider={
                    "excel_path": excel_path,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_load_from_excel"
                },
                decision_reason="EXCEL_ORDER_LOADING_FAILED"
            )
            return []
            
    def _merge_orders(self, orders: List[PlannedOrder]) -> List[PlannedOrder]:
        """
        Merge and deduplicate orders from multiple sources with session awareness.
        
        Args:
            orders: List of orders from all sources
            
        Returns:
            Deduplicated list of orders
        """
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Starting order merge and deduplication",
            context_provider={
                "total_orders_to_merge": len(orders),
                "sources_represented": len(set([self._get_order_source(order) for order in orders])),
                "merge_strategy": "priority_based_with_conflict_resolution"
            }
        )
            
        if not orders:
            return []
            
        # Use a dictionary to track unique orders by key with enhanced merging logic
        unique_orders = {}
        
        for order in orders:
            order_key = self._get_order_key(order)
            current_source = self._get_order_source(order)
            
            if order_key not in unique_orders:
                # New order - add to unique orders
                unique_orders[order_key] = (order, current_source)
                context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    "New unique order added",
                    symbol=order.symbol,
                    context_provider={
                        "order_key": order_key,
                        "source": current_source,
                        "unique_orders_count": len(unique_orders)
                    }
                )
            else:
                # Duplicate order - apply enhanced merging logic
                existing_order, existing_source = unique_orders[order_key]
                unique_orders[order_key] = self._resolve_order_conflict(
                    existing_order, existing_source, order, current_source
                )
                
        # Extract just the orders from the dictionary
        merged_count = len([order for order, _ in unique_orders.values()])
        
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Order merge completed",
            context_provider={
                "input_orders_count": len(orders),
                "output_orders_count": merged_count,
                "duplicates_removed": len(orders) - merged_count,
                "deduplication_efficiency_percent": round((merged_count / len(orders)) * 100, 2) if orders else 0
            },
            decision_reason="ORDER_MERGE_COMPLETED"
        )
            
        return [order for order, _ in unique_orders.values()]
        
    def _get_order_key(self, order: PlannedOrder) -> str:
        """
        Generate a unique key for an order for deduplication.
        
        Args:
            order: The PlannedOrder object
            
        Returns:
            String key representing order uniqueness
        """
        # Use symbol, action, and prices to identify unique orders
        # This matches the duplicate detection logic in OrderLifecycleManager
        return f"{order.symbol}_{order.action.value}_{order.entry_price:.4f}_{order.stop_loss:.4f}"

    def _get_order_source(self, order: PlannedOrder) -> str:
        """
        Determine the source of an order for conflict resolution.
        
        Args:
            order: The PlannedOrder object
            
        Returns:
            String representing order source
        """
        # This is a simplified implementation - you may need a more robust way
        # to track order sources in your actual implementation
        if hasattr(order, '_source'):
            return getattr(order, '_source', 'UNKNOWN')
        return 'UNKNOWN'
        
    def _get_source_priority(self, source: str) -> int:
        """
        Get priority level for order sources (higher = more authoritative).
        
        Args:
            source: The order source
            
        Returns:
            Priority integer (higher = better)
        """
        priority_map = {
            'IBKR': 3,    # Highest priority - reality
            'DATABASE': 2, # Medium priority - system state
            'EXCEL': 1,    # Lowest priority - user input
            'UNKNOWN': 0
        }
        return priority_map.get(source.upper(), 0)
        
    def _log_order_conflicts(self, all_orders: List[PlannedOrder], merged_orders: List[PlannedOrder]) -> None:
        """
        Log conflicts between orders from different sources for manual review.
        
        Args:
            all_orders: All orders from all sources before merging
            merged_orders: Orders after deduplication
        """
        if len(all_orders) <= len(merged_orders):
            return  # No conflicts
            
        conflict_count = len(all_orders) - len(merged_orders)
        
        context_logger.log_event(
            TradingEventType.ORDER_VALIDATION,
            "Order conflicts detected",
            context_provider={
                "conflict_count": conflict_count,
                "total_orders_before_merge": len(all_orders),
                "total_orders_after_merge": len(merged_orders),
                "conflict_resolution_strategy": "priority_based"
            },
            decision_reason="ORDER_CONFLICTS_DETECTED"
        )

    def _is_order_expired(self, db_order: PlannedOrderDB) -> bool:
        """
        Check if a database order should be considered expired based on position strategy.
        
        Args:
            db_order: The database order to check
            
        Returns:
            True if order is expired, False otherwise
        """
        if not db_order.position_strategy:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "No position strategy found - defaulting to not expired",
                symbol=db_order.symbol,
                context_provider={
                    "order_id": db_order.id,
                    "action_taken": "conservative_no_expiration"
                }
            )
            return False  # No strategy = never expire (conservative)
            
        strategy = db_order.position_strategy.name if hasattr(db_order.position_strategy, 'name') else str(db_order.position_strategy).upper()
        created_date = db_order.created_at.date() if db_order.created_at else datetime.datetime.now().date()
        current_date = datetime.datetime.now().date()
        
        if strategy == 'DAY':
            # DAY orders expire at the end of the trading day
            # For simplicity, consider expired if created before today
            is_expired = created_date < current_date
            
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "DAY strategy expiration check",
                symbol=db_order.symbol,
                context_provider={
                    "order_id": db_order.id,
                    "strategy": strategy,
                    "created_date": created_date.isoformat(),
                    "current_date": current_date.isoformat(),
                    "is_expired": is_expired,
                    "expiration_rule": "end_of_trading_day"
                }
            )
            return is_expired
            
        elif strategy == 'CORE':
            # CORE orders never expire (manual intervention only)
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "CORE strategy expiration check",
                symbol=db_order.symbol,
                context_provider={
                    "order_id": db_order.id,
                    "strategy": strategy,
                    "is_expired": False,
                    "expiration_rule": "never_expire"
                }
            )
            return False
            
        elif strategy == 'HYBRID':
            # HYBRID orders expire after 10 days
            expiry_date = created_date + datetime.timedelta(days=10)
            is_expired = current_date > expiry_date
            
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "HYBRID strategy expiration check",
                symbol=db_order.symbol,
                context_provider={
                    "order_id": db_order.id,
                    "strategy": strategy,
                    "created_date": created_date.isoformat(),
                    "current_date": current_date.isoformat(),
                    "expiry_date": expiry_date.isoformat(),
                    "days_until_expiry": (expiry_date - current_date).days if not is_expired else 0,
                    "is_expired": is_expired,
                    "expiration_rule": "10_day_window"
                }
            )
            return is_expired
            
        else:
            # Unknown strategy - default to not expired
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Unknown position strategy - defaulting to not expired",
                symbol=db_order.symbol,
                context_provider={
                    "order_id": db_order.id,
                    "unknown_strategy": strategy,
                    "action_taken": "conservative_no_expiration"
                },
                decision_reason="UNKNOWN_STRATEGY_HANDLED"
            )
            return False

    # <IBKR Order Discovery Methods - Begin>
    def _discover_ibkr_orders(self) -> List[PlannedOrder]:
        """
        Discover working orders from IBKR that should be tracked in our system.
        
        Returns:
            List of PlannedOrder objects representing working IBKR orders
        """
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Discovering IBKR orders",
            context_provider={
                "ibkr_client_available": self.ibkr_client is not None,
                "ibkr_client_connected": self.ibkr_client.connected if self.ibkr_client else False
            }
        )
            
        if not self.ibkr_client or not self.ibkr_client.connected:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR client not available or connected - skipping discovery",
                context_provider={
                    "action_taken": "skip_discovery"
                },
                decision_reason="IBKR_CLIENT_UNAVAILABLE"
            )
            return []
            
        try:
            # Get working orders from IBKR
            ibkr_orders = self.ibkr_client.get_open_orders()
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "IBKR orders retrieved",
                context_provider={
                    "ibkr_orders_count": len(ibkr_orders) if ibkr_orders else 0,
                    "ibkr_client_connected": self.ibkr_client.connected
                }
            )
            
            if not ibkr_orders:
                return []
                
            # Convert and filter IBKR orders
            planned_orders = []
            conversion_success_count = 0
            conversion_failure_count = 0
            
            for ibkr_order in ibkr_orders:
                try:
                    planned_order = self._convert_ibkr_order(ibkr_order)
                    if planned_order and self._is_ibkr_order_resumable(planned_order, ibkr_order):
                        planned_orders.append(planned_order)
                        conversion_success_count += 1
                    else:
                        conversion_failure_count += 1
                except Exception as e:
                    conversion_failure_count += 1
                    context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        "IBKR order conversion failed",
                        context_provider={
                            "ibkr_order_id": ibkr_order.order_id,
                            "symbol": ibkr_order.symbol,
                            "error_type": type(e).__name__,
                            "error_details": str(e),
                            "operation": "_convert_ibkr_order"
                        },
                        decision_reason="IBKR_ORDER_CONVERSION_FAILED"
                    )
                    continue
                    
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "IBKR order discovery completed",
                context_provider={
                    "total_ibkr_orders": len(ibkr_orders),
                    "successful_conversions": conversion_success_count,
                    "failed_conversions": conversion_failure_count,
                    "conversion_success_rate_percent": round((conversion_success_count / len(ibkr_orders)) * 100, 2) if ibkr_orders else 0,
                    "resumable_orders": len(planned_orders)
                },
                decision_reason="IBKR_ORDER_DISCOVERY_COMPLETED"
            )
            return planned_orders
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR order discovery failed",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_discover_ibkr_orders"
                },
                decision_reason="IBKR_ORDER_DISCOVERY_FAILED"
            )
            return []
            
    def _convert_ibkr_order(self, ibkr_order: IbkrOrder) -> Optional[PlannedOrder]:
        """
        Convert IBKR order format to PlannedOrder object.
        
        Args:
            ibkr_order: The IBKR order to convert
            
        Returns:
            PlannedOrder object or None if conversion fails
        """
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Converting IBKR order to PlannedOrder",
            symbol=ibkr_order.symbol,
            context_provider={
                "ibkr_order_id": ibkr_order.order_id,
                "symbol": ibkr_order.symbol,
                "action": ibkr_order.action,
                "order_type": ibkr_order.order_type
            }
        )
            
        try:
            # Extract basic order information from IBKR order
            # Note: This is a simplified conversion - you may need to adjust based on your IBKR order structure
            symbol = ibkr_order.symbol
            action = ibkr_order.action  # BUY/SELL
            order_type = ibkr_order.order_type  # LMT/MKT/STP
            
            # Use limit price for LMT orders, otherwise use current market data
            entry_price = ibkr_order.lmt_price if ibkr_order.lmt_price else 0.0
            
            # For stop orders, use aux price as stop loss
            stop_loss = ibkr_order.aux_price if ibkr_order.aux_price else 0.0
            
            # Create a basic PlannedOrder - you may need to adjust based on your requirements
            # This is a placeholder - you'll need to implement proper conversion based on your IBKR order structure
            planned_order = PlannedOrder(
                symbol=symbol,
                action=action,  # This should be an OrderAction enum value
                entry_price=entry_price,
                stop_loss=stop_loss,
                # Add other required fields with default values
                order_type=order_type,  # This should be an OrderType enum value
                security_type="STK",  # Default - adjust based on IBKR contract
                exchange="SMART",     # Default exchange
                currency="USD",       # Default currency
                risk_per_trade=0.01,  # Default risk
                risk_reward_ratio=2.0, # Default R:R
                priority=3,           # Default priority
                position_strategy="CORE"  # Default to CORE for IBKR orders
            )
            
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "IBKR order conversion successful",
                symbol=ibkr_order.symbol,
                context_provider={
                    "ibkr_order_id": ibkr_order.order_id,
                    "converted_symbol": symbol,
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "position_strategy": "CORE"
                },
                decision_reason="IBKR_ORDER_CONVERSION_SUCCESS"
            )
            return planned_order
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR order conversion failed",
                symbol=ibkr_order.symbol,
                context_provider={
                    "ibkr_order_id": ibkr_order.order_id,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_convert_ibkr_order"
                },
                decision_reason="IBKR_ORDER_CONVERSION_FAILED"
            )
            return None
            
    def _is_ibkr_order_resumable(self, planned_order: PlannedOrder, ibkr_order: IbkrOrder) -> bool:
        """
        Check if an IBKR order should be resumed to our system.
        
        Args:
            planned_order: The converted PlannedOrder
            ibkr_order: The original IBKR order
            
        Returns:
            True if order should be resumed, False otherwise
        """
        # Only resume CORE and HYBRID strategy orders
        # DAY orders are auto-expired and shouldn't be resumed
        strategy = planned_order.position_strategy.upper() if planned_order.position_strategy else "CORE"
        
        if strategy not in ['CORE', 'HYBRID']:
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "IBKR order skipped - incompatible strategy",
                symbol=planned_order.symbol,
                context_provider={
                    "ibkr_order_id": ibkr_order.order_id,
                    "strategy": strategy,
                    "compatible_strategies": ["CORE", "HYBRID"],
                    "action_taken": "skip_resumption"
                },
                decision_reason="IBKR_ORDER_STRATEGY_INCOMPATIBLE"
            )
            return False
            
        # Check if order is already in our database to avoid duplicates
        existing_db_order = self.db_session.query(PlannedOrderDB).filter_by(
            symbol=planned_order.symbol,
            action=planned_order.action.value,
            entry_price=planned_order.entry_price
        ).first()
        
        if existing_db_order:
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "IBKR order skipped - already in database",
                symbol=planned_order.symbol,
                context_provider={
                    "ibkr_order_id": ibkr_order.order_id,
                    "existing_order_id": existing_db_order.id,
                    "existing_order_status": existing_db_order.status,
                    "action_taken": "skip_duplicate"
                },
                decision_reason="IBKR_ORDER_ALREADY_IN_DATABASE"
            )
            return False
            
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "IBKR order marked as resumable",
            symbol=planned_order.symbol,
            context_provider={
                "ibkr_order_id": ibkr_order.order_id,
                "strategy": strategy,
                "resumption_criteria_met": True
            },
            decision_reason="IBKR_ORDER_RESUMABLE"
        )
        return True
    # <IBKR Order Discovery Methods - End>
            
    def get_loading_statistics(self, excel_path: str) -> Dict[str, Any]:
        """
        Get statistics about order loading from all sources.
        
        Args:
            excel_path: Path to Excel file
            
        Returns:
            Dictionary with loading statistics
        """
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Generating order loading statistics",
            context_provider={
                "excel_path": excel_path
            }
        )
            
        try:
            db_orders = self._load_from_database()
            excel_orders = self._load_from_excel(excel_path)
            ibkr_orders = self._discover_ibkr_orders()
            all_orders = db_orders + excel_orders + ibkr_orders
            merged_orders = self._merge_orders(all_orders)
            
            stats = {
                'database_orders': len(db_orders),
                'excel_orders': len(excel_orders),
                'ibkr_orders': len(ibkr_orders),
                'merged_orders': len(merged_orders),
                'duplicates_removed': len(all_orders) - len(merged_orders),
                'sources_available': 3,
                'ibkr_connected': self.ibkr_client and self.ibkr_client.connected,
                'timestamp': datetime.datetime.now()
            }
            
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Order loading statistics generated",
                context_provider=stats,
                decision_reason="LOADING_STATISTICS_GENERATED"
            )
            return stats
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error generating loading statistics",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "excel_path": excel_path,
                    "operation": "get_loading_statistics"
                },
                decision_reason="LOADING_STATISTICS_GENERATION_FAILED"
            )
            return {
                'error': str(e),
                'timestamp': datetime.datetime.now()
            }
    
    # ... (Existing code above change)

    # <Enhanced Order Conflict Resolution - Begin>
    def _resolve_order_conflict(self, existing_order: PlannedOrder, existing_source: str,
                              new_order: PlannedOrder, new_source: str) -> tuple[PlannedOrder, str]:
        """
        Resolve conflicts between duplicate orders from different sources.
        
        Args:
            existing_order: The order already in unique_orders
            existing_source: Source of existing order
            new_order: The new order with same key
            new_source: Source of new order
            
        Returns:
            Tuple of (selected_order, selected_source)
        """
        existing_priority = self._get_source_priority(existing_source)
        new_priority = self._get_source_priority(new_source)
        
        # Priority-based resolution
        if new_priority > existing_priority:
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order conflict resolved by source priority",
                symbol=new_order.symbol,
                context_provider={
                    "existing_source": existing_source,
                    "existing_priority": existing_priority,
                    "new_source": new_source,
                    "new_priority": new_priority,
                    "resolution": "new_order_selected",
                    "order_key": self._get_order_key(new_order)
                },
                decision_reason="ORDER_CONFLICT_PRIORITY_RESOLUTION"
            )
            return (new_order, new_source)
        elif new_priority < existing_priority:
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order conflict resolved by source priority",
                symbol=existing_order.symbol,
                context_provider={
                    "existing_source": existing_source,
                    "existing_priority": existing_priority,
                    "new_source": new_source,
                    "new_priority": new_priority,
                    "resolution": "existing_order_retained",
                    "order_key": self._get_order_key(existing_order)
                },
                decision_reason="ORDER_CONFLICT_PRIORITY_RESOLUTION"
            )
            return (existing_order, existing_source)
        else:
            # Same priority - use more sophisticated conflict resolution
            return self._resolve_same_priority_conflict(existing_order, new_order, existing_source)
            
    def _resolve_same_priority_conflict(self, order1: PlannedOrder, order2: PlannedOrder, 
                                      source: str) -> tuple[PlannedOrder, str]:
        """
        Resolve conflicts when orders have same source priority.
        
        Args:
            order1: First order
            order2: Second order  
            source: Common source
            
        Returns:
            Tuple of (selected_order, source)
        """
        # For same source conflicts, prefer the order with more recent data
        # This handles Excel updates to existing DB orders
        # <Fix NoneType Comparison - Begin>
        order1_import_time = getattr(order1, '_import_time', None)
        order2_import_time = getattr(order2, '_import_time', None)
        
        # Only compare if both have import times and order2 is newer
        if (order1_import_time is not None and 
            order2_import_time is not None and 
            order2_import_time > order1_import_time):
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order conflict resolved by import time",
                symbol=order2.symbol,
                context_provider={
                    "order1_import_time": order1_import_time.isoformat() if order1_import_time else None,
                    "order2_import_time": order2_import_time.isoformat() if order2_import_time else None,
                    "resolution": "newer_order_selected",
                    "order_key": self._get_order_key(order2)
                },
                decision_reason="ORDER_CONFLICT_TIME_RESOLUTION"
            )
            return (order2, source)
        else:
            # Default to first order if import times are not comparable
            context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                "Order conflict resolved by default (first order)",
                symbol=order1.symbol,
                context_provider={
                    "order1_import_time": order1_import_time.isoformat() if order1_import_time else None,
                    "order2_import_time": order2_import_time.isoformat() if order2_import_time else None,
                    "resolution": "first_order_retained",
                    "order_key": self._get_order_key(order1)
                },
                decision_reason="ORDER_CONFLICT_DEFAULT_RESOLUTION"
            )
            return (order1, source)
        # <Fix NoneType Comparison - End>
    # <Enhanced Order Conflict Resolution - End>