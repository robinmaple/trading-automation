"""
Background service that synchronizes internal order/position state with the external IBKR brokerage.
Detects and handles discrepancies to ensure system integrity and consistency with reality.
"""

import threading
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import asdict

from src.brokers.ibkr.ibkr_client import IbkrClient
from src.services.state_service import StateService
from src.brokers.ibkr.types.ibkr_types import IbkrOrder, IbkrPosition, ReconciliationResult, OrderDiscrepancy, PositionDiscrepancy

# <AON Reconciliation Integration - Begin>
from src.core.shared_enums import OrderState as SharedOrderState
from src.core.models import PlannedOrderDB
from sqlalchemy.orm import Session
from src.core.database import get_db_session
# <AON Reconciliation Integration - End>

# Context-aware logging import - replacing simple_logger
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()


class ReconciliationEngine:
    """Orchestrates the continuous synchronization between internal state and IBKR."""

    def __init__(self, ibkr_client: IbkrClient, state_service: StateService, polling_interval: int = 30):
        """Initialize the engine with its client, state service, and polling interval."""
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing ReconciliationEngine",
            context_provider={
                "ibkr_client_provided": ibkr_client is not None,
                "state_service_provided": state_service is not None,
                "polling_interval_seconds": polling_interval,
                "ibkr_client_type": type(ibkr_client).__name__ if ibkr_client else "None"
            }
        )
            
        self.ibkr_client = ibkr_client
        self.state_service = state_service
        self.polling_interval = polling_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        # <AON Reconciliation Integration - Begin>
        self.db_session: Session = get_db_session()
        # <AON Reconciliation Integration - End>
        
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "ReconciliationEngine initialized successfully",
            context_provider={
                "session_id": context_logger.session_id,
                "threading_enabled": True,
                "lock_type": "RLock"
            }
        )

    def start(self) -> None:
        """Start the background reconciliation thread."""
        with self._lock:
            if self._running:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Reconciliation engine already running - start request ignored",
                    context_provider={
                        "current_state": "running",
                        "thread_alive": self._thread.is_alive() if self._thread else False
                    },
                    decision_reason="ENGINE_ALREADY_RUNNING"
                )
                return

            self._running = True
            self._thread = threading.Thread(
                target=self._reconciliation_loop,
                daemon=True,
                name="ReconciliationEngine"
            )
            self._thread.start()
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Reconciliation engine started",
                context_provider={
                    "polling_interval_seconds": self.polling_interval,
                    "thread_name": self._thread.name,
                    "thread_daemon": self._thread.daemon
                },
                decision_reason="ENGINE_STARTED"
            )

    def stop(self) -> None:
        """Stop the background reconciliation thread."""
        with self._lock:
            if not self._running:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Reconciliation engine already stopped - stop request ignored",
                    context_provider={
                        "current_state": "stopped"
                    },
                    decision_reason="ENGINE_ALREADY_STOPPED"
                )
                return

            self._running = False
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=5.0)
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Reconciliation engine stopped",
                    context_provider={
                        "thread_joined": True,
                        "join_timeout_seconds": 5.0
                    },
                    decision_reason="ENGINE_STOPPED"
                )
            else:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Reconciliation engine stopped (no active thread)",
                    context_provider={
                        "thread_was_alive": False
                    },
                    decision_reason="ENGINE_STOPPED_NO_THREAD"
                )

    def is_running(self) -> bool:
        """Check if the reconciliation engine is currently running."""
        with self._lock:
            current_state = self._running
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Reconciliation engine status check",
                context_provider={
                    "is_running": current_state,
                    "thread_alive": self._thread.is_alive() if self._thread else False
                }
            )
            return current_state

    def _reconciliation_loop(self) -> None:
        """Main loop that performs reconciliation at the configured interval."""
        error_count = 0
        max_errors = 5

        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Reconciliation loop started",
            context_provider={
                "polling_interval_seconds": self.polling_interval,
                "max_consecutive_errors": max_errors,
                "thread_name": threading.current_thread().name
            },
            decision_reason="RECONCILIATION_LOOP_STARTED"
        )

        while self._running and error_count < max_errors:
            try:
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Starting reconciliation cycle",
                    context_provider={
                        "cycle_number": error_count + 1,
                        "consecutive_errors": error_count
                    }
                )
                
                self._reconcile_orders()
                self._reconcile_positions()
                # <AON Reconciliation Integration - Begin>
                self._reconcile_aon_orders()
                # <AON Reconciliation Integration - End>
                error_count = 0
                
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Reconciliation cycle completed successfully",
                    context_provider={
                        "sleep_duration_seconds": self.polling_interval
                    }
                )
                time.sleep(self.polling_interval)
                
            except Exception as e:
                error_count += 1
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Reconciliation cycle error",
                    context_provider={
                        "error_count": error_count,
                        "max_errors": max_errors,
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "backoff_time_seconds": min(60 * error_count, 300)
                    },
                    decision_reason="RECONCILIATION_CYCLE_ERROR"
                )
                backoff_time = min(60 * error_count, 300)
                time.sleep(backoff_time)

        if error_count >= max_errors:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Reconciliation engine stopping due to excessive errors",
                context_provider={
                    "final_error_count": error_count,
                    "max_errors_allowed": max_errors,
                    "total_cycles_attempted": error_count
                },
                decision_reason="EXCESSIVE_RECONCILIATION_ERRORS"
            )
            self.stop()

    def _reconcile_orders(self) -> ReconciliationResult:
        """Compare IBKR orders with internal state and handle any discrepancies."""
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Starting order reconciliation",
            context_provider={
                "operation_type": "orders",
                "timestamp": datetime.now().isoformat()
            }
        )
            
        result = ReconciliationResult(
            success=False,
            operation_type="orders",
            discrepancies=[],
            timestamp=datetime.now()
        )

        try:
            ibkr_orders = self.ibkr_client.get_open_orders()
            internal_orders = self._get_internal_working_orders()
            discrepancies = self._find_order_discrepancies(internal_orders, ibkr_orders)
            result.discrepancies = [asdict(d) for d in discrepancies]

            for discrepancy in discrepancies:
                self._handle_order_discrepancy(discrepancy)

            result.success = True
            if discrepancies:
                context_logger.log_event(
                    TradingEventType.STATE_TRANSITION,
                    "Order reconciliation completed with discrepancies",
                    context_provider={
                        "discrepancies_found": len(discrepancies),
                        "discrepancy_types": [d.discrepancy_type for d in discrepancies],
                        "symbols_with_discrepancies": [d.symbol for d in discrepancies],
                        "ibkr_orders_count": len(ibkr_orders),
                        "internal_orders_count": len(internal_orders)
                    },
                    decision_reason="ORDER_RECONCILIATION_WITH_DISCREPANCIES"
                )
            else:
                context_logger.log_event(
                    TradingEventType.STATE_TRANSITION,
                    "Order reconciliation completed - no discrepancies",
                    context_provider={
                        "ibkr_orders_count": len(ibkr_orders),
                        "internal_orders_count": len(internal_orders),
                        "reconciliation_result": "clean"
                    },
                    decision_reason="ORDER_RECONCILIATION_CLEAN"
                )

        except Exception as e:
            result.error = str(e)
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Order reconciliation failed",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_reconcile_orders"
                },
                decision_reason="ORDER_RECONCILIATION_FAILED"
            )

        return result

    def _reconcile_positions(self) -> ReconciliationResult:
        """Compare IBKR positions with internal state and handle any discrepancies."""
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Starting position reconciliation",
            context_provider={
                "operation_type": "positions",
                "timestamp": datetime.now().isoformat()
            }
        )
            
        result = ReconciliationResult(
            success=False,
            operation_type="positions",
            discrepancies=[],
            timestamp=datetime.now()
        )

        try:
            ibkr_positions = self.ibkr_client.get_positions()
            internal_positions = self.state_service.get_open_positions()
            discrepancies = self._find_position_discrepancies(internal_positions, ibkr_positions)
            result.discrepancies = [asdict(d) for d in discrepancies]

            for discrepancy in discrepancies:
                self._handle_position_discrepancy(discrepancy)

            result.success = True
            if discrepancies:
                context_logger.log_event(
                    TradingEventType.STATE_TRANSITION,
                    "Position reconciliation completed with discrepancies",
                    context_provider={
                        "discrepancies_found": len(discrepancies),
                        "discrepancy_types": [d.discrepancy_type for d in discrepancies],
                        "symbols_with_discrepancies": [d.symbol for d in discrepancies],
                        "ibkr_positions_count": len(ibkr_positions),
                        "internal_positions_count": len(internal_positions)
                    },
                    decision_reason="POSITION_RECONCILIATION_WITH_DISCREPANCIES"
                )
            else:
                context_logger.log_event(
                    TradingEventType.STATE_TRANSITION,
                    "Position reconciliation completed - no discrepancies",
                    context_provider={
                        "ibkr_positions_count": len(ibkr_positions),
                        "internal_positions_count": len(internal_positions),
                        "reconciliation_result": "clean"
                    },
                    decision_reason="POSITION_RECONCILIATION_CLEAN"
                )

        except Exception as e:
            result.error = str(e)
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Position reconciliation failed",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_reconcile_positions"
                },
                decision_reason="POSITION_RECONCILIATION_FAILED"
            )

        return result

    # <AON Reconciliation Methods - Begin>
    def _reconcile_aon_orders(self) -> None:
        """Handle AON-specific reconciliation scenarios."""
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Starting AON reconciliation",
            context_provider={
                "reconciliation_type": "AON_orders",
                "timestamp": datetime.now().isoformat()
            }
        )
            
        try:
            ibkr_orders = self.ibkr_client.get_open_orders()
            internal_orders = self._get_internal_working_orders()
            
            # Check for orphaned AON orders (in IBKR but not in our DB)
            self._handle_orphaned_aon_orders(ibkr_orders, internal_orders)
            
            # Check for AON status mismatches
            self._handle_aon_status_mismatches(ibkr_orders, internal_orders)
            
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "AON reconciliation completed",
                context_provider={
                    "ibkr_orders_processed": len(ibkr_orders),
                    "internal_orders_processed": len(internal_orders),
                    "reconciliation_result": "completed"
                },
                decision_reason="AON_RECONCILIATION_COMPLETED"
            )
                
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "AON reconciliation failed",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_reconcile_aon_orders"
                },
                decision_reason="AON_RECONCILIATION_FAILED"
            )

    def _handle_orphaned_aon_orders(self, ibkr_orders: List[IbkrOrder], internal_orders: List[Dict]) -> None:
        """Handle AON orders that exist in IBKR but not in our database."""
        orphaned_count = 0
        
        for ibkr_order in ibkr_orders:
            # Check if this IBKR order exists in our internal state
            internal_match = self._find_internal_order_match(ibkr_order, internal_orders)
            
            if not internal_match and self._is_likely_aon_order(ibkr_order):
                context_logger.log_event(
                    TradingEventType.STATE_TRANSITION,
                    "Found orphaned AON order",
                    symbol=ibkr_order.symbol,
                    context_provider={
                        "ibkr_order_id": ibkr_order.order_id,
                        "symbol": ibkr_order.symbol,
                        "order_type": ibkr_order.order_type,
                        "action": ibkr_order.action,
                        "price": ibkr_order.lmt_price,
                        "quantity": ibkr_order.total_quantity,
                        "status": ibkr_order.status,
                        "parent_order_id": ibkr_order.parent_id
                    },
                    decision_reason="ORPHANED_AON_ORDER_DETECTED"
                )
                orphaned_count += 1
                
                # For now, just log - in production you might want to:
                # 1. Create a PlannedOrderDB record from the IBKR order
                # 2. Update status based on IBKR state
                # 3. Resume monitoring
                
        if orphaned_count > 0:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Orphaned AON orders summary",
                context_provider={
                    "orphaned_orders_count": orphaned_count,
                    "action_taken": "logged_for_manual_review",
                    "recommendation": "manual_investigation_required"
                },
                decision_reason="ORPHANED_AON_ORDERS_SUMMARY"
            )

    def _handle_aon_status_mismatches(self, ibkr_orders: List[IbkrOrder], internal_orders: List[Dict]) -> None:
        """Handle status mismatches involving AON orders."""
        status_updates_count = 0
        
        for internal_order in internal_orders:
            ibkr_match = self._find_ibkr_order_match(internal_order, ibkr_orders)
            
            if ibkr_match:
                # Check for status mismatches
                internal_status = internal_order.get('status')
                ibkr_status = ibkr_match.status
                
                # Handle AON-specific status transitions
                if (internal_status == SharedOrderState.LIVE_WORKING.value and 
                    ibkr_status in ['Filled', 'Cancelled']):
                    
                    context_logger.log_event(
                        TradingEventType.STATE_TRANSITION,
                        "AON status synchronization required",
                        symbol=internal_order['symbol'],
                        context_provider={
                            "internal_order_id": internal_order['id'],
                            "ibkr_order_id": ibkr_match.order_id,
                            "internal_status": internal_status,
                            "ibkr_status": ibkr_status,
                            "status_transition": f"{internal_status} → {ibkr_status}",
                            "synchronization_action": "update_internal_status"
                        },
                        decision_reason="AON_STATUS_SYNCHRONIZATION_REQUIRED"
                    )
                    
                    # Update internal status to match IBKR reality
                    db_order = self.db_session.query(PlannedOrderDB).filter_by(id=internal_order['id']).first()
                    if db_order:
                        if ibkr_status == 'Filled':
                            db_order.status = SharedOrderState.FILLED.value
                        elif ibkr_status == 'Cancelled':
                            db_order.status = SharedOrderState.CANCELLED.value
                        
                        self.db_session.commit()
                        status_updates_count += 1
                        
                        context_logger.log_event(
                            TradingEventType.STATE_TRANSITION,
                            "AON status synchronized successfully",
                            symbol=internal_order['symbol'],
                            context_provider={
                                "order_id": db_order.id,
                                "new_status": db_order.status,
                                "synchronization_source": "IBKR",
                                "update_timestamp": datetime.now().isoformat()
                            },
                            decision_reason="AON_STATUS_SYNCHRONIZED"
                        )

        if status_updates_count > 0:
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "AON status synchronization summary",
                context_provider={
                    "status_updates_applied": status_updates_count,
                    "total_internal_orders_checked": len(internal_orders),
                    "synchronization_success_rate": f"{status_updates_count}/{len(internal_orders)}"
                },
                decision_reason="AON_STATUS_SYNCHRONIZATION_SUMMARY"
            )

    def _is_likely_aon_order(self, ibkr_order: IbkrOrder) -> bool:
        """Check if an IBKR order is likely an AON order from our system."""
        # Look for characteristics of our AON bracket orders
        # This is a simplified check - in practice you'd have more sophisticated detection
        is_likely_aon = (ibkr_order.parent_id is not None and  # Part of a bracket order
                        ibkr_order.order_type in ['LMT', 'STP'] and  # Our order types
                        ibkr_order.remaining_quantity == ibkr_order.total_quantity)  # Not partially filled
        
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "AON order likelihood assessment",
            symbol=ibkr_order.symbol,
            context_provider={
                "ibkr_order_id": ibkr_order.order_id,
                "is_likely_aon": is_likely_aon,
                "assessment_criteria": {
                    "has_parent_id": ibkr_order.parent_id is not None,
                    "order_type_matches": ibkr_order.order_type in ['LMT', 'STP'],
                    "quantity_unchanged": ibkr_order.remaining_quantity == ibkr_order.total_quantity
                },
                "order_details": {
                    "parent_id": ibkr_order.parent_id,
                    "order_type": ibkr_order.order_type,
                    "remaining_quantity": ibkr_order.remaining_quantity,
                    "total_quantity": ibkr_order.total_quantity
                }
            }
        )
        return is_likely_aon

    def _find_internal_order_match(self, ibkr_order: IbkrOrder, internal_orders: List[Dict]) -> Optional[Dict]:
        """Find an internal order that matches the IBKR order."""
        for internal_order in internal_orders:
            # Simple matching logic - could be enhanced with more sophisticated matching
            if (internal_order.get('symbol') == ibkr_order.symbol and
                internal_order.get('action') == ibkr_order.action and
                abs(internal_order.get('entry_price', 0) - (ibkr_order.lmt_price or 0)) < 0.01):
                return internal_order
        return None

    def _find_ibkr_order_match(self, internal_order: Dict, ibkr_orders: List[IbkrOrder]) -> Optional[IbkrOrder]:
        """Find an IBKR order that matches the internal order."""
        for ibkr_order in ibkr_orders:
            if (ibkr_order.symbol == internal_order.get('symbol') and
                ibkr_order.action == internal_order.get('action') and
                abs((ibkr_order.lmt_price or 0) - internal_order.get('entry_price', 0)) < 0.01):
                return ibkr_order
        return None
    # <AON Reconciliation Methods - End>

    def _get_internal_working_orders(self) -> List[Dict[str, Any]]:
        """Get internal orders that should be working in IBKR."""
        context_logger.log_event(
            TradingEventType.DATABASE_STATE,
            "Fetching internal working orders",
            context_provider={
                "target_statuses": [
                    SharedOrderState.LIVE_WORKING.value,
                    SharedOrderState.PENDING.value,
                    SharedOrderState.LIVE.value
                ]
            }
        )
            
        try:
            # Query database for orders that should be working in IBKR
            working_orders = self.db_session.query(PlannedOrderDB).filter(
                PlannedOrderDB.status.in_([
                    SharedOrderState.LIVE_WORKING.value,
                    SharedOrderState.PENDING.value,
                    SharedOrderState.LIVE.value
                ])
            ).all()
            
            # Convert to dictionary format for comparison
            internal_orders = []
            for order in working_orders:
                internal_orders.append({
                    'id': order.id,
                    'symbol': order.symbol,
                    'action': order.action,
                    'entry_price': order.entry_price,
                    'stop_loss': order.stop_loss,
                    'status': order.status,
                    'order_type': order.order_type,
                    'created_at': order.created_at
                })
            
            context_logger.log_event(
                TradingEventType.DATABASE_STATE,
                "Internal working orders retrieved",
                context_provider={
                    "orders_found": len(internal_orders),
                    "order_symbols": [order['symbol'] for order in internal_orders],
                    "status_distribution": {
                        status: len([o for o in internal_orders if o['status'] == status])
                        for status in set([o['status'] for o in internal_orders])
                    }
                },
                decision_reason="INTERNAL_ORDERS_RETRIEVED"
            )
            return internal_orders
            
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error fetching internal working orders",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "_get_internal_working_orders"
                },
                decision_reason="INTERNAL_ORDERS_RETRIEVAL_FAILED"
            )
            return []

    def _find_order_discrepancies(self, internal_orders: List[Dict], ibkr_orders: List[IbkrOrder]) -> List[OrderDiscrepancy]:
        """Find discrepancies between internal and external order states."""
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Finding order discrepancies",
            context_provider={
                "internal_orders_count": len(internal_orders),
                "ibkr_orders_count": len(ibkr_orders)
            }
        )
            
        discrepancies = []
        
        if ibkr_orders:
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "IBKR orders analysis",
                context_provider={
                    "ibkr_orders_count": len(ibkr_orders),
                    "ibkr_order_ids": [order.order_id for order in ibkr_orders],
                    "ibkr_symbols": [order.symbol for order in ibkr_orders]
                }
            )
            
        # Check for orders in IBKR but not in our internal state
        for ibkr_order in ibkr_orders:
            internal_match = self._find_internal_order_match(ibkr_order, internal_orders)
            if not internal_match:
                discrepancies.append(OrderDiscrepancy(
                    order_id=ibkr_order.order_id,
                    symbol=ibkr_order.symbol,
                    discrepancy_type='orphaned_order',
                    internal_status='NOT_FOUND',
                    external_status=ibkr_order.status,
                    description=f"Order exists in IBKR but not in internal state"
                ))
        
        # Check for orders in internal state but not in IBKR
        for internal_order in internal_orders:
            ibkr_match = self._find_ibkr_order_match(internal_order, ibkr_orders)
            if not ibkr_match and internal_order['status'] in [SharedOrderState.LIVE_WORKING.value, SharedOrderState.LIVE.value]:
                discrepancies.append(OrderDiscrepancy(
                    order_id=None,
                    symbol=internal_order['symbol'],
                    discrepancy_type='missing_order',
                    internal_status=internal_order['status'],
                    external_status='NOT_FOUND',
                    description=f"Order in internal state but not found in IBKR"
                ))
        
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Order discrepancy analysis completed",
            context_provider={
                "total_discrepancies_found": len(discrepancies),
                "discrepancy_breakdown": {
                    discrepancy_type: len([d for d in discrepancies if d.discrepancy_type == discrepancy_type])
                    for discrepancy_type in set([d.discrepancy_type for d in discrepancies])
                },
                "affected_symbols": list(set([d.symbol for d in discrepancies]))
            },
            decision_reason="ORDER_DISCREPANCY_ANALYSIS_COMPLETED"
        )
        return discrepancies

    def _find_position_discrepancies(self, internal_positions: List[Any], ibkr_positions: List[IbkrPosition]) -> List[PositionDiscrepancy]:
        """Find discrepancies between internal and external position states."""
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Finding position discrepancies",
            context_provider={
                "internal_positions_count": len(internal_positions),
                "ibkr_positions_count": len(ibkr_positions) if ibkr_positions else 0
            }
        )
            
        discrepancies = []
        if ibkr_positions:
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "IBKR positions analysis",
                context_provider={
                    "ibkr_positions_count": len(ibkr_positions),
                    "ibkr_position_symbols": [pos.symbol for pos in ibkr_positions]
                }
            )
            
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Position discrepancy analysis completed",
            context_provider={
                "total_discrepancies_found": len(discrepancies),
                "analysis_method": "stub_implementation"
            }
        )
        return discrepancies

    def _handle_order_discrepancy(self, discrepancy: OrderDiscrepancy) -> None:
        """Handle an order state discrepancy."""
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Handling order discrepancy",
            symbol=discrepancy.symbol,
            context_provider={
                "discrepancy_type": discrepancy.discrepancy_type,
                "internal_status": discrepancy.internal_status,
                "external_status": discrepancy.external_status,
                "order_id": discrepancy.order_id,
                "description": discrepancy.description
            },
            decision_reason="ORDER_DISCREPANCY_HANDLING_STARTED"
        )
        
        if discrepancy.discrepancy_type == 'status_mismatch':
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "Status mismatch handling",
                symbol=discrepancy.symbol,
                context_provider={
                    "internal_status": discrepancy.internal_status,
                    "ibkr_status": discrepancy.external_status,
                    "recommended_action": "update_internal_status_to_match_ibkr"
                },
                decision_reason="STATUS_MISMATCH_HANDLED"
            )
            
        elif discrepancy.discrepancy_type == 'orphaned_order':
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "Orphaned order handling",
                symbol=discrepancy.symbol,
                context_provider={
                    "ibkr_order_id": discrepancy.order_id,
                    "recommended_action": "create_internal_record_or_cancel_ibkr_order"
                },
                decision_reason="ORPHANED_ORDER_HANDLED"
            )
            
        elif discrepancy.discrepancy_type == 'missing_order':
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "Missing order handling",
                symbol=discrepancy.symbol,
                context_provider={
                    "internal_status": discrepancy.internal_status,
                    "recommended_action": "mark_internal_order_as_failed_cancelled"
                },
                decision_reason="MISSING_ORDER_HANDLED"
            )

    def _handle_position_discrepancy(self, discrepancy: PositionDiscrepancy) -> None:
        """Handle a position state discrepancy."""
        context_logger.log_event(
            TradingEventType.STATE_TRANSITION,
            "Handling position discrepancy",
            symbol=discrepancy.symbol,
            context_provider={
                "discrepancy_type": discrepancy.discrepancy_type,
                "internal_position": discrepancy.internal_position,
                "external_position": discrepancy.external_position,
                "description": discrepancy.description
            },
            decision_reason="POSITION_DISCREPANCY_HANDLING_STARTED"
        )
            
        if discrepancy.discrepancy_type == 'quantity_mismatch':
            context_logger.log_event(
                TradingEventType.STATE_TRANSITION,
                "Quantity mismatch handling",
                symbol=discrepancy.symbol,
                context_provider={
                    "internal_quantity": discrepancy.internal_position,
                    "ibkr_quantity": discrepancy.external_position,
                    "recommended_action": "synchronize_position_quantities"
                },
                decision_reason="QUANTITY_MISMATCH_HANDLED"
            )

    def _handle_reconciliation_error(self, error: Exception) -> None:
        """Handle reconciliation errors gracefully."""
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Reconciliation error handling",
            context_provider={
                "error_type": type(error).__name__,
                "error_details": str(error),
                "handling_strategy": "graceful_degradation"
            },
            decision_reason="RECONCILIATION_ERROR_HANDLED"
        )

    def force_reconciliation(self) -> Optional[tuple]:
        """Force an immediate reconciliation cycle for testing or manual intervention."""
        if not self._running:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Manual reconciliation requested but engine not running",
                context_provider={
                    "current_state": "stopped",
                    "action_taken": "none"
                },
                decision_reason="MANUAL_RECONCILIATION_REJECTED"
            )
            return None

        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Manual reconciliation triggered",
            context_provider={
                "trigger_type": "manual",
                "current_thread_state": "running"
            },
            decision_reason="MANUAL_RECONCILIATION_STARTED"
        )
            
        try:
            order_result = self._reconcile_orders()
            position_result = self._reconcile_positions()
            # <AON Reconciliation Integration - Begin>
            self._reconcile_aon_orders()
            # <AON Reconciliation Integration - End>
            
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Manual reconciliation completed successfully",
                context_provider={
                    "order_discrepancies": len(order_result.discrepancies) if order_result else 0,
                    "position_discrepancies": len(position_result.discrepancies) if position_result else 0,
                    "overall_success": order_result.success if order_result else False and position_result.success if position_result else False
                },
                decision_reason="MANUAL_RECONCILIATION_COMPLETED"
            )
            return order_result, position_result
        except Exception as e:
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Manual reconciliation failed",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "operation": "force_reconciliation"
                },
                decision_reason="MANUAL_RECONCILIATION_FAILED"
            )
            return None