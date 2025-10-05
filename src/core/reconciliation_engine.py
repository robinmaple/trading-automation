"""
Background service that synchronizes internal order/position state with the external IBKR brokerage.
Detects and handles discrepancies to ensure system integrity and consistency with reality.
"""

import threading
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import asdict

from src.core.ibkr_client import IbkrClient
from src.services.state_service import StateService
from src.core.ibkr_types import IbkrOrder, IbkrPosition, ReconciliationResult, OrderDiscrepancy, PositionDiscrepancy

# <AON Reconciliation Integration - Begin>
from src.core.shared_enums import OrderState as SharedOrderState
from src.core.models import PlannedOrderDB
from sqlalchemy.orm import Session
from src.core.database import get_db_session
# <AON Reconciliation Integration - End>

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)


class ReconciliationEngine:
    """Orchestrates the continuous synchronization between internal state and IBKR."""

    def __init__(self, ibkr_client: IbkrClient, state_service: StateService, polling_interval: int = 30):
        """Initialize the engine with its client, state service, and polling interval."""
        if logger:
            logger.debug("Initializing ReconciliationEngine")
            
        self.ibkr_client = ibkr_client
        self.state_service = state_service
        self.polling_interval = polling_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        # <AON Reconciliation Integration - Begin>
        self.db_session: Session = get_db_session()
        # <AON Reconciliation Integration - End>
        
        if logger:
            logger.info("ReconciliationEngine initialized successfully")

    def start(self) -> None:
        """Start the background reconciliation thread."""
        with self._lock:
            if self._running:
                if logger:
                    logger.warning("Reconciliation engine already running")
                return

            self._running = True
            self._thread = threading.Thread(
                target=self._reconciliation_loop,
                daemon=True,
                name="ReconciliationEngine"
            )
            self._thread.start()
            if logger:
                logger.info(f"Reconciliation engine started (interval: {self.polling_interval}s)")

    def stop(self) -> None:
        """Stop the background reconciliation thread."""
        with self._lock:
            if not self._running:
                return

            self._running = False
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=5.0)
            if logger:
                logger.info("Reconciliation engine stopped")

    def is_running(self) -> bool:
        """Check if the reconciliation engine is currently running."""
        with self._lock:
            return self._running

    def _reconciliation_loop(self) -> None:
        """Main loop that performs reconciliation at the configured interval."""
        error_count = 0
        max_errors = 5

        if logger:
            logger.info(f"Reconciliation loop started (interval: {self.polling_interval}s)")

        while self._running and error_count < max_errors:
            try:
                self._reconcile_orders()
                self._reconcile_positions()
                # <AON Reconciliation Integration - Begin>
                self._reconcile_aon_orders()
                # <AON Reconciliation Integration - End>
                error_count = 0
                time.sleep(self.polling_interval)
            except Exception as e:
                error_count += 1
                if logger:
                    logger.error(f"Reconciliation error ({error_count}/{max_errors}): {e}")
                backoff_time = min(60 * error_count, 300)
                time.sleep(backoff_time)

        if error_count >= max_errors:
            if logger:
                logger.error("Too many reconciliation errors, stopping engine")
            self.stop()

    def _reconcile_orders(self) -> ReconciliationResult:
        """Compare IBKR orders with internal state and handle any discrepancies."""
        if logger:
            logger.debug("Starting order reconciliation")
            
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
                if logger:
                    logger.info(f"Found {len(discrepancies)} order discrepancies")
            else:
                if logger:
                    logger.debug("No order discrepancies found")

        except Exception as e:
            result.error = str(e)
            if logger:
                logger.error(f"Order reconciliation failed: {e}")

        return result

    def _reconcile_positions(self) -> ReconciliationResult:
        """Compare IBKR positions with internal state and handle any discrepancies."""
        if logger:
            logger.debug("Starting position reconciliation")
            
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
                if logger:
                    logger.info(f"Found {len(discrepancies)} position discrepancies")
            else:
                if logger:
                    logger.debug("No position discrepancies found")

        except Exception as e:
            result.error = str(e)
            if logger:
                logger.error(f"Position reconciliation failed: {e}")

        return result

    # <AON Reconciliation Methods - Begin>
    def _reconcile_aon_orders(self) -> None:
        """Handle AON-specific reconciliation scenarios."""
        if logger:
            logger.debug("Starting AON reconciliation")
            
        try:
            ibkr_orders = self.ibkr_client.get_open_orders()
            internal_orders = self._get_internal_working_orders()
            
            # Check for orphaned AON orders (in IBKR but not in our DB)
            self._handle_orphaned_aon_orders(ibkr_orders, internal_orders)
            
            # Check for AON status mismatches
            self._handle_aon_status_mismatches(ibkr_orders, internal_orders)
            
            if logger:
                logger.debug("AON reconciliation completed")
                
        except Exception as e:
            if logger:
                logger.error(f"AON reconciliation failed: {e}")

    def _handle_orphaned_aon_orders(self, ibkr_orders: List[IbkrOrder], internal_orders: List[Dict]) -> None:
        """Handle AON orders that exist in IBKR but not in our database."""
        orphaned_count = 0
        
        for ibkr_order in ibkr_orders:
            # Check if this IBKR order exists in our internal state
            internal_match = self._find_internal_order_match(ibkr_order, internal_orders)
            
            if not internal_match and self._is_likely_aon_order(ibkr_order):
                if logger:
                    logger.warning(f"Found orphaned AON order: {ibkr_order.order_id} for {ibkr_order.symbol}")
                orphaned_count += 1
                
                # For now, just log - in production you might want to:
                # 1. Create a PlannedOrderDB record from the IBKR order
                # 2. Update status based on IBKR state
                # 3. Resume monitoring
                
        if orphaned_count > 0:
            if logger:
                logger.warning(f"Found {orphaned_count} orphaned AON orders (logged for manual review)")

    def _handle_aon_status_mismatches(self, ibkr_orders: List[IbkrOrder], internal_orders: List[Dict]) -> None:
        """Handle status mismatches involving AON orders."""
        for internal_order in internal_orders:
            ibkr_match = self._find_ibkr_order_match(internal_order, ibkr_orders)
            
            if ibkr_match:
                # Check for status mismatches
                internal_status = internal_order.get('status')
                ibkr_status = ibkr_match.status
                
                # Handle AON-specific status transitions
                if (internal_status == SharedOrderState.LIVE_WORKING.value and 
                    ibkr_status in ['Filled', 'Cancelled']):
                    
                    if logger:
                        logger.info(f"AON status sync: {internal_order['symbol']} {internal_status} → {ibkr_status}")
                    
                    # Update internal status to match IBKR reality
                    db_order = self.db_session.query(PlannedOrderDB).filter_by(id=internal_order['id']).first()
                    if db_order:
                        if ibkr_status == 'Filled':
                            db_order.status = SharedOrderState.FILLED.value
                        elif ibkr_status == 'Cancelled':
                            db_order.status = SharedOrderState.CANCELLED.value
                        
                        self.db_session.commit()
                        if logger:
                            logger.info(f"Updated {internal_order['symbol']} status to {db_order.status}")

    def _is_likely_aon_order(self, ibkr_order: IbkrOrder) -> bool:
        """Check if an IBKR order is likely an AON order from our system."""
        # Look for characteristics of our AON bracket orders
        # This is a simplified check - in practice you'd have more sophisticated detection
        return (ibkr_order.parent_id is not None and  # Part of a bracket order
                ibkr_order.order_type in ['LMT', 'STP'] and  # Our order types
                ibkr_order.remaining_quantity == ibkr_order.total_quantity)  # Not partially filled

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
        if logger:
            logger.debug("Fetching internal working orders")
            
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
            
            if logger:
                logger.debug(f"Found {len(internal_orders)} internal working orders")
            return internal_orders
            
        except Exception as e:
            if logger:
                logger.error(f"Error fetching internal working orders: {e}")
            return []

    def _find_order_discrepancies(self, internal_orders: List[Dict], ibkr_orders: List[IbkrOrder]) -> List[OrderDiscrepancy]:
        """Find discrepancies between internal and external order states."""
        if logger:
            logger.debug("Finding order discrepancies")
            
        discrepancies = []
        
        if ibkr_orders:
            if logger:
                logger.debug(f"IBKR has {len(ibkr_orders)} open orders")
            
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
        
        if logger:
            logger.debug(f"Found {len(discrepancies)} order discrepancies")
        return discrepancies

    def _find_position_discrepancies(self, internal_positions: List[Any], ibkr_positions: List[IbkrPosition]) -> List[PositionDiscrepancy]:
        """Find discrepancies between internal and external position states."""
        if logger:
            logger.debug("Finding position discrepancies")
            
        discrepancies = []
        if ibkr_positions:
            if logger:
                logger.debug(f"IBKR has {len(ibkr_positions)} positions")
        return discrepancies

    def _handle_order_discrepancy(self, discrepancy: OrderDiscrepancy) -> None:
        """Handle an order state discrepancy."""
        if logger:
            logger.info(f"Handling order discrepancy: {discrepancy.discrepancy_type} for {discrepancy.symbol}")
        
        if discrepancy.discrepancy_type == 'status_mismatch':
            if logger:
                logger.warning(f"Status mismatch: Internal={discrepancy.internal_status}, IBKR={discrepancy.external_status}")
            # In production, you would update the internal status to match IBKR
            
        elif discrepancy.discrepancy_type == 'orphaned_order':
            if logger:
                logger.warning(f"Orphaned order: {discrepancy.symbol} (Order ID: {discrepancy.order_id})")
            # In production, you would create an internal record for this order
            
        elif discrepancy.discrepancy_type == 'missing_order':
            if logger:
                logger.warning(f"Missing order: {discrepancy.symbol} (Internal status: {discrepancy.internal_status})")
            # In production, you would mark the internal order as failed/cancelled

    def _handle_position_discrepancy(self, discrepancy: PositionDiscrepancy) -> None:
        """Handle a position state discrepancy."""
        if logger:
            logger.info(f"Handling position discrepancy: {discrepancy.discrepancy_type} for {discrepancy.symbol}")
            
        if discrepancy.discrepancy_type == 'quantity_mismatch':
            if logger:
                logger.warning(f"Quantity mismatch: Internal={discrepancy.internal_position}, IBKR={discrepancy.external_position}")

    def _handle_reconciliation_error(self, error: Exception) -> None:
        """Handle reconciliation errors gracefully."""
        if logger:
            logger.error(f"Reconciliation error: {error}")

    def force_reconciliation(self) -> Optional[tuple]:
        """Force an immediate reconciliation cycle for testing or manual intervention."""
        if not self._running:
            if logger:
                logger.warning("Reconciliation engine not running")
            return None

        if logger:
            logger.info("Manual reconciliation triggered")
            
        try:
            order_result = self._reconcile_orders()
            position_result = self._reconcile_positions()
            # <AON Reconciliation Integration - Begin>
            self._reconcile_aon_orders()
            # <AON Reconciliation Integration - End>
            if logger:
                logger.info("Manual reconciliation completed")
            return order_result, position_result
        except Exception as e:
            if logger:
                logger.error(f"Manual reconciliation failed: {e}")
            return None