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


class ReconciliationEngine:
    """Orchestrates the continuous synchronization between internal state and IBKR."""

    def __init__(self, ibkr_client: IbkrClient, state_service: StateService, polling_interval: int = 30):
        """Initialize the engine with its client, state service, and polling interval."""
        self.ibkr_client = ibkr_client
        self.state_service = state_service
        self.polling_interval = polling_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

    def start(self) -> None:
        """Start the background reconciliation thread."""
        with self._lock:
            if self._running:
                print("⚠️  Reconciliation engine already running")
                return

            self._running = True
            self._thread = threading.Thread(
                target=self._reconciliation_loop,
                daemon=True,
                name="ReconciliationEngine"
            )
            self._thread.start()
            print("✅ Reconciliation engine started")

    def stop(self) -> None:
        """Stop the background reconciliation thread."""
        with self._lock:
            if not self._running:
                return

            self._running = False
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=5.0)
            print("✅ Reconciliation engine stopped")

    def is_running(self) -> bool:
        """Check if the reconciliation engine is currently running."""
        with self._lock:
            return self._running

    def _reconciliation_loop(self) -> None:
        """Main loop that performs reconciliation at the configured interval."""
        error_count = 0
        max_errors = 5

        print(f"🔄 Reconciliation loop started (interval: {self.polling_interval}s)")

        while self._running and error_count < max_errors:
            try:
                self._reconcile_orders()
                self._reconcile_positions()
                error_count = 0
                time.sleep(self.polling_interval)
            except Exception as e:
                error_count += 1
                print(f"❌ Reconciliation error ({error_count}/{max_errors}): {e}")
                backoff_time = min(60 * error_count, 300)
                time.sleep(backoff_time)

        if error_count >= max_errors:
            print("❌ Too many reconciliation errors, stopping engine")
            self.stop()

    def _reconcile_orders(self) -> ReconciliationResult:
        """Compare IBKR orders with internal state and handle any discrepancies."""
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
                print(f"🔍 Found {len(discrepancies)} order discrepancies")

        except Exception as e:
            result.error = str(e)
            print(f"❌ Order reconciliation failed: {e}")

        return result

    def _reconcile_positions(self) -> ReconciliationResult:
        """Compare IBKR positions with internal state and handle any discrepancies."""
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
                print(f"🔍 Found {len(discrepancies)} position discrepancies")

        except Exception as e:
            result.error = str(e)
            print(f"❌ Position reconciliation failed: {e}")

        return result

    def _get_internal_working_orders(self) -> List[Dict[str, Any]]:
        """Get internal orders that should be working in IBKR. Placeholder implementation."""
        return []

    def _find_order_discrepancies(self, internal_orders: List[Dict], ibkr_orders: List[IbkrOrder]) -> List[OrderDiscrepancy]:
        """Find discrepancies between internal and external order states. Placeholder implementation."""
        discrepancies = []
        if ibkr_orders:
            print(f"📋 IBKR has {len(ibkr_orders)} open orders")
        return discrepancies

    def _find_position_discrepancies(self, internal_positions: List[Any], ibkr_positions: List[IbkrPosition]) -> List[PositionDiscrepancy]:
        """Find discrepancies between internal and external position states. Placeholder implementation."""
        discrepancies = []
        if ibkr_positions:
            print(f"📊 IBKR has {len(ibkr_positions)} positions")
        return discrepancies

    def _handle_order_discrepancy(self, discrepancy: OrderDiscrepancy) -> None:
        """Handle an order state discrepancy. Placeholder implementation."""
        print(f"🔄 Handling order discrepancy: {discrepancy.discrepancy_type} for order {discrepancy.order_id}")
        if discrepancy.discrepancy_type == 'status_mismatch':
            print(f"   Status mismatch: Internal={discrepancy.internal_status}, IBKR={discrepancy.external_status}")

    def _handle_position_discrepancy(self, discrepancy: PositionDiscrepancy) -> None:
        """Handle a position state discrepancy. Placeholder implementation."""
        print(f"🔄 Handling position discrepancy: {discrepancy.discrepancy_type} for {discrepancy.symbol}")
        if discrepancy.discrepancy_type == 'quantity_mismatch':
            print(f"   Quantity mismatch: Internal={discrepancy.internal_position}, IBKR={discrepancy.external_position}")

    def _handle_reconciliation_error(self, error: Exception) -> None:
        """Handle reconciliation errors gracefully. Placeholder implementation."""
        print(f"❌ Reconciliation error: {error}")

    def force_reconciliation(self) -> Optional[tuple]:
        """Force an immediate reconciliation cycle for testing or manual intervention."""
        if not self._running:
            print("⚠️  Reconciliation engine not running")
            return None

        print("🔄 Manual reconciliation triggered")
        try:
            order_result = self._reconcile_orders()
            position_result = self._reconcile_positions()
            print("✅ Manual reconciliation completed")
            return order_result, position_result
        except Exception as e:
            print(f"❌ Manual reconciliation failed: {e}")
            return None