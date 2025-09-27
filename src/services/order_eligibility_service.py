"""
Service responsible for determining if a PlannedOrder is eligible for execution.
Evaluates criteria such as duplicates, expiration, risk limits, and market conditions
via the ProbabilityEngine to decide if an order should be executed.
"""

import datetime
# Phase B Additions - Begin
from src.core.models import ProbabilityScoreDB  # new table
# Phase B Additions - End


class OrderEligibilityService:
    """Evaluates and filters planned orders to find those eligible for execution."""

    # Extend __init__ to accept DB session
    # Phase B Additions - Begin
    def __init__(self, planned_orders, probability_engine, db_session=None):
        """Initialize with optional DB session for Phase B logging."""
        self.planned_orders = planned_orders
        self.probability_engine = probability_engine
        self.db_session = db_session  # SQLAlchemy session
    # Phase B Additions - End

    def can_trade(self, planned_order) -> bool:
        """Layer 2: Business logic validation - should this order be traded?"""
        try:
            # Stop loss business rule validation
            if not self._validate_stop_loss_rules(planned_order):
                return False
                
            # Check for duplicate active orders
            if self._is_duplicate_of_active_order(planned_order):
                return False
                
            # Check if order is expired (if expiration logic exists)
            if hasattr(planned_order, 'expiration') and self._is_order_expired(planned_order):
                return False
                
            # Additional business rules can be added here
            return True
            
        except Exception as e:
            print(f"❌ Business validation error for {planned_order.symbol}: {e}")
            return False

    def _validate_stop_loss_rules(self, order) -> bool:
        """Validate stop loss relative to entry price based on action."""
        if order.stop_loss is None:
            return True  # Some orders might not have stop losses
            
        if order.action == 'BUY':
            if order.stop_loss >= order.entry_price:
                print(f"❌ Stop loss must be below entry price for BUY orders")
                return False
        elif order.action == 'SELL':
            if order.stop_loss <= order.entry_price:
                print(f"❌ Stop loss must be above entry price for SELL orders")
                return False
                
        return True

    def _is_duplicate_of_active_order(self, order) -> bool:
        """Check if this order is a duplicate of an already active order."""
        # This would need access to active orders - might require dependency injection
        # For now, return False as placeholder
        return False

    def _is_order_expired(self, order) -> bool:
        """Check if the order has expired based on its setup or timeframe."""
        # Placeholder - implement based on your expiration logic
        return False

    def find_executable_orders(self) -> list:
        """Find all orders eligible for execution, enriched with probability scores and effective priority."""
        executable = []

        for order in self.planned_orders:
            if not self.can_trade(order):
                print(f"   ⚠️  {order.symbol}: Cannot place order (basic constraints failed)")
                continue

            # Phase B: compute probability score WITH comprehensive features
            fill_prob, features = self.probability_engine.score_fill(order, return_features=True)

            # Priority is manually supplied in template (default=1 if missing)
            base_priority = getattr(order, "priority", 1)
            effective_priority = base_priority * fill_prob

            # --- Phase B: persist probability score with comprehensive features ---
            if self.db_session:
                prob_score = ProbabilityScoreDB(
                    planned_order_id=getattr(order, "id", None),
                    symbol=order.symbol,
                    timestamp=datetime.datetime.now(),
                    fill_probability=fill_prob,
                    features=features,  # Use comprehensive features from probability engine
                    score=effective_priority,
                    engine_version="phaseB_v1",
                    source="eligibility_service"
                )
                self.db_session.add(prob_score)
                self.db_session.commit()
            # --- End Phase B ---

            print(f"   {order.action.value} {order.symbol}: "
                  f"Priority={base_priority}, FillProb={fill_prob:.3f}, "
                  f"EffectivePriority={effective_priority:.3f}")

            executable.append({
                'order': order,
                'fill_probability': fill_prob,
                'priority': base_priority,
                'effective_priority': effective_priority,
                'timestamp': datetime.datetime.now(),
                'features': features  # Include features in executable result
            })

        # Sort so that higher effective priority comes first
        executable.sort(key=lambda x: x['effective_priority'], reverse=True)
        return executable