"""
Service responsible for determining if a PlannedOrder is eligible for execution.
Evaluates criteria such as duplicates, expiration, risk limits, and market conditions
via the ProbabilityEngine to decide if an order should be executed.
"""

import datetime


class OrderEligibilityService:
    """Evaluates and filters planned orders to find those eligible for execution."""

    def __init__(self, planned_orders, probability_engine):
        """Initialize the service with the list of planned orders and the probability engine."""
        self.planned_orders = planned_orders
        self.probability_engine = probability_engine

    def can_trade(self, planned_order) -> bool:
        """
        Check if a single order is eligible to be traded based on basic constraints.
        Placeholder for future logic checking duplicates, expiration, risk limits, etc.
        """
        # TODO: Implement actual eligibility checks (duplicates, expiration, risk limits, etc.)
        # For now, return True as a placeholder - this should be replaced with real logic.
        return True

    def find_executable_orders(self) -> list:
        """Find all orders eligible for execution, enriched with probability scores and effective priority."""
        executable = []

        for order in self.planned_orders:
            if not self.can_trade(order):
                print(f"   ⚠️  {order.symbol}: Cannot place order (basic constraints failed)")
                continue

            # Phase A: compute probability score
            fill_prob = self.probability_engine.score_fill(order)

            # Priority is manually supplied in template (default=1 if missing)
            base_priority = getattr(order, "priority", 1)
            effective_priority = base_priority * fill_prob

            print(f"   {order.action.value} {order.symbol}: "
                  f"Priority={base_priority}, FillProb={fill_prob:.3f}, "
                  f"EffectivePriority={effective_priority:.3f}")

            executable.append({
                'order': order,
                'fill_probability': fill_prob,
                'priority': base_priority,
                'effective_priority': effective_priority,
                'timestamp': datetime.datetime.now()
            })

        # Sort so that higher effective priority comes first
        executable.sort(key=lambda x: x['effective_priority'], reverse=True)
        return executable
