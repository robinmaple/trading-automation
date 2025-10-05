"""
Phase B Order Eligibility Service
=================================

Purpose:
--------
Determines if PlannedOrders are eligible for execution based on Phase B business logic,
market conditions, and probability scoring. This service focuses on dynamic, real-time
factors that affect order execution quality.

Responsibilities (VALIDATE HERE):
---------------------------------
- Order expiration checks (time-based business rules)
- Market condition assessments (via ProbabilityEngine)
- Trading strategy quality evaluations
- Real-time risk and opportunity scoring
- Phase B specific business rules

Non-Responsibilities (VALIDATE ELSEWHERE):
------------------------------------------
- Basic data integrity (PlannedOrder.validate())
- Stop loss positioning (OrderLifecycleManager)
- Duplicate order detection (OrderLifecycleManager) 
- Risk limit compliance (OrderLifecycleManager/RiskManagementService)
- Open position checks (OrderLifecycleManager/StateService)

Architecture Context:
--------------------
This service operates AFTER fundamental validation. It assumes orders have already passed:
1. PlannedOrder data integrity checks (__post_init__)
2. OrderLifecycleManager system validation (validate_order)

Usage Flow:
----------
1. OrderLifecycleManager validates order fundamentals
2. This service evaluates Phase B eligibility criteria  
3. ProbabilityEngine scores fill probability
4. Orders are prioritized and executed accordingly

Phase B Features:
---------------
- Machine learning probability scoring
- Comprehensive feature logging for model training
- Dynamic priority adjustment based on market conditions
- Business rule enforcement for advanced strategies
"""

import datetime
# Phase B Additions - Begin
from src.core.models import ProbabilityScoreDB  # new table
# Phase B Additions - End

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)


class OrderEligibilityService:
    """Evaluates and filters planned orders to find those eligible for execution."""

    # Extend __init__ to accept DB session
    # Phase B Additions - Begin
    def __init__(self, planned_orders, probability_engine, db_session=None):
        """Initialize with optional DB session for Phase B logging."""
        if logger:
            logger.debug("Initializing OrderEligibilityService")
            
        self.planned_orders = planned_orders
        self.probability_engine = probability_engine
        self.db_session = db_session  # SQLAlchemy session
        
        if logger:
            logger.info("OrderEligibilityService initialized successfully")
    # Phase B Additions - End

    def can_trade(self, planned_order) -> bool:
        """Layer 2: Business logic validation - should this order be traded?"""
        if logger:
            logger.debug(f"Checking trade eligibility for {planned_order.symbol}")
            
        try:
            # <Remove Duplicate Validation - Begin>
            # STOP LOSS VALIDATION: Handled by OrderLifecycleManager.validate_order()
            # DUPLICATE CHECKING: Handled by OrderLifecycleManager.is_order_executable()
            # <Remove Duplicate Validation - End>
            
            # Check if order is expired (if expiration logic exists)
            if hasattr(planned_order, 'expiration') and self._is_order_expired(planned_order):
                if logger:
                    logger.warning(f"Order {planned_order.symbol} expired")
                return False
                
            # Additional Phase B business rules can be added here
            if logger:
                logger.debug(f"Order {planned_order.symbol} passed Phase B eligibility")
            return True
            
        except Exception as e:
            if logger:
                logger.error(f"Business validation error for {planned_order.symbol}: {e}")
            return False

    # <Remove Duplicate Methods - Begin>
    # REMOVED: _validate_stop_loss_rules() - Duplicate of OrderLifecycleManager logic
    # REMOVED: _is_duplicate_of_active_order() - Placeholder, handled by OrderLifecycleManager
    # <Remove Duplicate Methods - End>

    def _is_order_expired(self, order) -> bool:
        """Check if the order has expired based on its setup or timeframe."""
        # Placeholder - implement based on your expiration logic
        return False

    def find_executable_orders(self) -> list:
        """Find all orders eligible for execution, enriched with probability scores and effective priority."""
        if logger:
            logger.info(f"Finding executable orders from {len(self.planned_orders)} planned orders")
            
        executable = []

        for order in self.planned_orders:
            if not self.can_trade(order):
                if logger:
                    logger.debug(f"{order.symbol}: Cannot place order (Phase B constraints failed)")
                continue

            # Phase B: compute probability score WITH comprehensive features
            if logger:
                logger.debug(f"Computing probability score for {order.symbol}")
                
            fill_prob, features = self.probability_engine.score_fill(order, return_features=True)

            # Priority is manually supplied in template (default=1 if missing)
            base_priority = getattr(order, "priority", 1)
            effective_priority = base_priority * fill_prob

            # --- Phase B: persist probability score with comprehensive features ---
            if self.db_session:
                try:
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
                    if logger:
                        logger.debug(f"Saved probability score for {order.symbol}: {fill_prob:.3f}")
                except Exception as e:
                    if logger:
                        logger.error(f"Failed to save probability score for {order.symbol}: {e}")
            # --- End Phase B ---

            if logger:
                logger.debug(f"{order.action.value} {order.symbol}: Priority={base_priority}, "
                           f"FillProb={fill_prob:.3f}, EffectivePriority={effective_priority:.3f}")

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
        
        if logger:
            logger.info(f"Found {len(executable)} executable orders after Phase B filtering")
            
        return executable