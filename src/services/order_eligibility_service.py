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

# Context-aware logging import - Begin
from src.core.context_aware_logger import get_context_logger, TradingEventType
# Context-aware logging import - End


class OrderEligibilityService:
    """Evaluates and filters planned orders to find those eligible for execution."""

    # Extend __init__ to accept DB session
    # Phase B Additions - Begin
    def __init__(self, planned_orders, probability_engine, db_session=None):
        """Initialize with optional DB session for Phase B logging."""
        # Context-aware logging initialization - Begin
        self.context_logger = get_context_logger()
        # Context-aware logging initialization - End
        
        self.planned_orders = planned_orders
        self.probability_engine = probability_engine
        self.db_session = db_session  # SQLAlchemy session
        
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="OrderEligibilityService initialized",
            context_provider={
                'planned_orders_count': lambda: len(planned_orders),
                'has_db_session': lambda: db_session is not None
            },
            decision_reason="Service startup"
        )
    # Phase B Additions - End

    def can_trade(self, planned_order) -> bool:
        """Layer 2: Business logic validation - should this order be traded?"""
        self.context_logger.log_event(
            event_type=TradingEventType.ORDER_VALIDATION,
            message="Starting business logic validation",
            symbol=planned_order.symbol,
            context_provider={
                'order_action': lambda: planned_order.action.value if hasattr(planned_order, 'action') else 'unknown',
                'order_type': lambda: planned_order.order_type.value if hasattr(planned_order, 'order_type') else 'unknown'
            },
            decision_reason="Begin Phase B eligibility check"
        )
            
        try:
            # <Remove Duplicate Validation - Begin>
            # STOP LOSS VALIDATION: Handled by OrderLifecycleManager.validate_order()
            # DUPLICATE CHECKING: Handled by OrderLifecycleManager.is_order_executable()
            # <Remove Duplicate Validation - End>
            
            # Check if order is expired (if expiration logic exists)
            if hasattr(planned_order, 'expiration') and self._is_order_expired(planned_order):
                self.context_logger.log_event(
                    event_type=TradingEventType.ORDER_VALIDATION,
                    message="Order expired - business rule violation",
                    symbol=planned_order.symbol,
                    context_provider={
                        'order_action': lambda: planned_order.action.value if hasattr(planned_order, 'action') else 'unknown',
                        'expiration_check': lambda: self._get_expiration_context(planned_order)
                    },
                    decision_reason="Order expired based on business rules"
                )
                return False
                
            # Additional Phase B business rules can be added here
            self.context_logger.log_event(
                event_type=TradingEventType.EXECUTION_DECISION,
                message="Order passed Phase B eligibility",
                symbol=planned_order.symbol,
                context_provider={
                    'order_action': lambda: planned_order.action.value if hasattr(planned_order, 'action') else 'unknown',
                    'order_type': lambda: planned_order.order_type.value if hasattr(planned_order, 'order_type') else 'unknown',
                    'priority': lambda: getattr(planned_order, 'priority', 1)
                },
                decision_reason="All business rules satisfied"
            )
            return True
            
        except Exception as e:
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Business validation error",
                symbol=planned_order.symbol,
                context_provider={
                    'error_type': lambda: type(e).__name__,
                    'error_message': lambda: str(e)
                },
                decision_reason="Exception during business rule validation"
            )
            return False

    def _get_expiration_context(self, order) -> dict:
        """Safe context provider for expiration checks."""
        try:
            context = {}
            if hasattr(order, 'expiration'):
                context['has_expiration'] = True
                context['expiration_value'] = str(order.expiration)
                context['is_expired'] = self._is_order_expired(order)
            else:
                context['has_expiration'] = False
            return context
        except Exception:
            return {'error': 'Failed to get expiration context'}

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
        self.context_logger.log_event(
            event_type=TradingEventType.EXECUTION_DECISION,
            message="Starting batch eligibility evaluation",
            context_provider={
                'total_orders': lambda: len(self.planned_orders),
                'session_id': lambda: self.context_logger.session_id
            },
            decision_reason="Begin Phase B filtering"
        )
        
        executable = []
        rejected_count = 0

        for order in self.planned_orders:
            if not self.can_trade(order):
                rejected_count += 1
                continue

            # Phase B: compute probability score WITH comprehensive features
            fill_prob, features = self.probability_engine.score_fill(order, return_features=True)

            # Priority is manually supplied in template (default=1 if missing)
            base_priority = getattr(order, "priority", 1)
            effective_priority = base_priority * fill_prob

            # Log probability scoring result - Begin
            self.context_logger.log_event(
                event_type=TradingEventType.RISK_EVALUATION,
                message="Probability score computed",
                symbol=order.symbol,
                context_provider={
                    'fill_probability': lambda: round(fill_prob, 4),
                    'base_priority': lambda: base_priority,
                    'effective_priority': lambda: round(effective_priority, 4),
                    'feature_count': lambda: len(features) if features else 0
                },
                decision_reason="Probability engine scoring completed"
            )
            # Log probability scoring result - End

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
                    
                    self.context_logger.log_event(
                        event_type=TradingEventType.DATABASE_STATE,
                        message="Probability score persisted to database",
                        symbol=order.symbol,
                        context_provider={
                            'fill_probability': lambda: round(fill_prob, 4),
                            'effective_priority': lambda: round(effective_priority, 4)
                        },
                        decision_reason="Score saved for model training"
                    )
                except Exception as e:
                    self.context_logger.log_event(
                        event_type=TradingEventType.SYSTEM_HEALTH,
                        message="Database persistence failed for probability score",
                        symbol=order.symbol,
                        context_provider={
                            'error_type': lambda: type(e).__name__,
                            'error_message': lambda: str(e)[:100]  # Limit length
                        },
                        decision_reason="Database write error"
                    )
            # --- End Phase B ---

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
        
        # Log batch processing results - Begin
        self.context_logger.log_event(
            event_type=TradingEventType.EXECUTION_DECISION,
            message="Batch eligibility evaluation completed",
            context_provider={
                'total_processed': lambda: len(self.planned_orders),
                'executable_count': lambda: len(executable),
                'rejected_count': lambda: rejected_count,
                'success_rate': lambda: round(len(executable) / len(self.planned_orders) * 100, 2) if self.planned_orders else 0
            },
            decision_reason="Phase B filtering completed"
        )
        # Log batch processing results - End
        
        return executable