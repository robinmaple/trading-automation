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
        
        # <Context-Aware Logging - Service Initialization - Begin>
        self.context_logger.log_event(
            event_type=TradingEventType.SYSTEM_HEALTH,
            message="OrderEligibilityService initialized",
            context_provider={
                'planned_orders_count': len(planned_orders),
                'probability_engine_available': probability_engine is not None,
                'has_db_session': db_session is not None,
                'service_phase': 'Phase_B'
            },
            decision_reason="Service startup with Phase B features"
        )
        # <Context-Aware Logging - Service Initialization - End>
    # Phase B Additions - End

    def can_trade(self, planned_order) -> bool:
        """Layer 2: Business logic validation - should this order be traded?"""
        # <Context-Aware Logging - Single Order Validation Start - Begin>
        self.context_logger.log_event(
            event_type=TradingEventType.ORDER_VALIDATION,
            message="Starting business logic validation for single order",
            symbol=planned_order.symbol,
            context_provider={
                'order_action': planned_order.action.value if hasattr(planned_order, 'action') else 'unknown',
                'order_type': planned_order.order_type.value if hasattr(planned_order, 'order_type') else 'unknown',
                'security_type': planned_order.security_type.value if hasattr(planned_order, 'security_type') else 'unknown',
                'position_strategy': planned_order.position_strategy.value if hasattr(planned_order, 'position_strategy') else 'unknown'
            },
            decision_reason="Begin Phase B eligibility check"
        )
        # <Context-Aware Logging - Single Order Validation Start - End>
            
        try:
            # <Remove Duplicate Validation - Begin>
            # STOP LOSS VALIDATION: Handled by OrderLifecycleManager.validate_order()
            # DUPLICATE CHECKING: Handled by OrderLifecycleManager.is_order_executable()
            # <Remove Duplicate Validation - End>
            
            # Check if order is expired (if expiration logic exists)
            if hasattr(planned_order, 'expiration') and self._is_order_expired(planned_order):
                expiration_context = self._get_expiration_context(planned_order)
                # <Context-Aware Logging - Order Expired - Begin>
                self.context_logger.log_event(
                    event_type=TradingEventType.ORDER_VALIDATION,
                    message="Order expired - business rule violation",
                    symbol=planned_order.symbol,
                    context_provider=expiration_context,
                    decision_reason="Order expired based on business rules"
                )
                # <Context-Aware Logging - Order Expired - End>
                return False
                
            # Additional Phase B business rules can be added here
            # <Context-Aware Logging - Order Eligibility Passed - Begin>
            self.context_logger.log_event(
                event_type=TradingEventType.EXECUTION_DECISION,
                message="Order passed Phase B eligibility",
                symbol=planned_order.symbol,
                context_provider={
                    'order_action': planned_order.action.value if hasattr(planned_order, 'action') else 'unknown',
                    'order_type': planned_order.order_type.value if hasattr(planned_order, 'order_type') else 'unknown',
                    'priority': getattr(planned_order, 'priority', 1),
                    'risk_per_trade': float(getattr(planned_order, 'risk_per_trade', 0)) if hasattr(planned_order, 'risk_per_trade') else 0,
                    'risk_reward_ratio': float(getattr(planned_order, 'risk_reward_ratio', 0)) if hasattr(planned_order, 'risk_reward_ratio') else 0
                },
                decision_reason="All business rules satisfied"
            )
            # <Context-Aware Logging - Order Eligibility Passed - End>
            return True
            
        except Exception as e:
            # <Context-Aware Logging - Validation Exception - Begin>
            self.context_logger.log_event(
                event_type=TradingEventType.SYSTEM_HEALTH,
                message="Business validation error",
                symbol=planned_order.symbol,
                context_provider={
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'order_action': planned_order.action.value if hasattr(planned_order, 'action') else 'unknown'
                },
                decision_reason="Exception during business rule validation"
            )
            # <Context-Aware Logging - Validation Exception - End>
            return False

    def _get_expiration_context(self, order) -> dict:
        """Safe context provider for expiration checks."""
        try:
            context = {}
            if hasattr(order, 'expiration'):
                context['has_expiration'] = True
                context['expiration_value'] = str(order.expiration)
                context['is_expired'] = self._is_order_expired(order)
                # Add additional expiration details if available
                if hasattr(order, '_import_time'):
                    context['import_time'] = order._import_time.isoformat() if order._import_time else None
                if hasattr(order, 'position_strategy'):
                    context['position_strategy'] = order.position_strategy.value
            else:
                context['has_expiration'] = False
            return context
        except Exception as e:
            return {'error': f'Failed to get expiration context: {str(e)}'}

    # <Remove Duplicate Methods - Begin>
    # REMOVED: _validate_stop_loss_rules() - Duplicate of OrderLifecycleManager logic
    # REMOVED: _is_duplicate_of_active_order() - Placeholder, handled by OrderLifecycleManager
    # <Remove Duplicate Methods - End>

    def _is_order_expired(self, order) -> bool:
        """Check if the order has expired based on its setup or timeframe."""
        # <Context-Aware Logging - Expiration Check - Begin>
        # Placeholder - implement based on your expiration logic
        is_expired = False  # Default implementation
        
        # Log expiration check for monitoring
        self.context_logger.log_event(
            event_type=TradingEventType.ORDER_VALIDATION,
            message="Expiration check performed",
            symbol=order.symbol,
            context_provider={
                'is_expired': is_expired,
                'has_expiration_attr': hasattr(order, 'expiration'),
                'expiration_value': str(getattr(order, 'expiration', 'NO_EXPIRATION'))
            },
            decision_reason="Expiration logic evaluated"
        )
        # <Context-Aware Logging - Expiration Check - End>
        
        return is_expired

    def find_executable_orders(self) -> list:
        """Find all orders eligible for execution, enriched with probability scores and effective priority."""
        # <Context-Aware Logging - Batch Processing Start - Begin>
        self.context_logger.log_event(
            event_type=TradingEventType.EXECUTION_DECISION,
            message="Starting batch eligibility evaluation for all orders",
            context_provider={
                'total_orders': len(self.planned_orders),
                'session_id': self.context_logger.session_id,
                'db_session_available': self.db_session is not None
            },
            decision_reason="Begin Phase B filtering for execution candidates"
        )
        # <Context-Aware Logging - Batch Processing Start - End>
        
        executable = []
        rejected_count = 0
        processed_count = 0

        for order in self.planned_orders:
            processed_count += 1
            
            # <Context-Aware Logging - Individual Order Processing - Begin>
            self.context_logger.log_event(
                event_type=TradingEventType.EXECUTION_DECISION,
                message=f"Processing order {processed_count} of {len(self.planned_orders)}",
                symbol=order.symbol,
                context_provider={
                    'processing_index': processed_count,
                    'total_orders': len(self.planned_orders),
                    'order_action': order.action.value if hasattr(order, 'action') else 'unknown'
                }
            )
            # <Context-Aware Logging - Individual Order Processing - End>
            
            if not self.can_trade(order):
                rejected_count += 1
                continue

            # Phase B: compute probability score WITH comprehensive features
            fill_prob, features = self.probability_engine.score_fill(order, return_features=True)

            # Priority is manually supplied in template (default=1 if missing)
            base_priority = getattr(order, "priority", 1)
            effective_priority = base_priority * fill_prob

            # <Context-Aware Logging - Probability Scoring Result - Begin>
            self.context_logger.log_event(
                event_type=TradingEventType.RISK_EVALUATION,
                message="Probability score computed for order",
                symbol=order.symbol,
                context_provider={
                    'fill_probability': round(fill_prob, 4),
                    'base_priority': base_priority,
                    'effective_priority': round(effective_priority, 4),
                    'feature_count': len(features) if features else 0,
                    'probability_engine_used': True,
                    'features_available': features is not None
                },
                decision_reason="Probability engine scoring completed"
            )
            # <Context-Aware Logging - Probability Scoring Result - End>

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
                    
                    # <Context-Aware Logging - Database Persistence Success - Begin>
                    self.context_logger.log_event(
                        event_type=TradingEventType.DATABASE_STATE,
                        message="Probability score persisted to database",
                        symbol=order.symbol,
                        context_provider={
                            'fill_probability': round(fill_prob, 4),
                            'effective_priority': round(effective_priority, 4),
                            'database_id': prob_score.id if hasattr(prob_score, 'id') else 'unknown',
                            'feature_count': len(features) if features else 0
                        },
                        decision_reason="Score saved for model training and analysis"
                    )
                    # <Context-Aware Logging - Database Persistence Success - End>
                except Exception as e:
                    # <Context-Aware Logging - Database Persistence Error - Begin>
                    self.context_logger.log_event(
                        event_type=TradingEventType.SYSTEM_HEALTH,
                        message="Database persistence failed for probability score",
                        symbol=order.symbol,
                        context_provider={
                            'error_type': type(e).__name__,
                            'error_message': str(e)[:100],  # Limit length
                            'fill_probability': round(fill_prob, 4),
                            'db_session_available': True
                        },
                        decision_reason="Database write error for probability score"
                    )
                    # <Context-Aware Logging - Database Persistence Error - End>
            else:
                # <Context-Aware Logging - No Database Session - Begin>
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,  # event_type as positional first
                    "No database session available for probability score persistence",  # message as positional second
                    symbol=order.symbol,
                    context_provider={
                        'fill_probability': round(fill_prob, 4),
                        'db_session_available': False
                    },
                    decision_reason="Probability score computed but not persisted"
                )
                # <Context-Aware Logging - No Database Session - End>

            executable.append({
                'order': order,
                'fill_probability': fill_prob,
                'priority': base_priority,
                'effective_priority': effective_priority,
                'timestamp': datetime.datetime.now(),
                'features': features  # Include features in executable result
            })

            # <Context-Aware Logging - Order Added to Executable - Begin>
            self.context_logger.log_event(
                event_type=TradingEventType.EXECUTION_DECISION,
                message="Order added to executable list",
                symbol=order.symbol,
                context_provider={
                    'executable_index': len(executable),
                    'fill_probability': round(fill_prob, 4),
                    'effective_priority': round(effective_priority, 4)
                },
                decision_reason="Order meets all Phase B criteria"
            )
            # <Context-Aware Logging - Order Added to Executable - End>

        # Sort so that higher effective priority comes first
        executable.sort(key=lambda x: x['effective_priority'], reverse=True)
        
        # <Context-Aware Logging - Batch Processing Complete - Begin>
        success_rate = round(len(executable) / len(self.planned_orders) * 100, 2) if self.planned_orders else 0
        self.context_logger.log_event(
            event_type=TradingEventType.EXECUTION_DECISION,
            message="Batch eligibility evaluation completed",
            context_provider={
                'total_processed': len(self.planned_orders),
                'executable_count': len(executable),
                'rejected_count': rejected_count,
                'success_rate': success_rate,
                'top_effective_priority': round(executable[0]['effective_priority'], 4) if executable else 0,
                'lowest_effective_priority': round(executable[-1]['effective_priority'], 4) if executable else 0,
                'executable_symbols': [item['order'].symbol for item in executable] if executable else []
            },
            decision_reason=f"Phase B filtering completed: {len(executable)} executable orders found"
        )
        # <Context-Aware Logging - Batch Processing Complete - End>
        
        return executable