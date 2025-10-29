"""
Risk Management Service for trading automation system.
Handles position sizing constraints and loss-based trading halts.
Uses realized P&L from closed trades for risk calculations.
"""
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from decimal import Decimal

from src.trading.orders.planned_order import PlannedOrder, PositionStrategy, ActiveOrder
from src.services.state_service import StateService
from src.trading.orders.order_persistence_service import OrderPersistenceService

# Context-aware logging import - replacing simple_logger
from src.core.context_aware_logger import get_context_logger, TradingEventType

# Initialize context-aware logger
context_logger = get_context_logger()


class RiskManagementService:
    """Manages trading risk parameters and validations."""
    
    def __init__(self, state_service: StateService, 
                    persistence_service: OrderPersistenceService,
                    ibkr_client=None,
                    config: Optional[Dict] = None):
        """Initialize with optional risk configuration."""
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing RiskManagementService",
            context_provider={
                "state_service_provided": state_service is not None,
                "persistence_service_provided": persistence_service is not None,
                "ibkr_client_provided": ibkr_client is not None,
                "config_provided": config is not None,
                "config_source": "risk_config_module"
            }
        )
            
        self.state_service = state_service
        self.persistence = persistence_service
        self.ibkr_client = ibkr_client
        
        # Load risk configuration from config only - Begin
        # Remove any database dependency for risk limits
        from config.risk_config import DEFAULT_RISK_CONFIG, merge_risk_config
        self.config = merge_risk_config(config or {}, DEFAULT_RISK_CONFIG)
        
        # Extract risk limits directly from config
        risk_limits = self.config.get('risk_limits', {})
        self.loss_limits = {
            'daily': risk_limits.get('daily_loss_pct', Decimal('0.02')),
            'weekly': risk_limits.get('weekly_loss_pct', Decimal('0.05')),
            'monthly': risk_limits.get('monthly_loss_pct', Decimal('0.08'))
        }
        
        # Store max_risk_per_trade for validation (from config, not DB)
        self.max_risk_per_trade = risk_limits.get('max_risk_per_trade', Decimal('0.02'))
        
        # Store position limits from config
        self.position_limits = {
            'max_open_orders': risk_limits.get('max_open_orders', 5)
        }
        
        # Store simulation equity from config
        simulation_config = self.config.get('simulation', {})
        self.simulation_equity = simulation_config.get('default_equity', Decimal('100000'))
        
        # Cache for performance
        self._last_trading_halt_check = None
        self._trading_halted = False
        self._halt_reason = ""
        
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "RiskManagementService configured",
            context_provider={
                "daily_loss_limit": float(self.loss_limits['daily']),
                "weekly_loss_limit": float(self.loss_limits['weekly']),
                "monthly_loss_limit": float(self.loss_limits['monthly']),
                "max_risk_per_trade": float(self.max_risk_per_trade),
                "max_open_orders": self.position_limits['max_open_orders'],
                "simulation_equity": float(self.simulation_equity),
                "configuration_source": "risk_config_module"
            },
            decision_reason="RISK_SERVICE_INITIALIZED"
        )
        # Load risk configuration from config only - End

    # _get_total_equity - Begin (UPDATED)
    def _get_total_equity(self) -> Decimal:
        """Get total account equity. Halts trading if no valid numeric account values are found."""
        if self.ibkr_client and self.ibkr_client.connected:
            try:
                equity = Decimal(str(self.ibkr_client.get_account_value()))
                context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    "Live equity retrieved from IBKR",
                    context_provider={
                        "equity_amount": float(equity),
                        "data_source": "ibkr_live"
                    },
                    decision_reason="LIVE_EQUITY_RETRIEVED"
                )
                return equity
            except ValueError as e:
                # CRITICAL: No valid numeric account values found - halt trading immediately
                self._trading_halted = True
                self._halt_reason = f"No valid account values: {str(e)}"
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Trading halted - no valid numeric account values found",
                    context_provider={
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "halt_reason": self._halt_reason,
                        "safety_action": "halt_trading_no_account_values"
                    },
                    decision_reason="TRADING_HALTED_NO_VALID_ACCOUNT_VALUES"
                )
                # Re-raise to ensure calling code knows trading is halted
                raise TradingHaltedError(self._halt_reason)
            except Exception as e:
                # Other IBKR errors - use simulation as fallback but log warning
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "IBKR equity retrieval failed - using simulation",
                    context_provider={
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "fallback_action": "use_simulation_equity"
                    },
                    decision_reason="IBKR_EQUITY_RETRIEVAL_FAILED"
                )
                # Fall back to simulation for non-critical errors
                pass
                        
        # Use configurable default equity (for simulation or non-critical IBKR errors)
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Using simulation equity",
            context_provider={
                "equity_amount": float(self.simulation_equity),
                "data_source": "simulation_config",
                "trading_halted": self._trading_halted
            },
            decision_reason="SIMULATION_EQUITY_USED"
        )
        return self.simulation_equity
    # _get_total_equity - End

    # get_account_equity - Begin (UPDATED)
    def get_account_equity(self) -> Decimal:
        """Get current account equity from IBKR or use simulation value. Halts trading if no valid numeric account values."""
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Getting account equity",
            context_provider={
                "ibkr_client_available": self.ibkr_client is not None,
                "ibkr_client_connected": self.ibkr_client.is_connected() if self.ibkr_client else False
            }
        )
                
        if self.ibkr_client and self.ibkr_client.is_connected():
            try:
                equity = Decimal(str(self.ibkr_client.get_account_value()))
                context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    "Account equity retrieved from IBKR",
                    context_provider={
                        "equity_amount": float(equity),
                        "data_source": "ibkr_live"
                    },
                    decision_reason="ACCOUNT_EQUITY_RETRIEVED"
                )
                return equity
            except ValueError as e:
                # CRITICAL: No valid numeric account values found - halt trading immediately
                self._trading_halted = True
                self._halt_reason = f"No valid account values: {str(e)}"
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Trading halted - no valid numeric account values for equity check",
                    context_provider={
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "halt_reason": self._halt_reason,
                        "safety_action": "halt_trading_no_account_values"
                    },
                    decision_reason="TRADING_HALTED_NO_VALID_ACCOUNT_VALUES_EQUITY"
                )
                # Re-raise to ensure calling code knows trading is halted
                raise TradingHaltedError(self._halt_reason)
            except (ValueError, AttributeError) as e:
                # Other IBKR errors - use simulation as fallback
                context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "IBKR account equity retrieval failed",
                    context_provider={
                        "error_type": type(e).__name__,
                        "error_details": str(e),
                        "fallback_action": "use_simulation_equity"
                    },
                    decision_reason="IBKR_ACCOUNT_EQUITY_FAILED"
                )
                    
        # Fallback to simulation equity from config (for non-critical errors only)
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Using simulation equity as fallback",
            context_provider={
                "equity_amount": float(self.simulation_equity),
                "data_source": "simulation_config",
                "trading_halted": self._trading_halted
            },
            decision_reason="SIMULATION_EQUITY_FALLBACK"
        )
        return self.simulation_equity
    # get_account_equity - End

    def calculate_position_pnl(self, entry_price, exit_price, quantity, action):
        """Calculate position P&L with robust validation."""
        # 1. First check for None values
        if entry_price is None:
            raise ValueError("Entry price cannot be None")
        if exit_price is None:
            raise ValueError("Exit price cannot be None")
        if quantity is None:
            raise ValueError("Quantity cannot be None")
        if action is None:
            raise ValueError("Action cannot be None")
        
        # 2. Then check types
        if not isinstance(entry_price, (int, float, Decimal)):
            raise ValueError("Entry price must be numeric")
        if not isinstance(exit_price, (int, float, Decimal)):
            raise ValueError("Exit price must be numeric")
        if not isinstance(quantity, (int, float, Decimal)):
            raise ValueError("Quantity must be numeric")
        if not isinstance(action, str):
            raise ValueError("Action must be a string")
        
        # 3. Then check values
        if entry_price <= 0:
            raise ValueError("Entry price must be positive")
        if exit_price <= 0:
            raise ValueError("Exit price must be positive")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        
        # FIX: Make action validation case-insensitive
        if action.upper() not in ['BUY', 'SELL']:
            raise ValueError("Action must be 'BUY' or 'SELL'")
        
        # 4. Business logic (use uppercase for consistency)
        if action.upper() == 'BUY':
            return (exit_price - entry_price) * quantity
        else:
            return (entry_price - exit_price) * quantity

    def _cap_risk_to_max_limit(self, order: PlannedOrder) -> None:
        """
        Cap the order's risk_per_trade to the maximum allowed value if it exceeds the limit.
        Logs a warning when capping occurs.
        """
        if order.risk_per_trade is not None and order.risk_per_trade > self.max_risk_per_trade:
            original_risk = order.risk_per_trade
            order.risk_per_trade = self.max_risk_per_trade
            
            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Risk per trade capped to maximum limit",
                symbol=order.symbol,
                context_provider={
                    "original_risk_percent": float(original_risk),
                    "capped_risk_percent": float(self.max_risk_per_trade),
                    "max_risk_limit": float(self.max_risk_per_trade),
                    "reduction_percent": float((original_risk - self.max_risk_per_trade) / original_risk * 100),
                    "action": order.action.value
                },
                decision_reason="RISK_PER_TRADE_CAPPED"
            )
    # Risk Capping Implementation - End
    
    def can_place_order(self, order: PlannedOrder, 
                       active_orders: Dict[int, ActiveOrder], 
                       total_capital: float) -> bool:
        """
        Hard block check for all risk rules. Returns False if any risk rule is violated.
        """
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Checking if order can be placed",
            symbol=order.symbol,
            context_provider={
                "action": order.action.value,
                "order_type": order.order_type.value,
                "position_strategy": order.position_strategy.value if order.position_strategy else None,
                "total_capital": float(total_capital),
                "active_orders_count": len(active_orders)
            },
            decision_reason="ORDER_PLACEMENT_RISK_CHECK_STARTED"
        )
            
        # 1. Check trading halts first (highest priority - loss limits)
        if not self._check_trading_halts():
            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Order rejected - trading halted",
                symbol=order.symbol,
                context_provider={
                    "halt_reason": self._halt_reason,
                    "trading_halted": True,
                    "rejection_reason": "trading_halted"
                },
                decision_reason="ORDER_REJECTED_TRADING_HALTED"
            )
            return False
        
        # 2. Cap risk_per_trade to maximum allowed instead of rejecting
        self._cap_risk_to_max_limit(order)
            
        # 3. Check position sizing for CORE/HYBRID strategies
        if not self._validate_position_limits(order, active_orders, total_capital):
            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Order rejected - position limit violation",
                symbol=order.symbol,
                context_provider={
                    "rejection_reason": "position_limit_violation",
                    "position_strategy": order.position_strategy.value if order.position_strategy else None
                },
                decision_reason="ORDER_REJECTED_POSITION_LIMIT"
            )
            return False
            
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Order passed all risk checks",
            symbol=order.symbol,
            context_provider={
                "risk_checks_passed": True,
                "trading_halted": False,
                "position_limits_met": True,
                "risk_capping_applied": order.risk_per_trade is not None and order.risk_per_trade <= self.max_risk_per_trade
            },
            decision_reason="ORDER_PASSED_RISK_CHECKS"
        )
        return True
    
    def _validate_position_limits(self, order: PlannedOrder, 
                                active_orders: Dict[int, ActiveOrder], 
                                total_capital: float) -> bool:
        """
        Check 30% per trade and 60% total exposure for CORE/HYBRID strategies.
        Returns True if order complies with position limits.
        """
        if order.position_strategy not in self.position_limits:
            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "No position limits for strategy - validation passed",
                symbol=order.symbol,
                context_provider={
                    "position_strategy": order.position_strategy.value if order.position_strategy else None,
                    "strategy_has_limits": False
                }
            )
            return True  # No limits for other strategies
            
        limits = self.position_limits[order.position_strategy]
        
        try:
            # Calculate proposed position size
            quantity = order.calculate_quantity(total_capital)
            position_value = order.entry_price * quantity
            
            # Check single trade limit (30%)
            single_trade_limit = total_capital * limits['single_trade']
            if position_value > single_trade_limit:
                context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    "Single trade position limit violation",
                    symbol=order.symbol,
                    context_provider={
                        "position_value": float(position_value),
                        "single_trade_limit": float(single_trade_limit),
                        "exceeds_by": float(position_value - single_trade_limit),
                        "limit_percentage": float(limits['single_trade']),
                        "violation_type": "single_trade_limit"
                    },
                    decision_reason="SINGLE_TRADE_LIMIT_VIOLATION"
                )
                return False
                
            # Check total exposure limit (60%) for CORE/HYBRID strategies
            total_exposure = self._calculate_total_exposure(active_orders, total_capital)
            total_exposure_after_order = total_exposure + position_value
            total_exposure_limit = total_capital * limits['total_exposure']
            
            if total_exposure_after_order > total_exposure_limit:
                context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    "Total exposure limit violation",
                    symbol=order.symbol,
                    context_provider={
                        "current_exposure": float(total_exposure),
                        "proposed_exposure": float(total_exposure_after_order),
                        "total_exposure_limit": float(total_exposure_limit),
                        "exceeds_by": float(total_exposure_after_order - total_exposure_limit),
                        "limit_percentage": float(limits['total_exposure']),
                        "violation_type": "total_exposure_limit"
                    },
                    decision_reason="TOTAL_EXPOSURE_LIMIT_VIOLATION"
                )
                return False
                
            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Position limits validation passed",
                symbol=order.symbol,
                context_provider={
                    "position_value": float(position_value),
                    "single_trade_limit": float(single_trade_limit),
                    "current_exposure": float(total_exposure),
                    "proposed_exposure": float(total_exposure_after_order),
                    "total_exposure_limit": float(total_exposure_limit),
                    "utilization_percentages": {
                        "single_trade": float(position_value / single_trade_limit * 100),
                        "total_exposure": float(total_exposure_after_order / total_exposure_limit * 100)
                    }
                },
                decision_reason="POSITION_LIMITS_VALIDATION_PASSED"
            )
            return True
            
        except (ValueError, TypeError) as e:
            # If calculation fails (e.g., missing entry price), reject order
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Position limit calculation failed",
                symbol=order.symbol,
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "position_strategy": order.position_strategy.value if order.position_strategy else None,
                    "calculation_failed": True
                },
                decision_reason="POSITION_LIMIT_CALCULATION_FAILED"
            )
            return False
    
    def _calculate_total_exposure(self, active_orders: Dict[int, ActiveOrder], 
                                total_capital: float) -> float:
        """Calculate total capital committed to CORE/HYBRID strategies."""
        total_exposure = 0.0
        
        for active_order in active_orders.values():
            if not active_order.is_working():
                continue
                
            if active_order.planned_order.position_strategy in self.position_limits:
                total_exposure += active_order.capital_commitment
        
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Total exposure calculated",
            context_provider={
                "total_exposure": float(total_exposure),
                "active_orders_count": len(active_orders),
                "orders_with_exposure": len([o for o in active_orders.values() if o.is_working() and o.planned_order.position_strategy in self.position_limits]),
                "exposure_percentage": float(total_exposure / total_capital * 100) if total_capital > 0 else 0
            }
        )
        return total_exposure
    
    def _check_trading_halts(self) -> bool:
        """
        Check if trading is halted due to loss limits.
        Uses realized P&L from closed trades only.
        """
        # Cache check for performance (check every 5 minutes)
        current_time = datetime.now()
        if (self._last_trading_halt_check and 
            (current_time - self._last_trading_halt_check).total_seconds() < 300):
            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Using cached trading halt status",
                context_provider={
                    "cached_halted": self._trading_halted,
                    "cached_reason": self._halt_reason,
                    "cache_valid_seconds": 300,
                    "time_since_last_check_seconds": (current_time - self._last_trading_halt_check).total_seconds()
                }
            )
            return not self._trading_halted
            
        self._last_trading_halt_check = current_time
        
        try:
            total_equity = self._get_total_equity()
            if total_equity <= 0:
                self._trading_halted = True
                self._halt_reason = "Zero or negative equity"
                context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    "Trading halted - zero or negative equity",
                    context_provider={
                        "total_equity": float(total_equity),
                        "halt_reason": self._halt_reason,
                        "equity_condition": "zero_or_negative"
                    },
                    decision_reason="TRADING_HALTED_ZERO_EQUITY"
                )
                return False
            
            # Get realized P&L for different time periods
            daily_pnl = self.persistence.get_realized_pnl_period(days=1)
            weekly_pnl = self.persistence.get_realized_pnl_period(days=7)
            monthly_pnl = self.persistence.get_realized_pnl_period(days=30)
            
            # Convert to percentage loss (only consider losses)
            daily_loss_pct = abs(min(daily_pnl, Decimal('0'))) / total_equity
            weekly_loss_pct = abs(min(weekly_pnl, Decimal('0'))) / total_equity  
            monthly_loss_pct = abs(min(monthly_pnl, Decimal('0'))) / total_equity
            
            # Check against limits from config (not database)
            if daily_loss_pct >= self.loss_limits['daily']:
                self._trading_halted = True
                self._halt_reason = f"Daily loss limit exceeded: {daily_loss_pct:.2%} >= {self.loss_limits['daily']:.2%}"
                context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    "Trading halted - daily loss limit exceeded",
                    context_provider={
                        "daily_loss_percent": float(daily_loss_pct),
                        "daily_loss_limit": float(self.loss_limits['daily']),
                        "exceeds_by_percent": float(daily_loss_pct - self.loss_limits['daily']),
                        "daily_pnl_amount": float(daily_pnl),
                        "halt_reason": self._halt_reason
                    },
                    decision_reason="TRADING_HALTED_DAILY_LOSS"
                )
                return False
                
            if weekly_loss_pct >= self.loss_limits['weekly']:
                self._trading_halted = True
                self._halt_reason = f"Weekly loss limit exceeded: {weekly_loss_pct:.2%} >= {self.loss_limits['weekly']:.2%}"
                context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    "Trading halted - weekly loss limit exceeded",
                    context_provider={
                        "weekly_loss_percent": float(weekly_loss_pct),
                        "weekly_loss_limit": float(self.loss_limits['weekly']),
                        "exceeds_by_percent": float(weekly_loss_pct - self.loss_limits['weekly']),
                        "weekly_pnl_amount": float(weekly_pnl),
                        "halt_reason": self._halt_reason
                    },
                    decision_reason="TRADING_HALTED_WEEKLY_LOSS"
                )
                return False
                
            if monthly_loss_pct >= self.loss_limits['monthly']:
                self._trading_halted = True
                self._halt_reason = f"Monthly loss limit exceeded: {monthly_loss_pct:.2%} >= {self.loss_limits['monthly']:.2%}"
                context_logger.log_event(
                    TradingEventType.RISK_EVALUATION,
                    "Trading halted - monthly loss limit exceeded",
                    context_provider={
                        "monthly_loss_percent": float(monthly_loss_pct),
                        "monthly_loss_limit": float(self.loss_limits['monthly']),
                        "exceeds_by_percent": float(monthly_loss_pct - self.loss_limits['monthly']),
                        "monthly_pnl_amount": float(monthly_pnl),
                        "halt_reason": self._halt_reason
                    },
                    decision_reason="TRADING_HALTED_MONTHLY_LOSS"
                )
                return False
            
            # All checks passed - trading allowed
            self._trading_halted = False
            self._halt_reason = ""
            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Trading allowed - all loss limits within bounds",
                context_provider={
                    "daily_loss_percent": float(daily_loss_pct),
                    "weekly_loss_percent": float(weekly_loss_pct),
                    "monthly_loss_percent": float(monthly_loss_pct),
                    "daily_loss_limit": float(self.loss_limits['daily']),
                    "weekly_loss_limit": float(self.loss_limits['weekly']),
                    "monthly_loss_limit": float(self.loss_limits['monthly']),
                    "margin_to_limits": {
                        "daily": float(self.loss_limits['daily'] - daily_loss_pct),
                        "weekly": float(self.loss_limits['weekly'] - weekly_loss_pct),
                        "monthly": float(self.loss_limits['monthly'] - monthly_loss_pct)
                    },
                    "total_equity": float(total_equity)
                },
                decision_reason="TRADING_ALLOWED_LOSS_LIMITS_OK"
            )
            return True
            
        except Exception as e:
            # On error, halt trading for safety
            self._trading_halted = True
            self._halt_reason = f"Risk system error: {str(e)}"
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Trading halted due to risk system error",
                context_provider={
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "halt_reason": self._halt_reason,
                    "safety_action": "halt_trading"
                },
                decision_reason="TRADING_HALTED_RISK_SYSTEM_ERROR"
            )
            return False
    
    def record_trade_outcome(self, executed_order: ActiveOrder, pnl: float):
        """
        Record trade outcome for loss tracking. Called when position is closed.
        """
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Recording trade outcome",
            symbol=executed_order.symbol,
            context_provider={
                "order_id": executed_order.db_id,
                "pnl_amount": pnl,
                "account_number": getattr(executed_order, 'account_number', 'unknown'),
                "position_closed": True
            },
            decision_reason="TRADE_OUTCOME_RECORDING_STARTED"
        )
            
        try:
            # Use account-specific P&L recording if available
            if hasattr(executed_order, 'account_number'):
                self.persistence.record_realized_pnl(
                    order_id=executed_order.db_id,
                    symbol=executed_order.symbol,
                    pnl=Decimal(str(pnl)),
                    exit_date=datetime.now(),
                    account_number=executed_order.account_number
                )
                account_context = {"account_number": executed_order.account_number}
            else:
                # Fallback for backward compatibility
                self.persistence.record_realized_pnl(
                    order_id=executed_order.db_id,
                    symbol=executed_order.symbol,
                    pnl=Decimal(str(pnl)),
                    exit_date=datetime.now()
                )
                account_context = {"account_number": "default"}
                
            context_logger.log_event(
                TradingEventType.RISK_EVALUATION,
                "Trade outcome successfully recorded",
                symbol=executed_order.symbol,
                context_provider={
                    "order_id": executed_order.db_id,
                    "pnl_amount": pnl,
                    "recording_successful": True,
                    **account_context
                },
                decision_reason="TRADE_OUTCOME_RECORDED"
            )
        except Exception as e:
            # Log error but don't break execution
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Failed to record trade outcome",
                symbol=executed_order.symbol,
                context_provider={
                    "order_id": executed_order.db_id,
                    "error_type": type(e).__name__,
                    "error_details": str(e),
                    "pnl_amount": pnl,
                    "recording_failed": True
                },
                decision_reason="TRADE_OUTCOME_RECORDING_FAILED"
            )
    
    def get_risk_status(self) -> Dict:
        """Get current risk status for monitoring and reporting."""
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Getting risk status",
            context_provider={
                "cache_used": self._last_trading_halt_check is not None,
                "last_check_time": self._last_trading_halt_check.isoformat() if self._last_trading_halt_check else None
            }
        )
            
        total_equity = self._get_total_equity()
        
        status = {
            'trading_halted': self._trading_halted,
            'halt_reason': self._halt_reason,
            'total_equity': float(total_equity),
            'daily_pnl': float(self.persistence.get_realized_pnl_period(days=1)),
            'weekly_pnl': float(self.persistence.get_realized_pnl_period(days=7)),
            'monthly_pnl': float(self.persistence.get_realized_pnl_period(days=30)),
            'last_check': self._last_trading_halt_check
        }
        
        context_logger.log_event(
            TradingEventType.RISK_EVALUATION,
            "Risk status retrieved",
            context_provider=status,
            decision_reason="RISK_STATUS_RETRIEVED"
        )
        return status
    
    def force_risk_check(self):
        """Force immediate risk check (e.g., after manual intervention)."""
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Forcing immediate risk check",
            context_provider={
                "previous_check_time": self._last_trading_halt_check.isoformat() if self._last_trading_halt_check else None,
                "cache_reset": True
            },
            decision_reason="FORCED_RISK_CHECK_TRIGGERED"
        )
        self._last_trading_halt_check = None
        self._check_trading_halts()

# TradingHaltedError - Begin (UPDATED)
class TradingHaltedError(Exception):
    """Exception raised when trading is halted due to risk rules or account value retrieval failures."""
    pass
# TradingHaltedError - End
