"""
Risk Management Service for trading automation system.
Handles position sizing constraints and loss-based trading halts.
Uses realized P&L from closed trades for risk calculations.
"""
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from decimal import Decimal

from src.core.planned_order import PlannedOrder, PositionStrategy, ActiveOrder
from src.services.state_service import StateService
from src.services.order_persistence_service import OrderPersistenceService
import logging

logger = logging.getLogger(__name__)

class RiskManagementService:
    """Manages trading risk parameters and validations."""
    
    def __init__(self, state_service: StateService, 
                    persistence_service: OrderPersistenceService,
                    ibkr_client=None,
                    config: Optional[Dict] = None):
        """Initialize with optional risk configuration."""
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
        
        logger.info(f"RiskManagementService configured from config: {self.loss_limits}, "
                   f"max_open_orders: {self.position_limits['max_open_orders']}, "
                   f"max_risk_per_trade: {self.max_risk_per_trade}")
        # Load risk configuration from config only - End

    def _get_total_equity(self) -> Decimal:
        """Get total account equity, using configurable default for simulation."""
        if self.ibkr_client and self.ibkr_client.connected:
            try:
                return Decimal(str(self.ibkr_client.get_account_value()))
            except Exception:
                # Fall back to state service if IBKR fails
                pass
                    
        # Use configurable default equity
        return self.simulation_equity

    def get_account_equity(self) -> Decimal:
        """Get current account equity from IBKR or use simulation value."""
        if self.ibkr_client and self.ibkr_client.is_connected():
            try:
                return Decimal(str(self.ibkr_client.get_account_value()))
            except (ValueError, AttributeError) as e:
                logger.warning(f"Failed to get account equity from IBKR: {e}")
                
        # Fallback to simulation equity from config
        return self.simulation_equity
    
    # P&L Calculation Methods - Begin
    def calculate_position_pnl(self, entry_price: float, exit_price: float, 
                             quantity: float, action: str) -> float:
        """
        Calculate realized P&L for a closed position.
        
        Args:
            entry_price: Price at which position was entered
            exit_price: Price at which position was exited  
            quantity: Number of shares/contracts
            action: Trade action ('BUY' or 'SELL')
            
        Returns:
            float: Calculated profit/loss amount
            
        Raises:
            ValueError: If parameters are invalid or missing
            TypeError: If parameter types are incorrect
        """
        self._validate_pnl_parameters(entry_price, exit_price, quantity, action)
        
        if action == 'BUY':
            # Long position: profit = (exit - entry) * quantity
            return (exit_price - entry_price) * quantity
        else:
            # Short position: profit = (entry - exit) * quantity  
            return (entry_price - exit_price) * quantity
    
    def _validate_pnl_parameters(self, entry_price: float, exit_price: float,
                               quantity: float, action: str) -> None:
        """Validate P&L calculation parameters and raise exceptions on errors."""
        if entry_price is None:
            raise ValueError("Entry price cannot be None for P&L calculation")
        if exit_price is None:
            raise ValueError("Exit price cannot be None for P&L calculation")
        if quantity is None:
            raise ValueError("Quantity cannot be None for P&L calculation")
        if action is None:
            raise ValueError("Action cannot be None for P&L calculation")
            
        if not isinstance(entry_price, (int, float, Decimal)):
            raise TypeError(f"Entry price must be numeric, got {type(entry_price)}")
        if not isinstance(exit_price, (int, float, Decimal)):
            raise TypeError(f"Exit price must be numeric, got {type(exit_price)}")
        if not isinstance(quantity, (int, float, Decimal)):
            raise TypeError(f"Quantity must be numeric, got {type(quantity)}")
        if not isinstance(action, str):
            raise TypeError(f"Action must be string, got {type(action)}")
            
        if entry_price <= 0:
            raise ValueError(f"Entry price must be positive, got {entry_price}")
        if exit_price <= 0:
            raise ValueError(f"Exit price must be positive, got {exit_price}")
        if quantity <= 0:
            raise ValueError(f"Quantity must be positive, got {quantity}")
        if action.upper() not in ['BUY', 'SELL']:
            raise ValueError(f"Action must be 'BUY' or 'SELL', got {action}")
    # P&L Calculation Methods - End
    
    # Risk Capping Implementation - Begin
    def _cap_risk_to_max_limit(self, order: PlannedOrder) -> None:
        """
        Cap the order's risk_per_trade to the maximum allowed value if it exceeds the limit.
        Logs a warning when capping occurs.
        """
        if order.risk_per_trade is not None and order.risk_per_trade > self.max_risk_per_trade:
            original_risk = order.risk_per_trade
            order.risk_per_trade = self.max_risk_per_trade
            logger.warning(f"Risk per trade capped: {original_risk:.3%} -> {self.max_risk_per_trade:.3%} "
                          f"for order {order.symbol} {order.action.value}")
    # Risk Capping Implementation - End
    
    def can_place_order(self, order: PlannedOrder, 
                       active_orders: Dict[int, ActiveOrder], 
                       total_capital: float) -> bool:
        """
        Hard block check for all risk rules. Returns False if any risk rule is violated.
        """
        # 1. Check trading halts first (highest priority - loss limits)
        if not self._check_trading_halts():
            return False
        
        # 2. Cap risk_per_trade to maximum allowed instead of rejecting
        self._cap_risk_to_max_limit(order)
            
        # 3. Check position sizing for CORE/HYBRID strategies
        if not self._validate_position_limits(order, active_orders, total_capital):
            return False
            
        return True
    
    def _validate_position_limits(self, order: PlannedOrder, 
                                active_orders: Dict[int, ActiveOrder], 
                                total_capital: float) -> bool:
        """
        Check 30% per trade and 60% total exposure for CORE/HYBRID strategies.
        Returns True if order complies with position limits.
        """
        if order.position_strategy not in self.position_limits:
            return True  # No limits for other strategies
            
        limits = self.position_limits[order.position_strategy]
        
        try:
            # Calculate proposed position size
            quantity = order.calculate_quantity(total_capital)
            position_value = order.entry_price * quantity
            
            # Check single trade limit (30%)
            if position_value > total_capital * limits['single_trade']:
                return False
                
            # Check total exposure limit (60%) for CORE/HYBRID strategies
            total_exposure = self._calculate_total_exposure(active_orders, total_capital)
            total_exposure += position_value
            
            return total_exposure <= total_capital * limits['total_exposure']
            
        except (ValueError, TypeError):
            # If calculation fails (e.g., missing entry price), reject order
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
            return not self._trading_halted
            
        self._last_trading_halt_check = current_time
        
        try:
            total_equity = self._get_total_equity()
            if total_equity <= 0:
                self._trading_halted = True
                self._halt_reason = "Zero or negative equity"
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
                return False
                
            if weekly_loss_pct >= self.loss_limits['weekly']:
                self._trading_halted = True
                self._halt_reason = f"Weekly loss limit exceeded: {weekly_loss_pct:.2%} >= {self.loss_limits['weekly']:.2%}"
                return False
                
            if monthly_loss_pct >= self.loss_limits['monthly']:
                self._trading_halted = True
                self._halt_reason = f"Monthly loss limit exceeded: {monthly_loss_pct:.2%} >= {self.loss_limits['monthly']:.2%}"
                return False
            
            # All checks passed - trading allowed
            self._trading_halted = False
            self._halt_reason = ""
            return True
            
        except Exception as e:
            # On error, halt trading for safety
            self._trading_halted = True
            self._halt_reason = f"Risk system error: {str(e)}"
            return False
    
    def _get_total_equity(self) -> Decimal:
        """Get total account equity, preferring live data if available."""
        if self.ibkr_client and self.ibkr_client.connected:
            try:
                return Decimal(str(self.ibkr_client.get_account_value()))
            except Exception:
                # Fall back to state service if IBKR fails
                pass
                
        # Use simulation equity from config
        return self.simulation_equity
    
    def record_trade_outcome(self, executed_order: ActiveOrder, pnl: float):
        """
        Record trade outcome for loss tracking. Called when position is closed.
        """
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
            else:
                # Fallback for backward compatibility
                self.persistence.record_realized_pnl(
                    order_id=executed_order.db_id,
                    symbol=executed_order.symbol,
                    pnl=Decimal(str(pnl)),
                    exit_date=datetime.now()
                )
        except Exception as e:
            # Log error but don't break execution
            logger.warning(f"Failed to record P&L for {executed_order.symbol}: {e}")
    
    def get_risk_status(self) -> Dict:
        """Get current risk status for monitoring and reporting."""
        total_equity = self._get_total_equity()
        
        return {
            'trading_halted': self._trading_halted,
            'halt_reason': self._halt_reason,
            'total_equity': float(total_equity),
            'daily_pnl': float(self.persistence.get_realized_pnl_period(days=1)),
            'weekly_pnl': float(self.persistence.get_realized_pnl_period(days=7)),
            'monthly_pnl': float(self.persistence.get_realized_pnl_period(days=30)),
            'last_check': self._last_trading_halt_check
        }
    
    def force_risk_check(self):
        """Force immediate risk check (e.g., after manual intervention)."""
        self._last_trading_halt_check = None
        self._check_trading_halts()


class TradingHaltedError(Exception):
    """Exception raised when trading is halted due to risk rules."""
    pass