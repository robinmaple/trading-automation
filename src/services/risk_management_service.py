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

# Minimal safe logging import
from src.core.simple_logger import get_simple_logger
logger = get_simple_logger(__name__)


class RiskManagementService:
    """Manages trading risk parameters and validations."""
    
    def __init__(self, state_service: StateService, 
                    persistence_service: OrderPersistenceService,
                    ibkr_client=None,
                    config: Optional[Dict] = None):
        """Initialize with optional risk configuration."""
        if logger:
            logger.debug("Initializing RiskManagementService")
            
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
        
        if logger:
            logger.info(f"RiskManagementService configured: daily_loss={self.loss_limits['daily']:.2%}, "
                       f"weekly_loss={self.loss_limits['weekly']:.2%}, monthly_loss={self.loss_limits['monthly']:.2%}, "
                       f"max_open_orders={self.position_limits['max_open_orders']}, "
                       f"max_risk_per_trade={self.max_risk_per_trade:.2%}")
        # Load risk configuration from config only - End

    def _get_total_equity(self) -> Decimal:
        """Get total account equity, using configurable default for simulation."""
        if self.ibkr_client and self.ibkr_client.connected:
            try:
                equity = Decimal(str(self.ibkr_client.get_account_value()))
                if logger:
                    logger.debug(f"Retrieved live equity from IBKR: ${equity:,.2f}")
                return equity
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to get equity from IBKR, using simulation: {e}")
                # Fall back to state service if IBKR fails
                pass
                    
        # Use configurable default equity
        if logger:
            logger.debug(f"Using simulation equity: ${self.simulation_equity:,.2f}")
        return self.simulation_equity

    def get_account_equity(self) -> Decimal:
        """Get current account equity from IBKR or use simulation value."""
        if logger:
            logger.debug("Getting account equity")
            
        if self.ibkr_client and self.ibkr_client.is_connected():
            try:
                equity = Decimal(str(self.ibkr_client.get_account_value()))
                if logger:
                    logger.debug(f"Retrieved account equity from IBKR: ${equity:,.2f}")
                return equity
            except (ValueError, AttributeError) as e:
                if logger:
                    logger.warning(f"Failed to get account equity from IBKR: {e}")
                
        # Fallback to simulation equity from config
        if logger:
            logger.debug(f"Using simulation equity: ${self.simulation_equity:,.2f}")
        return self.simulation_equity
    
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
        # Log safely without formatting None values
        if logger:
            safe_entry = entry_price if entry_price is not None else 'None'
            safe_exit = exit_price if exit_price is not None else 'None'
            safe_quantity = quantity if quantity is not None else 'None'
            safe_action = action if action is not None else 'None'
            logger.debug(f"Calculating P&L: {safe_action} {safe_quantity} shares, entry=${safe_entry}, exit=${safe_exit}")
            
        self._validate_pnl_parameters(entry_price, exit_price, quantity, action)
        
        if action == 'BUY':
            # Long position: profit = (exit - entry) * quantity
            pnl = (exit_price - entry_price) * quantity
        else:
            # Short position: profit = (entry - exit) * quantity  
            pnl = (entry_price - exit_price) * quantity
            
        if logger:
            logger.debug(f"Calculated P&L: ${pnl:,.2f}")
        return pnl

    def _validate_pnl_parameters(self, entry_price: float, exit_price: float,
                            quantity: float, action: str) -> None:
        """Validate P&L calculation parameters and raise exceptions on errors."""
        if entry_price is None:
            error_msg = "Entry price cannot be None for P&L calculation"
            if logger:
                logger.error(error_msg)
            raise ValueError(error_msg)
        if exit_price is None:
            error_msg = "Exit price cannot be None for P&L calculation"
            if logger:
                logger.error(error_msg)
            raise ValueError(error_msg)
        if quantity is None:
            error_msg = "Quantity cannot be None for P&L calculation"
            if logger:
                logger.error(error_msg)
            raise ValueError(error_msg)
        if action is None:
            error_msg = "Action cannot be None for P&L calculation"
            if logger:
                logger.error(error_msg)
            raise ValueError(error_msg)
            
        if not isinstance(entry_price, (int, float, Decimal)):
            error_msg = f"Entry price must be numeric, got {type(entry_price)}"
            if logger:
                logger.error(error_msg)
            raise TypeError(error_msg)
        if not isinstance(exit_price, (int, float, Decimal)):
            error_msg = f"Exit price must be numeric, got {type(exit_price)}"
            if logger:
                logger.error(error_msg)
            raise TypeError(error_msg)
        if not isinstance(quantity, (int, float, Decimal)):
            error_msg = f"Quantity must be numeric, got {type(quantity)}"
            if logger:
                logger.error(error_msg)
            raise TypeError(error_msg)
        if not isinstance(action, str):
            error_msg = f"Action must be string, got {type(action)}"
            if logger:
                logger.error(error_msg)
            raise TypeError(error_msg)
            
        if entry_price <= 0:
            error_msg = f"Entry price must be positive, got {entry_price}"
            if logger:
                logger.error(error_msg)
            raise ValueError(error_msg)
        if exit_price <= 0:
            error_msg = f"Exit price must be positive, got {exit_price}"
            if logger:
                logger.error(error_msg)
            raise ValueError(error_msg)
        if quantity <= 0:
            error_msg = f"Quantity must be positive, got {quantity}"
            if logger:
                logger.error(error_msg)
            raise ValueError(error_msg)
        if action.upper() not in ['BUY', 'SELL']:
            error_msg = f"Action must be 'BUY' or 'SELL', got {action}"
            if logger:
                logger.error(error_msg)
            raise ValueError(error_msg)
        
    # Risk Capping Implementation - Begin
    def _cap_risk_to_max_limit(self, order: PlannedOrder) -> None:
        """
        Cap the order's risk_per_trade to the maximum allowed value if it exceeds the limit.
        Logs a warning when capping occurs.
        """
        if order.risk_per_trade is not None and order.risk_per_trade > self.max_risk_per_trade:
            original_risk = order.risk_per_trade
            order.risk_per_trade = self.max_risk_per_trade
            if logger:
                logger.warning(f"Risk per trade capped: {original_risk:.3%} -> {self.max_risk_per_trade:.3%} "
                              f"for order {order.symbol} {order.action.value}")
    # Risk Capping Implementation - End
    
    def can_place_order(self, order: PlannedOrder, 
                       active_orders: Dict[int, ActiveOrder], 
                       total_capital: float) -> bool:
        """
        Hard block check for all risk rules. Returns False if any risk rule is violated.
        """
        if logger:
            logger.debug(f"Checking if order can be placed: {order.symbol} {order.action.value}")
            
        # 1. Check trading halts first (highest priority - loss limits)
        if not self._check_trading_halts():
            if logger:
                logger.warning(f"Order {order.symbol} rejected: trading halted - {self._halt_reason}")
            return False
        
        # 2. Cap risk_per_trade to maximum allowed instead of rejecting
        self._cap_risk_to_max_limit(order)
            
        # 3. Check position sizing for CORE/HYBRID strategies
        if not self._validate_position_limits(order, active_orders, total_capital):
            if logger:
                logger.warning(f"Order {order.symbol} rejected: position limit violation")
            return False
            
        if logger:
            logger.debug(f"Order {order.symbol} passed all risk checks")
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
                if logger:
                    logger.warning(f"Position limit violation: {position_value:,.2f} > {total_capital * limits['single_trade']:,.2f}")
                return False
                
            # Check total exposure limit (60%) for CORE/HYBRID strategies
            total_exposure = self._calculate_total_exposure(active_orders, total_capital)
            total_exposure += position_value
            
            if total_exposure > total_capital * limits['total_exposure']:
                if logger:
                    logger.warning(f"Total exposure limit violation: {total_exposure:,.2f} > {total_capital * limits['total_exposure']:,.2f}")
                return False
                
            if logger:
                logger.debug(f"Position limits passed: {position_value:,.2f} <= {total_capital * limits['single_trade']:,.2f}, "
                           f"total exposure {total_exposure:,.2f} <= {total_capital * limits['total_exposure']:,.2f}")
            return True
            
        except (ValueError, TypeError) as e:
            # If calculation fails (e.g., missing entry price), reject order
            if logger:
                logger.error(f"Position limit calculation failed for {order.symbol}: {e}")
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
        
        if logger:
            logger.debug(f"Total exposure calculation: {total_exposure:,.2f}")
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
            if logger:
                logger.debug(f"Using cached trading halt status: {self._trading_halted}")
            return not self._trading_halted
            
        self._last_trading_halt_check = current_time
        
        try:
            total_equity = self._get_total_equity()
            if total_equity <= 0:
                self._trading_halted = True
                self._halt_reason = "Zero or negative equity"
                if logger:
                    logger.error(f"Trading halted: {self._halt_reason}")
                return False
            
            # Get realized P&L for different time periods
            daily_pnl = self.persistence.get_realized_pnl_period(days=1)
            weekly_pnl = self.persistence.get_realized_pnl_period(days=7)
            monthly_pnl = self.persistence.get_realized_pnl_period(days=30)
            
            # Convert to percentage loss (only consider losses)
            daily_loss_pct = abs(min(daily_pnl, Decimal('0'))) / total_equity
            weekly_loss_pct = abs(min(weekly_pnl, Decimal('0'))) / total_equity  
            monthly_loss_pct = abs(min(monthly_pnl, Decimal('0'))) / total_equity
            
            if logger:
                logger.debug(f"Loss percentages - Daily: {daily_loss_pct:.4%}, Weekly: {weekly_loss_pct:.4%}, Monthly: {monthly_loss_pct:.4%}")
            
            # Check against limits from config (not database)
            if daily_loss_pct >= self.loss_limits['daily']:
                self._trading_halted = True
                self._halt_reason = f"Daily loss limit exceeded: {daily_loss_pct:.2%} >= {self.loss_limits['daily']:.2%}"
                if logger:
                    logger.warning(f"Trading halted: {self._halt_reason}")
                return False
                
            if weekly_loss_pct >= self.loss_limits['weekly']:
                self._trading_halted = True
                self._halt_reason = f"Weekly loss limit exceeded: {weekly_loss_pct:.2%} >= {self.loss_limits['weekly']:.2%}"
                if logger:
                    logger.warning(f"Trading halted: {self._halt_reason}")
                return False
                
            if monthly_loss_pct >= self.loss_limits['monthly']:
                self._trading_halted = True
                self._halt_reason = f"Monthly loss limit exceeded: {monthly_loss_pct:.2%} >= {self.loss_limits['monthly']:.2%}"
                if logger:
                    logger.warning(f"Trading halted: {self._halt_reason}")
                return False
            
            # All checks passed - trading allowed
            self._trading_halted = False
            self._halt_reason = ""
            if logger:
                logger.debug("Trading allowed: all loss limits within bounds")
            return True
            
        except Exception as e:
            # On error, halt trading for safety
            self._trading_halted = True
            self._halt_reason = f"Risk system error: {str(e)}"
            if logger:
                logger.error(f"Trading halted due to risk system error: {e}")
            return False
    
    def record_trade_outcome(self, executed_order: ActiveOrder, pnl: float):
        """
        Record trade outcome for loss tracking. Called when position is closed.
        """
        if logger:
            logger.info(f"Recording trade outcome: {executed_order.symbol}, P&L: ${pnl:,.2f}")
            
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
            if logger:
                logger.debug(f"Successfully recorded P&L for {executed_order.symbol}")
        except Exception as e:
            # Log error but don't break execution
            if logger:
                logger.warning(f"Failed to record P&L for {executed_order.symbol}: {e}")
    
    def get_risk_status(self) -> Dict:
        """Get current risk status for monitoring and reporting."""
        if logger:
            logger.debug("Getting risk status")
            
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
        
        if logger:
            logger.debug(f"Risk status: {status}")
        return status
    
    def force_risk_check(self):
        """Force immediate risk check (e.g., after manual intervention)."""
        if logger:
            logger.info("Forcing immediate risk check")
        self._last_trading_halt_check = None
        self._check_trading_halts()


class TradingHaltedError(Exception):
    """Exception raised when trading is halted due to risk rules."""
    pass