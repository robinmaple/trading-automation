"""
Execution validation service handling all pre-execution validation checks.
"""

import math
from typing import Tuple, Optional, Any, Dict
from decimal import Decimal

from src.core.context_aware_logger import get_context_logger, TradingEventType


class ExecutionValidator:
    """Handles all order validation logic before execution."""
    
    def __init__(self, trading_manager, ibkr_client, order_persistence):
        self.context_logger = get_context_logger()
        self._trading_manager = trading_manager
        self._ibkr_client = ibkr_client
        self._order_persistence = order_persistence

    def _validate_order_basic(self, order) -> Tuple[bool, str]:
        """Layer 3a: Basic field validation as safety net before execution."""
        try:
            # Symbol validation
            symbol_str = ""
            try:
                symbol_str = str(order.symbol).strip()
            except Exception:
                symbol_str = ""
            if not symbol_str or symbol_str in ['', '0', 'nan', 'None', 'null']:
                return False, f"Invalid symbol: '{order.symbol}'"

            # Price validation
            if not hasattr(order, "entry_price") or order.entry_price is None or order.entry_price <= 0:
                return False, f"Invalid entry price: {getattr(order, 'entry_price', None)}"

            # Stop loss validation (basic syntax)
            if getattr(order, "stop_loss", None) is not None and order.stop_loss <= 0:
                return False, f"Invalid stop loss price: {order.stop_loss}"

            # Action validation - accept enums or strings
            action_val = None
            try:
                action_val = getattr(order.action, "value", None) or getattr(order.action, "name", None)
            except Exception:
                action_val = None

            if action_val is None:
                try:
                    action_val = str(order.action)
                except Exception:
                    action_val = ""

            action_str = str(action_val).upper().strip()
            if action_str not in ("BUY", "SELL"):
                return False, f"Invalid action: {order.action}"

            return True, "Basic validation passed"

        except Exception as e:
            return False, f"Basic validation error: {e}"

    def _validate_market_data_available(self, order) -> Tuple[bool, str]:
        """Layer 3b: Validate market data availability - but don't block execution if unavailable."""
        try:
            if not hasattr(self._trading_manager, 'data_feed'):
                # Don't block execution - just warn
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Data feed not available - proceeding with execution",
                    symbol=order.symbol,
                    context_provider={},
                    decision_reason="Market data unavailable but order execution allowed"
                )
                return True, "Execution allowed without market data"
                
            if not self._trading_manager.data_feed.is_connected():
                # Don't block execution - just warn  
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "Data feed not connected - proceeding with execution",
                    symbol=order.symbol,
                    context_provider={},
                    decision_reason="Data feed disconnected but order execution allowed"
                )
                return True, "Execution allowed with disconnected data feed"
                
            current_price = self._trading_manager.data_feed.get_current_price(order.symbol)
            if current_price is None or current_price <= 0:
                # Don't block execution - just warn
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    "No market data available - proceeding with execution",
                    symbol=order.symbol,
                    context_provider={
                        "current_price": current_price
                    },
                    decision_reason="Market data unavailable but order execution allowed"
                )
                return True, "Execution allowed without current market data"
                
            return True, "Market data available"
            
        except Exception as e:
            # Don't block execution on validation errors
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Market data validation error - proceeding with execution",
                symbol=order.symbol,
                context_provider={
                    "error": str(e)
                },
                decision_reason="Market data validation failed but order execution allowed"
            )
            return True, "Execution allowed despite market data validation error"

    def _validate_broker_connection(self) -> Tuple[bool, str]:
        """Layer 3c: Validate broker connection status."""
        if self._ibkr_client and self._ibkr_client.connected:
            return True, "Broker connected"
        
        return False, "Broker not connected"

    def _validate_order_margin(self, order, quantity, total_capital) -> Tuple[bool, str]:
        """Validate if the order has sufficient margin before execution."""
        try:
            is_valid, message = self._order_persistence.validate_sufficient_margin(
                order.symbol, quantity, order.entry_price
            )
            if not is_valid:
                return False, message
            
            return True, "Margin validation passed"
        except Exception as e:
            return False, f"Margin validation error: {e}"

    def _validate_profit_target_parameters(self, order) -> Tuple[bool, str]:
        """Validate parameters specifically required for profit target calculation in bracket orders."""
        try:
            # Check if risk_reward_ratio is present and valid
            risk_reward_ratio = getattr(order, 'risk_reward_ratio', None)
            if risk_reward_ratio is None:
                return False, "Missing risk_reward_ratio parameter"
            
            if not isinstance(risk_reward_ratio, (int, float, Decimal)):
                return False, f"Invalid risk_reward_ratio type: {type(risk_reward_ratio)}"
                
            if risk_reward_ratio <= 0:
                return False, f"Invalid risk_reward_ratio value: {risk_reward_ratio}"
                
            # Check if entry_price and stop_loss are valid for profit target calculation
            entry_price = getattr(order, 'entry_price', None)
            stop_loss = getattr(order, 'stop_loss', None)
            
            if entry_price is None or entry_price <= 0:
                return False, f"Invalid entry_price for profit target: {entry_price}"
                
            if stop_loss is None or stop_loss <= 0:
                return False, f"Invalid stop_loss for profit target: {stop_loss}"
                
            # Check if entry_price and stop_loss are meaningfully different
            if abs(entry_price - stop_loss) / entry_price < 0.001:  # 0.1% tolerance
                return False, f"Entry price and stop loss too close: {entry_price} vs {stop_loss}"
                
            # Validate that profit target can be reasonably calculated
            try:
                # Test profit target calculation
                if order.action.value == "BUY":
                    test_profit_target = entry_price + (abs(entry_price - stop_loss) * risk_reward_ratio)
                else:
                    test_profit_target = entry_price - (abs(entry_price - stop_loss) * risk_reward_ratio)
                    
                if test_profit_target <= 0:
                    return False, f"Calculated profit target is invalid: {test_profit_target}"
                    
                if abs(test_profit_target - entry_price) / entry_price < 0.001:
                    return False, f"Profit target too close to entry price: {test_profit_target}"
                    
            except Exception as calc_error:
                return False, f"Profit target calculation test failed: {calc_error}"
                
            return True, "Profit target parameters validated successfully"
            
        except Exception as e:
            return False, f"Profit target parameter validation error: {e}"

    def _get_current_market_price_for_order(self, order) -> Optional[float]:
        """
        Get current market price for an order, supporting dynamic price adjustment decisions.
        
        Args:
            order: PlannedOrder to get market price for
            
        Returns:
            float or None: Current market price if available
        """
        try:
            # First try to get price from market data manager via trading manager
            if (hasattr(self._trading_manager, 'data_feed') and 
                self._trading_manager.data_feed and 
                hasattr(self._trading_manager.data_feed, 'get_current_price')):
                
                price_data = self._trading_manager.data_feed.get_current_price(order.symbol)
                if price_data and 'price' in price_data and price_data['price'] > 0:
                    return float(price_data['price'])
            
            # Fallback: Try market data manager directly if available
            if (hasattr(self._trading_manager, 'market_data_manager') and 
                self._trading_manager.market_data_manager and
                hasattr(self._trading_manager.market_data_manager, 'get_current_price')):
                
                price_data = self._trading_manager.market_data_manager.get_current_price(order.symbol)
                if price_data and 'price' in price_data and price_data['price'] > 0:
                    return float(price_data['price'])
                    
            # Final fallback: Check if monitoring service has price
            if (hasattr(self._trading_manager, 'monitoring_service') and 
                self._trading_manager.monitoring_service and
                hasattr(self._trading_manager.monitoring_service, 'get_current_price')):
                
                current_price = self._trading_manager.monitoring_service.get_current_price(order.symbol)
                if current_price and current_price > 0:
                    return float(current_price)
            
            return None
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Failed to get market price for order",
                symbol=order.symbol,
                context_provider={'error': str(e)}
            )
            return None

    def _validate_market_data_available_with_price(self, order) -> Tuple[bool, str, Optional[float]]:
        """Enhanced market data validation that returns current market price if available."""
        try:
            if not hasattr(self._trading_manager, 'data_feed'):
                current_price = self._get_current_market_price_for_order(order)
                return True, "Execution allowed without data feed", current_price
                
            if not self._trading_manager.data_feed.is_connected():
                current_price = self._get_current_market_price_for_order(order)
                return True, "Execution allowed with disconnected data feed", current_price
                
            current_price = self._get_current_market_price_for_order(order)
            if current_price is None or current_price <= 0:
                return True, "Execution allowed without current market data", None
                
            return True, "Market data available", current_price
            
        except Exception as e:
            current_price = self._get_current_market_price_for_order(order)
            return True, f"Execution allowed despite market data error: {e}", current_price

    def _validate_execution_conditions(self, order, quantity, total_capital) -> Tuple[bool, str]:
        """Layer 3: Comprehensive pre-execution validation with detailed logging including price adjustment support."""
        try:
            # Basic field validation (safety net)
            basic_valid, basic_message = self._validate_order_basic(order)
            if not basic_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Basic validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": basic_message,
                        "entry_price": getattr(order, 'entry_price', None),
                        "stop_loss": getattr(order, 'stop_loss', None),
                        "action": getattr(order, 'action', None)
                    },
                    decision_reason="Basic validation failed"
                )
                return False, f"Basic validation failed: {basic_message}"
                
            # Enhanced market data availability check for price adjustment
            market_valid, market_message, current_market_price = self._validate_market_data_available_with_price(order)
            if not market_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Market data validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": market_message,
                        "data_feed_connected": hasattr(self._trading_manager, 'data_feed') and self._trading_manager.data_feed.is_connected(),
                        "current_market_price_available": current_market_price is not None
                    },
                    decision_reason="Market data validation failed"
                )
                # For LIMIT orders, we need market price for potential adjustment
                if hasattr(order, 'order_type') and getattr(order, 'order_type') is not None:
                    order_type = getattr(order, 'order_type').value.upper()
                    if order_type == 'LMT' and current_market_price is None:
                        return False, f"Market data issue: {market_message} - price required for LIMIT order adjustment"
                
            # Check if price adjustment might be beneficial
            if (current_market_price and 
                hasattr(order, 'order_type') and 
                getattr(order, 'order_type') is not None and
                getattr(order, 'order_type').value.upper() == 'LMT'):
                
                price_diff_pct = abs(current_market_price - order.entry_price) / order.entry_price
                adjustment_possible = False
                
                if order.action.value.upper() == "BUY" and current_market_price < order.entry_price:
                    adjustment_possible = True
                elif order.action.value.upper() == "SELL" and current_market_price > order.entry_price:
                    adjustment_possible = True
                    
                if adjustment_possible and price_diff_pct >= 0.005:  # 0.5% threshold
                    self.context_logger.log_event(
                        TradingEventType.EXECUTION_DECISION,
                        "Price adjustment opportunity detected",
                        symbol=order.symbol,
                        context_provider={
                            "current_market_price": current_market_price,
                            "planned_entry_price": order.entry_price,
                            "price_difference_percent": price_diff_pct * 100,
                            "adjustment_threshold_met": True,
                            "potential_improvement": order.entry_price - current_market_price if order.action.value.upper() == "BUY" else current_market_price - order.entry_price
                        },
                        decision_reason=f"Market price favorable for {'BUY' if order.action.value.upper() == 'BUY' else 'SELL'} order adjustment"
                    )
                
            # Profit target specific parameters
            profit_target_valid, profit_target_message = self._validate_profit_target_parameters(order)
            if not profit_target_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Profit target validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": profit_target_message,
                        "risk_reward_ratio": getattr(order, 'risk_reward_ratio', None),
                        "entry_price": getattr(order, 'entry_price', None),
                        "stop_loss": getattr(order, 'stop_loss', None)
                    },
                    decision_reason="Profit target parameter validation failed"
                )
                return False, f"Profit target validation failed: {profit_target_message}"
                
            # Broker connection
            broker_valid, broker_message = self._validate_broker_connection()
            if not broker_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Broker validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": broker_message,
                        "ibkr_connected": self._ibkr_client and self._ibkr_client.connected
                    },
                    decision_reason="Broker validation failed"
                )
                return False, f"Broker issue: {broker_message}"
                
            # Margin validation (existing)
            margin_valid, margin_message = self._validate_order_margin(order, quantity, total_capital)
            if not margin_valid:
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Margin validation failed for {order.symbol}",
                    symbol=order.symbol,
                    context_provider={
                        "reason": margin_message,
                        "quantity": quantity,
                        "total_capital": total_capital
                    },
                    decision_reason="Margin validation failed"
                )
                return False, f"Margin validation failed: {margin_message}"
                
            # All validations passed
            self.context_logger.log_event(
                TradingEventType.ORDER_VALIDATION,
                f"All execution conditions met for {order.symbol} including price adjustment readiness",
                symbol=order.symbol,
                context_provider={
                    "quantity": quantity,
                    "total_capital": total_capital,
                    "risk_reward_ratio": getattr(order, 'risk_reward_ratio', None),
                    "profit_target_parameters_valid": True,
                    "current_market_price_available": current_market_price is not None,
                    "price_adjustment_supported": True
                },
                decision_reason="All execution validations passed including price adjustment readiness"
            )
            return True, "All execution conditions met including price adjustment readiness"
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                f"Execution validation error for {order.symbol}",
                symbol=order.symbol,
                context_provider={
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                decision_reason="Execution validation exception"
            )
            return False, f"Execution validation error: {e}"