"""
Price adjustment and validation service for order execution.
"""

import math
from typing import Optional
from src.core.context_aware_logger import get_context_logger, TradingEventType


class PriceAdjustmentService:
    """Handles price validation, rounding, and adjustment logic."""
    
    def __init__(self, trading_manager):
        self.context_logger = get_context_logger()
        self._trading_manager = trading_manager

    def _validate_and_round_price(self, price: float, security_type: str, symbol: str = 'UNKNOWN', 
                                is_profit_target: bool = False) -> float:
        """
        Validate and round prices to conform to IBKR minimum price variation rules.
        For profit targets, round UP to the next valid price increment for better R/R.
        
        Args:
            price: Original price to validate
            security_type: Security type (STK, OPT, etc.)
            symbol: Symbol for logging
            is_profit_target: Whether this is a profit target (round UP if True)
            
        Returns:
            float: Rounded price that conforms to IBKR rules
        """
        try:
            if security_type.upper() == "STK":
                # Determine the appropriate price increment based on price tier
                if price < 1.0:
                    increment = 0.0001  # Penny stocks: $0.0001 increments
                elif price < 10.0:
                    increment = 0.005   # Low-price stocks: $0.005 increments  
                else:
                    increment = 0.01    # Regular stocks: $0.01 increments
                
                # For profit targets, round UP to the next valid increment for better R/R
                if is_profit_target:
                    # Round UP to the next valid increment
                    rounded_price = math.ceil(price / increment) * increment
                    rounding_direction = "UP"
                    improvement = rounded_price - price
                else:
                    # For entry and stop prices, use normal rounding
                    rounded_price = round(price / increment) * increment
                    rounding_direction = "NEAREST"
                    improvement = 0
                
                # Log the rounding operation if significant
                if abs(rounded_price - price) > 0.0001:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Price rounded {rounding_direction} for IBKR compliance",
                        symbol=symbol,
                        context_provider={
                            'original_price': price,
                            'rounded_price': rounded_price,
                            'security_type': security_type,
                            'price_increment': increment,
                            'rounding_direction': rounding_direction,
                            'is_profit_target': is_profit_target,
                            'improvement': improvement,
                            'price_tier': 'PENNY' if price < 1.0 else 'LOW' if price < 10.0 else 'REGULAR'
                        },
                        decision_reason=f"Price rounded {rounding_direction} from {price:.4f} to {rounded_price:.4f} for IBKR compliance"
                    )
                    print(f"🔧 PRICE ROUNDING {rounding_direction}: {symbol} - {price:.4f} → {rounded_price:.4f} (increment: {increment})")
                    
                return rounded_price
            else:
                # For other security types, use original rounding logic
                return round(price, 5)
                
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Price rounding error",
                symbol=symbol,
                context_provider={
                    'original_price': price,
                    'security_type': security_type,
                    'is_profit_target': is_profit_target,
                    'error': str(e)
                }
            )
            # Fallback to safe rounding
            return round(price, 2)

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