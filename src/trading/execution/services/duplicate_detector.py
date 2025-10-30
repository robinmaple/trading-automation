"""
Duplicate order detection service to prevent duplicate order placement.
"""

from typing import Dict, Any, List, Optional
from src.core.context_aware_logger import get_context_logger, TradingEventType


class DuplicateDetector:
    """Detects and prevents duplicate order placement."""
    
    def __init__(self, ibkr_client):
        self.context_logger = get_context_logger()
        self._ibkr_client = ibkr_client

    def _get_order_key_parameters(self, order) -> Dict[str, Any]:
        """
        Extract key parameters from an order for duplicate detection.
        
        Args:
            order: PlannedOrder or IBKR order
            
        Returns:
            Dict with key parameters for comparison
        """
        try:
            # Extract symbol
            symbol = getattr(order, 'symbol', '') if hasattr(order, 'symbol') else getattr(getattr(order, 'contract', None), 'symbol', '')
            
            # Extract action
            action = None
            if hasattr(order, 'action'):
                action_val = getattr(order.action, "value", None) or getattr(order.action, "name", None) or str(order.action)
                action = str(action_val).upper().strip()
            else:
                action = getattr(order, 'action', '').upper()
            
            # Extract prices and quantity
            entry_price = getattr(order, 'entry_price', None) or getattr(order, 'lmtPrice', None)
            stop_loss = getattr(order, 'stop_loss', None) or getattr(order, 'auxPrice', None)
            quantity = getattr(order, 'totalQuantity', None)
            
            # For planned orders, get additional bracket parameters
            risk_reward_ratio = getattr(order, 'risk_reward_ratio', None)
            risk_per_trade = getattr(order, 'risk_per_trade', None)
            
            return {
                'symbol': symbol,
                'action': action,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'quantity': quantity,
                'risk_reward_ratio': risk_reward_ratio,
                'risk_per_trade': risk_per_trade,
                'order_type': getattr(order, 'orderType', '') if hasattr(order, 'orderType') else getattr(getattr(order, 'order_type', None), 'value', '')
            }
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error extracting order key parameters",
                context_provider={'error': str(e)}
            )
            return {}

    def _detect_partial_bracket_duplicates(self, planned_order, open_orders, account_number: Optional[str] = None) -> bool:
        """
        Detect if there are partial bracket orders that would make this new order a duplicate.
        
        Args:
            planned_order: New planned order to check
            open_orders: List of open orders from IBKR
            account_number: Account number for logging
            
        Returns:
            bool: True if partial bracket duplicate detected, False otherwise
        """
        try:
            planned_params = self._get_order_key_parameters(planned_order)
            planned_symbol = planned_params.get('symbol', '')
            
            if not planned_symbol:
                return False
                
            # Filter orders for the same symbol
            symbol_orders = [o for o in open_orders if getattr(o.contract, 'symbol', '') == planned_symbol]
            
            if not symbol_orders:
                return False
                
            # Look for bracket components (orders with parentId or orders that could be bracket parts)
            bracket_candidates = []
            for open_order in symbol_orders:
                order_params = self._get_order_key_parameters(open_order)
                
                # Check if this open order could be part of a bracket similar to planned order
                if self._orders_match_core_parameters(planned_params, order_params):
                    bracket_candidates.append({
                        'order': open_order,
                        'params': order_params,
                        'parent_id': getattr(open_order, 'parentId', None)
                    })
            
            # Analyze bracket candidates
            if len(bracket_candidates) >= 2:  # At least entry + one child order
                # Check if we have a potential partial bracket
                parent_orders = [c for c in bracket_candidates if c['parent_id'] is None or c['parent_id'] == 0]
                child_orders = [c for c in bracket_candidates if c['parent_id'] is not None and c['parent_id'] != 0]
                
                # If we have a parent order and at least one child, it's likely a partial bracket
                if parent_orders and child_orders:
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "Partial bracket duplicate detected",
                        symbol=planned_symbol,
                        context_provider={
                            "parent_orders_count": len(parent_orders),
                            "child_orders_count": len(child_orders),
                            "planned_action": planned_params.get('action', ''),
                            "planned_entry_price": planned_params.get('entry_price'),
                            "planned_quantity": planned_params.get('quantity'),
                            "account_number": account_number,
                            "duplicate_type": "partial_bracket"
                        },
                        decision_reason="Partial bracket exists - preventing duplicate order"
                    )
                    return True
                    
            return False
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error detecting partial bracket duplicates",
                symbol=getattr(planned_order, 'symbol', 'UNKNOWN'),
                context_provider={
                    "error": str(e),
                    "account_number": account_number
                }
            )
            return False

    def _orders_match_core_parameters(self, planned_params: Dict, open_order_params: Dict) -> bool:
        """
        Check if two orders match on core parameters for duplicate detection.
        
        Args:
            planned_params: Parameters from planned order
            open_order_params: Parameters from open order
            
        Returns:
            bool: True if orders match core parameters
        """
        try:
            # Symbol must match
            if planned_params.get('symbol') != open_order_params.get('symbol'):
                return False
                
            # Action must match
            if planned_params.get('action') != open_order_params.get('action'):
                return False
                
            # Quantity must be similar (within 5%)
            planned_qty = planned_params.get('quantity')
            open_qty = open_order_params.get('quantity')
            if planned_qty and open_qty:
                qty_ratio = abs(planned_qty - open_qty) / max(planned_qty, open_qty)
                if qty_ratio > 0.05:  # More than 5% quantity difference
                    return False
                    
            # Entry price must be similar (within 1%)
            planned_entry = planned_params.get('entry_price')
            open_entry = open_order_params.get('entry_price')
            if planned_entry and open_entry and planned_entry > 0 and open_entry > 0:
                price_ratio = abs(planned_entry - open_entry) / planned_entry
                if price_ratio > 0.01:  # More than 1% price difference
                    return False
                    
            # Stop loss must be similar (within 1%) if both exist
            planned_stop = planned_params.get('stop_loss')
            open_stop = open_order_params.get('stop_loss')
            if planned_stop and open_stop and planned_stop > 0 and open_stop > 0:
                stop_ratio = abs(planned_stop - open_stop) / planned_stop
                if stop_ratio > 0.01:  # More than 1% stop difference
                    return False
                    
            # If we get here, core parameters match
            return True
            
        except Exception as e:
            # On error, assume no match to be safe
            return False

    def _is_duplicate_order_active(self, order, account_number: Optional[str] = None) -> bool:
        """
        Enhanced duplicate order detection that checks for active bracket orders including partial brackets.
        
        Returns True if duplicate found, False if safe to proceed.
        """
        try:
            if not self._ibkr_client or not self._ibkr_client.connected:
                return False  # No IBKR connection, can't check for duplicates
                
            # Get open orders from IBKR for this symbol
            open_orders = self._ibkr_client.get_open_orders()
            symbol_orders = [o for o in open_orders if getattr(o.contract, 'symbol', '') == order.symbol]
            
            if not symbol_orders:
                return False  # No open orders for this symbol
                
            # Enhanced: Check for partial bracket duplicates
            if self._detect_partial_bracket_duplicates(order, symbol_orders, account_number):
                return True
                
            # Enhanced: Check for bracket order components
            bracket_parent_orders = [o for o in symbol_orders if getattr(o, 'parentId', 0) == 0]
            
            # If there's already a bracket parent order for this symbol, check if it's similar
            if bracket_parent_orders:
                planned_params = self._get_order_key_parameters(order)
                for parent_order in bracket_parent_orders:
                    parent_params = self._get_order_key_parameters(parent_order)
                    if self._orders_match_core_parameters(planned_params, parent_params):
                        self.context_logger.log_event(
                            TradingEventType.ORDER_VALIDATION,
                            "Duplicate bracket order detected - active bracket already exists",
                            symbol=order.symbol,
                            context_provider={
                                "existing_parent_order_id": parent_order.orderId,
                                "existing_order_status": getattr(parent_order, 'status', 'UNKNOWN'),
                                "new_action": order.action.value,
                                "existing_entry_price": parent_params.get('entry_price'),
                                "planned_entry_price": planned_params.get('entry_price'),
                                "account_number": account_number,
                                "duplicate_type": "complete_bracket"
                            },
                            decision_reason="DUPLICATE_BRACKET_ORDER_PREVENTION"
                        )
                        return True
                
            # Original similarity check as fallback
            for open_order in symbol_orders:
                if self._orders_are_similar(open_order, order, account_number):
                    self.context_logger.log_event(
                        TradingEventType.ORDER_VALIDATION,
                        "Duplicate order detected - similar order already active",
                        symbol=order.symbol,
                        context_provider={
                            "existing_order_id": open_order.orderId,
                            "existing_action": getattr(open_order, 'action', 'UNKNOWN'),
                            "existing_price": getattr(open_order, 'lmtPrice', getattr(open_order, 'auxPrice', 0)),
                            "new_action": order.action.value,
                            "new_price": order.entry_price,
                            "account_number": account_number,
                            "duplicate_type": "similar_order"
                        },
                        decision_reason="DUPLICATE_ORDER_PREVENTION"
                    )
                    return True
                    
            return False
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error checking for duplicate orders",
                symbol=order.symbol,
                context_provider={
                    "error": str(e),
                    "account_number": account_number
                }
            )
            # On error, assume no duplicates to avoid blocking execution
            return False

    def _orders_are_similar(self, ibkr_order, planned_order, account_number: Optional[str] = None) -> bool:
        """
        Determine if two orders are similar enough to be considered duplicates.
        Enhanced with core parameter matching.
        """
        try:
            # Use the new parameter matching system
            planned_params = self._get_order_key_parameters(planned_order)
            ibkr_params = self._get_order_key_parameters(ibkr_order)
            
            return self._orders_match_core_parameters(planned_params, ibkr_params)
            
        except Exception as e:
            self.context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Error comparing orders for similarity",
                context_provider={
                    "error": str(e),
                    "symbol": planned_order.symbol
                }
            )
            return False