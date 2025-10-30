"""
Resource allocation service for order prioritization.
Handles capital allocation, slot management, and order allocation logic.
"""

import signal
import time
from functools import wraps
from typing import List, Dict, Optional
from src.core.context_aware_logger import get_context_logger, TradingEventType


class ResourceAllocator:
    """Handles resource allocation for order prioritization."""
    
    def __init__(self, sizing_service, config, scoring_service, component_calculator):
        self.context_logger = get_context_logger()
        self.sizing_service = sizing_service
        self.config = config
        self.scoring_service = scoring_service
        self.component_calculator = component_calculator

    def _prioritize_orders_legacy(self, executable_orders: List[Dict], total_capital: float,
                                current_working_orders: Optional[List] = None) -> List[Dict]:
        """Legacy single-layer prioritization for backward compatibility."""
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Using legacy single-layer prioritization",
            context_provider={
                "executable_orders_count": len(executable_orders),
                "total_capital": total_capital
            }
        )
        # <Context-Aware Logging Integration - End>
            
        if not executable_orders:
            return []
        
        committed_capital = 0.0
        working_order_count = 0
        if current_working_orders:
            committed_capital = sum(order.get('capital_commitment', 0) 
                                for order in current_working_orders)
            working_order_count = len(current_working_orders)
        
        available_capital = total_capital * self.config['max_capital_utilization'] - committed_capital
        available_capital = max(0, available_capital)
        
        available_slots = self.config['max_open_orders'] - working_order_count
        available_slots = max(0, available_slots)
        
        # Calculate scores using legacy method
        scored_orders = []
        for order_data in executable_orders:
            order = order_data['order']
            safe_symbol = getattr(order, 'symbol', 'Unknown')
            fill_prob = order_data.get('fill_probability', 0)
            
            score_result = self.scoring_service.calculate_deterministic_score(order, fill_prob, total_capital)
            
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity if order.entry_price else 0
            
            scored_order = {
                **order_data,
                'deterministic_score': score_result['final_score'],
                'score_components': score_result['components'],
                'quantity': quantity,
                'capital_commitment': capital_commitment,
                'allocated': False,
                'allocation_reason': 'Pending allocation',
                'viable': True  # All orders are viable in legacy mode
            }
            scored_orders.append(scored_order)
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                f"Legacy scoring completed for {safe_symbol}",
                symbol=safe_symbol,
                context_provider={
                    "deterministic_score": score_result['final_score'],
                    "capital_commitment": capital_commitment,
                    "fill_probability": fill_prob
                }
            )
            # <Context-Aware Logging Integration - End>
        
        # Sort by score (highest first)
        scored_orders.sort(key=lambda x: x['deterministic_score'], reverse=True)
        
        # Allocate to top orders
        allocated_orders = []
        total_allocated_capital = 0
        allocated_count = 0
        
        for order in scored_orders:
            safe_symbol = getattr(order['order'], 'symbol', 'Unknown')
            
            if allocated_count >= available_slots:
                order['allocation_reason'] = 'Max open orders reached'
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Legacy: Order {safe_symbol} not allocated - max open orders",
                    symbol=safe_symbol,
                    context_provider={
                        "allocated_count": allocated_count,
                        "available_slots": available_slots
                    }
                )
                # <Context-Aware Logging Integration - End>
                continue
                
            if total_allocated_capital + order['capital_commitment'] > available_capital:
                order['allocation_reason'] = 'Insufficient capital'
                # <Context-Aware Logging Integration - Begin>
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Legacy: Order {safe_symbol} not allocated - insufficient capital",
                    symbol=safe_symbol,
                    context_provider={
                        "total_allocated_capital": total_allocated_capital,
                        "order_capital_commitment": order['capital_commitment'],
                        "available_capital": available_capital
                    }
                )
                # <Context-Aware Logging Integration - End>
                continue
                
            order['allocated'] = True
            order['allocation_reason'] = 'Allocated'
            total_allocated_capital += order['capital_commitment']
            allocated_count += 1
            allocated_orders.append(order)
            
            # <Context-Aware Logging Integration - Begin>
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Legacy: Order {safe_symbol} allocated",
                symbol=safe_symbol,
                context_provider={
                    "deterministic_score": order['deterministic_score'],
                    "capital_commitment": order['capital_commitment']
                }
            )
            # <Context-Aware Logging Integration - End>
        
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Legacy prioritization completed",
            context_provider={
                "allocated_orders_count": len(allocated_orders),
                "total_allocated_capital": total_allocated_capital
            }
        )
        # <Context-Aware Logging Integration - End>
        return scored_orders

    @staticmethod
    def timeout(seconds=10, error_message="Function call timed out"):
        """Timeout decorator implementation."""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                def timeout_handler(signum, frame):
                    raise TimeoutError(error_message)
                
                # Set up signal handler (Unix/Linux/Mac only)
                try:
                    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(seconds)
                    
                    try:
                        result = func(*args, **kwargs)
                    finally:
                        # Restore original signal handler
                        signal.alarm(0)
                        signal.signal(signal.SIGALRM, old_handler)
                    
                    return result
                except (AttributeError, ValueError):
                    # Windows compatibility - signal.SIGALRM not available
                    # Fall back to no timeout on Windows
                    return func(*args, **kwargs)
            return wrapper
        return decorator

    @timeout(seconds=30, error_message="Two-layer prioritization timed out after 30 seconds")
    def _prioritize_orders_with_timeout(self, executable_orders: List[Dict], total_capital: float, 
                                    current_working_orders: Optional[List] = None) -> List[Dict]:
        """Two-layer prioritization with timeout protection.
        
        UPDATED: All orders are considered viable - probability affects sequence only.
        """
        # <Context-Aware Logging Integration - Begin>
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Starting two-layer prioritization with timeout protection",
            context_provider={
                "executable_orders_count": len(executable_orders),
                "total_capital": total_capital,
                "current_working_orders_count": len(current_working_orders) if current_working_orders else 0
            }
        )
        # <Context-Aware Logging Integration - End>
            
        committed_capital = 0.0
        working_order_count = 0
        if current_working_orders:
            committed_capital = sum(order.get('capital_commitment', 0) 
                                for order in current_working_orders)
            working_order_count = len(current_working_orders)
        
        available_capital = total_capital * self.config['max_capital_utilization'] - committed_capital
        available_capital = max(0, available_capital)
        
        available_slots = self.config['max_open_orders'] - working_order_count
        available_slots = max(0, available_slots)
        
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Capital and slot availability calculated",
            context_provider={
                "available_capital": available_capital,
                "available_slots": available_slots,
                "committed_capital": committed_capital,
                "working_order_count": working_order_count,
                "max_capital_utilization": self.config['max_capital_utilization'],
                "max_open_orders": self.config['max_open_orders']
            }
        )

        # UPDATED: First pass - ALL orders are considered viable, calculate quality scores for all
        viable_orders = []
        
        for order_data in executable_orders:
            order = order_data['order']
            safe_symbol = getattr(order, 'symbol', 'Unknown')
            
            # UPDATED: All orders are viable - probability affects sequence only
            # Calculate quality score for ALL orders with error handling
            try:
                quality_result = self.scoring_service.calculate_quality_score(order, total_capital)
                
                # Safe quantity and capital commitment calculation
                quantity = 0
                capital_commitment = 0
                try:
                    quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
                    if hasattr(order, 'entry_price') and order.entry_price is not None:
                        capital_commitment = order.entry_price * quantity
                except Exception as e:
                    self.context_logger.log_event(
                        TradingEventType.SYSTEM_HEALTH,
                        f"Capital commitment calculation failed for {safe_symbol}",
                        symbol=safe_symbol,
                        context_provider={
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        decision_reason="Capital calculation error"
                    )
                    # Skip orders with capital calculation errors
                    continue
                
                viable_order = {
                    **order_data,
                    'quality_score': quality_result['quality_score'],
                    'quality_components': quality_result['components'],
                    'quantity': quantity,
                    'capital_commitment': capital_commitment,
                    'viable': True,  # UPDATED: All orders are viable
                    'allocation_reason': 'Viable - awaiting allocation',
                    'allocated': False
                }
                viable_orders.append(viable_order)
                
                self.context_logger.log_event(
                    TradingEventType.ORDER_VALIDATION,
                    f"Order {safe_symbol} processed for prioritization",
                    symbol=safe_symbol,
                    context_provider={
                        "quality_score": quality_result['quality_score'],
                        "capital_commitment": capital_commitment,
                        "quantity": quantity,
                        "fill_probability": order_data.get('fill_probability', 0)
                    },
                    decision_reason="Order processed for prioritization - probability affects sequence"
                )
                
            except Exception as e:
                self.context_logger.log_event(
                    TradingEventType.SYSTEM_HEALTH,
                    f"Quality score calculation failed for {safe_symbol}",
                    symbol=safe_symbol,
                    context_provider={
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    decision_reason="Quality score calculation error - order skipped"
                )
                # Skip orders with quality calculation errors
        
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Viability processing completed - all orders considered viable",
            context_provider={
                "viable_orders_count": len(viable_orders),
                "original_orders_count": len(executable_orders)
            }
        )

        # Sort viable orders by quality score (highest first) - probability affects sequence here
        viable_orders.sort(key=lambda x: x.get('quality_score', 0), reverse=True)
        
        # Second pass: Allocate capital to top orders based on quality score
        allocated_orders = []
        total_allocated_capital = 0
        allocated_count = 0
        
        for order in viable_orders:
            safe_symbol = getattr(order['order'], 'symbol', 'Unknown')
            
            if allocated_count >= available_slots:
                order['allocation_reason'] = 'Max open orders reached'
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Order {safe_symbol} not allocated - max open orders reached",
                    symbol=safe_symbol,
                    context_provider={
                        "allocated_count": allocated_count,
                        "available_slots": available_slots
                    },
                    decision_reason="Order slot limit reached"
                )
                continue
                
            capital_commitment = order.get('capital_commitment', 0)
            if total_allocated_capital + capital_commitment > available_capital:
                order['allocation_reason'] = 'Insufficient capital'
                self.context_logger.log_event(
                    TradingEventType.POSITION_MANAGEMENT,
                    f"Order {safe_symbol} not allocated - insufficient capital",
                    symbol=safe_symbol,
                    context_provider={
                        "total_allocated_capital": total_allocated_capital,
                        "order_capital_commitment": capital_commitment,
                        "available_capital": available_capital
                    },
                    decision_reason="Capital limit reached"
                )
                continue
                
            order['allocated'] = True
            order['allocation_reason'] = 'Allocated'
            total_allocated_capital += capital_commitment
            allocated_count += 1
            allocated_orders.append(order)
            
            self.context_logger.log_event(
                TradingEventType.POSITION_MANAGEMENT,
                f"Order {safe_symbol} allocated successfully",
                symbol=safe_symbol,
                context_provider={
                    "quality_score": order.get('quality_score', 0),
                    "capital_commitment": capital_commitment,
                    "total_allocated_capital": total_allocated_capital,
                    "allocated_count": allocated_count
                },
                decision_reason="Order allocated based on quality score"
            )
        
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Prioritization allocation completed",
            context_provider={
                "allocated_orders_count": len(allocated_orders),
                "total_allocated_capital": total_allocated_capital,
                "available_capital_remaining": available_capital - total_allocated_capital,
                "available_slots_remaining": available_slots - allocated_count
            },
            decision_reason="Prioritization process completed - probability used for sequence only"
        )

        # Return all processed orders (both allocated and not allocated)
        result = viable_orders
        
        self.context_logger.log_event(
            TradingEventType.POSITION_MANAGEMENT,
            "Prioritization process finished",
            context_provider={
                "total_orders_processed": len(result),
                "allocated_orders": len(allocated_orders),
                "viable_orders_not_allocated": len(viable_orders) - len(allocated_orders)
            }
        )
        return result