"""
Prioritization Service for Phase B - Implements deterministic scoring and capital allocation.
Combines fill probability, manual priority, capital efficiency, and other factors to rank orders.
"""

from typing import List, Dict, Optional, Tuple
import datetime
from src.core.planned_order import PlannedOrder
from src.services.position_sizing_service import PositionSizingService

# <Advanced Feature Integration - Begin>
# New imports for advanced features
from src.services.market_context_service import MarketContextService
from src.services.historical_performance_service import HistoricalPerformanceService
# <Advanced Feature Integration - End>


# Prioritization Service - Main class definition - Begin
class PrioritizationService:
    """
    Service responsible for ranking and allocating capital to executable orders.
    Implements Phase B deterministic scoring algorithm with configurable weights.
    """

    def __init__(self, sizing_service: PositionSizingService, config: Optional[Dict] = None,
                market_context_service: Optional[MarketContextService] = None,
                historical_performance_service: Optional[HistoricalPerformanceService] = None):
        self.sizing_service = sizing_service
        self.config = config or self._get_default_config()
        
        # Validate and normalize the configuration
        self._validate_config(self.config)
        
        self.market_context_service = market_context_service
        self.historical_performance_service = historical_performance_service
        
        # Log configuration type for debugging
        two_layer_enabled = self.config.get('two_layer_prioritization', {}).get('enabled', False)
        print(f"📊 Prioritization Service: Two-layer system {'ENABLED' if two_layer_enabled else 'DISABLED'}")

    def _get_default_config(self) -> Dict:
        """Get default configuration that matches the new prioritization_config.py structure."""
        return {
            'weights': {
                'fill_prob': 0.35,      # Reduced from 0.45 to match new config
                'manual_priority': 0.20,
                'efficiency': 0.15,
                'timeframe_match': 0.15, # Increased from 0.08
                'setup_bias': 0.10,      # Increased from 0.02
                'size_pref': 0.03,       # Reduced from 0.10
                'timeframe_match_legacy': 0.01,
                'setup_bias_legacy': 0.01
            },
            'max_open_orders': 5,
            'max_capital_utilization': 0.8,
            'enable_advanced_features': True,
            'timeframe_compatibility_map': {
                '1min': ['1min', '5min'],
                '5min': ['1min', '5min', '15min'],
                '15min': ['5min', '15min', '30min', '1H'],
                '30min': ['15min', '30min', '1H'],
                '1H': ['30min', '1H', '4H', '15min'],
                '4H': ['1H', '4H', '1D'],
                '1D': ['4H', '1D', '1W'],
                '1W': ['1D', '1W']
            },
            'setup_performance_thresholds': {
                'min_trades_for_bias': 10,
                'min_win_rate': 0.4,
                'min_profit_factor': 1.2,
                'recent_period_days': 90,
                'confidence_threshold': 0.7
            },
            'two_layer_prioritization': {
                'enabled': True,
                'min_fill_probability': 0.4,
                'quality_weights': {
                    'manual_priority': 0.30,
                    'efficiency': 0.25,
                    'risk_reward': 0.25,
                    'timeframe_match': 0.10,
                    'setup_bias': 0.10
                }
            }
        }

    def _validate_config(self, config: Dict) -> bool:
        """Validate that the configuration has the expected structure."""
        if not config:
            return False
        
        # Check if this is the new two-layer config format
        if 'two_layer_prioritization' in config:
            two_layer = config['two_layer_prioritization']
            if not isinstance(two_layer, dict):
                return False
            if 'enabled' not in two_layer:
                two_layer['enabled'] = True  # Default to enabled
            if 'min_fill_probability' not in two_layer:
                two_layer['min_fill_probability'] = 0.4  # Default value
            if 'quality_weights' not in two_layer:
                two_layer['quality_weights'] = {
                    'manual_priority': 0.30,
                    'efficiency': 0.25,
                    'risk_reward': 0.25,
                    'timeframe_match': 0.10,
                    'setup_bias': 0.10
                }
        
        return True

    def calculate_efficiency(self, order: PlannedOrder, total_capital: float) -> float:
        """
        Calculate capital efficiency (reward per committed dollar)
        Returns 0.0 for invalid orders, None inputs, or calculation errors
        """
        # NULL CHECK MUST BE FIRST - BEFORE ANY ATTRIBUTE ACCESS
        if order is None:
            return 0.0
            
        # Check if object has required attributes
        if not hasattr(order, 'entry_price') or not hasattr(order, 'stop_loss'):
            return 0.0
            
        # Check if required price data is available
        if order.entry_price is None or order.stop_loss is None:
            return 0.0
            
        try:
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity
            
            if capital_commitment <= 0:
                return 0.0
                
            # ==================== SAFE ATTRIBUTE ACCESS - BEGIN ====================
            # Get action value safely with multiple fallbacks
            action_value = None
            try:
                if hasattr(order, 'action'):
                    if hasattr(order.action, 'value'):
                        action_value = order.action.value
                    elif isinstance(order.action, str):
                        action_value = order.action
            except:
                action_value = None
                
            if action_value is None:
                return 0.0
            # ==================== SAFE ATTRIBUTE ACCESS - END ====================
                
            if action_value == 'BUY':
                profit_target = order.entry_price + (order.entry_price - order.stop_loss) * order.risk_reward_ratio
                expected_profit_per_share = profit_target - order.entry_price
            else:
                profit_target = order.entry_price - (order.stop_loss - order.entry_price) * order.risk_reward_ratio
                expected_profit_per_share = order.entry_price - profit_target
                
            expected_profit_total = expected_profit_per_share * quantity
            efficiency = expected_profit_total / capital_commitment
            
            return max(0.0, efficiency)
            
        except (ValueError, ZeroDivisionError, AttributeError, TypeError):
            return 0.0

    # Calculate timeframe compatibility with market conditions - Begin
    def calculate_timeframe_match_score(self, order: PlannedOrder) -> float:
        if not self.config.get('enable_advanced_features', False) or not self.market_context_service:
            return 0.5
            
        try:
            dominant_timeframe = self.market_context_service.get_dominant_timeframe(order.symbol)
            order_timeframe = order.core_timeframe
            
            if order_timeframe == dominant_timeframe:
                return 1.0
            
            compatible_timeframes = self.config.get('timeframe_compatibility_map', {}).get(
                dominant_timeframe, []
            )
            if order_timeframe in compatible_timeframes:
                return 0.7
                
            return 0.3
            
        except Exception as e:
            print(f"Error calculating timeframe match for {order.symbol}: {e}")
            return 0.5
    # Calculate timeframe compatibility with market conditions - End

    # Calculate bias based on historical setup performance - Begin
    def calculate_setup_bias_score(self, order: PlannedOrder) -> float:
        if not self.config.get('enable_advanced_features', False) or not self.historical_performance_service:
            return 0.5
            
        try:
            setup_name = order.trading_setup
            if not setup_name:
                return 0.5
                
            performance = self.historical_performance_service.get_setup_performance(setup_name)
            
            if not performance:
                return 0.5
                
            thresholds = self.config.get('setup_performance_thresholds', {})
            min_trades = thresholds.get('min_trades_for_bias', 10)
            min_win_rate = thresholds.get('min_win_rate', 0.4)
            min_profit_factor = thresholds.get('min_profit_factor', 1.2)
            
            if (performance.get('total_trades', 0) < min_trades or
                performance.get('win_rate', 0) < min_win_rate or
                performance.get('profit_factor', 0) < min_profit_factor):
                return 0.3
                
            win_rate = performance.get('win_rate', 0.5)
            profit_factor = min(performance.get('profit_factor', 1.0), 5.0)
            
            score = (win_rate * 0.6) + (profit_factor * 0.4) / 5.0
            return max(0.1, min(score, 1.0))
            
        except Exception as e:
            print(f"Error calculating setup bias for {order.trading_setup}: {e}")
            return 0.5
    # Calculate bias based on historical setup performance - End

    # <Two-Layer Prioritization - Begin>
    def calculate_risk_reward_score(self, order: PlannedOrder) -> float:
        """Calculate score based on risk/reward ratio quality."""
        rr_ratio = order.risk_reward_ratio
        
        # Base scoring: 1:1 → 0.5, 3:1 → 1.0, 5:1 → 1.2 (capped)
        rr_score = min(0.5 + (rr_ratio - 1) * 0.25, 1.2)
        
        # Adjust for probability of achieving reward (higher R/R often has lower probability)
        probability_adjustment = 1.0 - (rr_ratio - 1) * 0.1
        rr_score *= max(probability_adjustment, 0.6)
        
        return rr_score

    def calculate_quality_score(self, order: PlannedOrder, total_capital: float) -> Dict:
        """Calculate quality score for viable orders only."""
        two_layer_config = self.config.get('two_layer_prioritization', {})
        quality_weights = two_layer_config.get('quality_weights', {})
        
        # Manual priority normalization
        priority_norm = (6 - order.priority) / 5.0
        
        # Capital efficiency
        efficiency = self.calculate_efficiency(order, total_capital)
        
        # Risk/reward score
        risk_reward_score = self.calculate_risk_reward_score(order)
        
        # Advanced features (if enabled)
        timeframe_match = self.calculate_timeframe_match_score(order)
        setup_bias = self.calculate_setup_bias_score(order)
        
        # Calculate quality score
        quality_score = (
            quality_weights.get('manual_priority', 0.3) * priority_norm +
            quality_weights.get('efficiency', 0.25) * efficiency +
            quality_weights.get('risk_reward', 0.25) * risk_reward_score +
            quality_weights.get('timeframe_match', 0.1) * timeframe_match +
            quality_weights.get('setup_bias', 0.1) * setup_bias
        )
        
        return {
            'quality_score': quality_score,
            'components': {
                'priority_norm': priority_norm,
                'efficiency': efficiency,
                'risk_reward_score': risk_reward_score,
                'timeframe_match': timeframe_match,
                'setup_bias': setup_bias
            },
            'weights': quality_weights
        }

    def is_order_viable(self, order_data: Dict) -> Tuple[bool, str]:
        """Check if order meets minimum viability criteria."""
        two_layer_config = self.config.get('two_layer_prioritization', {})
        min_fill_prob = two_layer_config.get('min_fill_probability', 0.4)
        
        fill_prob = order_data.get('fill_probability', 0)
        
        if fill_prob < min_fill_prob:
            return False, f"Fill probability below minimum ({fill_prob:.2f} < {min_fill_prob})"
        
        # Add additional viability checks here if needed
        # Example: minimum volume, maximum spread, etc.
        
        return True, "Viable"
    # <Two-Layer Prioritization - End>

    # Compute final score using Phase B formula - Begin
    def calculate_deterministic_score(self, order: PlannedOrder, fill_prob: float, 
                                   total_capital: float, current_scores: Optional[List[float]] = None) -> Dict:
        weights = self.config['weights']
        
        priority_norm = (6 - order.priority) / 5.0
        
        efficiency = self.calculate_efficiency(order, total_capital)
        efficiency_norm = efficiency
        if current_scores:
            max_eff = max([s.get('efficiency', 0) for s in current_scores] + [efficiency])
            min_eff = min([s.get('efficiency', 0) for s in current_scores] + [efficiency])
            if max_eff > min_eff:
                efficiency_norm = (efficiency - min_eff) / (max_eff - min_eff)
        
        try:
            quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
            capital_commitment = order.entry_price * quantity
            size_pref = 1.0 - min(capital_commitment / total_capital, 1.0)
        except (ValueError, ZeroDivisionError):
            size_pref = 0.5
            
        timeframe_match = self.calculate_timeframe_match_score(order)
        setup_bias = self.calculate_setup_bias_score(order)
        
        score = (
            weights['fill_prob'] * fill_prob +
            weights['manual_priority'] * priority_norm +
            weights['efficiency'] * efficiency_norm +
            weights['size_pref'] * size_pref +
            weights['timeframe_match'] * timeframe_match +
            weights['setup_bias'] * setup_bias
        )
        
        return {
            'final_score': score,
            'components': {
                'fill_prob': fill_prob,
                'priority_norm': priority_norm,
                'efficiency': efficiency,
                'efficiency_norm': efficiency_norm,
                'size_pref': size_pref,
                'timeframe_match': timeframe_match,
                'setup_bias': setup_bias
            },
            'weights': weights,
            'capital_commitment': order.entry_price * self.sizing_service.calculate_order_quantity(order, total_capital) 
                                  if order.entry_price else 0
        }
    # Compute final score using Phase B formula - End

    # Prioritize orders and allocate capital based on scoring - Begin
    def prioritize_orders(self, executable_orders: List[Dict], total_capital: float, 
                        current_working_orders: Optional[List] = None) -> List[Dict]:
        if not executable_orders:
            return []
            
        # Check if two-layer prioritization is enabled
        two_layer_config = self.config.get('two_layer_prioritization', {})
        two_layer_enabled = two_layer_config.get('enabled', False)
        
        if not two_layer_enabled:
            # Fall back to legacy single-layer prioritization
            return self._prioritize_orders_legacy(executable_orders, total_capital, current_working_orders)

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
        
        # <Two-Layer Prioritization - Begin>
        # First pass: Check viability and calculate quality scores
        viable_orders = []
        non_viable_orders = []
        
        for order_data in executable_orders:
            order = order_data['order']
            
            # Check viability
            is_viable, reason = self.is_order_viable(order_data)
            
            if is_viable:
                # Calculate quality score for viable orders
                quality_result = self.calculate_quality_score(order, total_capital)
                
                quantity = self.sizing_service.calculate_order_quantity(order, total_capital)
                capital_commitment = order.entry_price * quantity if order.entry_price else 0
                
                viable_order = {
                    **order_data,
                    'quality_score': quality_result['quality_score'],
                    'quality_components': quality_result['components'],
                    'quantity': quantity,
                    'capital_commitment': capital_commitment,
                    'viable': True,
                    'allocation_reason': 'Viable - awaiting allocation',
                    'allocated': False
                }
                viable_orders.append(viable_order)
            else:
                non_viable_order = {
                    **order_data,
                    'viable': False,
                    'allocation_reason': reason,
                    'allocated': False
                }
                non_viable_orders.append(non_viable_order)
        
        # Sort viable orders by quality score (highest first)
        viable_orders.sort(key=lambda x: x['quality_score'], reverse=True)
        
        # Second pass: Allocate capital to top viable orders
        allocated_orders = []
        total_allocated_capital = 0
        allocated_count = 0
        
        for order in viable_orders:
            if allocated_count >= available_slots:
                order['allocation_reason'] = 'Max open orders reached'
                continue
                
            if total_allocated_capital + order['capital_commitment'] > available_capital:
                order['allocation_reason'] = 'Insufficient capital'
                continue
                
            order['allocated'] = True
            order['allocation_reason'] = 'Allocated'
            total_allocated_capital += order['capital_commitment']
            allocated_count += 1
            allocated_orders.append(order)
        
        # Combine all orders for return (viable allocated, viable not allocated, non-viable)
        return viable_orders + non_viable_orders
        # <Two-Layer Prioritization - End>

    def _prioritize_orders_legacy(self, executable_orders: List[Dict], total_capital: float,
                                current_working_orders: Optional[List] = None) -> List[Dict]:
        """Legacy single-layer prioritization for backward compatibility."""
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
            fill_prob = order_data.get('fill_probability', 0)
            
            score_result = self.calculate_deterministic_score(order, fill_prob, total_capital)
            
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
        
        # Sort by score (highest first)
        scored_orders.sort(key=lambda x: x['deterministic_score'], reverse=True)
        
        # Allocate to top orders
        allocated_orders = []
        total_allocated_capital = 0
        allocated_count = 0
        
        for order in scored_orders:
            if allocated_count >= available_slots:
                order['allocation_reason'] = 'Max open orders reached'
                continue
                
            if total_allocated_capital + order['capital_commitment'] > available_capital:
                order['allocation_reason'] = 'Insufficient capital'
                continue
                
            order['allocated'] = True
            order['allocation_reason'] = 'Allocated'
            total_allocated_capital += order['capital_commitment']
            allocated_count += 1
            allocated_orders.append(order)
        
        return scored_orders

    # Generate summary of prioritization results - Begin
    def get_prioritization_summary(self, prioritized_orders: List[Dict]) -> Dict:
        allocated = [o for o in prioritized_orders if o.get('allocated', False)]
        not_allocated = [o for o in prioritized_orders if not o.get('allocated', False)]
        
        # Handle both two-layer and legacy modes
        if prioritized_orders and 'viable' in prioritized_orders[0]:
            viable = [o for o in prioritized_orders if o.get('viable', False)]
            non_viable = [o for o in prioritized_orders if not o.get('viable', False)]
            avg_score_key = 'quality_score'
        else:
            # Legacy mode - all orders are considered viable
            viable = prioritized_orders
            non_viable = []
            avg_score_key = 'deterministic_score'
        
        total_commitment = sum(o.get('capital_commitment', 0) for o in allocated)
        
        # Calculate average score
        viable_scores = [o.get(avg_score_key, 0) for o in viable]
        avg_score = sum(viable_scores) / len(viable_scores) if viable_scores else 0
        
        return {
            'total_allocated': len(allocated),
            'total_rejected': len(not_allocated),
            'total_viable': len(viable),
            'total_non_viable': len(non_viable),
            'total_capital_commitment': total_commitment,
            'average_score': avg_score,
            'allocation_reasons': {
                reason: sum(1 for o in not_allocated if o.get('allocation_reason') == reason)
                for reason in set(o.get('allocation_reason') for o in not_allocated)
            }
        }
    # Generate summary of prioritization results - End
# Prioritization Service - Main class definition - End